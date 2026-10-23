"""Processing and analysis of fluorescence spectra."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from copy import copy
from contextlib import contextmanager
from functools import wraps
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import ArrayLike, NDArray
    from ..process.anodes import PositionAnode


@dataclass
class Spectrum:
    data: NDArray[np.float64] = field(repr=False)
    anode: PositionAnode
    bins: int | NDArray[np.float64] = 1024
    range: tuple[float, float] = field(default=(0.0, 1.0), init=False)
    time: int | None = None
    norm: list[tuple[float, float]] | None = None
    _x_min_idx: int | None = field(default=None, init=False, repr=False)
    _x_max_idx: int | None = field(default=None, init=False, repr=False)
    _y_min: int | None = field(default=None, init=False, repr=False)
    _y_max: int | None = field(default=None, init=False, repr=False)
    _cache: tuple | None = field(default=None, init=False, repr=False)

    def __post_init__(self):
        if not isinstance(self.bins, int):
            self.bins = np.asarray(self.bins, dtype=np.float64)

        # remove roi from anode so that it can be handled here
        # to avoid artifacts from the roi not matching the binning
        if self.anode.roi:
            (x_min, x_max), (self._y_min, self._y_max) = self.anode.roi
            self._set_x_roi(x_min, x_max)
            self.anode = replace(self.anode, roi=None)

    def _set_x_roi(self, x_min: float, x_max: float):
        if isinstance(self.bins, int):
            dx = (self.range[1] - self.range[0]) / self.bins
            self._x_min_idx = int(np.ceil((x_min - self.range[0]) / dx))
            self._x_max_idx = int(np.ceil((x_max - self.range[0]) / dx))
        else:
            self._x_min_idx = np.searchsorted(self.bins, x_min)
            self._x_max_idx = np.searchsorted(self.bins, x_max)

    def with_data(self, data: NDArray[np.float64], *norm) -> Spectrum:
        spec = copy(self)
        spec.data = data
        spec.norm = norm
        return spec

    def bin_edges(self):
        if isinstance(self.bins, int):
            return np.linspace(*self.range, self.bins + 1)
        else:
            return self.bins

    @contextmanager
    def rotate(self, angle: float):
        anode = self.anode
        cache = self._cache
        try:
            self.anode = replace(anode, rotation=anode.rotation + angle)
            self._cache = None
            yield None
        finally:
            self.anode = anode
            self._cache = cache

    @contextmanager
    def roi(
        self,
        x: tuple[float, float] | None = None,
        y: tuple[float, float] | None = None,
    ):
        x_min_idx, x_max_idx = self._x_min_idx, self._x_max_idx
        y_min, y_max = self._y_min, self._y_max
        cache = self._cache
        try:
            if x:
                self._set_x_roi(*x)
            if y:
                self._y_min, self._y_max = y
            self._cache = None
            yield None
        finally:
            self._x_min_idx, self._x_max_idx = x_min_idx, x_max_idx
            self._y_min, self._y_max = y_min, y_max
            self._cache = cache

    def __call__(self):
        if self._cache:
            return self._cache

        xy = self.anode.process(self.data)

        # apply y roi filter
        if self._y_min:
            xy = xy[xy[:, 1] > self._y_min]
        if self._y_max:
            xy = xy[xy[:, 1] < self._y_max]

        # histogram the data
        hist = np.histogram(xy[:, 0], bins=self.bins, range=self.range)[0]
        hist = np.asarray(hist, dtype=np.float64)

        # apply x roi filter
        if self._x_min_idx:
            hist[:self._x_min_idx] = 0
        if self._x_max_idx:
            hist[self._x_max_idx:] = 0

        err = None
        if self.norm:
            norm_value, rel_variance = 1.0, 0.0
            for val, err in self.norm:
                norm_value *= val
                rel_variance += (err / val) ** 2

            spec_rel_variance = np.zeros_like(hist)
            np.divide(1.0, hist, out=spec_rel_variance, where=hist > 0)
            hist = hist / norm_value
            err = hist * np.sqrt(spec_rel_variance + rel_variance)
        else:
            err = np.sqrt(hist)

        if self.time:
            hist = hist / self.time
            err = err / self.time

        self._cache = hist, err

        return hist, err


def sum_spectra(
    spectra: list[callable],
) -> callable:
    def summed_spectrum(det_bin_edges: ArrayLike) -> tuple[NDArray, NDArray]:
        total_spec = np.zeros(len(det_bin_edges) - 1, dtype=np.float64)
        total_err = np.zeros(len(det_bin_edges) - 1, dtype=np.float64)

        for spectrum in spectra:
            spec, err = spectrum(det_bin_edges)
            total_spec += spec
            total_err = np.sqrt(total_err**2 + err**2)  # TODO: ????

        return total_spec, total_err

    return summed_spectrum


def subtract_background(bkg: callable, spec: callable) -> callable:
    @wraps(spec)
    def background_subtracted_spectrum(
        det_bin_edges: ArrayLike,
    ) -> tuple[NDArray, NDArray]:
        bkg_hist, bkg_err = bkg(det_bin_edges)
        hist, err = spec(det_bin_edges)

        # Subtract background
        hist -= bkg_hist

        # Propagate uncertainties
        err = np.sqrt(err**2 + bkg_err**2)

        return hist, err


def correct_qeff(qeff: callable, spec: callable) -> callable:
    @wraps(spec)
    def corrected_spectrum(
        det_bin_edges: ArrayLike,
    ) -> tuple[NDArray, NDArray]:
        hist, err = spec(det_bin_edges)
        qeff_val, qeff_err = qeff(det_bin_edges)

        # Correct for quantum efficiency
        err = np.sqrt(
            (err / qeff_val) ** 2 + (hist * qeff_err / qeff_val**2) ** 2
        )
        hist /= qeff_val

        return hist, err

    return corrected_spectrum


def calibrate_spectrum(wl2pos: callable, spec: callable) -> callable:
    def calibrated_spectrum(bin_edges: ArrayLike):
        """Histogram the calibrated spectrum data.

        Parameters
        ----------
        bin_edges : ArrayLike
            Bin edges as wavelengths.

        Returns
        -------
        spec : NDArray
            Binned spectrum.
        err : NDArray
            Uncertainties for each bin.

        """
        det_bin_edges = wl2pos(bin_edges)
        return spec(det_bin_edges)

    return calibrated_spectrum
