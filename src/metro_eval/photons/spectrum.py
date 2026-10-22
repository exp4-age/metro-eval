"""Processing and analysis of fluorescence spectra."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
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
    bins: int | NDArray[np.float64] = field(default=1024)
    range: tuple[float, float] = field(default=(0.0, 1.0), init=False)
    roi: tuple[tuple[float, float], tuple[float, float]] | None = None
    _x_min_idx: int | None = field(default=None, init=False)
    _x_max_idx: int | None = field(default=None, init=False)

    def __post_init__(self):
        if not isinstance(self.bins, int):
            self.bins = np.asarray(self.bins, dtype=np.float64)

        # remove roi from anode so that it can be handled here
        # to avoid artifacts from the roi not matching the binning
        if self.anode.roi:
            if self.roi is None:
                self.roi = self.anode.roi
            self.anode = replace(self.anode, roi=None)

    def bin_edges(self):
        if isinstance(self.bins, int):
            return np.linspace(*self.range, self.bins + 1)
        else:
            return self.bins


def processor(
    bins: int | ArrayLike,
    anode: PositionAnode,
) -> callable[NDArray, tuple[NDArray, NDArray]]:
    """Spectrum processor binning pos#pos data.

    Parameters
    ----------
    bins : int or ArrayLike
        Detector bin edges with maximum range (0, 1).
    x_roi : tuple[float, float], optional
        Region of interest for the x-axis (detector position).
    y_roi : tuple[float, float], optional
        Region of interest for the y-axis (detector position).

    Returns
    -------
    callable
        Function processing and binning raw data.

    """
    det_bin_edges = None
    if isinstance(bins, int):
        det_bin_edges = np.arange(bins + 1) / bins
    else:
        det_bin_edges = np.asarray(bins)

    y_min, y_max = y_roi

    # prepare x roi for application after binning, which is faster
    # shifts the roi to match bin edges to avoid weird artifacts
    x_min, x_max = x_roi
    idx_min = np.searchsorted(det_bin_edges, x_min)
    idx_max = np.searchsorted(det_bin_edges, x_max)

    def histogram(
        data: ArrayLike,
        rotation: int | float | None = None,
        x_roi: tuple[float, float] | None = None,
        y_roi: tuple[float, float] | None = None,
    ) -> NDArray:
        """Histogram the spectrum data.

        Parameters
        ----------
        xy : ArrayLike
            Unbinned photon hit positions with shape (n,2).

        Returns
        -------
        spec : NDArray
            Binned spectrum.

        """
        if rotation:
            xy = replace(anode, rotation=rotation).process(data)
        else:
            xy = anode.process(data)

        # apply y roi filter
        xy = xy[(xy[:, 1] > y_min) & (xy[:, 1] < y_max)]

        # histogram the data
        hist = np.histogram(xy[:, 0], bins=det_bin_edges)[0]

        # apply x roi filter
        hist[:idx_min] = 0
        hist[idx_max - 1 :] = 0

        return hist

    return histogram


def normalize(
    spec: ArrayLike | tuple[ArrayLike, ArrayLike],
    *norm: float | tuple[float, float],
) -> tuple[NDArray, NDArray]:
    """Normalize spectrum by a given value with uncertainty.

    Parameters
    ----------
    spec : tuple[np.ndarray, np.ndarray]
        Binned spectrum with uncertainties.
    norm : tuple[float, float]
        Normalization value and its uncertainty.

    Returns
    -------
    spec
        Normalized binned spectrum.
    err
        Propagated uncertainties.

    """
    value, rel_variance = 1.0, 0.0
    for val in norm:
        if isinstance(val, tuple):
            value *= val[0]
            rel_variance += (val[1] / val[0]) ** 2
        elif isinstance(val, (float, int)):
            value *= val

    if isinstance(spec, tuple):
        err = np.asarray(spec[1], dtype=np.float64)
        spec = np.asarray(spec[0], dtype=np.float64)
        norm_spec = spec / value
        spec_rel_variance = np.zeros_like(spec)
        np.divide(err, spec, out=spec_rel_variance, where=spec > 0)
        norm_err = norm_spec * np.sqrt(spec_rel_variance**2 + rel_variance)
        return norm_spec, norm_err
    else:
        spec = np.asarray(spec, dtype=np.float64)
        norm_spec = spec / value
        spec_rel_variance = np.zeros_like(spec)
        np.divide(1.0, spec, out=spec_rel_variance, where=spec > 0)
        norm_err = norm_spec * np.sqrt(spec_rel_variance + rel_variance)
        return norm_spec, norm_err


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
