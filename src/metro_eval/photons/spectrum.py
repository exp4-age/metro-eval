"""Processing and analysis of fluorescence spectra."""

from __future__ import annotations

from functools import wraps
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import ArrayLike, NDArray


def spectrum(
    xy: NDArray,
    time: int | float = 1,
    roi: ArrayLike = ((0, 1), (0, 1)),
) -> callable[[ArrayLike], tuple[NDArray, NDArray]]:
    x_min, x_max = roi[0]
    y_min, y_max = roi[1]

    # Apply y roi filter
    xy = xy[xy[:, 1] > y_min]
    xy = xy[xy[:, 1] < y_max]

    # Don't need y values anymore: project to x axis
    x = xy[:, 0].flatten()

    def hist_spectrum(det_bin_edges: ArrayLike) -> tuple[NDArray, NDArray]:
        """Histogram the spectrum data.

        Parameters
        ----------
        det_bin_edges : ArrayLike
            Detector bin edges.

        Returns
        -------
        spec : NDArray
            Binned spectrum.
        err : NDArray
            Uncertainties for each bin.

        """
        # Histogram the data
        spec = np.histogram(x, bins=det_bin_edges)[0]
        spec = np.asarray(spec, dtype=np.float64)

        # Apply x roi filter
        idx_min = np.searchsorted(det_bin_edges, x_min)
        idx_max = np.searchsorted(det_bin_edges, x_max)
        spec[:idx_min] = 0
        spec[idx_max - 1 :] = 0

        # Poisson uncertainties for each bin
        err = np.sqrt(spec)

        # Normalize to counts / s
        spec /= time
        err /= time

        return spec, err

    return hist_spectrum


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


def normalize_spectrum(norm: tuple[float, float], spec: callable) -> callable:
    norm_val, norm_err = norm

    @wraps(spec)
    def normalized_spectrum(bin_edges: ArrayLike) -> tuple[NDArray, NDArray]:
        hist, err = spec(bin_edges)

        # Normalize spectrum
        err = np.sqrt(
            (err / norm_val) ** 2 + (hist * norm_err / norm_val**2) ** 2
        )
        hist /= norm_val

        return hist, err

    return normalized_spectrum


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
