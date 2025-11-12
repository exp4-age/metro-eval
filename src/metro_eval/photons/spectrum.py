"""Processing and analysis of fluorescence spectra."""

from __future__ import annotations

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


def subtract_background(bkg: callable) -> callable:
    def hist_background(det_bin_edges: ArrayLike) -> callable:
        bkg_spec, bkg_err = bkg(det_bin_edges)

        def subtract(spectrum: callable) -> tuple[NDArray, NDArray]:
            spec, err = spectrum(det_bin_edges)

            # Subtract background
            spec -= bkg_spec

            # Propagate uncertainties
            err = np.sqrt(err**2 + bkg_err**2)

            return spec, err

        return subtract

    return hist_background
