from __future__ import annotations

from functools import partial
import numpy as np

from metro_eval.calib import pos2wl_converter, wl2pos_converter

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray


def pifs_pos2wl_converter(
    grating_pos: int,
    lines_per_mm: int = 1200,
    focal_length: float = 1000.0,
    angle_of_incidence: float = 7.5,
    detector_width: float = 75.0,
) -> callable[[NDArray[np.float64]], NDArray[np.float64]]:
    pos2wl = pos2wl_converter(
        lines_per_mm=lines_per_mm,
        focal_length=focal_length,
        angle_of_incidence=angle_of_incidence,
    )

    # angle of incidence 0th order in rad
    phi = np.deg2rad(angle_of_incidence)

    # central wavelength (1st order) for a 600 l/mm grating in nm
    wl_c = grating_pos * 0.1

    # grating constant for 600 l/mm in nm
    d = 1e6 / 600

    # determine the corresponding grating rotation angle
    theta = np.arccos(wl_c / (2 * d * np.sin(phi)))

    return partial(pos2wl, theta=theta, scale=detector_width)


def pifs_wl2pos_converter(
    grating_pos: int,
    lines_per_mm: int = 1200,
    focal_length: float = 1000.0,
    angle_of_incidence: float = 7.5,
    detector_width: float = 75.0,
) -> callable[[NDArray[np.float64]], NDArray[np.float64]]:
    wl2pos = wl2pos_converter(
        lines_per_mm=lines_per_mm,
        focal_length=focal_length,
        angle_of_incidence=angle_of_incidence,
    )

    # angle of incidence 0th order in rad
    phi = np.deg2rad(angle_of_incidence)

    # central wavelength (1st order) for a 600 l/mm grating in nm
    wl_c = grating_pos * 0.1

    # grating constant for 600 l/mm in nm
    d = 1e6 / 600

    # determine the corresponding grating rotation angle
    theta = np.arccos(wl_c / (2 * d * np.sin(phi)))

    return partial(wl2pos, theta=theta, scale=detector_width)
