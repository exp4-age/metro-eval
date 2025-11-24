from __future__ import annotations

import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray


def pos2wl_converter(
    lines_per_mm: int = 1200,
    focal_length: float = 1000.0,
    angle_of_incidence: float = 7.5,
) -> callable[[NDArray[np.float64], float, float], NDArray[np.float64]]:
    # angle of incidence 0th order in rad
    phi = np.deg2rad(angle_of_incidence)

    # grating constant in nm
    d = 1e6 / lines_per_mm

    def pos2wl(
        x: NDArray[np.float64], theta: float, scale: float
    ) -> NDArray[np.float64]:
        beta = np.arctan((x - 0.5) * scale / focal_length) + phi + theta
        return d * (np.sin(phi - theta) + np.sin(beta))

    return pos2wl


def wl2pos_converter(
    lines_per_mm: int = 1200,
    focal_length: float = 1000.0,
    angle_of_incidence: float = 7.5,
) -> callable[[NDArray[np.float64], float, float], NDArray[np.float64]]:
    # angle of incidence 0th order in rad
    phi = np.deg2rad(angle_of_incidence)

    # grating constant in nm
    d = 1e6 / lines_per_mm

    def wl2pos(
        wl: NDArray[np.float64], theta: float, scale: float
    ) -> NDArray[np.float64]:
        beta = np.arcsin(wl / d - np.sin(phi - theta))
        x = focal_length * np.tan(beta - (phi + theta))
        return x / scale + 0.5

    return wl2pos
