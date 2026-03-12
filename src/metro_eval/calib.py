"""Common energy calibration functions for our spectrometers."""

from __future__ import annotations

import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray


def wavelength_converter(
    lines_per_mm: int = 1200,
    focal_length: float = 1000.0,
    angle_of_incidence: float = 7.5,
) -> tuple[callable, callable]:
    """Return functions to convert between detector positions and wavelengths.

    Parameters
    ----------
    lines_per_mm: int, optional
        Number of lines per mm of the grating.
    focal_length: float, optional
        Focal length of the spectrometer in mm.
    angle_of_incidence: float, optional
        Angle of incidence of the light on the grating in degrees.

    Returns
    -------
    tuple
        A tuple containing two functions::
            - x_to_λ(x, θ, scale): Convert detector positions to wavelengths.
            - λ_to_x(λ, θ, scale): Convert wavelengths to detector positions.

    """
    # angle of incidence 0th order in rad
    φ = np.deg2rad(angle_of_incidence)

    # grating constant in nm
    d = 1e6 / lines_per_mm

    def x_to_λ(
        x: NDArray[np.float64], θ: float, scale: float
    ) -> NDArray[np.float64]:
        β = np.arctan((x - 0.5) * scale / focal_length) + φ + θ
        return d * (np.sin(φ - θ) + np.sin(β))

    def λ_to_x(
        λ: NDArray[np.float64], θ: float, scale: float
    ) -> NDArray[np.float64]:
        β = np.arcsin(λ / d - np.sin(φ - θ))
        x = focal_length * np.tan(β - (φ + θ))
        return x / scale + 0.5

    return x_to_λ, λ_to_x
