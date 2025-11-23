from __future__ import annotations

from dataclasses import dataclass
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray


@dataclass(frozen=True)
class SeyaNamioka:
    grating_pos: int
    lines_per_mm: int = 1200
    focal_length: float = 1000.0
    detector_diameter: float = 87.0
    angle_of_incidence: float = 7.5

    def __post_init__(self) -> None:
        # angle of incidence 0th order in rad
        phi = np.deg2rad(self.angle_of_incidence, dtype=np.float64)

        # grating constant in nm
        object.__setattr__(self, "d", 1e6 / self.lines_per_mm)

        # central wavelength (1st order) for a 600 l/mm grating in nm
        wl_c = self.grating_pos * 0.1

        # grating constant for 600 l/mm in nm
        d = 1e6 / 600

        # determine the corresponding grating rotation angle
        theta = np.arccos(wl_c / (2 * d * np.sin(phi)))

        # angle of incidence and reflection
        object.__setattr__(self, "alpha", phi - theta)
        object.__setattr__(self, "beta_center", phi + theta)

    def pos2wl(
        self,
        x: NDArray[np.float64],
        n: int = 1,
    ) -> NDArray[np.float64]:
        # scale x to physical dimensions and center around 0
        x = x * self.detector_diameter - self.detector_diameter * 0.5

        # angles of reflection
        beta = np.arctan(x / self.focal_length) + self.beta_center

        return self.d * (np.sin(self.alpha) + np.sin(beta)) / n

    def wl2pos(
        self,
        wl: NDArray[np.float64],
        n: int = 1,
    ) -> NDArray[np.float64]:
        # angles of reflection
        beta = np.arcsin(n * wl / self.d - np.sin(self.alpha))

        # position on detector
        x = self.focal_length * np.tan(beta - self.beta_center)

        # scale to [0, 1]
        return (x + self.detector_diameter * 0.5) / self.detector_diameter
