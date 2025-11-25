from __future__ import annotations

from functools import cache
import numpy as np
from pint import UnitRegistry

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Literal
    from numpy.typing import NDArray


ureg = UnitRegistry()
Q_ = ureg.Quantity


def target_density(
    values: NDArray[np.float64],
    device: Literal["MKS946", "MKS270B", "unknown"],
    readout: str,
    correction_factor: float | None = None,
) -> callable[[str], NDArray[np.float64]]:
    if device == "MKS946":
        data_unit = "torr"

        if readout == "analog":
            values = 10 ** ((values - 7.2) / 0.6)

        elif readout != "digital":
            errmsg = f"Unknown readout '{readout}' for device MKS946"
            raise ValueError(errmsg)

    elif device == "MKS270B":
        data_unit = "torr"

        if readout == "X1":
            values *= 0.1

        elif readout == "X.1":
            values *= 0.01

        elif readout == "X.01":
            values *= 0.001

        else:
            errmsg = f"Unknown readout '{readout}' for device MKS270B"
            raise ValueError(errmsg)

    elif device == "unknown":
        data_unit = readout

    else:
        errmsg = f"Unknown device '{device}'"
        raise ValueError(errmsg)

    if correction_factor is not None:
        values *= correction_factor

    mean = np.mean(values)
    std = np.std(values, ddof=1, mean=mean)

    @cache
    def norm(unit: str) -> tuple[float, float]:
        return Q_(mean, data_unit).m_as(unit), Q_(std, data_unit).m_as(unit)

    return norm


def beamline_flux(
    values: NDArray[np.float64],
    device: str,
) -> callable[[str], NDArray[np.float64]]:
    if device.startswith("Keithley"):
        data_unit = "A"

    else:
        data_unit = device

    mean = np.mean(values)
    std = np.std(values, ddof=1, mean=mean)

    @cache
    def norm(unit: str) -> tuple[float, float]:
        return Q_(mean, data_unit).m_as(unit), Q_(std, data_unit).m_as(unit)

    return norm
