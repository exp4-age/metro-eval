from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass


_VALID_KEY_PATTERN = re.compile(
    r"^"
    r"(?:"
    r"(?:[1-9]?E{1,4})"
    r"|"
    r"(?:[1-9]?P{1,2})"
    r"|"
    r"(?:E{1,2}P)"
    r"|"
    r"(?:[1-9]E[1-9]P{1,2})"
    r")"
    r"$"
)


@dataclass(frozen=True)
class CoincidenceKey:
    raw: str
    electrons: int
    photons: int

    @property
    def n_electrons(self) -> int:
        return self.electrons

    @property
    def n_photons(self) -> int:
        return self.photons


def is_valid_key(value: str) -> bool:
    if not isinstance(value, str):
        return False
    return bool(_VALID_KEY_PATTERN.match(value.strip()))


def parse_coincidence_key(value: str) -> CoincidenceKey:
    if not isinstance(value, str):
        raise TypeError(f"Coincidence key must be a string, got {type(value).__name__}.")

    normalized = value.strip()
    if not normalized:
        raise ValueError("Coincidence key cannot be empty.")
    if not is_valid_key(normalized):
        raise ValueError(f"Expected a valid coincidence key, got {value!r}.")

    electrons = 0
    photons = 0

    for match in re.finditer(r"(\d*)([EP])", normalized):
        count = int(match.group(1) or 1)
        if match.group(2) == "E":
            electrons += count
        else:
            photons += count

    return CoincidenceKey(normalized, electrons, photons)


def validate_overlap_params(overlap_params: Mapping[str, object]) -> dict:
    """Normalize overlap configuration to the dictionary format expected by the processing code."""
    if not isinstance(overlap_params, Mapping):
        raise TypeError("Overlap parameters must be a mapping.")

    repetition_time = overlap_params.get("repetition_time")
    if repetition_time is None:
        repetition_time = overlap_params.get("reptime")
    if repetition_time is None:
        raise ValueError("Missing repetition time in overlap parameters.")

    roi_first = overlap_params.get("ROI_first")
    if roi_first is None:
        roi_first = [
            overlap_params.get("roi_first_min"),
            overlap_params.get("roi_first_max"),
        ]
    roi_last = overlap_params.get("ROI_last")
    if roi_last is None:
        roi_last = [
            overlap_params.get("roi_last_min"),
            overlap_params.get("roi_last_max"),
        ]

    try:
        repetition_time = float(repetition_time)
        roi_first = [float(roi_first[0]), float(roi_first[1])]
        roi_last = [float(roi_last[0]), float(roi_last[1])]
    except (TypeError, ValueError, IndexError) as exc:
        raise ValueError("Overlap parameters must contain numeric ROI bounds and a repetition time.") from exc

    return {
        "repetition_time": repetition_time,
        "ROI_first": roi_first,
        "ROI_last": roi_last,
    }
