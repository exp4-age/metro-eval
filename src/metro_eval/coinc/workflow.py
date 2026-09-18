from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from metro_eval.coinc.postprocessing import overlap
from metro_eval.coinc.validation import parse_coincidence_key


@dataclass
class CoincidenceWorkflow:
    """Small state holder for the active coincidence dataset and its transformations."""

    raw: np.ndarray | None = None
    postproc: np.ndarray | None = None
    calibrated: np.ndarray | None = None
    current: np.ndarray | None = None
    coincidence_key: str | None = None
    status: dict[str, object] = field(default_factory=lambda: {"Bunch overlap": "N/A", "Calibrated": "N/A"})

    def clear(self) -> None:
        self.raw = None
        self.postproc = None
        self.calibrated = None
        self.current = None
        self.coincidence_key = None
        self.status = {"Bunch overlap": "N/A", "Calibrated": "N/A"}

    def set_loaded_data(self, data: np.ndarray, key: str | None = None) -> None:
        if data is None:
            raise ValueError("Loaded array cannot be None.")
        self.raw = np.asarray(data)
        self.postproc = self.raw.copy()
        self.calibrated = self.raw.copy()
        self.current = self.raw.copy()
        self.coincidence_key = key

    def reset_to_raw(self) -> None:
        if self.raw is None:
            return
        self.postproc = self.raw.copy()
        self.calibrated = self.raw.copy()
        self.current = self.raw.copy()
        self.status["Bunch overlap"] = "N/A"
        self.status["Calibrated"] = "N/A"

    def apply_overlap(self, overlap_params: dict, key: str | None = None) -> np.ndarray:
        if self.raw is None:
            raise ValueError("Cannot apply overlap without a raw dataset.")

        coincidence_key = key or self.coincidence_key or "E"
        try:
            parsed = parse_coincidence_key(coincidence_key)
            p_amount = parsed.n_photons
        except ValueError:
            p_amount = 0

        self.postproc = overlap(
            self.raw,
            float(overlap_params["repetition_time"]),
            overlap_params["ROI_first"],
            overlap_params["ROI_last"],
            nPhotons=p_amount,
        )
        self.calibrated = self.postproc.copy()
        self.current = self.postproc.copy()
        self.status["Bunch overlap"] = True
        self.status["Calibrated"] = "N/A"
        return self.current

    def apply_mask(self, filters: list[tuple[int, tuple[float, float]]]) -> np.ndarray:
        if self.current is None:
            raise ValueError("Cannot apply mask without a current dataset.")

        current = self.current.copy()
        for col_idx, (min_value, max_value) in filters:
            if current.size == 0:
                break
            if current.ndim == 1:
                current = current[(current >= min_value) & (current <= max_value)]
            else:
                current = current[(current[:, col_idx] >= min_value) & (current[:, col_idx] <= max_value)]

        self.current = current
        return self.current
