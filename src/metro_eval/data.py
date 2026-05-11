"""Load data from hdf5 files created by metro2hdf."""

from __future__ import annotations

from dataclasses import dataclass, field, InitVar
from functools import Placeholder, partial
from pathlib import Path
import h5py
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray

__all__ = [
    "MetroRun",
]


@dataclass(frozen=True)
class MetroRun:
    num: str
    data_dir: InitVar[Path | str] = field(default=Path.cwd())
    path: Path = field(init=False)
    channels: frozenset[str] = field(init=False)
    scans: list[str] = field(init=False)
    steps: list[str] = field(init=False)

    def __post_init__(self, data_dir):
        # get matching file path
        match = list(Path(data_dir).glob(f"{self.num}_*.h5"))

        if len(match) == 0:
            errmsg = f"Could not find measurement {self.num}"
            raise FileNotFoundError(errmsg)

        if len(match) > 1:
            errmsg = f"Found multiple measurements with {self.num}"
            raise FileNotFoundError(errmsg)

        object.__setattr__(self, "path", match[0].resolve())

        channels, scans, steps = self._scan()
        object.__setattr__(self, "channels", channels)
        object.__setattr__(self, "scans", scans)
        object.__setattr__(self, "steps", steps)

    def _scan(self):
        channels, steps = [], set()
        n_scans, n_steps = 0, 0

        with h5py.File(self.path, "r") as h5f:
            for name, obj in h5f.items():
                if isinstance(obj, h5py.Dataset):
                    # skip datasets as there should be non here
                    continue

                channels.append(name)

                for scan_idx in range(len(obj)):
                    scan_key = str(scan_idx)

                    if scan_key not in obj:
                        break

                    if scan_idx + 1 > n_scans:
                        n_scans = scan_idx + 1

                    scan = obj[scan_key]

                    if isinstance(scan, h5py.Dataset):
                        # 'step' data is stored in a single dataset
                        # without step labels
                        continue

                    if len(scan) <= n_steps:
                        continue

                    n_steps = 0

                    for step_key, step in scan.items():
                        if not isinstance(step, h5py.Dataset):
                            continue

                        n_steps += 1
                        steps.add(step_key)

        if n_scans == 0:
            errmsg = f"No scans found in {self.num}"
            raise ValueError(errmsg)

        scans = [str(i) for i in range(n_scans)]

        return frozenset(channels), scans, sorted(steps)

    def _read_dset(
        self, h5f: h5py.File, channel: str, scan: str, step: str
    ) -> NDArray:
        if channel not in h5f:
            errmsg = f"Channel {channel} not found in {self.num}"
            raise ValueError(errmsg)

        scans = h5f[channel]

        if scan not in scans:
            errmsg = f"Scan {scan} not found in {self.num}/{channel}"
            raise ValueError(errmsg)

        steps = scans[scan]

        if isinstance(steps, h5py.Dataset):
            try:
                idx = self.steps.index(step)
            except ValueError:
                errmsg = f"Step {step} not found in {self.num}/{channel}"
                raise ValueError(errmsg) from None

            return steps[idx]

        if step not in steps:
            errmsg = f"Step {step} not found in {self.num}/{channel}"
            raise ValueError(errmsg)

        return np.array(steps[step], order="F").squeeze()

    def __call__(
        self, channel: str, scan: str = "0", step: str | None = None
    ) -> NDArray:
        if step is None:
            step = self.steps[0]

        with h5py.File(self.path, "r") as h5f:
            return self._read_dset(h5f, channel, scan, step)

    def read_steps(self, scan: str = "0"):
        with h5py.File(self.path, "r") as h5f:
            for step in self.steps:
                reader = partial(self._read_dset, h5f, Placeholder, scan, step)
                yield step, reader
