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
    data_dir: InitVar[Path | str] = field(default_factory=Path.cwd)
    path: Path = field(init=False)
    channels: frozenset[str] = field(init=False)
    scans: frozenset[str] = field(init=False)
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

        self.path = match[0].resolve()
        self.channels, self.scans, self.steps = self._scan()

    def _scan(self):
        channels, scans, steps = [], set(), set()
        n_steps = 0

        with h5py.File(self.path, "r") as h5f:
            for name, obj in h5f.items():
                if isinstance(obj, h5py.Dataset):
                    # skip datasets as there should be non here
                    continue

                channels.append(name)

                for scan_key, scan in obj.items():
                    if isinstance(scan, h5py.Dataset):
                        # skip datasets as there should be non here
                        continue

                    scans.add(scan_key)

                    if len(scan) <= n_steps:
                        continue

                    n_steps = 0

                    for step_key, step in scan.items():
                        if not isinstance(step, h5py.Dataset):
                            continue

                        n_steps += 1
                        steps.add(step_key)

        if "0" not in scans:
            errmsg = f"Scan '0' not found in {self.num}"
            raise ValueError(errmsg)

        return frozenset(channels), frozenset(scans), sorted(steps)

    def _read_dset(
        self, h5f: h5py.File, channel: str, scan: str, step: str
    ) -> NDArray:
        if channel not in h5f:
            errmsg = f"Channel {channel} not found in {self.num}"
            raise ValueError(errmsg)

        if scan not in h5f[channel]:
            errmsg = f"Scan {scan} not found in {self.num}/{channel}"
            raise ValueError(errmsg)

        if step not in h5f[channel][scan]:
            errmsg = f"Step {step} not found in {self.num}/{channel}"
            raise ValueError(errmsg)

        return np.array(h5f[channel][scan][step], order="F").squeeze()

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
