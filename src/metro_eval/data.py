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
    "MetroData",
    "MetroRun",
    "MetroEvents",
]


@dataclass()
class MetroData:
    data_dir: InitVar[Path | str | None] = field(default=None)
    event_dir: InitVar[Path | str | None] = field(default=None)
    runs: dict[str, MetroRun] = field(init=False)
    events: dict[str, MetroEvents] = field(init=False)

    def __post_init__(self, data_dir, event_dir):
        if data_dir is None and event_dir is None:
            data_dir = Path.cwd()

        self.runs = {}
        self.events = {}

        if data_dir is not None:
            data_dir = Path(data_dir).resolve()

            for num, file_path in self._scan_dir(data_dir):
                self.runs[num] = MetroRun(num, data_dir=file_path)

        if event_dir is not None:
            event_dir = Path(event_dir).resolve()

            for num, file_path in self._scan_dir(event_dir):
                self.events[num] = MetroEvents(num, data_dir=file_path)

    def _scan_dir(self, data_dir: Path):
        for num_digits in range(1, 5):
            pattern = "".join(["[0-9]"] * num_digits) + "_*.h5"

            # get matching files
            matches = list(data_dir.glob(pattern))

            if len(matches) == 0:
                continue

            for match in matches:
                yield match.name[:num_digits], match


@dataclass(frozen=True)
class MetroRun:
    num: str
    data_dir: InitVar[Path | str] = field(default=Path.cwd())
    path: Path = field(init=False)
    channels: frozenset[str] = field(init=False)
    scans: list[str] = field(init=False)
    steps: list[str] = field(init=False)

    def __post_init__(self, data_dir):
        data_dir = Path(data_dir)

        if data_dir.is_file():
            if not data_dir.name.startswith(self.num + "_"):
                errmsg = f"File {data_dir} does not match run {self.num}"
                raise ValueError(errmsg)

            if not data_dir.name.endswith(".h5"):
                errmsg = f"File {data_dir} is not an hdf5 file"
                raise ValueError(errmsg)

            object.__setattr__(self, "path", data_dir.resolve())

        else:
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


@dataclass(frozen=True)
class MetroEvents(MetroRun):
    def _scan(self):
        channels, scans, steps = set(), [], set()
        n_steps, n_channels = 0, 0

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

                scan = obj[scan_key]

                if isinstance(scan, h5py.Dataset):
                    errmsg = f"Scan {scan_key} is a dataset in {self.num}"
                    raise ValueError(errmsg)

                scans.append(scan_key)

                if len(scan) <= n_steps:
                    continue

                n_steps = 0

                for step_key, step in scan.items():
                    if isinstance(step, h5py.Dataset):
                        # there should be no datasets here
                        continue

                    n_steps += 1
                    steps.add(step_key)

                    if len(step) <= n_channels:
                        continue

                    n_channels = 0

                    for channel_key, channel in step.items():
                        if not isinstance(channel, h5py.Dataset):
                            continue

                        n_channels += 1
                        channels.add(channel_key)

        if len(scans) == 0:
            errmsg = f"No scans found in {self.num}"
            raise ValueError(errmsg)

        if len(steps) == 0:
            errmsg = f"No steps found in {self.num}"
            raise ValueError(errmsg)

        if len(channels) == 0:
            errmsg = f"No channels found in {self.num}"
            raise ValueError(errmsg)

        return frozenset(channels), scans, sorted(steps)

    def _read_dset(
        self, h5f: h5py.File, channel: str, scan: str, step: str
    ) -> NDArray:
        if scan not in h5f:
            errmsg = f"Scan {scan} not found in {self.num}"
            raise ValueError(errmsg)

        steps = h5f[scan]

        if step not in steps:
            errmsg = f"Step {step} not found in {self.num}/{scan}"
            raise ValueError(errmsg)

        channels = steps[step]

        if channel not in channels:
            errmsg = f"Channel {channel} not found in {self.num}"
            raise ValueError(errmsg)

        return np.array(channels[channel], order="F").squeeze()
