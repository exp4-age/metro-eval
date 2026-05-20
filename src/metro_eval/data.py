"""Load data from hdf5 files created by metro2hdf and sort_events."""

from __future__ import annotations

from dataclasses import dataclass, field, InitVar
from functools import Placeholder, partial
from pathlib import Path
import h5py
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from numpy.typing import NDArray

__all__ = [
    "MetroData",
    "MetroRun",
    "MetroEvents",
]


@dataclass()
class MetroData:
    """Find all metro runs and event data in the given directories
    and scan the files for available channels, scans, and steps.

    .. warning:: If more than one file with the same run number is
        present in the directory, all are scanned but only the last
        one is kept in the `runs` and `events` dictionaries.

    .. warning:: A given `data_dir` must only contain hdf5 files
        created by metro2hdf, and a given `event_dir` must only
        contain hdf5 files created by sort_events.

    Parameters
    ----------
    data_dir : Path | str | None, optional
        Directory to search for metro run data files.
    event_dir : Path | str | None, optional
        Directory to search for metro event data files.

    Attributes
    ----------
    runs : dict[str, MetroRun]
        Dictionary mapping run numbers to MetroRun objects for the
        found run data files.
    events : dict[str, MetroEvents]
        Dictionary mapping run numbers to MetroEvents objects for
        the found event data files.

    """

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
                self.runs[num] = MetroRun(file_path)

        if event_dir is not None:
            event_dir = Path(event_dir).resolve()

            for num, file_path in self._scan_dir(event_dir):
                self.events[num] = MetroEvents(file_path)

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
    """Load metro data from an hdf5 file created by metro2hdf.

    The hdf5 file should have the following structure:
    - channel_1
        - scan_1
            - step_1 (dataset)
            - step_2 (dataset)
            - ...
        - scan_2
            - step_1 (dataset)
            - step_2 (dataset)
            - ...
        - ...
    - ...

    Upon initialization, the file is scanned to determine the available
    channels, scans, and steps.
    Actual data is only read and loaded into memory when requested by
    calling `__call__` or `read_steps`.

    .. warning:: The scan does not guarantee that all combinations of
        channels, scans, and steps are present in the file (e.g. in
        case of missing data).

    .. note:: glob'ing for the hdf5 file with the run number will fail
        if more than one file with the same run number is present in
        the directory.

    Parameters
    ----------
    run : str | Path | tuple[str, Path] | tuple[str, str]
        The hdf5 file containing the metro data can be specified with:
        - a string or Path pointing directly to the hdf5 file
        - a tuple of (num, dir) where num is the measurement number and
          dir is the directory containing the hdf5 file. The file is
          expected to be named {num}_*.h5.
        - a string num interpreted as a measurement number which
          is searched for in the current working directory.

    Attributes
    ----------
    path : Path
        Path to the hdf5 file containing the metro data
    channels : frozenset[str]
        Data channels in the hdf5 file (e.g. "dld_rd#raw")
    scans : list[str]
        Scans in the hdf5 file with names "0", "1", etc.
    steps : list[str]
        Steps within a scan with the value of the scan variable
        as the name (typically "0.0" for single step runs)

    """

    run: InitVar[str | Path | tuple[str, Path] | tuple[str, str]]
    path: Path = field(init=False)
    channels: frozenset[str] = field(init=False)
    scans: list[str] = field(init=False)
    steps: list[str] = field(init=False)

    def __post_init__(self, run):
        if isinstance(run, str):
            if run.endswith(".h5"):
                run = Path(run)

            else:
                run = (run, Path.cwd())

        if isinstance(run, Path):
            if not run.name.endswith(".h5"):
                errmsg = f"File {run} is not an hdf5 file"
                raise ValueError(errmsg)

            object.__setattr__(self, "path", run.resolve())

        elif isinstance(run, tuple):
            if len(run) != 2:
                errmsg = f"Run tuple must have 2 elements, got {len(run)}"
                raise ValueError(errmsg)

            if not isinstance(run[0], str):
                errmsg = (
                    "First element of run tuple must be a string, "
                    f"got {type(run[0])}"
                )
                raise ValueError(errmsg)

            if not isinstance(run[1], (str, Path)):
                errmsg = (
                    "Second element of run tuple must be a string or "
                    f"Path, got {type(run[1])}"
                )
                raise ValueError(errmsg)

            # get matching file path
            match = list(Path(run[1]).glob(f"{run[0]}_*.h5"))

            if len(match) == 0:
                errmsg = f"Could not find measurement {run[0]} in {run[1]}"
                raise FileNotFoundError(errmsg)

            if len(match) > 1:
                errmsg = (
                    f"Found multiple measurements with {run[0]} in {run[1]}"
                )
                raise FileNotFoundError(errmsg)

            object.__setattr__(self, "path", match[0].resolve())

        else:
            errmsg = f"Invalid run specification: {run}"
            raise ValueError(errmsg)

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
            errmsg = f"No scans found in {self.path}"
            raise ValueError(errmsg)

        scans = [str(i) for i in range(n_scans)]

        return frozenset(channels), scans, sorted(steps)

    def _read_dset(
        self, h5f: h5py.File, channel: str, scan: str, step: str
    ) -> NDArray:
        if channel not in h5f:
            errmsg = f"Channel {channel} not found in {self.path}"
            raise ValueError(errmsg)

        scans = h5f[channel]

        if scan not in scans:
            errmsg = f"Scan {scan} not found in {self.path}/{channel}"
            raise ValueError(errmsg)

        steps = scans[scan]

        if isinstance(steps, h5py.Dataset):
            try:
                idx = self.steps.index(step)
            except ValueError:
                errmsg = f"Step {step} not found in {self.path}/{channel}"
                raise ValueError(errmsg) from None

            return steps[idx]

        if step not in steps:
            errmsg = f"Step {step} not found in {self.path}/{channel}"
            raise ValueError(errmsg)

        return np.array(steps[step], order="F").squeeze()

    def __call__(
        self, channel: str, scan: str = "0", step: str | None = None
    ) -> NDArray:
        """Read data for the given data channel, scan, and step.

        Parameters
        ----------
        channel : str
            Data channel to read (e.g. "dld_rd#raw")
        scan : str, optional
            Scan to read (default: "0")
        step : str, optional
            Step to read, defaults to the first step if not specified.

        Returns
        -------
        NDArray
            Data for the given channel, scan, and step.

        Raises
        ------
        ValueError
            If the specified channel, scan, or step is not found in the file.

        """
        if step is None:
            step = self.steps[0]

        with h5py.File(self.path, "r") as h5f:
            return self._read_dset(h5f, channel, scan, step)

    def read_steps(self, scan: str = "0") -> Iterator[tuple[str, callable]]:
        """Iterator over steps for the given scan.

        Keeps the hdf5 file open while iterating over the steps to avoid
        the overhead of opening and closing the file for each step.

        Parameters
        ----------
        scan : str, optional
            Scan over which to iterate

        Yields
        ------
        step : str
            Step name (e.g. "0.0")
        reader : callable
            Function that takes a channel name and reads and returns
            the data for the given channel.

        """
        with h5py.File(self.path, "r") as h5f:
            for step in self.steps:
                reader = partial(self._read_dset, h5f, Placeholder, scan, step)
                yield step, reader


@dataclass(frozen=True)
class MetroEvents(MetroRun):
    """Load metro data from an hdf5 file created by sort_events.

    The hdf5 file should have the following structure:
    - scan_1
        - step_1
            - E (dataset)
            - EE (dataset)
            - ...
        - step_2
            - E (dataset)
            - EE (dataset)
            - ...
        - ...
    - ...

    Upon initialization, the file is scanned to determine the available
    coincedence types, scans, and steps.
    Actual data is only read and loaded into memory when requested by
    calling `__call__` or `read_steps`.

    .. warning:: The scan does not guarantee that all combinations of
        coincidence types, scans, and steps are present in the file
        (e.g. in case of missing data).

    .. note:: glob'ing for the hdf5 file with the run number will fail
        if more than one file with the same run number is present in
        the directory.

    Parameters
    ----------
    run : str | Path | tuple[str, Path] | tuple[str, str]
        The hdf5 file containing the metro data can be specified with:
        - a string or Path pointing directly to the hdf5 file
        - a tuple of (num, dir) where num is the measurement number and
          dir is the directory containing the hdf5 file. The file is
          expected to be named {num}_*.h5.
        - a string num interpreted as a measurement number which
          is searched for in the current working directory.

    Attributes
    ----------
    path : Path
        Path to the hdf5 file containing the metro data
    channels : frozenset[str]
        Data channels (coincidence types) in the hdf5 file
    scans : list[str]
        Scans in the hdf5 file with names "0", "1", etc.
    steps : list[str]
        Steps within a scan with the value of the scan variable
        as the name (typically "0.0" for single step runs)

    """

    def _scan(self):
        channels, scans, steps = set(), [], set()
        n_steps, n_channels = 0, 0

        with h5py.File(self.path, "r") as h5f:
            for scan_idx in range(len(h5f)):
                scan_key = str(scan_idx)

                if scan_key not in h5f:
                    break

                scan = h5f[scan_key]

                if isinstance(scan, h5py.Dataset):
                    errmsg = f"Scan {scan_key} is a dataset in {self.path}"
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
            errmsg = f"No scans found in {self.path}"
            raise ValueError(errmsg)

        if len(steps) == 0:
            errmsg = f"No steps found in {self.path}"
            raise ValueError(errmsg)

        if len(channels) == 0:
            errmsg = f"No channels found in {self.path}"
            raise ValueError(errmsg)

        return frozenset(channels), scans, sorted(steps)

    def _read_dset(
        self, h5f: h5py.File, channel: str, scan: str, step: str
    ) -> NDArray:
        if scan not in h5f:
            errmsg = f"Scan {scan} not found in {self.path}"
            raise ValueError(errmsg)

        steps = h5f[scan]

        if step not in steps:
            errmsg = f"Step {step} not found in {self.path}/{scan}"
            raise ValueError(errmsg)

        channels = steps[step]

        if channel not in channels:
            errmsg = f"Channel {channel} not found in {self.path}"
            raise ValueError(errmsg)

        if channel == "other":
            return self._read_other(h5f, scan, step)

        return np.array(channels[channel], order="F").squeeze()

    def _read_other(self, h5f: h5py.File, scan: str, step: str) -> NDArray:
        errmsg = "Reading 'other' channel is not implemented"
        raise NotImplementedError(errmsg)
