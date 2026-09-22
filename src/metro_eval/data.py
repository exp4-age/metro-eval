"""Load data from hdf5 files created by metro2hdf."""

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
    "load_runs",
    "MetroRun",
]


def glob_runs(path: Path):
    for num_digits in range(1, 5):
        pattern1 = "".join(["[0-9]"] * num_digits) + ".h5"
        pattern2 = "".join(["[0-9]"] * num_digits) + "_*.h5"

        # get matching files
        matches = list(path.glob(pattern1)) + list(path.glob(pattern2))

        if len(matches) == 0:
            continue

        for match in matches:
            yield match.name[:num_digits], match


def load_runs(path: str | Path | None = None):
    path = Path.cwd() if path is None else Path(path)

    run_dict = {}
    for num, file_path in glob_runs(path):
        run_dict[num] = MetroRun(file_path)

    return run_dict


@dataclass(frozen=True)
class MetroRun:
    """Load metro data from an hdf5 file created by metro2hdf.

    Upon initialization, the file is scanned to determine the available
    channels, scans, and steps.
    Actual data is only read and loaded into memory when requested by
    calling `__call__` or `read_steps`.

    .. warning:: The scan does not guarantee that all combinations of
        channels, scans, and steps are present in the file (e.g. in
        case of missing data or empty coincidence categories).

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
        Data channels in the hdf5 file (e.g. "dld_rd#raw" or "2E1P")
    scans : list[str]
        Scans in the hdf5 file with names "0", "1", etc.
    steps : list[str]
        Steps within a scan with the value of the scan variable
        as the name (typically "0.0" for single step runs)

    """

    run: InitVar[str | Path | tuple[str, Path] | tuple[str, str]]
    path: Path = field(init=False)
    channels: frozenset[str] = field(init=False)
    scans: list[int] = field(init=False)
    steps: list[str] = field(init=False)

    def __post_init__(self, run):
        if isinstance(run, str):
            if run.endswith(".h5"):
                # convert path str to Path
                run = Path(run)
            else:
                # convert run number to (num, dir) tuple assuming
                # dir to be the current working directory
                run = (run, Path.cwd())

        # find the hdf5 file either directly or by glob'ing using
        # a comnbination of run number and directory and store
        # the path for future access
        if isinstance(run, Path):
            if run.suffix != ".h5" or not run.is_file():
                errmsg = f"File {run} is not an hdf5 file"
                raise ValueError(errmsg)

            object.__setattr__(self, "path", run.resolve())

        elif isinstance(run, tuple):
            # check tuple length and types
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

            # glob for run number matches if short name does not exist
            match = [Path(run[1]) / f"{run[0]}.h5"]
            if not match[0].is_file():
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

        # scan the hdf5 file assuming the metro2hdf file structure
        # and create lists of available channels, scans and steps
        channels, scans, steps = self._scan()
        object.__setattr__(self, "channels", channels)
        object.__setattr__(self, "scans", scans)
        object.__setattr__(self, "steps", steps)

    def _scan(self):
        channels, scans, steps = [], [], []

        with h5py.File(self.path, "r") as h5f:
            # metro scans are stored by index and should start
            # from 0, anything non matching objects will be ignored
            for scan_idx in range(len(h5f)):
                scan_key = str(scan_idx)

                if scan_key not in h5f:
                    continue

                scan = h5f[scan_key]

                if isinstance(scan, h5py.Dataset):
                    errmsg = f"Scan {scan_key} is a dataset in {self.path}"
                    raise ValueError(errmsg)

                scans.append(scan_idx)

            if len(scans) == 0:
                errmsg = f"No scans found in {self.path}"
                raise ValueError(errmsg)

            # step values are only taken from scan 0 as all scans
            # should contain the same steps (unless a scan was interrupted)
            for step_key, step in h5f["0"].items():
                if isinstance(step, h5py.Dataset):
                    # there should be no datasets here
                    continue

                # skip the "by_idx" group, which is used for access
                # to steps by index instead of step value
                if step_key == "by_idx":
                    continue

                steps.append(step_key)

            if len(steps) == 0:
                errmsg = f"No steps found in {self.path}"
                raise ValueError(errmsg)

            # channel names are only taken from scan 0 and the first
            # step, so e.g. coincidence categories might not show up
            # here, if they are only present in the following steps
            for channel_key, channel in h5f["0"][steps[0]].items():
                if not isinstance(channel, h5py.Dataset):
                    continue

                channels.append(channel_key)

            if len(channels) == 0:
                errmsg = f"No channels found in {self.path}"
                raise ValueError(errmsg)

        return frozenset(channels), scans, sorted(steps)

    def _read_by_idx(
        self, h5f: h5py.File, channel: str, scan_idx: int, step_idx: int
    ) -> NDArray:
        scan_key = str(scan_idx)

        if scan_key not in h5f:
            errmsg = f"Scan {scan_idx} not found in {self.path}"
            raise ValueError(errmsg)

        steps = h5f[scan_key]

        # metro2hdf stores datasets by step index in a
        # "scan_idx/by_idx/step_idx" group and creates a link
        # "scan_idx/step_val" if the step value was succesfully
        # parsed for any of the channels
        step_key = f"by_idx/{step_idx}"

        if step_key not in steps:
            errmsg = f"Step {step_key} not found in {self.path}/{scan_idx}"
            raise ValueError(errmsg)

        channels = steps[step_key]

        if channel not in channels:
            errmsg = f"Channel {channel} not found in {self.path}"
            raise ValueError(errmsg)

        # TODO: hdf5 loads the data in row-major order ("C"), but for
        # most types of data analysis a column-major order would
        # be benificial, so the conversion is done here. Some testing
        # with actual data is needed to decide if this should be kept
        # or made optional...
        return np.array(channels[channel], order="F").squeeze()

    def _read_by_val(
        self, h5f: h5py.File, channel: str, scan_idx: int, step_val: str
    ) -> NDArray:
        scan_key = str(scan_idx)

        if scan_key not in h5f:
            errmsg = f"Scan {scan_idx} not found in {self.path}"
            raise ValueError(errmsg)

        steps = h5f[scan_key]

        if step_val not in steps:
            errmsg = f"Step {step_val} not found in {self.path}/{scan_idx}"
            raise ValueError(errmsg)

        channels = steps[step_val]

        if channel not in channels:
            errmsg = f"Channel {channel} not found in {self.path}"
            raise ValueError(errmsg)

        return np.array(channels[channel], order="F").squeeze()

    def __call__(
        self,
        channel: str,
        scan_idx: int = 0,
        step_idx: int = 0,
        step_val: str | None = None,
    ) -> NDArray:
        """Read data for the given data channel, scan, and step.

        Parameters
        ----------
        channel : str
            Data channel to read (e.g. "dld_rd#raw" or "2E1P")
        scan_idx : int, optional
            Scan to read (default: 0)
        step_idx : int, optional
            Step to read (default: 0)
        step_val : str or None, optional
            Access dataset by step value taking precedence over
            the default access by step index.

        Returns
        -------
        NDArray
            Data for the given channel, scan, and step.

        Raises
        ------
        ValueError
            If the specified channel, scan, or step is not found in the file.

        """
        with h5py.File(self.path, "r") as h5f:
            if step_val is None:
                return self._read_by_idx(h5f, channel, scan_idx, step_idx)
            else:
                return self._read_by_val(h5f, channel, scan_idx, step_val)

    def read_steps(self, scan_idx: int = 0) -> Iterator[tuple[str, callable]]:
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
            for step_val in self.steps:
                reader = partial(
                    self._read_by_val, h5f, Placeholder, scan_idx, step_val
                )
                yield step_val, reader
