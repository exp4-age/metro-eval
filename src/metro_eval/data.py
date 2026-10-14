"""Load data from hdf5 files created by metro2hdf."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
import json
import h5py
import numpy as np
import polars as pl

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from numpy.typing import NDArray

__all__ = [
    "RunTable",
    "MetroRun",
]


def default_schema() -> dict[str, pl.DataType]:
    return {
        "num": pl.Utf8,
        "start": pl.Float64,
        "end": pl.Float64,
        "each": pl.Float64,
        "time": pl.Int32,
        "comment": pl.Utf8,
    }


@dataclass
class RunTable:
    """Loads or creates a run table with attached
    metadata and searches for the corresponding run files.

    The run table can be imported from (exported to) a csv
    file. Additional metadata is imported from (exported to)
    a json file. Items in the run table may reference
    objects in the json metadata.

    Searches the given path for metro run files processed
    by metro2hdf using glob matching looking for ".h5" files
    starting with 1 to 5 numbers either followed directly by
    the file extension or "_*.h5".

    .. warning:: In case of multiple files with the same run number
        only the last found run is kept.

    """

    path: Path = field(default_factory=Path.cwd, repr=False)
    csv: Path = field(default="run_table.csv")
    json: Path = field(default="metadata.json")
    beamtime: str = field(default=None)
    schema: dict = field(default_factory=default_schema, repr=False)
    runs: dict[str, MetroRun] = field(init=False, repr=False)
    df: pl.DataFrame = field(init=False, repr=False)
    md: dict = field(init=False, repr=False)

    def __post_init__(self):
        self.path = Path(self.path)
        if not self.path.is_dir():
            errmsg = f"{self.path} is not a directory"
            raise ValueError(errmsg)

        # find the hdf5 files
        self.runs = {}
        for num, file_path in self.glob_runs(self.path):
            self.runs[num] = MetroRun(num=num, path=file_path)

        # check if run table and metadata already exists
        if (self.path / self.csv).is_file():
            self.csv = self.path / self.csv
            self.load_run_table()
        elif Path(self.csv).is_file():
            self.csv = Path(self.csv)
            self.load_run_table()
        else:
            self.csv = self.path / self.csv
            self.df = pl.DataFrame({"num": list(self.runs.keys())})
        if (self.path / self.json).is_file():
            self.json = self.path / self.json
            self.load_metadata()
        elif Path(self.json).is_file():
            self.json = Path(self.json)
            self.load_metadata()
        else:
            self.json = self.path / self.json
            self.md = {}

        # try to find the beamtime name if not provided
        if self.beamtime is None:
            if "beamtime" in self.md:
                self.beamtime = self.md["beamtime"]
            else:
                path = self.path.resolve()
                while path != path.parent:
                    if path.match("[2-9][0-9][0-9][0-9]-[0-1][1-9]*"):
                        self.beamtime = path.stem
                        break
                    path = path.parent

    @staticmethod
    def glob_runs(path: Path):
        for num_digits in range(1, 5):
            pattern1 = "".join(["[0-9]"] * num_digits) + ".h5"
            pattern2 = "".join(["[0-9]"] * num_digits) + "_*.h5"

            # get matching files
            matches = list(path.rglob(pattern1)) + list(path.rglob(pattern2))

            if len(matches) == 0:
                continue

            for match in matches:
                yield match.name[:num_digits], match

    def load_run_table(self, path: Path | None = None):
        if path is None:
            path = self.csv

        if not path.is_file() or path.suffix != ".csv":
            errmsg = f"Given path {path} is not a csv file"
            raise FileNotFoundError(errmsg)

        self.df = pl.read_csv(path, schema_overrides=self.schema)
        self.csv = path

    def save_run_table(self, path: Path | None = None):
        if path is None:
            path = self.csv

        self.df.write_csv(path)
        self.csv = path

    def load_metadata(self, path: Path | None = None):
        if path is None:
            path = self.json

        if not path.is_file() or path.suffix != ".json":
            errmsg = f"Given path {path} is not a json file"
            raise FileNotFoundError(errmsg)

        with open(path, "r") as f:
            self.md = json.load(f)

        self.json = path

    def save_metadata(self, path: Path | None = None):
        if path is None:
            path = self.json

        with open(path, "w") as f:
            json.dump(self.md, f, indent=2)

        self.json = path


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
    scans : list[str]
        Scans in the hdf5 file with names "0", "1", etc.
    steps : list[str]
        Steps within a scan with the value of the scan variable
        as the name (typically "0.0" for single step runs)

    """

    num: str = field(default=None)
    path: Path = field(default=".", repr=False)

    def __post_init__(self):
        if not isinstance(self.path, (Path, str)):
            errmsg = f"Expected Path or str, got {type(self.path)}"
            raise TypeError(errmsg)

        path = Path(self.path)
        if path.is_file() and path.suffix == ".h5":
            object.__setattr__(self, "path", path.resolve())
            num = path.stem.split("_")[0]
            if self.num is None:
                object.__setattr__(self, "num", num)
            elif self.num != num:
                errmsg = f"Filepath {path} does not match run number {num}"
                raise ValueError(errmsg)
        elif path.is_dir() and self.num is not None:
            # glob for run number matches if short name does not exist
            match = [path / f"{self.num}.h5"]
            if not match[0].is_file():
                match = list(path.glob(f"{self.num}_*.h5"))

            if len(match) == 0:
                errmsg = f"Could not find measurement {self.num} in {path}"
                raise FileNotFoundError(errmsg)

            if len(match) > 1:
                errmsg = (
                    f"Found multiple measurements with {self.num} in {path}"
                )
                raise FileNotFoundError(errmsg)

            object.__setattr__(self, "path", match[0].resolve())
        else:
            errmsg = f"Invalid run: num={self.num}, path={path}"
            raise ValueError(errmsg)

    def list_scans(self) -> list[int]:
        """Lists the scans found in the run by index (name).

        Does not guarantee that all found scans contain data.

        Returns
        -------
        list
            Scan indices read from the group names.

        """
        scans = []

        with h5py.File(self.path, "r") as h5f:
            # metro scans are stored by index and should start
            # from 0, any non matching objects will be ignored
            for idx in range(len(h5f)):
                key = str(idx)
                if key in h5f and isinstance(h5f[key], h5py.Group):
                    scans.append(idx)

        return scans

    def _steps(self, h5f: h5py.File, scan_idx: int) -> Iterator[str]:
        scan_key = str(scan_idx)
        if scan_key not in h5f:
            errmsg = f"Scan {scan_idx} not found in {self.path}"
            raise ValueError(errmsg)

        for step_key, step in h5f[scan_key].items():
            if isinstance(step, h5py.Dataset) or step_key == "by_idx":
                # skip the "by_idx" group, which is used for access
                # to steps by index instead of step value
                continue

            yield step_key

    def list_steps(self, scan_idx: int = 0) -> list[str]:
        """Lists all step values found in the given scan.

        Parameters
        ----------
        scan_idx : int, optional
            Specify a scan in which to search for step values.

        Returns
        -------
        list
            Step values read from the group names.

        """
        with h5py.File(self.path, "r") as h5f:
            return list(self._steps(h5f, scan_idx))

    def _channels(
        self,
        h5f: h5py.File,
        scan_idx: int,
        step: str | int
    ) -> Iterator[str]:
        scan_key = str(scan_idx)
        if scan_key not in h5f:
            errmsg = f"Scan {scan_idx} not found in {self.path}"
            raise ValueError(errmsg)

        step_key = step if isinstance(step, str) else f"by_idx/{step}"
        if step_key not in h5f[scan_key]:
            errmsg = f"Step {step_key} not found in {self.path}/{scan_idx}"
            raise ValueError(errmsg)

        for channel_key, channel in h5f[scan_key][step_key].items():
            if not isinstance(channel, h5py.Dataset):
                continue

            yield channel_key

    def list_channels(self, step: str | int, scan_idx: int = 0) -> list[str]:
        """Lists all channels found in the given scan and step.

        .. note:: The channel list may differ between steps in case
            of empty datasets (e.g. empty coincidence categories)

        Parameters
        ----------
        step: str or int
            Step index or value specifying in which group to
            search for datasets.
        scan_idx : int, optional
            Specify a scan from which to select the step.

        Returns
        -------
        list
            Step values read from the group names.
        """
        with h5py.File(self.path, "r") as h5f:
            return list(self._channels(h5f, scan_idx, step))

    def __call__(
        self,
        channel: str,
        scan_idx: int = 0,
        step: str | int = 0,
    ) -> NDArray:
        """Read data for the given data channel, scan, and step.

        Parameters
        ----------
        channel : str
            Data channel to read (e.g. "dld_rd#raw" or "2E1P")
        scan_idx : int, optional
            Scan index to read (default: 0)
        step : str or int, optional
            Step index or value to read (default: 0)

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
            scan_key = str(scan_idx)
            if scan_key not in h5f:
                errmsg = f"Scan {scan_idx} not found in {self.path}"
                raise ValueError(errmsg)

            # metro2hdf stores datasets by step index in a
            # "scan_idx/by_idx/step_idx" group and creates a link
            # "scan_idx/step_val" if the step value was succesfully
            # parsed for any of the channels
            step_key = step if isinstance(step, str) else f"by_idx/{step}"
            if step_key not in h5f[scan_key]:
                errmsg = f"Step {step_key} not found in {self.path}/{scan_idx}"
                raise ValueError(errmsg)

            return read_dset(h5f[scan_key][step_key], channel)

    def read_steps(self, scan_idx: int = 0) -> Iterator[tuple[str, callable]]:
        """Iterator over steps for the given scan.

        Keeps the hdf5 file open while iterating over the steps to avoid
        the overhead of opening and closing the file for each step.

        Parameters
        ----------
        scan_idx : int, optional
            Scan over which to iterate

        Yields
        ------
        step_val : str
            Step name (e.g. "0.0")
        reader : callable
            Function that takes a channel name and reads the data
            for the given channel.

        """
        with h5py.File(self.path, "r") as h5f:
            for step_val in self._steps(h5f, scan_idx):
                reader = partial(read_dset, h5f[str(scan_idx)][step_val])
                yield step_val, reader


def read_dset(h5g: h5py.Group, key: str):
    if key not in h5g:
        errmsg = f"Dataset {key} not found"
        raise ValueError(errmsg)

    # TODO: hdf5 loads the data in row-major order ("C"), but for
    # most types of data analysis a column-major order would
    # be benificial, so the conversion is done here. Some testing
    # with actual data is needed to decide if this should be kept
    # or made optional...
    return np.array(h5g[key], order="F").squeeze()
