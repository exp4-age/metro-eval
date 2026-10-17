"""Load data from hdf5 files created by metro2hdf."""

from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
import h5py
import numpy as np
from astropy.table import Row, Table, TableAttribute

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from numpy.typing import NDArray

__all__ = [
    "MetroData",
    "MetroRun",
    "RunTable",
]


class MetroRun(Row):
    """Representation of a row in a RunTable object.

    A MetroRun object is returned when a RunTable object is indexed
    with an integer or when iterating over a table.

    """

    @property
    def data(self):
        key = self["num"]
        return self._table._data.get(key)

    @data.setter
    def data(self, obj):
        key = self["num"]
        self._table._data[key] = obj

    @data.deleter
    def data(self):
        key = self["num"]
        self._table._data.pop(key, None)

    def get_meta(self, key, default=None, /):
        if key in self._table.columns and self[key] in self._table[key].meta:
            return self._table[key].meta[self[key]]
        else:
            return default


class RunTable(Table):
    """Metro run metadata with optionally attached data loading
    interface.

    Examples
    --------
    >>> from metro_eval.data import RunTable
    >>> rt = RunTable.read("path/to/run_table.ecsv", format="ecsv")
    >>> rt.attach_data("path/to/data_dir")

    """
    # a Row object is returned when a Table object is indexed with an
    # integer or when iterating over a table
    Row = MetroRun

    # default metadata attributes
    year = TableAttribute(default="YYYY")
    month = TableAttribute(default="MM")
    facility = TableAttribute(default="Facility")
    beamline = TableAttribute(default="Beamline")
    topic = TableAttribute(default="Topic")
    PI = TableAttribute(default="P. Investigator")
    personnel = TableAttribute(default=[])
    experiment = TableAttribute(default="SETUP")

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._data = {}

    def attach_data(self, data_dir: str | Path):
        if "num" not in self.colnames:
            errmsg = "Attaching data requires a 'num' column"
            raise KeyError(errmsg)

        path = Path(data_dir)
        if not path.is_dir():
            errmsg = f"{data_dir} is not a directory"
            raise NotADirectoryError(errmsg)

        for num, in self.iterrows("num"):
            for match in path.rglob(f"{num}*.h5"):
                self._data[num] = MetroData(num=num, path=match)
                break  # attach the first match


@dataclass(frozen=True)
class MetroData:
    """Load metro data from an hdf5 file created by metro2hdf.

    Data is only read and loaded into memory when requested by
    calling `__call__` or `read_steps`.

    Attributes
    ----------
    num : str
        Run number
    path : Path
        Path to the hdf5 file containing the metro data

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
        raise KeyError(errmsg)

    # TODO: hdf5 loads the data in row-major order ("C"), but for
    # most types of data analysis a column-major order would
    # be benificial, so the conversion is done here. Some testing
    # with actual data is needed to decide if this should be kept
    # or made optional...
    return np.array(h5g[key], order="F").squeeze()
