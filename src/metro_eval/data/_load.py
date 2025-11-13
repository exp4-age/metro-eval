from __future__ import annotations

import warnings
from contextlib import contextmanager
from pathlib import Path
import h5py
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import NDArray


def load_all(
    glob_dir: str | Path = ".",
    data_dir: str | None = None,
    pattern: str = "[2-9][0-9][0-9][0-9]-[0-1][1-9]-*-*",
) -> dict[str, dict[str, callable]]:
    """Load all beamtimes in the specified directory.

    Parameters
    ----------
    glob_dir: str or Path, optional
        The specified path is glob'ed recursively for
        beamtime directories following the the naming
        convention 'yyyy-mm-facility-topic'.
    data_dir: str, optional
        Directory relative to the beamtime directories
        containing the hdf5 files (e.g. 'events' or 'data').

    Returns
    -------
    dict
        Dictionary with beamtime names as keys and
        beamtime dictionaries as values (see `load_beamtime`).

    """
    if not isinstance(glob_dir, Path):
        glob_dir = Path(glob_dir)

    beamtimes = {}

    if glob_dir.match(pattern):
        matches = [glob_dir]

    else:
        matches = list(glob_dir.rglob(pattern))

    for match in matches:
        if not match.is_dir():
            continue

        beamtime = match.resolve().name

        if data_dir is not None:
            match = match / data_dir

            if not match.is_dir():
                continue

        beamtimes[beamtime] = load_beamtime(match)

    return beamtimes


def load_beamtime(data_dir: str | Path = ".") -> dict[str, callable]:
    """Load all measurments of a single beamtime.

    Parameters
    ----------
    data_dir: str or Path, optional
        Path to the directory containing the hdf5 files.

    Returns
    -------
    dict
        Dictionary with measurement numbers as keys and
        a loader function as values (see `load_measurement`).

    """
    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)

    data_dir = data_dir.resolve()

    measurements = {}

    for num_digits in range(1, 5):
        pattern = "".join(["[0-9]"] * num_digits) + "_*.h5"

        # Get matching files
        matches = list(data_dir.glob(pattern))

        if len(matches) == 0:
            continue

        for match in matches:
            num = match.name[:num_digits]
            measurements[num] = load_measurement(num, match.parent)

    return measurements


def load_measurement(
    num: str, data_dir: str | Path = "."
) -> callable[[str, str, str | None], dict[str, NDArray] | NDArray]:
    """Load data from an hdf5 file created by metro2hdf.

    Parameters
    ----------
    num: str
        Metro measurement number (e.g. `"042"`).
    data_dir: str or Path, optional
        Path to the directory containing the hdf5 file.

    Returns
    -------
    callable
        Loader function.

    Examples
    --------
    >>> data = metroload("042", data_dir="2024-07-BESSY-H2/")
    >>> spec = data("dld_rd#raw", step_key="12.269")

    """
    # Cache once loaded data for faster return next time
    cache = {}

    if not isinstance(data_dir, Path):
        data_dir = Path(data_dir)

    # Get matching file path
    match = list(data_dir.glob(f"{num}_*.h5"))

    if len(match) == 0:
        errmsg = f"Could not find measurement {num}"
        raise FileNotFoundError(errmsg)

    if len(match) > 1:
        errmsg = f"Found multiple measurements with {num}"
        raise FileNotFoundError(errmsg)

    data_file = match[0].resolve()

    def loader(
        data_key: str, scan_key: str = "0", step_key: str | None = None
    ) -> dict[str, NDArray] | NDArray:
        """Loads specified data from the hdf5 file.

        When one or all steps for one data key are loaded all steps
        are cached for future calls.

        Parameters
        ----------
        data_key: str
            Name of the metro data stream (e.g. `"device#value"`).
        scan_key: str, optional
            Index of the scan to be loaded. Usually `"0"` in case of
            measurements with one or no scan.
        step_key: str, optional
            Index of the step to be loaded. If `None` all datasets
            in the scan are returned.

        Returns
        -------
        dict or np.ndarray
            Dictionary of names (step values) and corresponding
            datasets or a single dataset if `step_key` is specified.

        """
        if data_key in cache and scan_key in cache[data_key]:
            if len(cache[data_key][scan_key]) == 1:
                return next(iter(cache[data_key][scan_key].values()))

            elif step_key is None:
                return cache[data_key][scan_key].copy()

            elif step_key in cache[data_key][scan_key]:
                return cache[data_key][scan_key][step_key]

            else:
                errmsg = f"Could not find {step_key} in {num}"
                raise KeyError(errmsg)

        with h5py.File(data_file, "r") as h5f:
            if contains_sorted_events(h5f):
                data = load_sorted_events(h5f, data_key, scan_key=scan_key)

            else:
                data = load_data_stream(h5f, data_key, scan_key=scan_key)

        # Update the cache
        cache.update({data_key: {scan_key: data}})

        return loader(data_key, scan_key=scan_key, step_key=step_key)

    return loader


@contextmanager
def open_metro_h5(num: str, data_dir: str | Path = ".") -> h5py.File:
    """Open an hdf5 file produced by metro2hdf.

    Convenience wrapper around h5py.File for easier access using
    the measurement number to glob the hdf5 file in the specified
    directory.

    Parameters
    ----------
    num: str
        Metro measurement number (e.g. `"042"`).
    data_dir: str, optional
        Path to the directory containing the hdf5 file.

    Yields
    ------
    h5py.File
        Open hdf5 file.

    """
    # Create a Path instance
    if isinstance(data_dir, str):
        data_dir = Path(data_dir)

    # glob pattern
    pattern = f"{num}*.h5"

    # Get matching file path
    match = list(data_dir.glob(pattern))

    if len(match) == 0:
        errmsg = f"Could not find measurement {num}"
        raise FileNotFoundError(errmsg)

    with h5py.File(match[0].resolve(), "r") as h5f:
        yield h5f


def contains_sorted_events(h5f: h5py.File) -> bool:
    return "0" in h5f


def load_data_stream(
    h5f: h5py.File,
    data_key: str,
    scan_key: str = "0",
) -> dict[str, NDArray]:
    """Load 'continuous' metro data streams from a scan
    in an open hdf5 file.

    Parameters
    ----------
    h5f: h5py.File
        Open hdf5 file (see `open_metro_h5`).
    data_key: str
        Name of the metro data stream (e.g. `"device#value"`).
    scan_key: str, optional
        Index of the scan to be loaded. Usually `"0"` in case of
        measurements with one or no scan.

    Returns
    -------
    dict
        Dictionary of names (step values) and corresponding
        datasets.

    """
    # Check if the data stream is found
    if data_key not in h5f:
        errmsg = f"Could not find channel {data_key} in {h5f.filename}"
        raise KeyError(errmsg)

    # Check if the data is a continuous data stream (metro)
    if "Frequency" not in h5f[data_key].attrs:
        errmsg = f"Channel {data_key} is missing 'Frequency' attribute"
        raise ValueError(errmsg)

    if h5f[data_key].attrs["Frequency"] != "continuous":
        errmsg = f"Channel {data_key} data is not 'continuous'"
        raise ValueError(errmsg)

    # Check if the scan index is present
    if scan_key not in h5f[data_key]:
        errmsg = f"Could not find scan {data_key}/{scan_key} in {h5f.filename}"
        raise KeyError(errmsg)

    data = {}

    for step_key, dset in h5f[data_key][scan_key].items():
        if dset.size == 0:
            wrnmsg = f"Empty step {data_key}/{scan_key}/{step_key} in {h5f.filename}"
            warnings.warn(wrnmsg, stacklevel=1)
            continue

        data[step_key] = np.squeeze(dset)

    return data


def load_sorted_events(
    h5f: h5py.File,
    data_key: str,
    scan_key: str = "0",
) -> dict[str, NDArray]:
    """Load coincidence data from sorted events.

    If a step index `step_key` is specified, the data is
    returned as a single `np.ndarray`.

    Parameters
    ----------
    h5f: h5py.File
        Open hdf5 file (see `open_metro_h5`).
    data_key: str
        Type of coincedences (e.g. `"EEP"`).
    scan_key: str, optional
        Index of the scan to be loaded. Usually `"0"` in case of
        measurements with one or no scan.

    Returns
    -------
    dict
        Dictionary of names (step values) and corresponding
        datasets.

    """
    # Check if the scan index is present
    if scan_key not in h5f:
        errmsg = f"Could not find scan {scan_key} in {h5f.filename}"
        raise KeyError(errmsg)

    data = {}

    for step_key, step_group in h5f[scan_key].items():
        if data_key not in step_group:
            wrnmsg = f"Could not find {scan_key}/{step_key}/{data_key} in {h5f.filename}"
            warnings.warn(wrnmsg, stacklevel=1)
            continue

        data[step_key] = np.squeeze(step_group[data_key])

    return data
