import argparse
import warnings
import os
from pathlib import Path
from datetime import datetime
import h5py
import numpy as np
from rich.live import Live
from rich.table import Table

from metro_eval.metro2hdf.index_ascii import index_ascii_file

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TextIO


def generate_table(runs: dict) -> Table:
    table = Table()
    table.add_column("run", justify="right", style="cyan", width=5)
    table.add_column(
        "status", style="magenta", max_width=50, width=50, no_wrap=True
    )
    table.add_column(
        "progress", justify="center", style="green", min_width=9, width=9
    )

    for run_num, cols in runs.items():
        table.add_row(
            run_num,
            cols.get("status", ""),
            cols.get("progress", ""),
        )

    return table


"""
def txt_to_hdf5(
    asciif: TextIO, h5f: h5py.File, scans: dict, compression_opts: int
) -> None:
    for scan_key, scan in scans.items():
        grp = h5f.require_group(scan_key)

        for step_key in scan:
            # Read a single step from the txt file
            data = np.genfromtxt(
                asciif,
                skip_header=scan[step_key]["start"],
                max_rows=scan[step_key]["length"],
            )

            # Write the data to the hdf5 file
            grp.create_dataset(
                step_key,
                shape=data.shape,
                dtype=np.float64,
                data=data,
                compression="gzip",
                compression_opts=compression_opts,
            )
"""


def group_runs(
    file_list: list[Path], live: Live | None = None
) -> dict[str, list[str]]:
    runs = {}

    for file_path in file_list:
        if not file_path.is_file():
            continue

        file_name = file_path.stem
        parts = file_name.split("_")

        if len(parts) < 2:
            continue

        run_num = parts[0]

        try:
            int(run_num)

        except ValueError:
            continue

        if run_num not in runs:
            runs[run_num] = {"files": [], "name": "_".join(parts[1:])}

        else:
            name = "_".join(parts[1:])
            runs[run_num]["name"] = os.path.commonprefix(
                [runs[run_num]["name"], name]
            )

        runs[run_num]["files"].append(str(file_path.resolve()))

        if live is not None:
            n = len(runs[run_num]["files"])
            runs[run_num]["status"] = f"found file ...{parts[-1]}"
            runs[run_num]["progress"] = f"- / {n}"

            live.update(generate_table(runs))

    for run_num in runs:
        try:
            name_parts = runs[run_num]["name"].split("_")
            runs[run_num]["date"] = datetime.strptime(name_parts[-2], "%d%m%Y")
            runs[run_num]["time"] = datetime.strptime(name_parts[-1], "%H%M%S")

        except Exception:
            runs[run_num]["date"] = None
            runs[run_num]["time"] = None

        else:
            runs[run_num]["name"] = "_".join(name_parts[:-2])

    if live is not None:
        for run_num in runs:
            n = len(runs[run_num]["files"])
            name = runs[run_num]["name"]
            runs[run_num]["status"] = f"{name} with {n} files"

        live.update(generate_table(runs))

    return runs


def process_ascii_file(file_path: str, h5f: h5py.File, compr_lvl: int):
    index = index_ascii_file(file_path)

    if "Name" not in index["attrs"]:
        return "missing attribute 'Name'"

    channel = index["attrs"]["Name"]
    channel_grp = h5f.require_group(channel)

    if "Shape" not in index["attrs"]:
        return "missing attribute 'Shape'"

    shape = int(index["attrs"]["Shape"])

    if shape == 0:
        empty_step = np.full(1, np.nan)

    else:
        empty_step = np.full((1, shape), np.nan)

    for attr, val in index["attrs"].items():
        channel_grp.attrs[attr] = val

    for scan_idx, scan in index["scans"].items():
        if "steps" not in scan:
            skip_header = scan["start"]

            if "end" in scan:
                max_rows = scan["end"] - skip_header

            else:
                max_rows = None

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)

                data = np.genfromtxt(
                    file_path,
                    skip_header=skip_header,
                    max_rows=max_rows,
                )

            if data.size == 0:
                print(
                    f"found empty scan in {h5f.attrs['number']}:",
                    f"channel {channel}, scan: {scan_idx}",
                )
                data = empty_step

            if data.size == 1:
                compression = {}

            else:
                compression = {
                    "compression": "gzip",
                    "compression_opts": compr_lvl,
                }

            channel_grp.create_dataset(
                scan_idx,
                shape=data.shape,
                dtype=np.float64,
                data=data,
                **compression,
            )

            continue

        scan_grp = channel_grp.require_group(scan_idx)

        for step_idx in scan["steps"]:
            skip_header = scan["steps"][step_idx]["start"]

            if "end" in scan["steps"][step_idx]:
                max_rows = scan["steps"][step_idx]["end"] - skip_header

            else:
                max_rows = None

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)

                data = np.genfromtxt(
                    file_path,
                    skip_header=skip_header,
                    max_rows=max_rows,
                )

            step_val = scan["steps"][step_idx]["value"]

            if data.size == 0:
                num = h5f.attrs["number"]
                print(
                    f"found empty step in {num}: channel {channel},",
                    f"scan: {scan_idx}, step: {step_val}",
                )
                data = empty_step

            if data.size == 1:
                compression = {}

            else:
                compression = {
                    "compression": "gzip",
                    "compression_opts": compr_lvl,
                }

            scan_grp.create_dataset(
                step_val,
                shape=data.shape,
                dtype=np.float64,
                data=data,
                **compression,
            )


def process_run(
    run_num: str,
    runs: dict,
    h5f: h5py.File,
    compr_lvl: int,
    live: Live | None = None,
):
    skipped = []

    for i, file_path in enumerate(runs[run_num]["files"], 1):
        if live is not None:
            suffix = file_path.rsplit("_", 1)[-1]
            runs[run_num]["status"] = f"processing file {suffix}"
            live.update(generate_table(runs))

        if file_path.endswith(".txt"):
            process_ascii_file(file_path, h5f, compr_lvl)

        elif live is not None:
            suffix = file_path.rsplit(".", 1)[-1]
            skipped.append(suffix)
            runs[run_num]["status"] = (
                f"skipping unsupported file type .{suffix}"
            )
            live.update(generate_table(runs))

        if live is not None:
            progress_str = f"{i} / {len(runs[run_num]['files'])}"
            runs[run_num]["progress"] = progress_str
            live.update(generate_table(runs))

    if live is not None:
        status = "done"

        if len(skipped) > 0:
            status += f" (skipped: {', '.join(skipped)})"

        runs[run_num]["status"] = status
        live.update(generate_table(runs))


def run() -> None:
    args = parser().parse_args()

    if isinstance(args.output_dir, str):
        output_dir = Path(args.output_dir)

    else:
        output_dir = args.output_dir

    compr_lvl = args.compression

    with Live(None, refresh_per_second=4) as live:
        verbose_live = live if args.verbose else None

        live.console.print("glob'ing files...", end="")
        matches = list(Path.cwd().glob(args.glob_str))

        if len(matches) == 0:
            raise FileNotFoundError()

        live.console.print("grouping runs...", end="")
        runs = group_runs(matches, live=verbose_live)

        live.console.print("process runs...", end="")
        for run_num, run in runs.items():
            file_name = run_num

            if not args.short_name:
                file_name += "_" + run["name"]

                if args.full_name:
                    if run["date"] is not None:
                        file_name += "_" + run["date"].strftime("%d%m%Y")

                    if run["time"] is not None:
                        file_name += "_" + run["time"].strftime("%H%M%S")

            output_path = output_dir / (file_name + ".h5")

            if output_path.is_file() and not args.replace:
                if verbose_live is not None:
                    runs[run_num]["status"] = "skipping existing file..."
                    verbose_live.update(generate_table(runs))

                continue

            with h5py.File(output_path, "w") as h5f:
                h5f.attrs["number"] = run_num
                h5f.attrs["name"] = run["name"]

                if run["date"] is not None:
                    h5f.attrs["date"] = run["date"].strftime("%Y-%m-%d")

                if run["time"] is not None:
                    h5f.attrs["time"] = run["time"].strftime("%H:%M:%S")

                process_run(run_num, runs, h5f, compr_lvl, live=verbose_live)

        live.console.print("", end="")


def parser() -> argparse.ArgumentParser:
    cli = argparse.ArgumentParser(
        prog="metro2hdf", description="Converts METRO data files to hdf5"
    )

    cli.add_argument(
        "--glob",
        dest="glob_str",
        action="store",
        type=str,
        metavar="pattern",
        default="*",
        help="specify a pattern to glob for to narrow down conversion "
        "(default: *)",
    )

    cli.add_argument(
        "--output-dir",
        dest="output_dir",
        action="store",
        type=str,
        metavar="path",
        default=Path.cwd(),
        help="output directory for hdf5 files",
    )

    cli.add_argument(
        "--replace",
        dest="replace",
        action="store_true",
        help="replace already existing output files",
    )

    cli.add_argument(
        "--verbose",
        dest="verbose",
        action="store_true",
        help="give more detailed messages if possible",
    )

    shortening_group = cli.add_mutually_exclusive_group()

    shortening_group.add_argument(
        "--short-name",
        dest="short_name",
        action="store_true",
        help="use only number as filename for the output files",
    )

    shortening_group.add_argument(
        "--full-name",
        dest="full_name",
        action="store_true",
        help="use full name with datetime as filename for the output files",
    )

    hdf_group = cli.add_argument_group("HDF5 options")

    hdf_group.add_argument(
        "--driver",
        dest="driver",
        action="store",
        type=str,
        metavar="name",
        default=None,
        choices=["sec2", "stdio", "core"],
        help="specify a particular low-level driver for HDF5 to use",
    )

    hdf_group.add_argument(
        "--compress",
        dest="compression",
        action="store",
        type=int,
        metavar="level",
        const=4,
        default=4,
        nargs="?",
        help="use gzip compression with optionally specified level "
        "(default: 4) for datasets above 1024 bytes",
    )

    hptdc_group = cli.add_argument_group("HPTDC options")

    hptdc_group.add_argument(
        "--hptdc-chunk-size",
        dest="hptdc_chunk_size",
        action="store",
        type=int,
        metavar="size",
        default=10000,
        help="the number of data elements to read, convert and store at a "
        "time (default: 1e5).",
    )

    hptdc_group.add_argument(
        "--hptdc-ignore-tables",
        dest="hptdc_ignore_tables",
        action="store_true",
        help="ignore the scan and step tables in a TDC file and try to "
        "rebuild them by searching for its markers.",
    )

    hptdc_group.add_argument(
        "--hptdc-word-format",
        dest="hptdc_word_format",
        action="store",
        type=str,
        choices=["raw", "decoded"],
        default="raw",
        help="store the words generated in certain operation modes directly "
        "in raw form (4 byte per word, default) or decoded into its type "
        "and argument (8 byte per word).",
    )

    hptdc_group.add_argument(
        "--hptdc-with-legacy",
        dest="hptdc_legacy_channels",
        action="store",
        type=str,
        metavar="channel",
        nargs="+",
        default=[],
        help="treat these channels as legacy RoentDek HPTDC raw hit stream "
        "files (recorded before February 2017) if they were not "
        "identified automatically",
    )

    return cli
