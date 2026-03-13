from __future__ import annotations

import argparse
import warnings
from pathlib import Path
import h5py
from rich.live import Live
from rich.table import Table
from rich.console import Console

from ._process_ascii import process_ascii
from ._process_hptdc import process_hptdc
from ._group_runs import group_runs


def generate_table(
    run_table: dict[str, dict[str, str]],
) -> callable[[dict], Table]:
    run_table = run_table.copy()

    def update_table(
        num: str | None = None,
        status: str | None = None,
        progress: str | None = None,
        skip_finished: bool = True,
    ) -> Table:
        if num is not None and num in run_table:
            if status is not None:
                run_table[num]["status"] = status

            if progress is not None:
                run_table[num]["progress"] = progress

        table = Table(expand=True)
        table.add_column("run", justify="right", style="cyan", ratio=1)
        table.add_column("status", style="magenta", ratio=8)
        table.add_column("progress", justify="center", style="green", ratio=1)

        printed_ellipsis = False

        for num in run_table:
            if skip_finished and run_table[num]["status"].startswith("done"):
                if not printed_ellipsis:
                    table.add_row("...", "...", "...")
                    printed_ellipsis = True

                continue

            table.add_row(
                num, run_table[num]["status"], run_table[num]["progress"]
            )

        return table

    return update_table


def process_channels(
    num: str,
    run: dict,
    h5f: h5py.File,
    args: argparse.Namespace,
    live: Live,
    update_table: callable,
) -> None:
    warns = []

    counter = 0
    total = len(run["channels"])

    for channel, file_path in run["channels"].items():
        status = f"processing channel {channel}..."
        live.update(update_table(num=num, status=status))

        channel_grp = h5f.require_group(channel)

        if file_path.endswith(".txt"):
            empty = process_ascii(
                file_path,
                channel_grp,
                compression="gzip",
                compression_opts=args.compression,
            )

            if empty is not None:
                warns.append(empty)

        elif file_path.endswith(".tdc"):
            with warnings.catch_warnings(record=True) as wrns:
                warnings.simplefilter("always")

                process_hptdc(
                    file_path,
                    channel_grp,
                    chunk_size=args.hptdc_chunk_size,
                    ignore_tables=args.hptdc_ignore_tables,
                    word_format=args.hptdc_word_format,
                    compression=args.compression,
                )
                if len(wrns) > 0:
                    tdc_warnings = "; ".join([str(w.message) for w in wrns])
                    warns.append(f"(HPTDC warnings: {tdc_warnings})")

        else:
            file_type = channel + "." + file_path.rsplit(".", 1)[-1]
            warns.append(f"(Unknown file type {file_type}, skipping...)")

        counter += 1

        progress = f"{counter} / {total}"
        live.update(update_table(num=num, progress=progress))

    status = "done "
    if len(warns) > 0:
        status += " ".join(warns)

    live.update(update_table(num=num, status=status))


def process_run(
    num: str,
    run: dict,
    args: argparse.Namespace,
    live: Live,
    update_table: callable,
) -> None:
    attrs = run["attrs"]

    # construct output file name
    file_name = num

    if not args.short_name:
        file_name += "_" + attrs["name"]

        if args.full_name:
            if "date" in attrs:
                file_name += "_" + attrs["date"].strftime("%d%m%Y")

            if "time" in attrs:
                file_name += "_" + attrs["time"].strftime("%H%M%S")

    file_path = args.output_dir / (file_name + ".h5")

    if file_path.is_file() and not args.replace:
        status = "done (hdf5 exists: skipping...)"
        progress = "0 / 0"
        live.update(update_table(num=num, status=status, progress=progress))
        return

    with h5py.File(file_path, "w", driver=args.driver) as h5f:
        h5f.attrs["number"] = num
        h5f.attrs["name"] = attrs["name"]

        if "date" in attrs:
            h5f.attrs["date"] = attrs["date"].strftime("%Y-%m-%d")

        if "time" in attrs:
            h5f.attrs["time"] = attrs["time"].strftime("%H:%M:%S")

        process_channels(num, run, h5f, args, live, update_table)


def main(args: argparse.Namespace) -> None:
    if isinstance(args.output_dir, str):
        args.output_dir = Path(args.output_dir)

    # glob files with given pattern
    matches = list(Path.cwd().glob(args.glob_str))

    if len(matches) == 0:
        raise FileNotFoundError()

    # group files into runs
    runs = group_runs(matches)

    # create a table generator for the live display
    update_table = generate_table(runs)

    with Live(update_table(), refresh_per_second=4, transient=True) as live:
        for num, run in runs.items():
            process_run(num, run, args, live, update_table)

    console = Console()
    console.print(update_table(skip_finished=False))


def parser(subparsers):
    parser = subparsers.add_parser(
        "metro2hdf", description="Converts METRO data files to hdf5"
    )

    parser.add_argument(
        "--glob",
        dest="glob_str",
        action="store",
        type=str,
        metavar="pattern",
        default="*",
        help="specify a pattern to glob for to narrow down conversion "
        "(default: *)",
    )

    parser.add_argument(
        "--output-dir",
        dest="output_dir",
        action="store",
        type=str,
        metavar="path",
        default=Path.cwd(),
        help="output directory for hdf5 files",
    )

    parser.add_argument(
        "--replace",
        dest="replace",
        action="store_true",
        help="replace already existing output files",
    )

    shortening_group = parser.add_mutually_exclusive_group()

    shortening_group.add_argument(
        "--short-name",
        dest="short_name",
        action="store_true",
        help="use only number for the output files",
    )

    shortening_group.add_argument(
        "--full-name",
        dest="full_name",
        action="store_true",
        help="use full name with datetime for the output files",
    )

    hdf_group = parser.add_argument_group("HDF5 options")

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

    hptdc_group = parser.add_argument_group("HPTDC options")

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

    parser.set_defaults(func=main)
