import argparse
import warnings
import os
from pathlib import Path
import h5py
from rich.live import Live
from rich.table import Table

from ._process_ascii import process_ascii
from ._process_hptdc import process_hptdc
from ._group_runs import group_runs


def generate_table(
    runs: dict,
    start: int = 0,
    max_rows: int | None = None,
) -> Table:
    table = Table(expand=True)
    table.add_column("run", justify="right", style="cyan", ratio=1)
    table.add_column("status", style="magenta", ratio=8)
    table.add_column("progress", justify="center", style="green", ratio=1)

    row_number = 0
    row_count = 0

    for run_num, cols in runs.items():
        row_number += 1

        if row_number < start:
            continue

        if max_rows is not None and row_count >= max_rows:
            break

        table.add_row(
            run_num,
            cols.get("status", ""),
            cols.get("progress", ""),
        )

        row_count += 1

    return table


def process_run(
    num: str,
    runs: dict,
    h5f: h5py.File,
    args: argparse.Namespace,
    live: Live,
    start: int,
    max_rows: int | None,
) -> None:
    skipped = []
    empty = {}
    tdc_warnings = ""

    progress = 0
    total = len(runs[num]["channels"])

    for channel, file_path in runs[num]["channels"].items():
        if args.verbose:
            runs[num]["status"] = f"processing channel {channel}..."
            live.update(generate_table(runs, start=start, max_rows=max_rows))

        channel_grp = h5f.require_group(channel)

        if file_path.endswith(".txt"):
            with warnings.catch_warnings(record=True) as wrns:
                warnings.simplefilter("always")

                process_ascii(
                    file_path,
                    channel_grp,
                    compression=args.compression,
                )

                for w in wrns:
                    if args.verbose:
                        if channel not in empty:
                            empty[channel] = 0

                        empty[channel] += 1

                    else:
                        print(w.message)

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

                for w in wrns:
                    tdc_warnings += f"{w.message} "

                if not args.verbose and len(wrns) > 0:
                    print(f"HPTDC warnings in {file_path}: {tdc_warnings}")

        elif args.verbose:
            suffix = file_path.rsplit(".", 1)[-1]
            skipped.append(channel + "." + suffix)

        progress += 1

        if args.verbose:
            runs[num]["progress"] = f"{progress} / {total}"
            live.update(generate_table(runs, start=start, max_rows=max_rows))

    if args.verbose:
        status = "done"

        if len(skipped) > 0:
            status += f" (skipped: {', '.join(skipped)})"

        for channel, count in empty.items():
            status += f" ({count} empty steps in {channel})"

        if tdc_warnings != "":
            status += f" (HPTDC warnings: {tdc_warnings.strip()})"

        runs[num]["status"] = status
        live.update(generate_table(runs, start=start, max_rows=max_rows))


def run() -> None:
    args = parser().parse_args()

    if isinstance(args.output_dir, str):
        output_dir = Path(args.output_dir)

    else:
        output_dir = args.output_dir

    with Live(None, refresh_per_second=4) as live:
        # glob files with given pattern
        matches = list(Path.cwd().glob(args.glob_str))

        if len(matches) == 0:
            raise FileNotFoundError()

        # group files into runs
        runs = group_runs(matches, verbose=args.verbose)

        if args.verbose:
            live.update(generate_table(runs))

        row_number = 0
        max_rows = max(os.get_terminal_size().lines - 8, 8)

        for num, run in runs.items():
            row_number += 1
            start = row_number - (row_number % max_rows)

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

            file_path = output_dir / (file_name + ".h5")

            if file_path.is_file() and not args.replace:
                if args.verbose:
                    runs[num]["status"] = "hdf5 exists: skipping..."
                    live.update(
                        generate_table(runs, start=start, max_rows=max_rows)
                    )

                continue

            with h5py.File(file_path, "w", driver=args.driver) as h5f:
                h5f.attrs["number"] = num
                h5f.attrs["name"] = attrs["name"]

                if "date" in attrs:
                    h5f.attrs["date"] = attrs["date"].strftime("%Y-%m-%d")

                if "time" in attrs:
                    h5f.attrs["time"] = attrs["time"].strftime("%H:%M:%S")

                process_run(num, runs, h5f, args, live, start, max_rows)

        if args.verbose:
            live.update(generate_table(runs))


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
        help="use only number for the output files",
    )

    shortening_group.add_argument(
        "--full-name",
        dest="full_name",
        action="store_true",
        help="use full name with datetime for the output files",
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

    return cli
