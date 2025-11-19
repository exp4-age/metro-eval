import argparse
from pathlib import Path
import h5py
from rich.live import Live
from rich.table import Table

from ._process_ascii import process_ascii
from ._process_hptdc import process_hptdc
from ._group_runs import group_runs


def generate_table(runs: dict) -> Table:
    table = Table()
    table.add_column("run", justify="right", style="cyan", width=5)
    table.add_column(
        "status", style="magenta", max_width=75, width=75, no_wrap=True
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


def process_run(
    num: str,
    runs: dict,
    h5f: h5py.File,
    args: argparse.Namespace,
    live: Live,
) -> None:
    skipped = []
    progress = 0
    total = len(runs[num]["channels"])

    for channel, file_path in runs[num]["channels"].items():
        if args.verbose:
            runs[num]["status"] = f"processing channel {channel}..."
            live.update(generate_table(runs))

        channel_grp = h5f.require_group(channel)

        if file_path.endswith(".txt"):
            process_ascii(
                file_path,
                channel_grp,
                compression=args.compression,
            )

        elif file_path.endswith(".tdc"):
            process_hptdc(
                file_path,
                channel_grp,
                chunk_size=args.hptdc_chunk_size,
                ignore_tables=args.hptdc_ignore_tables,
                word_format=args.hptdc_word_format,
                compression=args.compression,
            )

        elif args.verbose:
            suffix = file_path.rsplit(".", 1)[-1]
            skipped.append(channel + "." + suffix)

        progress += 1

        if args.verbose:
            runs[num]["progress"] = f"{progress} / {total}"
            live.update(generate_table(runs))

    if args.verbose:
        status = "done"

        if len(skipped) > 0:
            status += f" (skipped: {', '.join(skipped)})"

        runs[num]["status"] = status
        live.update(generate_table(runs))


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

        for num, run in runs.items():
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
                    live.update(generate_table(runs))

                continue

            with h5py.File(file_path, "w", driver=args.driver) as h5f:
                h5f.attrs["number"] = num
                h5f.attrs["name"] = attrs["name"]

                if "date" in attrs:
                    h5f.attrs["date"] = attrs["date"].strftime("%Y-%m-%d")

                if "time" in attrs:
                    h5f.attrs["time"] = attrs["time"].strftime("%H:%M:%S")

                process_run(num, runs, h5f, args, live)


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
