import argparse
from pathlib import Path
import h5py
import numpy as np
from rich.live import Live
from rich.table import Table

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


def group_runs(
    file_list: list[Path], live: Live | None = None
) -> dict[str, list[str]]:
    runs = {}
    print_table = {}

    for file_path in file_list:
        if not file_path.is_file():
            continue

        file_name = file_path.name
        parts = file_name.split("_")

        if len(parts) < 2:
            continue

        run_num = parts[0]

        try:
            int(run_num)

        except ValueError:
            continue

        if run_num not in runs:
            runs[run_num] = []

            if live is not None:
                print_table[run_num] = {}

        runs[run_num].append(str(file_path.resolve()))

        if live is not None:
            n = len(runs[run_num])
            print_table[run_num]["status"] = f"found file ...{parts[-1]}"
            print_table[run_num]["progress"] = f"- / {n}"

            live.update(generate_table(print_table))

    if live is not None:
        for run_num in print_table:
            n = len(runs[run_num])
            print_table[run_num]["status"] = f"found {n} files..."

        live.update(generate_table(print_table))

    return runs


def index_files(runs: dict, num: str, live: Live | None = None):
    pass


def run() -> None:
    args = parser().parse_args()

    if isinstance(args.output_dir, str):
        output_dir = Path(args.output_dir)

    else:
        output_dir = args.output_dir

    compression_opts = args.compression

    with Live(None, refresh_per_second=4) as live:
        verbose_live = live if args.verbose else None

        live.console.print("glob'ing files...", end="")
        matches = list(Path.cwd().glob(args.glob_str))

        if len(matches) == 0:
            raise FileNotFoundError()

        live.console.print("grouping runs...", end="")
        runs = group_runs(matches, live=verbose_live)

        live.console.print("indexing files...", end="")
        for run_num in runs:
            index_files(runs, run_num, live=verbose_live)

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
        "--shorter-name",
        dest="shorter_name",
        action="store_true",
        help="use only number and name as filename for the output files",
    )

    shortening_group.add_argument(
        "--shortest-name",
        dest="shortest_name",
        action="store_true",
        help="use only the number as filename for the output files",
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
