from __future__ import annotations

import argparse
import asyncio
import warnings
import os
from pathlib import Path
import h5py
from rich.live import Live
from rich.table import Table
from rich.console import Console

from ._process_ascii import process_ascii
from ._process_hptdc import process_hptdc
from ._group_runs import group_runs


def get_table_generator(
    run_table: dict[str, dict[str, str]],
    start: int = 0,
    max_rows: int | None = None,
) -> callable[[dict], Table]:
    run_table = run_table.copy()
    last_table = {}

    def get_table(
        runs: dict[str, dict[str, str]] | None = None,
    ) -> Table:
        if runs is None:
            if "table" in last_table:
                return last_table["table"]

        else:
            for num in runs:
                if "status" in runs[num]:
                    run_table[num]["status"] = runs[num]["status"]

                if "progress" in runs[num]:
                    run_table[num]["progress"] = runs[num]["progress"]

        table = Table(expand=True)
        table.add_column("run", justify="right", style="cyan", ratio=1)
        table.add_column("status", style="magenta", ratio=8)
        table.add_column("progress", justify="center", style="green", ratio=1)

        row_number = 0
        row_count = 0

        for num in run_table:
            row_number += 1

            if row_number < start:
                continue

            if max_rows is not None and row_count >= max_rows:
                break

            table.add_row(
                num,
                run_table[num].get("status", ""),
                run_table[num].get("progress", ""),
            )

            row_count += 1

        last_table["table"] = table

        return table

    return get_table


def process_channels(
    num: str,
    run: dict,
    h5f: h5py.File,
    args: argparse.Namespace,
    live: Live,
    generate_table: callable[[dict], Table],
) -> None:
    empty = {}
    skipped = []
    tdc_warnings = ""

    progress = 0
    total = len(run["channels"])

    for channel, file_path in run["channels"].items():
        status = f"processing channel {channel}..."
        live.update(generate_table({num: {"status": status}}))

        channel_grp = h5f.require_group(channel)

        if file_path.endswith(".txt"):
            # TODO: async messes with warnings here
            # maybe use return value instead
            with warnings.catch_warnings(record=True) as wrns:
                warnings.simplefilter("always")

                process_ascii(
                    file_path,
                    channel_grp,
                    compression=args.compression,
                )

                if len(wrns) > 0:
                    empty[channel] = len(wrns)

        elif file_path.endswith(".tdc"):
            # TODO: async messes with warnings here
            # maybe use return value instead
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
                    tdc_warnings = " ".join([str(w.message) for w in wrns])

        else:
            suffix = file_path.rsplit(".", 1)[-1]
            skipped.append(channel + "." + suffix)

        progress += 1

        pr = f"{progress} / {total}"
        live.update(generate_table({num: {"progress": pr}}))

    status = "done"

    if len(skipped) > 0:
        status += f" (skipped: {', '.join(skipped)})"

    for channel, count in empty.items():
        status += f" ({count} empty steps in {channel})"

    if tdc_warnings != "":
        status += f" (HPTDC warnings: {tdc_warnings})"

    live.update(generate_table({num: {"status": status}}))


def process_run(
    num: str,
    run: dict,
    args: argparse.Namespace,
    live: Live,
    generate_table: callable[[dict], Table],
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
        status = "hdf5 exists: skipping..."
        live.update(generate_table({num: {"status": status}}))

        return

    with h5py.File(file_path, "w", driver=args.driver) as h5f:
        h5f.attrs["number"] = num
        h5f.attrs["name"] = attrs["name"]

        if "date" in attrs:
            h5f.attrs["date"] = attrs["date"].strftime("%Y-%m-%d")

        if "time" in attrs:
            h5f.attrs["time"] = attrs["time"].strftime("%H:%M:%S")

        process_channels(
            num,
            run,
            h5f,
            args,
            live,
            generate_table,
        )


async def run_processor(
    num: str,
    run: dict,
    args: argparse.Namespace,
    semaphore: asyncio.Semaphore,
    live: Live,
    generate_table: callable[[dict], Table],
) -> None:
    async with semaphore:
        loop = asyncio.get_running_loop()

        await loop.run_in_executor(
            None,
            process_run,
            num,
            run,
            args,
            live,
            generate_table,
        )


async def process_runs(runs: dict, args: argparse.Namespace) -> None:
    semaphore = asyncio.Semaphore(args.workers)

    max_lines = os.get_terminal_size().lines - 8
    chunk_size = max(max_lines // args.workers * args.workers, args.workers)

    chunks = []
    for i, run in enumerate(runs):
        if i % chunk_size == 0:
            chunks.append({})

        chunks[-1][run] = runs[run]

    console = Console()
    console.print(
        f"[bold]Processing {len(runs)} runs in {len(chunks)} chunks[/bold]"
    )
    console.print(f"[bold]Using {args.workers} parallel workers[/bold]\n")

    for i, chunk in enumerate(chunks):
        console.print(f"[bold]Processing chunk {i + 1} / {len(chunks)}[/bold]")

        generate_table = get_table_generator(chunk, max_rows=chunk_size)

        with Live(generate_table(), refresh_per_second=4) as live:
            async with asyncio.TaskGroup() as tg:
                for num in chunk:
                    tg.create_task(
                        run_processor(
                            num,
                            chunk[num],
                            args,
                            semaphore,
                            live,
                            generate_table,
                        )
                    )


def main() -> None:
    args = parser().parse_args()

    if isinstance(args.output_dir, str):
        args.output_dir = Path(args.output_dir)

    # glob files with given pattern
    matches = list(Path.cwd().glob(args.glob_str))

    if len(matches) == 0:
        raise FileNotFoundError()

    # group files into runs
    runs = group_runs(matches)

    asyncio.run(process_runs(runs, args))


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
        "--workers",
        dest="workers",
        action="store",
        type=int,
        metavar="number",
        default=4,
        help="number of concurrent workers to use for conversion",
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
