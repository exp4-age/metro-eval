import argparse
from pathlib import Path
import h5py
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import TextIO


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


def run() -> None:
    args = parser.parse_args()

    if isinstance(args.output_dir, str):
        output_dir = Path(args.output_dir)

    else:
        output_dir = args.output_dir

    compression_opts = args.compression

    try:
        from metro_eval.metro2hdf.glob_metro_files import group_runs
        from metro_eval.metro2hdf.index_ascii_file_cy import index_ascii_file

    except ImportError:
        print("ERROR: Could not import required modules!")
        return

    run_map = group_runs(args.glob_str)

    # matches = Path.cwd().glob(glob_str)
    # for match in matches:
    #    if not match.is_file():
    #        continue

    if len(run_map) == 0:
        print("No matching METRO files found, exiting!")
        return

    for num, channel_files in run_map.items():
        out_path = output_dir / f"{num}_data.h5"

        if args.short_name:
            out_path = output_dir / f"{num}.h5"

        elif args.full_name:
            for cf in channel_files:
                if cf.endswith((".jpg", ".jpeg", ".png")):
                    name = Path(cf).stem
                    out_path = output_dir / f"{num}_{name}.h5"

        if out_path.is_file() and not args.replace:
            print("Found existing file, skipping!")
            continue

        with h5py.File(out_path, "w", driver=args.driver) as h5f:
            h5f.attrs["number"] = num

            for channel_file in channel_files:
                match channel_file[channel_file.rfind(".") + 1 :]:
                    case "txt":
                        scans = index_ascii_file(channel_file)
                        with open(channel_file, "r") as asciif:
                            txt_to_hdf5(asciif, h5f, scans, compression_opts)

                    case _:
                        print("WARNING: Unknown file format, skipping!")


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
