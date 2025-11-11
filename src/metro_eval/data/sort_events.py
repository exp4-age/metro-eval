import time
import glob
from pathlib import Path
import sys

import numpy as np
import h5py


# Pure Python implementation
def analyze_words_python(events, words):
    cur_event_E = []
    cur_event_P = []

    n_events = 0
    e_counter = 0
    p_counter = 0

    for word_idx in range(len(words)):
        word = words[word_idx]
        type_ = word[0]

        if type_ == b"RL":
            n_events += 1
            # Start of a new bunch, so analyze the previous one

            # E
            if e_counter == 1 and p_counter == 0:
                events["E"].append(cur_event_E[0])

            # P
            elif e_counter == 0 and p_counter == 1:
                events["P"].append(cur_event_P[0])

            # EP
            elif e_counter == 1 and p_counter == 1:
                events["EP"].append([cur_event_E[0], cur_event_P[0]])

            # EE
            elif e_counter == 2 and p_counter == 0:
                events["EE"].append(cur_event_E[:])

            # PP
            elif e_counter == 0 and p_counter == 2:
                events["PP"].append(cur_event_P[:])

            # EEP
            elif e_counter == 2 and p_counter == 1:
                events["EEP"].append(
                    [cur_event_E[0], cur_event_E[1], cur_event_P[0]]
                )

            # EEE
            elif e_counter == 3 and p_counter == 0:
                events["EEE"].append(cur_event_E[:])

            # EEEE
            elif e_counter == 4 and p_counter == 0:
                events["EEEE"].append(cur_event_E[:])

            elif e_counter > 0 or p_counter > 0:
                events["other"].append(
                    "{0}E{1}P|{2}|{3}".format(
                        e_counter,
                        p_counter,
                        ",".join([str(v) for v in cur_event_E]),
                        ",".join([str(v) for v in cur_event_P]),
                    )
                )

            e_counter = 0
            p_counter = 0

            cur_event_E.clear()
            cur_event_P.clear()

        elif type_ == b"GR":
            pass  # ignore

        elif type_ == b"FL":
            if word[1] == 1:
                cur_event_E.append(word[3])
                e_counter += 1
            elif word[1] == 2:
                cur_event_P.append(word[3])
                p_counter += 1

    return n_events


def run(
    glob_str="*",
    output_dir=None,
    replace=False,
    verbose=False,
    datatype="EP",
    **kwargs,
):
    # Folder where to write created HDF5 files
    output_dir = Path.cwd() if output_dir is None else Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Folder with data to parse
    files = Path(glob_str).resolve()

    # Type of particles measured: EI as optional 2nd CL argument for EI coinc.
    par2 = "I" if datatype == "EI" else "P"

    print(f"Sort TDC events (E/{par2}) from Metro HDF files:\n\t{files}")

    # Try to import the Cython version
    try:
        from metro_eval.data.sorting_tdc import analyze_words_native
    except ImportError:
        print("(using pure python implementation)")
        analyze_words = analyze_words_python
    else:
        print("(using native implementation)")
        analyze_words = analyze_words_native

    groups_dtype = np.dtype(
        [("type", "S2"), ("arg1", "i1"), ("arg2", "i1"), ("arg3", "<i4")]
    )

    filelist = sorted(glob.glob(glob_str))
    if filelist:
        print("\nParsing files...")
    else:
        print("\nNo matching files found!")

    # Parse the files
    for run_file in filelist:
        run_file = Path(run_file)
        filename = run_file.name
        run_nr = filename[: filename.find("_")]
        out_file = output_dir / f"{run_nr}_ev.h5"

        if out_file.is_file() and not replace:
            print(" * Skipping already processed", filename)
            continue
        elif run_file.stat().st_size > 100 * 2**30:  # 100 Gbyte max.
            print(" * WARNING! Skipping too large", filename)
            continue

        print(" * Processing file", filename)

        with (
            h5py.File(run_file, "r") as h5in,
            h5py.File(out_file, "w") as h5out,
        ):
            groups_name = None

            for channel_name in h5in:
                if channel_name.endswith("#groups"):
                    scan0 = h5in[channel_name]["0"]

                    if scan0[next(iter(scan0))].dtype == groups_dtype:
                        groups_name = channel_name
                        break

            if groups_name is None:
                print("\tCould not find any TDC groups channel!")
                continue

            in_groups = h5in[groups_name]

            for scan_idx in in_groups:
                in_scan = in_groups[scan_idx]
                out_scan = h5out.create_group(str(scan_idx))

                for step_value in in_scan:
                    start_time = time.time()

                    in_step = in_scan[step_value]
                    out_step = out_scan.create_group(step_value)

                    n_words = in_step.shape[0]

                    if n_words == 0:
                        print(
                            "\tScan {0} Step {1:.2f} empty".format(
                                scan_idx, float(step_value)
                            )
                        )
                        continue

                    # Holds the event data as found by analyze_words
                    events = {
                        "E": [],
                        "EE": [],
                        "EEE": [],
                        "EEEE": [],
                        "P": [],
                        "PP": [],
                        "EP": [],
                        "EEP": [],
                        "other": [],
                    }

                    # Holds the event lists converted to numpy arrays to
                    # save memory during further analysis
                    bufs = {
                        "E": [],
                        "EE": [],
                        "EEE": [],
                        "EEEE": [],
                        "P": [],
                        "PP": [],
                        "EP": [],
                        "EEP": [],
                    }

                    word_start = 0
                    n_events = 0

                    while word_start < n_words:
                        word_end = min(word_start + 10000000, n_words)

                        try:
                            while in_step[word_end + 1][0] != b"GR":
                                word_end += 1
                        except (ValueError, IndexError):
                            word_end = n_words

                        words_slice = np.asarray(in_step[word_start:word_end])
                        n_events += analyze_words(events, words_slice)
                        words_slice = None

                        for buf_type, buf_list in bufs.items():
                            event_list = events[buf_type]

                            if len(event_list) > 0:
                                buf_list.append(np.array(event_list))
                                event_list.clear()

                        word_start = word_end

                    n_res = sum([len(v) for v in events.values()])

                    out_step.attrs["n_events"] = n_events

                    other_data = "\n".join(events["other"]).replace("P", par2)
                    out_step.create_dataset("other", data=other_data)

                    for buf_type, buf_list in bufs.items():
                        buf_data = (
                            np.concatenate(buf_list)
                            if len(buf_list) > 0
                            else []
                        )

                        ev_type = buf_type.replace("P", par2)
                        out_step.create_dataset(
                            ev_type,
                            data=buf_data,
                            compression="gzip",
                            compression_opts=4,
                        )

                    end_time = time.time()

                    total_time = end_time - start_time
                    word_rate = len(in_step) / total_time

                    print(
                        "\tScan {0} Step {1:.2f} in {2:.1f}s ({3:.1f} MWords/s) "
                        "with {4} unsorted events".format(
                            scan_idx,
                            float(step_value),
                            total_time,
                            word_rate / 1e6,
                            n_events - n_res - 1,
                        )
                    )

    print("\ndone")


def main():
    import argparse

    # Define command line arguments
    cli = argparse.ArgumentParser(
        prog="sort_events.py",
        description="Creates a hdf5 file containing sorted TDC events.",
    )

    cli.add_argument(
        "--glob",
        dest="glob_str",
        action="store",
        type=str,
        metavar="pattern",
        default="*.h5",
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

    cli.add_argument(
        "--event-counter",
        dest="counter",
        action="store_true",
        help="also store the event number alongside the event data",
    )

    cli.add_argument(
        "--type",
        dest="datatype",
        action="store",
        type=str,
        metavar="particles",
        default="EP",
        choices=["EP", "EI"],
        help="type of recorded particles, either EP or EI",
    )

    # Parse them!
    args, argv_left = cli.parse_known_args()

    try:
        run(**vars(args))
    except Exception as e:
        if args.verbose:
            print("FATAL EXCEPTION")
            raise e
        else:
            print("FATAL:", str(e))

        sys.exit(0)
