from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime
import numpy as np


def group_runs(
    file_list: list[Path],
    verbose: bool = False,
) -> dict:
    runs = {}

    # group files by run number
    for file_path in file_list:
        if not file_path.is_file():
            continue

        file_name = file_path.name
        parts = file_name.split("_")

        if len(parts) < 2:
            continue

        num = parts[0]

        try:
            int(num)

        except Exception:
            print(f"could not group {file_name}: skipping...")
            continue

        if num not in runs:
            runs[num] = []

        runs[num].append(file_path.resolve())

    # sort runs by run number
    sorted_inds = np.argsort([int(num) for num in runs.keys()])
    sorted_nums = np.array(list(runs.keys()))[sorted_inds]

    sorted_runs = {}

    for num in sorted_nums:
        num = str(num)
        sorted_runs[num] = {"attrs": {}, "channels": {}}

        # extract common name prefix for the run
        name = (
            os.path.commonprefix([f.stem for f in runs[num]])
            .rstrip("_")
            .replace(f"{num}_", "")
        )

        sorted_runs[num]["attrs"]["name"] = name

        # extract channel names
        for file_path in runs[num]:
            channel = file_path.stem.replace(f"{num}", "")
            channel = channel.replace(f"{name}", "")
            channel = channel.strip("_")

            if channel == "":
                continue

            sorted_runs[num]["channels"][channel] = str(file_path.resolve())

        # extract date and time from run name
        try:
            parts = name.split("_")
            date = datetime.strptime(parts[-2], "%d%m%Y")
            time = datetime.strptime(parts[-1], "%H%M%S")

        except Exception:
            pass

        else:
            sorted_runs[num]["attrs"]["date"] = date
            sorted_runs[num]["attrs"]["time"] = time
            sorted_runs[num]["attrs"]["name"] = "_".join(parts[:-2])

    if verbose:
        for num in sorted_runs:
            n = str(len(sorted_runs[num]["channels"]))
            name = sorted_runs[num]["attrs"]["name"]

            sorted_runs[num]["status"] = f"found {name} with {n} files"
            sorted_runs[num]["progress"] = f"0 / {n}"

    return sorted_runs
