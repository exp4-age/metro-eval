import warnings
import h5py
import numpy as np

from ._index_ascii import index_ascii


def process_ascii(
    file_path: str,
    channel: h5py.Group,
    **kwargs,
) -> str | None:
    # parse attributes and find scan/step indices and line numbers
    index = index_ascii(file_path)

    if "Frequency" not in index["attrs"]:
        return f"Frequency attribute not found in {channel.name}, skipping..."

    freq = index["attrs"]["Frequency"]

    if freq not in ["continuous", "step"]:
        return f"Unknown Frequency '{freq}' in {channel.name}, skipping..."

    # write attributes to channel
    for attr, val in index["attrs"].items():
        channel.attrs[attr] = val

    if freq == "step":
        return process_step(index["scans"], file_path, channel, kwargs)
    else:
        return process_continuous(index["scans"], file_path, channel, kwargs)


def process_step(
    scans: dict,
    file_path: str,
    channel: h5py.Group,
    kwargs: dict,
) -> str | None:
    empty_scans = 0

    for scan_idx, scan in scans.items():
        skip_header = scan["start"]

        if "end" in scan:
            max_rows = scan["end"] - skip_header

            if max_rows <= 0:
                empty_scans += 1
                continue

        else:
            max_rows = None

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=UserWarning)

            data = np.genfromtxt(
                file_path,
                dtype=np.float32,
                skip_header=skip_header,
                max_rows=max_rows,
            )

        compression = kwargs.copy()

        if data.size == 0:
            empty_scans += 1
            continue
        elif data.size < 128:
            compression.pop("compression")
            compression.pop("compression_opts")

        channel.create_dataset(
            scan_idx,
            shape=data.shape,
            dtype=np.float32,
            data=data,
            **compression,
        )

    if empty_scans > 0:
        return f"({empty_scans} empty scans in {channel.name})"

    return None


def process_continuous(
    scans: dict,
    file_path: str,
    channel: h5py.Group,
    kwargs: dict,
) -> str | None:
    empty_steps = 0

    for scan_idx, scan in scans.items():
        scan_grp = channel.require_group(scan_idx)

        for step_idx in scan["steps"]:
            step_val = scan["steps"][step_idx]["value"]

            skip_header = scan["steps"][step_idx]["start"]

            if "end" in scan["steps"][step_idx]:
                max_rows = scan["steps"][step_idx]["end"] - skip_header

                if max_rows <= 0:
                    empty_steps += 1
                    continue

            else:
                max_rows = None

            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)

                data = np.genfromtxt(
                    file_path,
                    dtype=np.float32,
                    skip_header=skip_header,
                    max_rows=max_rows,
                )

            compression = kwargs.copy()

            if data.size == 0:
                empty_steps += 1
                continue
            elif data.size < 128:
                compression.pop("compression")
                compression.pop("compression_opts")

            scan_grp.create_dataset(
                step_val,
                shape=data.shape,
                dtype=np.float32,
                data=data,
                **compression,
            )

    if empty_steps > 0:
        return f"({empty_steps} empty steps in {channel.name})"

    return None
