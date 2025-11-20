import warnings
import h5py
import numpy as np

from metro_eval.metro2hdf._index_ascii import index_ascii


def process_ascii(
    file_path: str,
    channel: h5py.Group,
    compression: int = 4,
) -> str:
    compress_args = {
        "compression": "gzip",
        "compression_opts": compression,
    }

    wrnmsg = f"WARNING: Found empty in {file_path}!"

    index = index_ascii(file_path)

    for attr, val in index["attrs"].items():
        channel.attrs[attr] = val

    for scan_idx, scan in index["scans"].items():
        if "steps" not in scan:
            skip_header = scan["start"]

            if "end" in scan:
                max_rows = scan["end"] - skip_header

                if max_rows <= 0:
                    warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                    continue

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
                warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                continue

            if data.size < 128:
                local_compress_args = {}

            else:
                local_compress_args = compress_args

            channel.create_dataset(
                scan_idx,
                shape=data.shape,
                dtype=np.float64,
                data=data,
                **local_compress_args,
            )

            continue

        scan_grp = channel.require_group(scan_idx)

        for step_idx in scan["steps"]:
            skip_header = scan["steps"][step_idx]["start"]

            if "end" in scan["steps"][step_idx]:
                max_rows = scan["steps"][step_idx]["end"] - skip_header

                if max_rows <= 0:
                    warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                    continue

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
                warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                continue

            if data.size < 128:
                local_compress_args = {}

            else:
                local_compress_args = compress_args

            scan_grp.create_dataset(
                step_val,
                shape=data.shape,
                dtype=np.float64,
                data=data,
                **local_compress_args,
            )
