from __future__ import annotations

import warnings
import collections
import os
import struct
import numpy

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import h5py


MetroRun = collections.namedtuple(
    "MetroRun", ["number", "name", "root", "path", "time", "date", "channels"]
)

HptdcRawWord = numpy.dtype("<u4")
HptdcDecodedWord = numpy.dtype(
    [("type", "S2"), ("arg1", "<i1"), ("arg2", "<i1"), ("arg3", "<i4")],
    align=True,
)
HptdcHit = numpy.dtype(
    [
        ("time", "<i8"),
        ("channel", "<u1"),
        ("type", "<u1"),
        ("bin", "<u2"),
        ("align", "<i4"),
    ]
)
HptdcStepEntry32 = numpy.dtype(
    [("value", "a32"), ("data_offset", "<i4"), ("data_size", "<i4")]
)
HptdcStepEntry64 = numpy.dtype(
    [("value", "a32"), ("data_offset", "<i8"), ("data_size", "<i8")]
)

HptdcWordDefinitions = {
    b"FL": {"type_len": 2, "type_val": 2, "arg1": (29, 24), "arg3": (23, 0)},
    b"RS": {"type_len": 2, "type_val": 3, "arg1": (29, 24), "arg3": (23, 0)},
    b"ER": {
        "type_len": 2,
        "type_val": 1,
        "arg1": (29, 24),
        "arg2": (23, 16),
        "arg3": (15, 0),
    },
    b"GR": {"type_len": 4, "type_val": 0, "arg1": (27, 24), "arg3": (23, 0)},
    b"RL": {"type_len": 8, "type_val": 16, "arg3": (23, 0)},
    b"LV": {"type_len": 5, "type_val": 3, "arg1": (26, 21), "arg3": (20, 0)},
}


def _bitmask(start, end):
    return ((1 << (start - end)) - 1) << end


def rebuild_hptdc_tables(
    fp,
    scan_marker,
    step_marker,
    data_end,
    read_length,
    HptdcStepEntry=HptdcStepEntry64,
):
    wrnmsg = "scanning for markers..."
    warnings.warn(wrnmsg, UserWarning, stacklevel=1)

    # First we have to get "aligned" to the begin of this file's
    # data section, which always consists of a scan marker. So
    # we skip the magic code and search for this marker in the
    # two kiB of the file.

    fp.seek(5)
    buf = fp.read(2048)

    data_begin = -1
    scan_marker_length = len(scan_marker)

    for i in range(2048 - scan_marker_length):
        if buf[i : i + scan_marker_length] == scan_marker:
            data_begin = 5 + i
            break

    if data_begin < 0:
        wrnmsg = "WARNING: Could not find first scan marker, skipping!"
        warnings.warn(wrnmsg, UserWarning, stacklevel=1)
        return False

    fp.seek(data_begin)

    eof = False
    buf = b""

    offset = fp.tell()
    marker_pos = []

    # The outer loop reads a chunk of our file, while the inner
    # loop searches for markers.

    while not eof:
        new_buf = fp.read(read_length)

        if len(new_buf) < read_length:
            eof = True

        buf += new_buf

        buf_len = len(buf)
        buf_pos = 0

        while buf_pos < buf_len:
            next_scan_pos = buf[buf_pos:].find(scan_marker)
            next_step_pos = buf[buf_pos:].find(step_marker)

            if next_scan_pos < 0 and next_step_pos < 0:
                # Nothing anymore in this block, truncate to the
                # last checked position or the length of a
                # marker

                margin_pos = max(
                    buf_pos, len(buf) - max(len(scan_marker), len(step_marker))
                )
                buf = buf[margin_pos:]
                offset += margin_pos

                break

            if next_scan_pos > -1:
                if next_scan_pos < next_step_pos or next_step_pos < 0:
                    marker_pos.append(offset + buf_pos + next_scan_pos)
                    marker_pos[-1] = -marker_pos[-1]
                    buf_pos += next_scan_pos + len(scan_marker)

            if next_step_pos > -1:
                if next_step_pos < next_scan_pos or next_scan_pos < 0:
                    marker_pos.append(offset + buf_pos + next_step_pos)
                    buf_pos += next_step_pos + len(step_marker)

    n_markers = len(marker_pos)
    scan_idx = -1
    step_idx = 0
    step_tables = []

    for i in range(n_markers):
        if marker_pos[i] < 0:
            scan_idx += 1
            step_count = 0
            step_idx = 0

            for j in range(i + 1, n_markers):
                if marker_pos[j] < 0:
                    break

                step_count += 1

            step_tables.append(
                numpy.zeros((step_count,), dtype=HptdcStepEntry)
            )

        else:
            entry = step_tables[scan_idx][step_idx]

            entry["value"] = str(float(step_idx)).encode("ascii")
            entry["data_offset"] = marker_pos[i]

            try:
                next_marker = marker_pos[i + 1]
            except IndexError:
                next_marker = data_end

            entry["data_size"] = next_marker - entry["data_offset"]

            step_idx += 1

    return scan_idx + 1, step_tables


def convert_hptdc_group_data_raw(data):
    return data


def convert_hptdc_group_data_decoded(inp):
    outp = numpy.zeros_like(inp, dtype=HptdcDecodedWord)
    known_mask = numpy.zeros_like(inp, dtype=bool)

    for type_str, type_def in HptdcWordDefinitions.items():
        type_mask = numpy.equal(
            inp >> (32 - type_def["type_len"]), type_def["type_val"]
        )

        if not type_mask.any():
            continue

        known_mask |= type_mask

        numpy.place(outp[:]["type"], type_mask, type_str)

        for arg_name in ("arg1", "arg2", "arg3"):
            if arg_name not in type_def:
                continue

            arg_def = type_def[arg_name]

            numpy.place(
                outp[:][arg_name],
                type_mask,
                ((inp[type_mask] & _bitmask(*arg_def)) >> arg_def[1]).astype(
                    HptdcDecodedWord.fields[arg_name][0]
                ),
            )

    unknown_mask = ~known_mask

    if unknown_mask.any():
        numpy.place(outp[:]["type"], unknown_mask, b"??")
        numpy.place(
            outp[:]["arg3"], unknown_mask, inp[unknown_mask].astype("i4")
        )

    return outp


def convert_hptdc_hits_data(data):
    return data[["time", "channel", "type", "bin"]]


def process_hptdc(
    file_path: str,
    channel_grp: h5py.Group,
    chunk_size: int = 10000,
    word_format: str = "raw",
    ignore_tables: bool = False,
    compression: int = 4,
) -> None:
    compress_args = {"compression": "gzip", "compression_opts": compression}

    channel_grp.attrs["Type"] = "hptdc"

    with open(file_path, "rb") as fp:
        # First check for the magic code
        if fp.read(5) != b"HPTDC":
            wrnmsg = "WARNING: Invalid magic code, skipping!"
            warnings.warn(wrnmsg, UserWarning, stacklevel=1)
            return False

        # Starting in around October 2017, a reworked (and extendable)
        # header format was introduced. We try to distinguish by the
        # DATA marker that should be directly behind the header. So read
        # the file as if it's the new format and fall back if not.
        old_style = False

        header_size, version = struct.unpack("<ii", fp.read(8))
        mode = fp.read(4)
        scan_table_offset, scan_count, param_table_offset, param_table_size = (
            struct.unpack("<qiqi", fp.read(24))
        )

        # A header above 4096 bytes in size is highly unlikely, so we
        # only continue if it is smaller
        if header_size > 4096:
            old_style = True
        else:
            # Skip extra header
            fp.read(header_size - 32)

            if fp.read(4) != b"DATA":
                old_style = True

        if old_style:
            # Reset to header directly after magic code and re-read
            fp.seek(5)
            (
                scan_table_offset,
                scan_count,
                param_table_offset,
                param_table_size,
                mode,
            ) = struct.unpack("<iiiic", fp.read(17))

            # Always assume HITS mode without a mode marker
            mode = b"GRPS" if mode == b"G" else b"HITS"

            HptdcStepEntry = HptdcStepEntry32

        else:
            HptdcStepEntry = HptdcStepEntry64

        if mode == b"GRPS":
            if word_format == "raw":
                out_dtype = HptdcRawWord
                convert_data_func = convert_hptdc_group_data_raw
            elif word_format == "decoded":
                out_dtype = HptdcDecodedWord
                convert_data_func = convert_hptdc_group_data_decoded

            in_dtype = HptdcRawWord
            scan_marker = b"\x00\x00\x00\x00\xa0\x00\x00\x00"
            step_marker = b"\x00\x00\x00\x00\xb0\x00\x00\x00"

        elif mode == b"HITS":
            convert_data_func = convert_hptdc_hits_data
            in_dtype = HptdcHit
            out_dtype = HptdcHit
            scan_marker = (
                b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xa0\x00\x00"
                b"\x00\x00\x00\x00"
            )
            step_marker = (
                b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xb0\x00\x00"
                b"\x00\x00\x00\x00"
            )

        else:
            wrnmsg = "FATAL: Unknown TDC mode encountered, skipping!"
            warnings.warn(wrnmsg, UserWarning, stacklevel=1)
            return False

        channel_grp.attrs["Mode"] = mode

        if scan_table_offset > 0 and not ignore_tables:
            fp.seek(scan_table_offset)

            step_tables = []

            for scan_idx in range(scan_count):
                step_count, step_table_size = struct.unpack("<ii", fp.read(8))

                step_table = numpy.frombuffer(
                    fp.read(step_table_size), dtype=HptdcStepEntry
                )

                for step_idx in range(step_count):
                    data_size = step_table[step_idx]["data_size"]

                    if data_size < 0 or data_size % in_dtype.itemsize != 0:
                        wrnmsg = (
                            "WARNING: Invalid data_size entry {0} for step "
                            "{1}, ignoring "
                            "tables...".format(data_size, step_idx)
                        )
                        warnings.warn(wrnmsg, UserWarning, stacklevel=1)

                        step_table = None
                        break

                if step_table is None:
                    step_tables = None
                    break

                step_tables.append(step_table)
        else:
            step_tables = None

            if scan_table_offset == 0 and not ignore_tables:
                wrnmsg = (
                    "WARNING: Tables are probably corrupted, trying to "
                    "rebuild..."
                )
                warnings.warn(wrnmsg, UserWarning, stacklevel=1)

        if step_tables is None:
            # If the scan_table_offset is zero, the file was not closed
            # properly (at which point the offset is written), so the
            # data continues until the end. We can therefore use the
            # total file size as an effective scan_table_offset

            if scan_table_offset == 0:
                scan_table_offset = os.path.getsize(file_path)

            scan_count, step_tables = rebuild_hptdc_tables(
                fp,
                scan_marker,
                step_marker,
                scan_table_offset,
                max(len(scan_marker), len(step_marker)) * chunk_size,
                HptdcStepEntry,
            )

        for scan_idx in range(scan_count):
            h5scan = channel_grp.create_group(str(scan_idx))

            for step_idx in range(step_tables[scan_idx].shape[0]):
                step_entry = step_tables[scan_idx][step_idx]

                try:
                    step_value = step_entry["value"].decode("ascii")
                except ValueError:
                    wrnmsg = "WARNING: Corrupted step table, skipping!"
                    warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                    return False

                fp.seek(step_entry["data_offset"])
                fp.read(len(step_marker))  # marker

                data_len = step_entry["data_size"] - len(step_marker)

                if data_len < 0:
                    wrnmsg = "WARNING: Corrupted step table, skipping!"
                    warnings.warn(wrnmsg, UserWarning, stacklevel=1)
                    return False

                elif data_len == 0:
                    try:
                        column_count = len(out_dtype.names)
                    except TypeError:
                        column_count = 1

                    h5scan.create_dataset(
                        step_value, shape=(0, column_count), dtype=out_dtype
                    )
                    continue

                elif data_len < 1024:
                    local_compress_args = {}

                else:
                    local_compress_args = compress_args

                h5step = h5scan.create_dataset(
                    step_value,
                    shape=(data_len // in_dtype.itemsize,),
                    dtype=out_dtype,
                    **local_compress_args,
                )

                chunk_len = int(in_dtype.itemsize * chunk_size)
                start_idx = 0

                for offset in range(0, data_len, chunk_len):
                    data = convert_data_func(
                        numpy.frombuffer(
                            fp.read(min(chunk_len, data_len - offset)),
                            dtype=in_dtype,
                        )
                    )

                    h5step[start_idx : start_idx + data.shape[0]] = data
                    start_idx += data.shape[0]

        try:
            fp.seek(param_table_offset)
        except OSError:
            # Usually the header is damaged, so skip the param table
            wrnmsg = (
                "WARNING: Invalid offset for parameters table in header, "
                "probably not present in dataset..."
            )
            warnings.warn(wrnmsg, UserWarning, stacklevel=1)
            return True

        # We read one byte less to omit the last newline character
        try:
            param_lines = (
                fp.read(param_table_size - 1).decode("ascii").split("\n")
            )
        except Exception:
            wrnmsg = (
                "WARNING: Corrupted parameters table, not present in "
                "dataset..."
            )
            warnings.warn(wrnmsg, UserWarning, stacklevel=1)
            return True
        else:
            if param_lines and param_lines[0]:
                for line in param_lines:
                    key, value = line.split(" ")
                    channel_grp.attrs[key] = value
            else:
                wrnmsg = "WARNING: Empty parameters table..."
                warnings.warn(wrnmsg, UserWarning, stacklevel=1)

    return True
