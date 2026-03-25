import h5py


def process_hdf5(
    file_path: str,
    channel: h5py.Group,
    **kwargs,
) -> str | None:
    # Check first whether we can open the file. We still want to use a
    # resource manager later on, so we close it immediately
    try:
        h5in = h5py.File(file_path, "r")
    except Exception:
        return f"Could not open hdf5 channel data {channel.name}, skipping..."
    else:
        h5in.close()

    with h5py.File(file_path, "r") as h5in:
        if "freq" not in h5in.attrs:
            return f"Attribute 'freq' not found in {channel.name}, skipping..."

        freq = h5in.attrs["freq"]

        # Frequency and hint were saved with their constant rather than
        # their string for a while, so accept both.
        if freq == 0 or freq == "cont":
            return f"{channel.name} is a continuous multi-file channel, skipping..."

        # write attributes to channel
        for attr, val in h5in.attrs.items():
            channel.attrs[attr] = val

        for k in h5in:
            # Scan groups may be missing, so create them now.
            if isinstance(h5in[k], h5py.Dataset):
                dest_name = "0/" + k
            else:
                dest_name = k

            h5in.copy(k, channel, dest_name)

    return None
