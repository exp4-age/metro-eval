try:
    import cython

except ImportError:

    class _fake_cython:
        compiled = False

        def cfunc(self, func):
            return func

        def ccall(self, func):
            return func

        def __getattr__(self, type_name):
            return "object"

    cython = _fake_cython()


def index_ascii_file(file_path: str) -> dict:
    scans: dict = {}
    result: dict = {"attrs": {}}
    line: str
    line_number: int = 0
    marker: str
    marker_split: list[str]
    scan_idx: str = "0"
    step_idx: str = "0"
    step_val: str = "0"
    attr_name: str
    attr_val: str

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line_number += 1

            # Remove leading/trailing whitespace
            line = line.strip()

            # Check for SCAN marker
            if line.startswith("# SCAN "):
                if scan_idx in scans:
                    scans[scan_idx]["end"] = line_number - 1

                    if "steps" in scans[scan_idx]:
                        scans[scan_idx]["steps"]["end"] = line_number - 1

                scan_idx = line[7:].strip()

                if scan_idx not in scans:
                    scans[scan_idx] = {"start": line_number}

            # Check for STEP marker
            elif line.startswith("# STEP "):
                if scan_idx not in scans:  # TODO: ???
                    scans[scan_idx] = {"start": 1, "steps": {}}

                if "steps" not in scans[scan_idx]:
                    scans[scan_idx]["steps"] = {}

                if step_idx in scans[scan_idx]["steps"]:
                    scans[scan_idx]["steps"][step_idx]["end"] = line_number - 1

                # Parse "# STEP 0: Value"
                marker = line[7:]

                if ": " in marker:
                    marker_split = marker.split(": ")
                    step_idx = marker_split[0].strip()

                    if len(marker_split) == 2:
                        step_val = marker_split[1].strip()

                    else:
                        step_val = step_idx

                    scans[scan_idx]["steps"][step_idx] = {
                        "start": line_number,
                        "value": step_val,
                    }

                else:
                    step_idx = marker.strip()
                    scans[scan_idx]["steps"][step_idx] = {
                        "start": line_number,
                        "value": step_idx,
                    }

            elif line.startswith("# "):
                marker_split = line[2:].split(": ")

                if len(marker_split) != 2:
                    continue

                attr_name = marker_split[0].strip()
                attr_val = marker_split[1].strip()

                result["attrs"][attr_name] = attr_val

    result["scans"] = scans

    return result
