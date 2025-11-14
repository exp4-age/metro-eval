from libc.stdlib cimport free
from libc.stddef cimport size_t

cdef extern from "glob_metro_files.h":
    ctypedef struct FileList:
        char **files
        size_t count
        size_t capacity

    ctypedef struct NumGroup:
        char *num
        FileList file_list

    ctypedef struct Measurements:
        NumGroup *groups
        size_t count
        size_t capacity

    int group_files_by_num(const char *pattern, Measurements *m)
    void free_measurements(Measurements *m)

def group_runs(pattern: str) -> dict:
    """
    Groups files matching the pattern by their numeric prefix.

    Args:
        pattern: The glob pattern to match files (e.g., "*", "*.txt")

    Returns:
        A dictionary with numeric prefixes as keys and lists of file paths as values

    Raises:
        RuntimeError: If the C function returns an error
    """
    cdef Measurements measurements
    cdef int result
    cdef size_t i, j
    cdef bytes pattern_bytes = pattern.encode('utf-8')

    # Call the C function
    result = group_files_by_num(pattern_bytes, &measurements)

    if result != 0:
        raise RuntimeError(f"group_files_by_num failed with error code {result}")

    try:
        # Convert C structure to Python dict
        output = {}

        for i in range(measurements.count):
            group = measurements.groups[i]
            num = group.num.decode('utf-8')

            # Extract file list
            files = []
            for j in range(group.file_list.count):
                file_path = group.file_list.files[j].decode('utf-8')
                files.append(file_path)

            output[num] = files

        return output

    finally:
        # Always free the C memory
        free_measurements(&measurements)
