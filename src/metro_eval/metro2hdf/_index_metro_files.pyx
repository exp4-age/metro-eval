# cython: language_level=3
from pathlib import Path
from libc.stdio cimport FILE, fopen, fclose, fgets
from libc.string cimport strncmp, strlen
from libc.stdlib cimport atoi, atof
cimport cython

cdef extern from "string.h":
    char *strtok(char *str, const char *delim)
    char *strncpy(char *dest, const char *src, size_t n)

@cython.boundscheck(False)
@cython.wraparound(False)
        


@cython.boundscheck(False)
@cython.wraparound(False)
def index_ascii_file(str filename):
    cdef FILE* f
    cdef char line[8192]  # Support long lines
    cdef char line_copy[8192]
    cdef char* token
    cdef long long line_num = 0
    cdef int scan_idx
    cdef double step_val

    cdef str scan = None
    cdef str step = None

    result = {}

    # Open file
    f = fopen(filename.encode('utf-8'), "rb")
    if f == NULL:
        raise IOError(f"Cannot open {filename}")

    try:
        while fgets(line, sizeof(line), f) != NULL:
            # Check for scan marker: #Scan <int>
            if strncmp(line, b"#Scan", 5) == 0:
                # Save previous step data if exists
                if scan is not None and step is not None:
                    result[scan][step]["len"] = line_num - result[scan][step]["start"]

                # Parse scan index
                strncpy(line_copy, line, sizeof(line_copy))
                token = strtok(line_copy, b" \t\n\r")  # "#Scan"
                token = strtok(NULL, b" \t\n\r")  # index

                if token != NULL:
                    scan_idx = atoi(token)
                    scan = str(scan_idx)
                    step = None

                else:
                    scan = None

                line_num += 1
                continue

            elif scan is None:
                line_num += 1
                continue

            # Check for step marker: #Step <float>
            if strncmp(line, b"#Step", 5) == 0:
                # Save previous step data if exists
                if scan is not None and step is not None:
                    result[scan][step]["len"] = line_num - result[scan][step]["start"]

                # Parse step value
                strncpy(line_copy, line, sizeof(line_copy))
                token = strtok(line_copy, b" \t\n\r")  # "#Step"
                token = strtok(NULL, b" \t\n\r")  # value

                if token != NULL:
                    step_val = atof(token)
                    step = str(step_val)
                    result.setdefault(scan, {})[step] = {
                        "start": line_num,
                        "len": 0
                    }

                else:
                    step = None

            line_num += 1

        # Save final step data
        if scan is not None and step is not None:
            result[scan][step]["len"] = line_num - result[scan][step]["start"]

        return result

    finally:
        fclose(f)

