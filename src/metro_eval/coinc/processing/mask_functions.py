"""Helpers for filtering arrays by a specific column range.
"""

import numpy as np


def mask_by_column(file, column, filter_range, verbalize=False):
    """Return rows for which one column falls within the requested range.

    Parameters
    ----------
    file : numpy.ndarray
        Input array containing the data.
    column : int
        Column index to inspect.
    filter_range : tuple[float, float]
        Lower and upper accepted bounds for the selected values.
    verbalize : bool, optional
        If true, print a short diagnostic line.

    Returns
    -------
    numpy.ndarray
        Filtered array containing only the rows that satisfy the range check.
    """
    if verbalize:
        print(f"Filtering for column {column} in range {filter_range}.")

    col = file[:, column]
    lo, hi = filter_range
    col_mask = np.logical_and(col > lo, col < hi)

    return file[col_mask]
