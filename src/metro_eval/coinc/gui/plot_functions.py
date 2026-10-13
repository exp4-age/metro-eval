"""Plot helpers for coincidence data and histogram inspection.

The functions in this module contain the small set of arithmetic helpers used to
inspect 1D and 2D event distributions. The actual Qt widget lives under the
widget package and is not kept here.
"""

import numpy as np


def hist_1D(array, column, bins=None, range=None, density=False):
    """Return the histogram values and bin edges for one column.

    Parameters
    ----------
    array : numpy.ndarray
        2D array containing the data.
    column : int
        Column index to histogram.
    bins : int or sequence, optional
        Histogram bins.
    range : tuple, optional
        Histogram range as ``(min, max)``.
    density : bool, optional
        Whether to normalize the histogram to a probability density.

    Returns
    -------
    tuple
        A pair ``(values, edges)`` as returned by ``numpy.histogram``.
    """
    hist_kwargs = dict(bins=bins, range=range, density=density)
    values, edges = np.histogram(array[:, column], **hist_kwargs)
    return values, edges
