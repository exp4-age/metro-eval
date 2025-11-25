from __future__ import annotations

from functools import wraps
import timeit
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import Any
    from numpy.typing import ArrayLike


def cache_binned(func: callable) -> callable:
    cache = {}

    @wraps(func)
    def cached_func(bin_edges: ArrayLike) -> Any:
        n = bin_edges.size
        bin_range = (bin_edges[0], bin_edges[-1])

        start = timeit.default_timer()

        if n in cache:
            for cached_range, cached_result in cache[n].items():
                if np.allclose(bin_range, cached_range):
                    end = timeit.default_timer()
                    print(f"Found cached result in {end - start:.6f} s")
                    return cached_result

        result = func(bin_edges)

        end = timeit.default_timer()
        print(f"Calculated result in {end - start:.6f} s")

        cache.setdefault(n, {})[bin_range] = result

        return result

    return cached_func
