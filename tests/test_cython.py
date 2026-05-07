import importlib.util
import os
import pytest


def test_sorting_tdc():
    PURE_PYTHON = os.getenv("PURE_PYTHON", default=None)

    if PURE_PYTHON is None:
        pytest.skip("missing environment variable PURE_PYTHON")

    spec = importlib.util.find_spec("metro_eval.cli.sort_events.sorting_tdc")
    assert (spec is None) == (PURE_PYTHON == "1")
