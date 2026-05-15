import importlib.util


def test_sorting_tdc():
    cython = importlib.util.find_spec("Cython")
    spec = importlib.util.find_spec("metro_eval.cli.sort_events.sorting_tdc")
    assert (spec is None) == (cython is None)
