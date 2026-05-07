from pathlib import Path
import numpy as np
from metro_eval.process import anodes


def test_dld_xy():
    # load test data
    data = np.genfromtxt(Path("tests/data/dld_xy#raw.txt"))
    xy = anodes.DldAnodeXY(29.0).process(data)

    # load metro pos#pos data
    xy_metro = np.genfromtxt(Path("tests/data/dld_xy!pos#pos.txt"))

    assert np.allclose(xy, xy_metro)
