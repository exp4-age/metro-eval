import numpy as np
from metro_eval.fitting import interactive


def model(x, a, b):
    return a + b * x**2


rng = np.random.default_rng(4)

truth = 1, 2
x = np.linspace(0, 1, 20)
yt = model(x, *truth)
ye = 0.4 * x**5 + 0.1
y = rng.normal(yt, ye)

interactive(x, y, ye)
