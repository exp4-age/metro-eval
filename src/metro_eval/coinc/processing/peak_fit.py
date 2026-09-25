"""Peak fitting helpers built on top of lmfit models."""

from lmfit.models import GaussianModel, LinearModel


def fit_peak(x, y):
    """Fit a Gaussian peak with a linear background to a 1D profile.

    Parameters
    ----------
    x : array-like
        Independent variable values.
    y : array-like
        Dependent signal values.

    Returns
    -------
    lmfit.model.ModelResult
        Result of the Gaussian-plus-linear-fit optimization.
    """
    peak = GaussianModel(prefix="g_")
    bg = LinearModel(prefix="b_")

    model = peak + bg

    pars = model.make_params()

    pars["g_center"].set(value=x[y.argmax()])
    pars["g_sigma"].set(value=(x.max() - x.min()) / 10)
    pars["g_amplitude"].set(value=y.max())

    result = model.fit(y, pars, x=x)
    return result
