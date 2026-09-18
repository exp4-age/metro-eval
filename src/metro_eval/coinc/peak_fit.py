from lmfit.models import GaussianModel, LinearModel


def fit_peak(x, y):

    peak = GaussianModel(prefix="g_")
    bg = LinearModel(prefix="b_")

    model = peak + bg

    pars = model.make_params()

    pars["g_center"].set(
        value=x[y.argmax()]
    )

    pars["g_sigma"].set(
        value=(x.max() - x.min()) / 10
    )

    pars["g_amplitude"].set(
        value=y.max()
    )

    result = model.fit(
        y,
        pars,
        x=x
    )

    return result
