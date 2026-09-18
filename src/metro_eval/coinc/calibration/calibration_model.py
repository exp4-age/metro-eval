"""Pure calibration data model used by the coincidence analysis package.

This module contains the metadata and fit model used to represent a calibration.
The UI and file I/O concerns live in ``calibration_manager.py`` so the model can be
reused without pulling in Qt or filesystem helpers.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any, Dict

import numpy as np
from matplotlib.figure import Figure
from scipy.odr import Model, ODR, RealData
from scipy.optimize import curve_fit
from scipy.stats import norm

from . import models


@dataclass(slots=True)
class CalibrationMetadata:
    """Metadata that identifies a calibration file and its acquisition context."""

    experiment: str = ""
    setting: str = ""
    author: str = ""
    version: str = ""
    index: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CalibrationMetadata":
        """Build an instance from a calibration dictionary while ignoring irrelevant fields."""
        field_names = {field.name for field in fields(cls)}
        filtered = {key: value for key, value in data.items() if key in field_names}
        return cls(**filtered)

    def generate_filename(self) -> str:
        """Return the canonical calibration filename generated from the metadata."""
        return (
            f"{self.experiment}_{self.setting}_{self.index}_"
            f"{self.author}_{self.version}.json"
        )


# Backward-compatible alias for older imports.
ExperimentMetadata = CalibrationMetadata


class Calibration:
    """Model and fitting container for calibration curves.

    The object stores calibration-point data, the fit method, model function and
    bunch-overlap metadata used to convert TOF values to energy values. It also
    provides plotting and uncertainty helpers used by the Qt calibration editor.
    """

    def __init__(self, calibration_dict: Dict | None = None):
        self.calibration_dict = calibration_dict
        self.x_values = np.array([])
        self.x_err = np.array([])
        self.y_values = np.array([])
        self.y_err = np.array([])
        self.comments = ""
        self.method = None
        self.model_func = None
        self.p0 = None
        self.popt = None
        self.perr = None
        self.pcov = None
        self.bunch_overlap_params = {
            "repetition_time": None,
            "ROI_first": [None, None],
            "ROI_last": [None, None],
        }
        self.created_date = ""
        self.metadata = CalibrationMetadata()

        if calibration_dict is not None:
            self.load_dict(calibration_dict)
            self.populate_metadata(calibration_dict)

            if self.method is not None and self.model_func is not None and self.p0 is not None:
                self.get_conversion(set_values=True)
                self.__format_parameters()

    def populate_metadata(self, data: dict[str, Any]) -> None:
        """Populate calibration metadata from a calibration dictionary."""
        self.metadata = CalibrationMetadata.from_dict(data)

    def load_dict(self, data_dict: Dict) -> None:
        """Populate the object from a calibration JSON dictionary."""
        calibration_points = data_dict.get("calibration_points")
        if calibration_points:
            points = np.asarray(calibration_points)
            self.x_values = points[:, 0]
            self.y_values = points[:, 2]
            self.x_err = points[:, 1]
            self.y_err = points[:, 3]

        if "method" in data_dict:
            self.method = data_dict["method"]
        if "initial_parameters" in data_dict:
            self.p0 = data_dict["initial_parameters"]["p0"]
        if "model_type" in data_dict:
            self.model_func = models.MODELS[data_dict["model_type"]]
        if "bunch_overlap" in data_dict:
            self.bunch_overlap_params = data_dict["bunch_overlap"]
        if "Comments" in data_dict:
            self.comments = data_dict["Comments"]
        if "created_date" in data_dict:
            self.created_date = data_dict["created_date"]

        if self.x_values.size == 0 or self.y_values.size == 0:
            raise ValueError("Calibration dictionary must contain calibration points.")

    def get_conversion(
        self,
        model_func=None,
        method=None,
        set_values=False,
        verbalize=False,
        p0=None,
        **kwargs,
    ):
        """Fit the stored calibration points and optionally store the result on the object."""
        x_values = self.x_values
        y_values = self.y_values
        x_err = self.x_err
        y_err = self.y_err

        if p0 is None:
            p0 = self.p0
        if model_func is None:
            model_func = self.model_func
        if method is None:
            method = self.method
        if verbalize:
            print(f"Used fitting method: {method}")

        if set_values:
            self.method = method
            self.model_func = model_func

        if method == "odr" and (x_err is not None or y_err is not None):
            def model_odr(params, x):
                return model_func(x, *params)

            model = Model(model_odr)
            data = RealData(x_values, y_values, sx=x_err, sy=y_err)
            odr = ODR(data, model, beta0=p0 or np.ones(3), **kwargs)
            output = odr.run()
            if verbalize:
                print("odr fit performed!")
            if set_values:
                self.popt = output.beta
                self.perr = output.sd_beta
                self.pcov = output.cov_beta
            return output.beta, output.sd_beta, output.cov_beta

        sigma = y_err if y_err is not None else None
        popt, pcov = curve_fit(
            model_func,
            x_values,
            y_values,
            p0=p0,
            sigma=sigma,
            absolute_sigma=True,
            **kwargs,
        )
        perr = np.sqrt(np.diag(pcov))
        if verbalize:
            print("curve_fit performed!")
        if set_values:
            self.popt = popt
            self.perr = perr
            self.pcov = pcov
        return popt, perr, pcov

    def convert(self, x, model_func=None):
        """Apply the fitted calibration model to input values."""
        if model_func is None:
            model_func = self.model_func
        if self.popt is None:
            raise ValueError("No calibration fit has been computed yet.")
        return model_func(x, *self.popt)

    def get_uncertainty(self, x0=None, stds=1):
        """Return fitted values and confidence band limits for a calibration curve."""
        if x0 is None:
            x0 = np.linspace(np.min(self.x_values), np.max(self.x_values), 200)

        y_low, y_high = self.__confidence_bands(x0, stds=stds)
        y_fit = self.model_func(x0, *self.popt)

        return {
            "x0": x0,
            "y_fit": y_fit,
            "y_low": y_low,
            "y_high": y_high,
        }

    def __format_parameters(self, param_names=None, precision=1, fmt="e", verbalize=False):
        """Return a compact text block describing the fitted parameters."""
        if self.popt is None or self.perr is None:
            return "No fit performed"

        formatting = f"{{:.{precision}{fmt}}}"
        if param_names is None:
            param_names = [f"p{i}" for i in range(len(self.popt))]

        lines = []
        for name, p, e in zip(param_names, self.popt, self.perr):
            p_str = formatting.format(p)
            e_str = formatting.format(e)
            lines.append(f"{name} = {p_str} ± {e_str}")
            if verbalize:
                print(lines)
        self.parameter_result_string = "\n".join(lines)
        return self.parameter_result_string

    def plot(self, fig_kws=None, xlabel="Electron time of flight / ns", ylabel="Electron kinetic energy / eV", param_names=None, precision=1, nr_of_bins=200, stds=2, verbalize=False, **kwargs):
        """Create a matplotlib figure showing the calibration curve and points."""
        beamtime = self.metadata.experiment
        setting = self.metadata.setting
        fig = Figure()
        ax = fig.add_subplot()

        if self.popt is not None:
            grid = np.linspace(self.x_values.min(), self.x_values.max(), nr_of_bins)
            confidence_interval = self.get_uncertainty(x0=grid, stds=stds)
            calibration_curve, = ax.plot(grid, confidence_interval["y_fit"], label="Calibration curve", **kwargs)
            legend_str = self.__format_parameters(param_names, precision, verbalize=verbalize)
            ax.text(
                0.98,
                0.97,
                legend_str,
                transform=ax.transAxes,
                fontsize=9,
                verticalalignment="top",
                ha="right",
                bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.8),
            )

            curve_color = calibration_curve.get_color()
            ax.fill_between(
                grid,
                confidence_interval["y_low"],
                confidence_interval["y_high"],
                color=curve_color,
                alpha=0.3,
                label="Confidence interval",
            )

        ax.errorbar(
            self.x_values,
            self.y_values,
            xerr=self.x_err,
            yerr=self.y_err,
            label="Calibration points",
            ls="none",
            **kwargs,
        )

        if self.popt is not None:
            handles, labels = ax.get_legend_handles_labels()
            order = [2, 0, 1]
            ax.legend([handles[i] for i in order], [labels[i] for i in order])

        ax.set_title(beamtime + ", " + setting)
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.grid(alpha=0.3)
        return fig, ax

    def calc_jacobian(self, x0, verbalize=False):
        """Determine the numerical Jacobian of the calibration model with respect to fit parameters."""
        if self.popt is None:
            print("No fit is performed yet!")
            return None

        eps = 1e-8
        jacobian = []
        for xi in x0:
            grad = []
            for k, _ in enumerate(self.popt):
                dp = np.zeros_like(self.popt)
                dp[k] = eps
                grad_k = (
                    self.model_func(xi, *(self.popt + dp))
                    - self.model_func(xi, *(self.popt - dp))
                ) / (2 * eps)
                grad.append(grad_k)
            jacobian.append(grad)

        result = np.asarray(jacobian)
        if verbalize:
            print(result.shape)
        return result

    def __std_deviation(self, x0, verbalize=False):
        """Compute the fit-standard-deviation at a set of x positions."""
        jacobian = self.calc_jacobian(x0)
        variance = np.einsum("ij,jk,ik->i", jacobian, self.pcov, jacobian)
        sigma = np.sqrt(variance)
        if verbalize:
            print(sigma.shape)
        return sigma

    def __confidence_bands(self, x0, stds=2, confidence=None, **kwargs):
        """Return lower and upper confidence bands for the fitted calibration curve."""
        if confidence is not None:
            stds = norm.ppf(confidence)

        sigma = self.__std_deviation(x0)
        y = self.model_func(x0, *self.popt)
        y_low = y - stds * sigma
        y_high = y + stds * sigma
        return y_low, y_high

    def calc_uncertainty_of_difference(self, tof1, tof2):
        """Compute the propagated uncertainty for the difference between two TOF points."""
        jacobian1 = self.calc_jacobian(tof1)
        jacobian2 = self.calc_jacobian(tof2)
        jacobian_diff = jacobian2 - jacobian1
        variance_of_difference = jacobian_diff @ self.pcov @ jacobian_diff.T
        sigma_of_difference = np.sqrt(variance_of_difference)
        return sigma_of_difference

    def generate_filename(self) -> str:
        """Return the calibration filename derived from the metadata object."""
        return self.metadata.generate_filename()


__all__ = ["CalibrationMetadata", "ExperimentMetadata", "Calibration"]
