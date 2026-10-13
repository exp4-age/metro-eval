"""Calibration file and plot helpers for the coincidence analysis tools.

This module is responsible for calibration persistence and Qt plotting support.
The pure data model and fitting logic live in ``calibration_model.py`` so that the
calibration objects can be used independently of filesystem and GUI concerns.
The public API is intentionally small and focused on the JSON calibration files
stored under the local ``calibrations`` directory.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np
import pyqtgraph as pg

from .calibration_model import Calibration, CalibrationMetadata

# The JSON calibration files live next to the package root, not inside the
# calibration subpackage directory that contains this implementation file.
PACKAGE_DIR = Path(__file__).resolve().parents[1]
CALIBRATION_DIR = PACKAGE_DIR / "calibrations"


def get_calibration_path(calibration_dict: dict[str, Any]) -> Path:
    """Return the canonical file path for a calibration definition dictionary."""
    experiment = calibration_dict["experiment"]
    setting = calibration_dict["setting"]
    author = calibration_dict["author"]
    version = calibration_dict["version"]
    index = calibration_dict["index"]

    filename = f"{experiment}_{setting}_{index}_{author}_{version}.json"
    return CALIBRATION_DIR / filename


def get_calibration_filepath(calibration_dict: dict[str, Any]) -> Path:
    """Backward-compatible alias for :func:`get_calibration_path`."""
    return get_calibration_path(calibration_dict)


def save_calibration(calibration_dict: dict[str, Any]) -> Path:
    """Write a calibration definition dict to disk and return its file path."""
    filepath = get_calibration_path(calibration_dict)
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with filepath.open("w", encoding="utf-8") as handle:
        json.dump(calibration_dict, handle, indent=4)
    return filepath


def load_calibration(
    experiment: str | None = None,
    setting: str | None = None,
    index: str | None = None,
    author: str | None = None,
    version: str | None = None,
    filename: str | None = None,
) -> dict[str, Any]:
    """Load a calibration definition from a JSON file in the calibration directory."""
    if filename is None:
        filename = f"{experiment}_{setting}_{index}_{author}_{version}.json"

    filepath = CALIBRATION_DIR / filename
    with filepath.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def iter_calibration_files() -> list[Path]:
    """Return all stored calibration JSON files in sorted order."""
    return sorted(CALIBRATION_DIR.glob("*.json"))


def list_calibrations() -> list[Path]:
    """Backward-compatible alias for :func:`iter_calibration_files`."""
    return iter_calibration_files()

def plot_calibration_pg(calibration, plot_widget,
                        xlabel="Electron time of flight / ns",
                        ylabel="Electron kinetic energy / eV",
                        bins=1000,
                        stds=2):
    """
    Plot a calibration object into an existing pyqtgraph PlotWidget.
    """
    
    plot_widget.clear()
    
    # Labels
    plot_widget.setLabel('bottom', xlabel)
    plot_widget.setLabel('left', ylabel)

    # Grid
    plot_widget.showGrid(x=True, y=True, alpha=0.3)


    if calibration.x_values.size == 0:
        return
    
    
    # Title
    plot_widget.setTitle(
        f"{calibration.metadata.experiment}_"
        f"{calibration.metadata.setting}"
    )

    
    # Calibration points
    plot_widget.plot(
        calibration.x_values,
        calibration.y_values,
        pen=None,
        symbol='o',
        name="Calibration points"
    )

    # Error bars
    if calibration.y_err is not None:
        err = pg.ErrorBarItem(
            x=calibration.x_values,
            y=calibration.y_values,
            height=2 * calibration.y_err,
            beam=0.0
        )
        plot_widget.addItem(err)
    add_xerrorbars(
        plot_widget,
        calibration.x_values,
        calibration.y_values,
        calibration.x_err
    )

    # Fit + confidence interval
    if calibration.popt is not None:
        grid = np.linspace(
            calibration.x_values.min(),
            calibration.x_values.max(),
            bins
        )

        ci = calibration.get_uncertainty(
            x0=grid,
            stds=stds
        )

        # Fit curve
        plot_widget.plot(
            grid,
            ci["y_fit"],
            pen=pg.mkPen(width=2),
            name="Calibration curve"
        )

        # Confidence interval band
        upper = pg.PlotCurveItem(grid, ci["y_high"])
        lower = pg.PlotCurveItem(grid, ci["y_low"])

        band = pg.FillBetweenItem(
            upper,
            lower,
            brush=(100, 100, 255, 60)
        )

        plot_widget.addItem(upper)
        plot_widget.addItem(lower)
        plot_widget.addItem(band)



def add_xerrorbars(plot_widget, x, y, xerr,
                   pen=None):
    '''
    Helper function to draw proper x_err bars to a pg plot
    '''
    if pen is None:
        pen = pg.mkPen(width=1)

    y_range = np.max(y) - np.min(y)

    for xi, yi, xe in zip(x, y, xerr):

        # horizontal line
        plot_widget.plot(
            [xi - xe, xi + xe],
            [yi, yi],
            pen=pen
        )

        # left cap
        plot_widget.plot(
            [xi - xe, xi - xe],
            [yi , yi],
            pen=pen
        )

        # right cap
        plot_widget.plot(
            [xi + xe, xi + xe],
            [yi, yi],
            pen=pen
        )

    
    