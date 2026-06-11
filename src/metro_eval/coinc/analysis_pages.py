import sys
import numpy as np
import json
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QPushButton,
    QTabWidget,
    QTableWidgetItem,
    QLabel,
    QTextEdit,
    QTableWidget,
    QSplitter,
    QHeaderView
)
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qtagg import NavigationToolbar2QT as NavigationToolbar

import pyqtgraph as pg
from metro_eval.coinc.plot_functions import interactive
from metro_eval.coinc.calibration_manager import Calibration

from typing import TYPE_CHECKING
from numpy.typing import ArrayLike, NDArray

# ==========================================================
# Base class
# ==========================================================

class AnalysisPage(QWidget):
    """
    Base class for all analysis pages.
    Provides:
        - title
        - common layout
        - metadata storage
    """

    def __init__(self, title: str):
        super().__init__()

        self.title = title
        self.metadata = {}

        self.main_layout = QVBoxLayout(self)

        self.title_label = QLabel(f"<b>{title}</b>")
        self.main_layout.addWidget(self.title_label)

    def export(self):
        """
        Placeholder for future export functionality.
        """
        raise NotImplementedError


# ==========================================================
# Signal Page
# ==========================================================

class SignalPage(AnalysisPage):
    """
    Simple 1D signal plot page.
    """

    def __init__(self, x, y):
        super().__init__("1D Signal")

        self.plot_widget = pg.PlotWidget()

        self.plot_widget.showGrid(x=True, y=True)

        self.plot_widget.plot(
            x,
            y,
            pen=pg.mkPen(width=2)
        )

        self.main_layout.addWidget(self.plot_widget)

    def export(self):
        print("Export signal page")

# ==========================================================
# Histogram Page
# ==========================================================

class HistogramPage(AnalysisPage):
    """
    Simple 1D histogram plot page.
    """

    def __init__(self, values, edges, xlabel="", ylabel="", plot_kwargs={}):
        super().__init__("1D Histogram")

        self.plot_widget = pg.PlotWidget()

        self.plot_widget.showGrid(x=True, y=True)

        self.plot_widget.plot(
            edges,
            values,
            pen=pg.mkPen(width=2),
            xlabel=xlabel,
            ylabel=ylabel,
            **plot_kwargs
        )

        self.main_layout.addWidget(self.plot_widget)

    def export(self):
        print("Export histogram page")

# ==========================================================
# Coincmap Page
# ==========================================================

class CoincmapPage(AnalysisPage):
    """
    interactive 2D coincidence map page.
    """

    def __init__(self, 
                 data: NDArray,
        bins: int = 50,
        range: ArrayLike | None = None,  # noqa
        xlabel: str = "first electron",
        ylabel: str = "second electron",
        units: str | None = None,):
        super().__init__("2D Coincidence Map")

        self.plot_widget = interactive(data, 
                                       bins=bins, 
                                       range=range, 
                                       xlabel=xlabel, 
                                       ylabel=ylabel, 
                                       units=units)


        self.main_layout.addWidget(self.plot_widget)

    def export(self):
        print("Export Coincidence Map page")


# ==========================================================
# CalibrationView Page
# ==========================================================

class CalibrationViewPage(AnalysisPage):
    """
    Should show the parameters and fit result of an existing calibration 
    """

    def __init__(self, 
                 calib: Calibration,
                 ):
        super().__init__("Calibration View")

        self.calibration=calib
        self._build_ui()

    
    def _build_ui(self):
        
        splitter = QSplitter()

        # ----------------------
        # Left side
        # ----------------------

        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)

        self.json_view = QTextEdit()
        data = self.calibration.calibration_dict
        info = {k: v for k, v in data.items() if k != "calibration_points"}
        points= data["calibration_points"]
        self.json_view.setPlainText(json.dumps(info, 
                                               indent=4))
        self.json_view.setReadOnly(True)


        self.param_box = QTextEdit()
        self.param_box.setReadOnly(True)

        self.param_box.setPlainText("Fit parameters: \n"+
            self.calibration.parameter_result_string
        )
        left_layout.addWidget(
            self.json_view,
            3
        )

        left_layout.addWidget(
            self.param_box,
            1
        )
        # ----------------------
        # Right side
        # ----------------------
        self.right_box = QWidget()
        self.layout_right=QVBoxLayout()

        self.plot_widget = pg.PlotWidget()
        '''
        self.plot_widget = CalibrationPlotWidget(self.calibration)
        '''
        plot_calibration_pg(self.calibration, self.plot_widget)
        
        self.table = QTableWidget()
        self.table.setRowCount(len(points))
        self.table.setColumnCount(len(points[0]))
        self.table.horizontalHeader().setSectionResizeMode(
                QHeaderView.Stretch)
        self.table.setHorizontalHeaderLabels(
            [
                "TOF",
                "ΔTOF",
                "E",
                "ΔE"
            ]
        )
        for row_idx, row in enumerate(points):
            for col_idx, value in enumerate(row):
                self.table.setItem(row_idx, col_idx, QTableWidgetItem(str(value)))
        
        
        self.layout_right.addWidget(self.plot_widget,3)
        self.layout_right.addWidget(self.table,1)
        self.right_box.setLayout(self.layout_right)
        splitter.addWidget(left_widget)
        splitter.addWidget(self.right_box)

        splitter.setSizes([400, 800])

        self.main_layout.addWidget(splitter)



def plot_calibration_pg(calibration, plot_widget,
                        xlabel="Electron time of flight / ns",
                        ylabel="Electron kinetic energy / eV",
                        bins=1000,
                        stds=2):
    """
    Plot a calibration object into an existing pyqtgraph PlotWidget.
    """

    plot_widget.clear()

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

    # Labels
    plot_widget.setLabel('bottom', xlabel)
    plot_widget.setLabel('left', ylabel)

    # Title
    plot_widget.setTitle(
        f"{calibration.metadata.experiment}_"
        f"{calibration.metadata.setting}"
    )

    # Grid
    plot_widget.showGrid(x=True, y=True, alpha=0.3)

def add_xerrorbars(plot_widget, x, y, xerr,
                   pen=None):
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
