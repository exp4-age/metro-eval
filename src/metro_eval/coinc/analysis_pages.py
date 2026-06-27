import sys
import numpy as np
import json
from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QListWidget,
    QTableWidgetItem,
    QLabel,
    QTextEdit,
    QTableWidget,
    QSplitter,
    QHeaderView,
    QHBoxLayout,
    QFormLayout,
    QDoubleSpinBox,
    QCheckBox,
    QLineEdit,
    QSizePolicy,
    QPushButton
)

from PySide6.QtCore import Signal

import pyqtgraph as pg
from metro_eval.coinc.plot_functions import interactive
from metro_eval.coinc.calibration_manager import Calibration, plot_calibration_pg
from metro_eval.coinc.peak_fit import fit_peak

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

class ScanAnalysisPage(QWidget):

    point_submitted = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.scan_data = None

        self.curves = []

        self.selected_index = None

        main_layout = QHBoxLayout(self)

        left_layout = QVBoxLayout()

        #
        # left panel
        #
        self.scan_list = QListWidget()
        self.scan_list.setMaximumWidth(140)

        left_layout.addWidget(self.scan_list,0)

        #
        # controls
        #
        controls = QFormLayout()

        self.normalize_cb = QCheckBox()
        self.normalize_cb.setChecked(True)

        self.offset_spin = QDoubleSpinBox()
        self.offset_spin.setValue(0.5)
        self.offset_spin.setSingleStep(0.1)
        self.offset_spin.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        self.offset_spin.setMaximumWidth(80)
        self.offset_spin.setDecimals(1)

        controls.addRow("Normalize",self.normalize_cb)
        controls.addRow("Offset",self.offset_spin)

        left_layout.addLayout(controls)

        right_layout = QVBoxLayout()

        #
        # plot
        #
        self.plot = pg.PlotWidget()
        self.plot.showGrid(x=True,y=True)

        #
        # Add calibration points
        #

        point_adder_layout = QHBoxLayout()

        self.fit_point_entries = {}
        self.fit_point_entries["x"] = QLineEdit()
        self.fit_point_entries["xerr"] = QLineEdit()
        self.fit_point_entries["y"] = QLineEdit()
        self.fit_point_entries["y"].setPlaceholderText("E")
        self.fit_point_entries["yerr"] = QLineEdit()
        self.fit_point_entries["yerr"].setPlaceholderText("ΔE")

        self.submit_btn = QPushButton("Add point")
        self.submit_btn.clicked.connect(self.request_point_submission)

        for widget in self.fit_point_entries.values():
            point_adder_layout.addWidget(widget)
        point_adder_layout.addWidget(self.submit_btn)

        right_layout.addWidget(self.plot, 3)
        right_layout.addLayout(point_adder_layout)

        main_layout.addLayout(left_layout)
        main_layout.addLayout(right_layout)        

        #
        # ROI
        #
        self.roi = pg.LinearRegionItem()

        self.plot.addItem(self.roi)

        self.roi.sigRegionChangeFinished.connect(self.fit_roi)

        # fit curve
        self.fit_curve = self.plot.plot([],[],pen=pg.mkPen("r",width=3))

        # connections        
        self.scan_list.currentRowChanged.connect(
            self.select_spectrum
        )

        self.normalize_cb.stateChanged.connect(
            self.update_plot
        )

        self.offset_spin.valueChanged.connect(
            self.update_plot
        )
    
    def set_scan_data(self,scan_data):

        self.scan_data = scan_data
        self.scan_list.clear()
        self.scan_list

        for spectrum in scan_data.spectra:

            self.scan_list.addItem(
                f"{spectrum.scan_value:.2f}"
            )

        self.update_plot()

        if len(scan_data.spectra):
            self.scan_list.setCurrentRow(0)


    def update_plot(self):

        if self.scan_data is None:
            return

        self.plot.clear()

        self.curves = []

        offset_scale = self.offset_spin.value()

        for i, spectrum in enumerate(
            self.scan_data.spectra
            ):

            y = spectrum.y.copy()

            if self.normalize_cb.isChecked():

                ymax = np.max(y)

                if ymax > 0:
                    y = y / ymax

            y += float(i) * offset_scale

            curve = self.plot.plot(
                spectrum.x,
                y,
                pen="w"
            )

            self.curves.append(curve)

        self.plot.addItem(self.roi)
        self.plot.addItem(self.fit_curve)

        self.highlight_selection()

    def request_point_submission(self):
        
        request = self.fit_point_entries
        self.point_submitted.emit(request)


    def select_spectrum(self,index):

        self.selected_index = index
        self.highlight_selection()
        self.fit_roi()

    def highlight_selection(self):

        for i, curve in enumerate(
            self.curves):

            if i == self.selected_index:
                curve.setPen(pg.mkPen("y",width=3))
            else:
                curve.setPen(pg.mkPen("w",width=1))

    def fit_roi(self):

        if self.selected_index is None:
            return

        spectrum = self.scan_data.spectra[
            self.selected_index]

        x = spectrum.x
        y = spectrum.y

        # Apply same normalization as plot:
        if self.normalize_cb.isChecked():

            ymax = np.max(y)

            if ymax > 0:
                y = y / ymax
        
        # ROI selection:
        xmin, xmax = self.roi.getRegion()

        mask = (
            (x >= xmin)
            &
            (x <= xmax)
        )

        if np.count_nonzero(mask) < 5:
            return
        
        # Fit:
        result = fit_peak(
            x[mask],
            y[mask]
        )
        
        # Plot fit:
        
        y_fit_offset = result.best_fit + (self.selected_index * self.offset_spin.value())
        self.fit_curve.setData(
            x[mask],
            y_fit_offset
        )
        
        # Update fields:
        center = result.params[
            "g_center"
        ].value

        sigma = result.params[
            "g_sigma"
        ].value

        self.fit_point_entries["x"].setText(
            f"{center:.2f}"
        )

        self.fit_point_entries["xerr"].setText(
            f"{sigma:.2f}"
        )


class CoincExplorerPage(AnalysisPage):
    '''
    It is intended that you can easily select a coincidence type and
    corresponding columns and quickly explore different coincidence sets and column combinations.
    '''
    
    def __init__(self, parent=None):
        super().__init__(parent)

        self.build_ui()

    def build_ui(self):
        
        main_layout = QHBoxLayout(self)

        control_layout = self._add_control_panel()
        plot_layout = self._add_plot_panel()

        

    def _add_control_panel(self):
        
        layout = QVBoxLayout()

        return layout

    def _add_plot_panel(self):
        pass


