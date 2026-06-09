import sys
import numpy as np

from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QPushButton,
    QTabWidget,
    QLabel,
    QSplitter,
)

import pyqtgraph as pg


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

