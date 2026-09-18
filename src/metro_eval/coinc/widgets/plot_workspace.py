from PySide6.QtWidgets import QWidget, QVBoxLayout, QTabWidget
import numpy as np
from metro_eval.coinc.analysis_pages import (SignalPage, 
                                             HistogramPage, 
                                             CoincmapPage, 
                                             CalibrationViewPage, 
                                             ScanAnalysisPage)
from metro_eval.coinc.calibration_manager import Calibration




class PlotWorkspace(QWidget):
    """Tabbed workspace for visualizations generated from the active dataset.

    It manages plot tabs and keeps plotting concerns separated from the core
    workflow logic.
    """

    def __init__(self, parent=None):
        super().__init__(parent)

        self.counter = 1

        layout = QVBoxLayout(self)

        self.tabs = QTabWidget()

        self.tabs.setTabsClosable(True)
        self.tabs.tabCloseRequested.connect(
            self.tabs.removeTab
        )

        layout.addWidget(self.tabs)

    def add_page(self, page: QWidget, title: str = None):
        
        if title is None:
            title = f"Plot {self.counter}"
        index = self.tabs.addTab(
            page,
            title
        )
        self.tabs.setCurrentIndex(index)
        self.counter += 1

    def add_signal_plot(self, x, y):

        page = SignalPage(x, y)
        self.add_page(page)

    def add_histogram_plot(self, values, edges, xlabel=None, ylabel=None):
        page = HistogramPage(values, edges, xlabel=xlabel, ylabel=ylabel)
        self.add_page(page)

    def add_coincidence_map(self, data, bins=50, range=None, xlabel="first electron", ylabel="second electron", units=None):
        page = CoincmapPage(data, bins=bins, range=range, xlabel=xlabel, ylabel=ylabel, units=units)
        self.add_page(page)
        
    def add_calibration_view(self, calib:Calibration):
        page = CalibrationViewPage(calib)
        self.add_page(page)

    def add_scan_analysis(self, 
                          scan_data
                          ):

        page = ScanAnalysisPage()
        page.set_scan_data(scan_data)
        self.add_page(page)
        return page
        

    def add_random_map(self):
        data = np.random.rand(10000, 2)
        self.add_coincidence_map(data)

    def create_random_signal(self):

        x = np.arange(1000)
        y = np.random.randn(1000).cumsum()

        self.add_signal_plot(x, y)
