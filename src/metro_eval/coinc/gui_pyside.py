import os
import sys
from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QFileDialog,
    QGroupBox,
    QLabel,
    QPushButton,
    QComboBox,
    QLineEdit,
    QTextEdit,
    QListWidget,
    QCheckBox,
    QScrollArea,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QSizePolicy,
    QFrame,
)
import numpy as np

from metro_eval.coinc.file_handler import get_keys, read_coinc
from metro_eval.coinc.plot_functions import plot_1D, hist_1D
from metro_eval.coinc.analysis_pages import SignalPage, HistogramPage

class FilterRow(QWidget):
    """
    Reusable filter row widget
    """

    def __init__(self):
        super().__init__()

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.enabled = QCheckBox()
        self.operator = QComboBox()
        self.operator.addItems(["=", "!=", ">", "<", ">=", "<="])

        self.column = QLineEdit()
        self.column.setPlaceholderText("Column")

        self.value = QLineEdit()
        self.value.setPlaceholderText("Value")

        self.remove_btn = QPushButton("Remove")
        self.remove_btn.setFixedWidth(80)

        layout.addWidget(self.enabled)
        layout.addWidget(self.operator)
        layout.addWidget(self.column)
        layout.addWidget(self.value)
        layout.addWidget(self.remove_btn)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Analysis GUI")
        self.resize(1400, 750)


        # =========================
        # STORING VARIABLES
        # =========================

        self.file_path = None

        self.data_raw = None
        self.data_calibrated = None
        self.data_current = None

        # =========================
        # CENTRAL WIDGET
        # =========================

        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)

        # =========================
        # LEFT PANEL
        # =========================

        left_panel = QVBoxLayout()

        # -------------------------
        # DATA SELECTION
        # -------------------------

        data_group = QGroupBox("Data selection")
        data_layout = QHBoxLayout()

        self.file_label = QLabel("No file selected")

        self.browse_btn = QPushButton("Browse File")
        self.browse_btn.clicked.connect(self.browse_file)

        self.dataset_combo = QComboBox()

        self.load_btn = QPushButton("Load data")
        self.load_btn.clicked.connect(self.load_data)

        data_layout.addWidget(self.file_label, 2)
        data_layout.addWidget(self.browse_btn)
        data_layout.addWidget(self.dataset_combo)
        data_layout.addWidget(self.load_btn)

        data_group.setLayout(data_layout)

        # -------------------------
        # POSTPROCESSING
        # -------------------------

        post_group = QGroupBox("Postprocessing")
        post_layout = QVBoxLayout()

        # CALIBRATION BOX

        calibration_box = QGroupBox("Calibration")
        calibration_layout = QGridLayout()

        self.calibration_combo = QComboBox()
        self.calibration_combo.addItems(["Select calibration"])
        self.calibration_combo.setCurrentIndex(0)

        calibration_layout.addWidget(self.calibration_combo,0,0)

        calib_buttons = QGridLayout()

        self.apply_calib_btn = QPushButton("Apply")
        self.open_calib_btn = QPushButton("Open")
        self.new_calib_btn = QPushButton("New")
        self.remove_calib_btn = QPushButton("Remove")

        calib_buttons.addWidget(self.apply_calib_btn, 0, 0)
        calib_buttons.addWidget(self.open_calib_btn, 0, 1)
        calib_buttons.addWidget(self.new_calib_btn, 1, 0)
        calib_buttons.addWidget(self.remove_calib_btn, 1, 1)

        calibration_layout.addLayout(calib_buttons, 1,0)

        calibration_box.setLayout(calibration_layout)

        # OVERLAP BOX

        overlap_box = QGroupBox("Overlap bunches (manual)")
        overlap_layout = QGridLayout()

        overlap_layout.addWidget(QLabel("Repetition time (ns)"), 0, 0)
        overlap_layout.addWidget(QLineEdit(), 0, 1)

        overlap_layout.addWidget(QLabel("ROI_first (ns)"), 1, 0)
        overlap_layout.addWidget(QLineEdit(), 1, 1)
        overlap_layout.addWidget(QLineEdit(), 1, 2)

        overlap_layout.addWidget(QLabel("ROI_last (ns)"), 2, 0)
        overlap_layout.addWidget(QLineEdit(), 2, 1)
        overlap_layout.addWidget(QLineEdit(), 2, 2)

        overlap_layout.addWidget(QPushButton("Apply overlap"), 3, 0)
        overlap_layout.addWidget(QPushButton("Reset overlap"), 3, 1)

        overlap_box.setLayout(overlap_layout)
        
        post_layout.addWidget(overlap_box, 1)
        post_layout.addWidget(calibration_box, 1)

        post_group.setLayout(post_layout)

        # -------------------------
        # MASKING
        # -------------------------

        masking_group = QGroupBox("Masking")
        masking_layout = QVBoxLayout()

        # LEFT SIDE

        masking_normal = QVBoxLayout()

        masking_by_column = QGroupBox("Masking by column")
        masking_by_column_layout = QVBoxLayout()

        self.filter_container = QVBoxLayout()

        self.add_filter()

        self.add_filter_btn = QPushButton("Add Filter")
        self.add_filter_btn.clicked.connect(self.add_filter)

        masking_by_column_layout.addLayout(self.filter_container)
        masking_by_column_layout.addWidget(self.add_filter_btn)

        masking_by_column.setLayout(masking_by_column_layout)

        filter_buttons = QHBoxLayout()

        self.apply_filters_btn = QPushButton("Apply filters")
        self.remove_filters_btn = QPushButton("Remove filters")

        filter_buttons.addWidget(self.apply_filters_btn)
        filter_buttons.addWidget(self.remove_filters_btn)

        masking_normal.addWidget(masking_by_column)
        masking_normal.addLayout(filter_buttons)

        

        masking_layout.addLayout(masking_normal, 2)

        masking_group.setLayout(masking_layout)

        # -------------------------
        # EXIT BUTTON
        # -------------------------

        bottom_layout = QHBoxLayout()
        bottom_layout.addStretch()

        self.exit_btn = QPushButton("Exit")
        self.exit_btn.clicked.connect(self.close)

        bottom_layout.addWidget(self.exit_btn)
        bottom_layout.addStretch()

        # -------------------------
        # ADD TO LEFT PANEL
        # -------------------------

        left_panel.addWidget(data_group)
        left_panel.addWidget(post_group)
        left_panel.addWidget(masking_group)
        left_panel.addStretch()
        left_panel.addLayout(bottom_layout)

        # =========================
        # CENTER PANEL
        # =========================

        center_panel = QVBoxLayout()

        # STATUS

        status_group = QGroupBox("Data status")
        status_layout = QGridLayout()

        labels = [
            "File:",
            "Coincidence:",
            "Postprocessing:",
            "bunch overlap:",
            "Calibrated:",
        ]

        for i, text in enumerate(labels):
            status_layout.addWidget(QLabel(text), i, 0)
            status_layout.addWidget(QLabel(""), i, 1)

        status_group.setLayout(status_layout)

        # PLOT SETTINGS

        plot_group = QGroupBox("Plot settings")
        plot_layout = QVBoxLayout()

        # 1D PLOT

        plot1d_group = QGroupBox("1D Spectra")
        plot1d_layout = QGridLayout()

        plot1d_layout.addWidget(QLabel("Column:"), 0, 0)
        plot1d_layout.addWidget(QLabel("range:"), 0, 1)
        plot1d_layout.addWidget(QLabel("bins:"), 0, 3)

        self.plot1d_column_combo = QComboBox()
        self.plot1d_column_combo.setCurrentIndex(0)
        plot1d_layout.addWidget(self.plot1d_column_combo, 1, 0)
        self.plot1d_lineedits = {}
        self.plot1d_lineedits['range_lo'] = QLineEdit()
        self.plot1d_lineedits['range_lo'].setPlaceholderText("0")
        plot1d_layout.addWidget(self.plot1d_lineedits['range_lo'], 1, 1)
        self.plot1d_lineedits['range_hi'] = QLineEdit()
        self.plot1d_lineedits['range_hi'].setPlaceholderText("400")
        plot1d_layout.addWidget(self.plot1d_lineedits['range_hi'], 1, 2)
        self.plot1d_lineedits['bins'] = QLineEdit()
        self.plot1d_lineedits['bins'].setPlaceholderText("200")
        plot1d_layout.addWidget(self.plot1d_lineedits['bins'], 1, 3)


        plot1d_layout.addWidget(QPushButton("Advanced settings"), 2, 0, 1, 2)
        
        self.plot_1d_btn = QPushButton("Plot")
        self.plot_1d_btn.clicked.connect(self.plot_1d)
        plot1d_layout.addWidget(self.plot_1d_btn, 2, 2, 1, 2)

        plot1d_group.setLayout(plot1d_layout)

        # 2D PLOT

        plot2d_group = QGroupBox("2D coincidence map")
        plot2d_layout = QGridLayout()

        plot2d_layout.addWidget(QLabel("Columns:"), 0, 0)
        plot2d_layout.addWidget(QLabel("ranges:"), 0, 1)
        plot2d_layout.addWidget(QLabel("bins:"), 0, 3)

        plot2d_layout.addWidget(QComboBox(), 1, 0)
        plot2d_layout.addWidget(QLineEdit(), 1, 1)
        plot2d_layout.addWidget(QLineEdit(), 1, 2)
        plot2d_layout.addWidget(QLineEdit(), 1, 3)

        plot2d_layout.addWidget(QComboBox(), 2, 0)
        plot2d_layout.addWidget(QLineEdit(), 2, 1)
        plot2d_layout.addWidget(QLineEdit(), 2, 2)
        plot2d_layout.addWidget(QLineEdit(), 2, 3)

        plot2d_layout.addWidget(QPushButton("Advanced settings"), 3, 0, 1, 2)
        plot2d_layout.addWidget(QPushButton("Plot"), 3, 2, 1, 2)

        plot2d_group.setLayout(plot2d_layout)

        plot_layout.addWidget(plot1d_group)
        plot_layout.addWidget(plot2d_group)

        plot_group.setLayout(plot_layout)

        center_panel.addWidget(status_group)
        center_panel.addWidget(plot_group)
        center_panel.addStretch()

        # =========================
        # RIGHT PANEL       
        # =========================

        right_panel = QVBoxLayout()

        self.plot_workspace = PlotWorkspace()

        button = QPushButton("Create Plot")
        button.clicked.connect(
            self.plot_workspace.create_random_signal
        )

        right_panel.addWidget(button)
        right_panel.addWidget(self.plot_workspace)


        # =========================
        # MAIN LAYOUT
        # =========================

        main_layout.addLayout(left_panel, 1)
        main_layout.addLayout(center_panel, 1)
        main_layout.addLayout(right_panel, 2)

    # ======================================
    # BUTTON FUNCTIONS
    # ======================================

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileNames(
            self,
            "Select file",
            "",
            "All Files (*.*)"
        )

        if file_path:
            self.file_path = list(file_path)
            label=""
            for file in file_path:
                label += os.path.basename(file)+", "
            label = label[:-2]
            self.file_label.setText(label)

            # Populate the dropdown with the keys from the files
            keys=self.load_keys_from_file()
            self.dataset_combo.clear()
            self.dataset_combo.addItems(keys)

    def load_data(self):
        '''
        
        Read the data of the selected files, from the selected coincidence
        key into self.data
        Also store it in self.current_data and self.data_postproc
        
        '''
        arrays = []
        key = self.dataset_combo.currentText()
        
        for path in self.file_path:
            arr = read_coinc(path, key)
            arrays.append(arr)
        self.data_raw = np.concatenate(arrays, axis=0)
        
        self.data_current = self.data_raw
        self.data_postproc = self.data_raw
        self.data_calibrated = self.data_raw

        self.on_array_change()

        '''
        label_old = self.file_label.text()
        if "Loaded" in self.file_label.text():
            label = label_old
        elif label_old == self.file_label.text():
            label = f"Select a file to load {key} data"
        else:
            label = f"Loaded {key} data from {label_old}"
        self.file_label.setText(label)
        '''
        print(f"Loaded data with shape {self.data_raw.shape} from files: {self.file_path}")

        # self.update_plot_columns()
    

    def add_filter(self):
        row = FilterRow()

        row.remove_btn.clicked.connect(
            lambda: self.remove_filter(row)
        )

        self.filter_container.addWidget(row)

    def remove_filter(self, row):
        row.setParent(None)
        row.deleteLater()

    def plot_1d(self):
        '''
        In construction from gui.py

        Take the information from self.plot_settings_1d and plot a 1D histogram
        using plot_functions.plot_1D

        '''
        print("Plotting 1D spectrum with settings:")
        plot_settings_1D = {}
    
        for key, widget in self.plot1d_lineedits.items():
            value = widget.text()
            plot_settings_1D[key] = value
        
        try:
            plot_settings_1D['bins'] = int(plot_settings_1D['bins'])

        except ValueError:
            print("Invalid input for bins, using default value of 100")
            plot_settings_1D['bins'] = 100
        try:
            plot_settings_1D['range_lo'] = float(plot_settings_1D['range_lo'])
        except ValueError:
            print("Invalid input for range_lo, using default value of 0")
            plot_settings_1D['range_lo'] = 0
        try:
            plot_settings_1D['range_hi'] = float(plot_settings_1D['range_hi'])
        except ValueError:
            print("Invalid input for range_hi, using default value of 200")
            plot_settings_1D['range_hi'] = 200
        try:
            plot_settings_1D['column'] = int(self.plot1d_column_combo.currentText())-1
        except ValueError:
            print("Invalid input for column, using default value of 1")
            plot_settings_1D['column'] = 1

        if self.data_current is None:
            print("No data loaded, cannot plot")
            return
    
        column = plot_settings_1D['column']
        bins = plot_settings_1D['bins']
        range_lo = plot_settings_1D['range_lo'] 
        range_hi = plot_settings_1D['range_hi']

        #TODO
        ## Here, we need to add the extraction for the other values in the
        ## self.plot_settings_1d dictionary, which can be added in the 
        ## advanced options later
        
        hist_kwargs={}
        plot_kwargs={"drawstyle": 'steps-mid'}

        if True:
            xlabel = f"Particle {column+1} TOF (ns)"
            ylabel = "Intensity (arb. units)"
        values, edges = hist_1D(self.data_current, column, range=(range_lo, range_hi), bins=bins, **hist_kwargs)
        self.plot_workspace.add_histogram_plot(values, edges[:-1], xlabel=xlabel, ylabel=ylabel, plot_kwargs=plot_kwargs)
        
    
    # ======================================
    # HELPER FUNCTIONS
    # ======================================

    def on_array_change(self):
        '''
        This function should be called whenever self.data_current is updated, to
        update the status labels, the plot column dropdowns, ...

        '''
        # Update status labels
        if self.data_current is None:
            return
        
        if self.data_current is self.data_raw:
            post_status = "No postprocessing"
            calib_status = "Not calibrated"
        else:
            post_status = "Postprocessed"
            if self.data_current is self.data_calibrated:
                calib_status = "Calibrated"
            else:
                calib_status = "Not calibrated"
        
        self.plot1d_column_combo.clear()
        if self.data_current is not None:
            num_columns = self.data_current.shape[1]
            column_names = [f"{i+1}" for i in range(num_columns)]
            self.plot1d_column_combo.addItems(column_names)
            self.plot1d_column_combo.setCurrentIndex(0)
        
        '''
        #TODO
        Work in progress: we need to add the update for the status labels here, but we first need to 
        decide how to store the information about the current state of the data (raw, postprocessed, calibrated, ...) 
        in a way that is easy to check in this function and in the other functions that need to know about it 
        (e.g. the plot functions, which might want to change their behavior based on the state of the data)
        '''


    def load_keys_from_file(self):

        list_of_key_lists = []
        for file in self.file_path:
            list_of_key_lists.append(get_keys(file))
        
        key_list = []
        for lst in list_of_key_lists:
            key_list += lst
            
        key_set = list(dict.fromkeys(key_list))
        
        return key_set


import numpy as np

from PySide6.QtWidgets import (
    QWidget,
    QVBoxLayout,
    QTabWidget,
)



class PlotWorkspace(QWidget):
    """
    Reusable plotting workspace.

    Can be embedded into any MainWindow.
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

    def add_histogram_plot(self, values, edges, xlabel="", ylabel="", plot_kwargs=None):
        page = HistogramPage(values, edges, xlabel=xlabel, ylabel=ylabel, plot_kwargs=plot_kwargs)
        self.add_page(page)

    def create_random_signal(self):

        x = np.arange(1000)
        y = np.random.randn(1000).cumsum()

        self.add_signal_plot(x, y)


# ==========================================
# RUN APPLICATION
# ==========================================

def start(blocking: bool = True):
        app = QApplication.instance()
        if app is None:
            app = QApplication([])
    
        window = MainWindow()
        window.show()
    
        if blocking:
            sys.exit(app.exec())
        return window


if __name__ == "__main__":
    start()



