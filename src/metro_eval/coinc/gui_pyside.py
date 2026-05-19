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
        self.resize(1000, 850)


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

        data_layout.addWidget(self.file_label, 2)
        data_layout.addWidget(self.browse_btn)
        data_layout.addWidget(self.dataset_combo)
        data_layout.addWidget(self.load_btn)

        data_group.setLayout(data_layout)

        # -------------------------
        # POSTPROCESSING
        # -------------------------

        post_group = QGroupBox("Postprocessing")
        post_layout = QHBoxLayout()

        # CALIBRATION BOX

        calibration_box = QGroupBox("Calibration")
        calibration_layout = QVBoxLayout()

        calibration_layout.addWidget(QLabel("Select calibration"))

        self.calibration_combo = QComboBox()

        calibration_layout.addWidget(self.calibration_combo)

        calib_buttons = QHBoxLayout()

        self.apply_calib_btn = QPushButton("Apply")
        self.open_calib_btn = QPushButton("Open")
        self.new_calib_btn = QPushButton("New")
        self.remove_calib_btn = QPushButton("Remove")

        calib_buttons.addWidget(self.apply_calib_btn)
        calib_buttons.addWidget(self.open_calib_btn)
        calib_buttons.addWidget(self.new_calib_btn)
        calib_buttons.addWidget(self.remove_calib_btn)

        calibration_layout.addLayout(calib_buttons)

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

        post_layout.addWidget(calibration_box, 2)
        post_layout.addWidget(overlap_box, 1)

        post_group.setLayout(post_layout)

        # -------------------------
        # MASKING
        # -------------------------

        masking_group = QGroupBox("Masking")
        masking_layout = QHBoxLayout()

        # LEFT SIDE

        left_masking = QVBoxLayout()

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

        left_masking.addWidget(masking_by_column)
        left_masking.addLayout(filter_buttons)

        # RIGHT SIDE

        advanced_masking = QGroupBox("Advanced masking")
        advanced_layout = QVBoxLayout()

        advanced_layout.addStretch()
        advanced_layout.addWidget(QPushButton("Advanced options"))
        advanced_layout.addStretch()

        advanced_masking.setLayout(advanced_layout)

        masking_layout.addLayout(left_masking, 2)
        masking_layout.addWidget(advanced_masking, 1)

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
        # RIGHT PANEL
        # =========================

        right_panel = QVBoxLayout()

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

        plot1d_layout.addWidget(QComboBox(), 1, 0)
        plot1d_layout.addWidget(QLineEdit(), 1, 1)
        plot1d_layout.addWidget(QLineEdit(), 1, 2)
        plot1d_layout.addWidget(QLineEdit(), 1, 3)

        plot1d_layout.addWidget(QPushButton("Advanced settings"), 2, 0, 1, 2)
        plot1d_layout.addWidget(QPushButton("Plot"), 2, 2, 1, 2)

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

        right_panel.addWidget(status_group)
        right_panel.addWidget(plot_group)
        right_panel.addStretch()

        # =========================
        # MAIN LAYOUT
        # =========================

        main_layout.addLayout(left_panel, 2)
        main_layout.addLayout(right_panel, 1)

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
        key = self.dataset_combo.Text()
        
        for path in self.file_path:
            arr = read_coinc(path, key)
            arrays.append(arr)
        self.data_raw = np.concatenate(arrays, axis=0)
        
        self.data_current = self.data_raw
        self.data_postproc = self.data_raw
        self.data_calibrated = self.data_raw
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

    
    # ======================================
    # HELPER FUNCTIONS
    # ======================================

    def load_keys_from_file(self):

        list_of_key_lists = []
        for file in self.file_path:
            list_of_key_lists.append(get_keys(file))
        
        key_list = []
        for lst in list_of_key_lists:
            key_list += lst
            
        key_set = list(dict.fromkeys(key_list))
        
        return key_set


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



