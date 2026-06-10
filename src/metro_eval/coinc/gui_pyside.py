import os
import sys
from PySide6.QtCore import Qt, Signal, QLocale
from PySide6.QtWidgets import (
    QApplication,
    QFormLayout,
    QLayout,
    QMainWindow,
    QSizePolicy,
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
    QButtonGroup,
    QTabWidget,
    QTableWidget,
    QHeaderView,
    QMessageBox,
    QRadioButton,
    QDoubleSpinBox,
    QSpinBox,
)

QLocale.setDefault(QLocale(QLocale.C))  # "C" locale = dot decimal

import numpy as np

from metro_eval.coinc.file_handler import get_keys, read_coinc
from metro_eval.coinc.plot_functions import hist_1D
from metro_eval.coinc.analysis_pages import CoincmapPage, SignalPage, HistogramPage
from metro_eval.coinc.postprocessing import overlap


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

        self.status = {
            "File(s)": "N/A",
            "Coincidence": "N/A",
            "Bunch overlap": "N/A",
            "Calibrated": "N/A",
            "Masks applied": "N/A",
            "Data shape (raw)": "N/A",
            "Data shape (current)": "N/A",
        }
        self.pinned_keys = [
            "E", "EE", "EEE", "EEEE",
            "EP", "EEP", "P", "PP"
        ]

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

        left_panel_width = 400
        data_group = self._add_data_selection_group()
        data_group.setFixedWidth(left_panel_width)
        post_group = self._add_postprocessing_group()
        post_group.setFixedWidth(left_panel_width)
        masking_group = self._add_masking_group()
        masking_group.setFixedWidth(left_panel_width)

        left_panel.addWidget(data_group)
        left_panel.addWidget(post_group)
        left_panel.addWidget(masking_group)
        left_panel.addStretch()

        # =========================
        # CENTER PANEL
        # =========================

        center_panel = QVBoxLayout()

        center_panel_width = 450
        status_group = self._add_status_group()
        status_group.setFixedWidth(center_panel_width)
        plot_group = self._add_plot_settings_group()
        plot_group.setFixedWidth(center_panel_width)


        center_panel.addWidget(status_group)
        center_panel.addWidget(plot_group)
        center_panel.addStretch()

        # =========================
        # RIGHT PANEL       
        # =========================

        right_panel = QVBoxLayout()

        self.plot_workspace = PlotWorkspace()


        test_btn_1d = QPushButton("Create Plot")
        test_btn_1d.clicked.connect(
            self.plot_workspace.create_random_signal
        )
        test_btn_2d = QPushButton("Create 2D Plot")
        test_btn_2d.clicked.connect(
            self.plot_workspace.add_random_map
        )

        self.plot_workspace.setMinimumWidth(700)

        right_panel.addWidget(test_btn_1d)
        right_panel.addWidget(test_btn_2d)
        right_panel.addWidget(self.plot_workspace)


        # =========================
        # MAIN LAYOUT
        # =========================

        main_layout.addLayout(left_panel)
        main_layout.addLayout(center_panel)
        main_layout.addLayout(right_panel)

    # =========================
    # GUI LAYOUT
    # =========================

    def _add_data_selection_group(self):
        ''' 
        Reusable function to create the data selection group, which contains
        the file selection, the dataset selection and the load button
        '''

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
        return data_group


    def _add_postprocessing_group(self):
        '''
        Reusable function to create the postprocessing group, which contains
        the calibration and the overlap options
        '''

        post_group = QGroupBox("Postprocessing")
        post_layout = QVBoxLayout()

        choice_radio_group = QGroupBox()
        choice_radio_layout = QHBoxLayout()
        self.radio_off = QRadioButton("No Postprocessing")
        self.radio_off.setChecked(True)
        self.radio_off.toggled.connect(self.enable_postprocessing)
        self.radio_manual = QRadioButton("Manual")
        self.radio_manual.toggled.connect(self.enable_postprocessing)
        self.radio_automatic = QRadioButton("automatic")
        self.radio_automatic.toggled.connect(self.enable_postprocessing)
        choice_radio_layout.addWidget(self.radio_off)
        choice_radio_layout.addWidget(self.radio_manual)
        choice_radio_layout.addWidget(self.radio_automatic)


        choice_radio_group.setLayout(choice_radio_layout)
        
        # CALIBRATION BOX

        self.calibration_box = QGroupBox("Calibration")
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

        self.calibration_box.setLayout(calibration_layout)

        # OVERLAP BOX

        self.overlap_box = QGroupBox("Overlap bunches (manual)")
        overlap_layout = QGridLayout()

        self.overlap_lineEdit = {}
        overlap_layout.addWidget(QLabel("Repetition time (ns)"), 0, 0)
        self.overlap_lineEdit["reptime"] = QLineEdit()
        self.overlap_lineEdit['reptime'].setText("318.76") #setPlaceholderText("e.g. 318.76")
        overlap_layout.addWidget(self.overlap_lineEdit['reptime'], 0, 1)

        overlap_layout.addWidget(QLabel("ROI_first (ns)"), 1, 0)
        self.overlap_lineEdit['roi_first_min'] = QLineEdit()
        self.overlap_lineEdit['roi_first_max'] = QLineEdit()
        overlap_layout.addWidget(self.overlap_lineEdit['roi_first_min'], 1, 1)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_first_max'], 1, 2)


        overlap_layout.addWidget(QLabel("ROI_last (ns)"), 2, 0)
        self.overlap_lineEdit['roi_last_min'] = QLineEdit()
        self.overlap_lineEdit['roi_last_max'] = QLineEdit()
        overlap_layout.addWidget(self.overlap_lineEdit['roi_last_min'], 2, 1)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_last_max'], 2, 2)

        apply_overlap_btn = QPushButton("Apply overlap")
        reset_overlap_btn = QPushButton("Reset  overlap")

        apply_overlap_btn.clicked.connect(self.apply_overlap)

        overlap_layout.addWidget(apply_overlap_btn, 3, 0)
        overlap_layout.addWidget(reset_overlap_btn, 3, 1)

        self.overlap_box.setLayout(overlap_layout)
        
        post_layout.addWidget(choice_radio_group, 1)
        post_layout.addWidget(self.overlap_box, 1)
        post_layout.addWidget(self.calibration_box, 1)

        post_group.setLayout(post_layout)

        self.enable_postprocessing()
        return post_group
        
    def _add_masking_group(self):
        '''
        Reusable function to create the masking group, which contains
        the options for masking the data, which can be by column or by ... other options in the future
        '''

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
        return masking_group

    def _add_status_group(self):
        '''
        #TODO
        Display the status of the data. Implementation necessary.
        '''
        self.status_group = QGroupBox("Data status")
        self.status_widget = QWidget()
        self.status_layout = QFormLayout(self.status_widget)

        self.status_labels = {}

        for key in self.status.keys():
            label = QLabel("N/A")
            self.status_labels[key] = label
            self.status_layout.addRow(f"{key}:", label)

        self.status_widget.setLayout(self.status_layout)
        self.status_group.setLayout(QVBoxLayout())
        self.status_group.layout().addWidget(self.status_widget)

        return self.status_group
    
    def _add_plot_settings_group(self):

        plot_group = QGroupBox("Plot settings")
        plot_layout = QVBoxLayout()

        self.plot_widget = PlotDefinitionWidget()
        self.plot_widget.histogram_requested.connect(self.handle_histogram_request)
        self.plot_widget.xy_requested.connect(self.handle_xy_request)

        plot_layout.addWidget(self.plot_widget)

        plot_group.setLayout(plot_layout)

        return plot_group
    
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
        self.status_reset_upon_loading()
        self.set_status("File(s)",self.file_label.text())
        self.set_status("Coincidence", key)
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
    


    def apply_overlap(self):
        
        overlap_params = {}
        for key in self.overlap_lineEdit.keys():
            try:
                overlap_params[key] = float(self.overlap_lineEdit[key].text().strip())
            except ValueError:
                print("Select values for all boxes!")
                
        
        roi_first = (overlap_params['roi_first_min'], overlap_params['roi_first_max'])
        roi_last = (overlap_params['roi_last_min'], overlap_params['roi_last_max'])
        e_amount, p_amount = self.EP_number_from_string(self.status['Coincidence'])
        
        self.data_postproc = overlap(self.data_raw, overlap_params['reptime'], 
                                     roi_first, roi_last, nPhotons=p_amount)
        self.data_current = self.data_postproc
        
        self.set_status("Bunch overlap", True)
        self.set_status("Masks applied", "N/A")
        self.on_array_change()


    def status_reset_upon_loading(self):
        for key in self.status.keys():
            if key == "File(s)" or key == "Coincidence":
                self.set_status(key, "N/A")


    def add_filter(self):
        row = FilterRow()

        row.remove_btn.clicked.connect(
            lambda: self.remove_filter(row)
        )

        self.filter_container.addWidget(row)

    def remove_filter(self, row):
        row.setParent(None)
        row.deleteLater()

    

    def handle_histogram_request(self, request):
        print("Received histogram request:", request)
        col_idx = request['column']-1
        bins = request['bins']
        range_lo = request['min']
        range_hi = request['max']

        if self.data_current is None:
            print("No data loaded, cannot plot")
            return

        x, y = hist_1D(self.data_current, col_idx, range=(range_lo, range_hi), bins=bins)
        self.plot_workspace.add_histogram_plot(x, y[:-1], xlabel=f"Particle {col_idx+1}", ylabel="Intensity")

    def handle_xy_request(self, request):

        if self.data_current is None:
            print("No data loaded, cannot plot")
            return

        col_idx = (request['x']['column']-1, request['y']['column']-1)
        bins = (request['x']['bins'], request['y']['bins'])
        range_1 = (request['x']['min'], request['x']['max'])   
        range_2 = (request['y']['min'], request['y']['max'])
        units = "ns"  #TODO change the units for the case of calibrated data
        
        
        self.plot_workspace.add_coincidence_map(self.data_current[:, [col_idx[0], col_idx[1]]],
                                                bins=bins, range=(range_1, range_2), 
                                                units=units)



    # ======================================
    # HELPER FUNCTIONS
    # ======================================

    def enable_postprocessing(self):
        if self.radio_off.isChecked():
            self.overlap_box.setEnabled(False)
            self.calibration_box.setEnabled(False)
            self.open_calib_btn.setEnabled(True)
        elif self.radio_manual.isChecked():
            self.overlap_box.setEnabled(True)
            self.calibration_box.setEnabled(False)
        elif self.radio_automatic.isChecked():
            self.overlap_box.setEnabled(False)
            self.calibration_box.setEnabled(True)
        


    def on_array_change(self):
        '''
        This function should be called whenever self.data_current is updated, to
        update the status labels, the plot column dropdowns, ...

        '''
        # Update status labels
        if self.data_current is None:
            return
        self.set_status("Data shape (raw)", self.data_raw.shape)
        self.set_status("Data shape (current)", self.data_current.shape)
        
        '''
        #TODO
        Work in progress: we need to add the update for the status labels here, but we first need to 
        decide how to store the information about the current state of the data (raw, postprocessed, calibrated, ...) 
        in a way that is easy to check in this function and in the other functions that need to know about it 
        (e.g. the plot functions, which might want to change their behavior based on the state of the data)
        '''

    def set_status(self, key, value):
        '''
        Transfers the contents of status to the display
        '''
        self.status[key] = value
        self.status_labels[key].setText(str(value))

    def load_keys_from_file(self):

        key_list = []

        for file in self.file_path:
            key_list.extend(get_keys(file))

        unique_keys = list(dict.fromkeys(key_list))  # preserves first-seen order

        pinned = []
        rest = []

        pinned_set = set(self.pinned_keys)

        for k in unique_keys:
            if k in pinned_set:
                pinned.append(k)
            else:
                rest.append(k)

        rest.sort()  # alphabetical

        return pinned + rest
    
    @staticmethod
    def EP_number_from_string(string):
        if string.isalpha():
            e_amount = string.count("E")
            p_amount = string.count("P")
        else:
            e_index = string.find("E")
            p_index = string.find("P")
            if e_index != -1:
                try:
                    e_amount = int(string[:e_index])
                    p_amount = int(string[e_index+1:p_index])
                except ValueError:
                    print(f"Warning: Could not evaluate {string}.")
        return e_amount, p_amount

    

class PlotDefinitionWidget(QWidget):

    histogram_requested = Signal(dict)
    xy_requested = Signal(dict)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.table = QTableWidget(0, 7)

        self.table.setHorizontalHeaderLabels(
            [
                "X",
                "Y",
                "Column",
                "Min",
                "Max",
                "Bins",
                "Plot",
            ]
        )
        
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )

        header = self.table.horizontalHeader()

        header.setSectionResizeMode(0, QHeaderView.Fixed)        # X radio
        header.setSectionResizeMode(1, QHeaderView.Fixed)        # Y radio
        header.setSectionResizeMode(2, QHeaderView.Stretch)                 # Column
        header.setSectionResizeMode(3, QHeaderView.Fixed)        # Min
        header.setSectionResizeMode(4, QHeaderView.Fixed)        # Max
        header.setSectionResizeMode(5, QHeaderView.Fixed)        # Bins
        header.setSectionResizeMode(6, QHeaderView.Fixed)        # Button

        self.table.setColumnWidth(0, 25)   # X
        self.table.setColumnWidth(1, 25)   # Y
        self.table.setColumnWidth(2, 120)  # Column (will still stretch visually)
        self.table.setColumnWidth(3, 70)   # Min
        self.table.setColumnWidth(4, 70)   # Max
        self.table.setColumnWidth(5, 60)   # Bins
        self.table.setColumnWidth(6, 60)   # Plot

        # Create button groups for X and Y radio buttons
        self.x_group = QButtonGroup(self)
        self.y_group = QButtonGroup(self)
        self.x_group.setExclusive(True)
        self.y_group.setExclusive(True)


        self.add_button = QPushButton("Add Row")
        self.remove_button = QPushButton("Remove Selected Rows")
        self.plot_xy_button = QPushButton("Plot XY")

        button_layout = QHBoxLayout()
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.remove_button)
        button_layout.addStretch()
        button_layout.addWidget(self.plot_xy_button)

        layout = QVBoxLayout(self)
        layout.addWidget(self.table)
        layout.addLayout(button_layout)

        self.add_button.clicked.connect(self.add_row)
        self.remove_button.clicked.connect(
            self.remove_selected_rows
        )
        self.plot_xy_button.clicked.connect(
            self.request_xy_plot
        )

        self.add_row()

    # --------------------------------------------------
    # Row management
    # --------------------------------------------------

    def add_row(self):

        row = self.table.rowCount()
        self.table.insertRow(row)

        x_radio = QRadioButton()
        x_radio.setText("")
        x_radio.setFixedSize(20,20)

        y_radio = QRadioButton()
        y_radio.setText("")
        y_radio.setFixedSize(20,20)

        self.x_group.addButton(x_radio)
        self.y_group.addButton(y_radio)
        
        
        col_combo = QComboBox()
        col_combo.addItems([f"{i+1}" for i in range(10)])



        min_dsb = QDoubleSpinBox()
        min_dsb.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        min_dsb.setRange(-1e10, 1e10)
        min_dsb.setDecimals(1)
        min_dsb.setValue(0)
        min_dsb.setButtonSymbols(QDoubleSpinBox.NoButtons)

        max_dsb = QDoubleSpinBox()
        max_dsb.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        max_dsb.setRange(-1e10, 1e10)
        max_dsb.setDecimals(1)
        max_dsb.setValue(400)
        max_dsb.setButtonSymbols(QDoubleSpinBox.NoButtons)

        bins_sb = QSpinBox()
        bins_sb.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
        bins_sb.setRange(1, 10000)
        bins_sb.setValue(100)
        bins_sb.setButtonSymbols(QSpinBox.NoButtons)

        plot_button = QPushButton("Plot")

        plot_button.clicked.connect(
            self.histogram_button_clicked
        )

        self.table.setCellWidget(row, 0, x_radio)
        self.table.setCellWidget(row, 1, y_radio)
        self.table.setCellWidget(row, 2, col_combo)
        self.table.setCellWidget(row, 3, min_dsb)
        self.table.setCellWidget(row, 4, max_dsb)
        self.table.setCellWidget(row, 5, bins_sb)
        self.table.setCellWidget(row, 6, plot_button)

    def remove_selected_rows(self):

        rows = sorted(
            {idx.row() for idx in self.table.selectedIndexes()},
            reverse=True,
        )

        for row in rows:
            self.table.removeRow(row)

    # --------------------------------------------------
    # Radio button handling
    # --------------------------------------------------


    def get_selected_x_row(self):

        for row in range(self.table.rowCount()):

            if self.table.cellWidget(row, 0).isChecked():
                return row

        return None

    def get_selected_y_row(self):

        for row in range(self.table.rowCount()):

            if self.table.cellWidget(row, 1).isChecked():
                return row

        return None

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def get_row_definition(self, row):

        column = (
            self.table.cellWidget(row, 2)
            .currentText()
            .strip()
        )

        min_text = (
            self.table.cellWidget(row, 3)
            .text()
            .strip()
        )

        max_text = (
            self.table.cellWidget(row, 4)
            .text()
            .strip()
        )

        bins_text = (
            self.table.cellWidget(row, 5)
            .text()
            .strip()
        )

        try:
            bins = int(bins_text)
        except ValueError:
            bins = 100

        return {
            "column": int(column),
            "min": float(min_text)
            if min_text
            else None,
            "max": float(max_text)
            if max_text
            else None,
            "bins": bins
        }

    # --------------------------------------------------
    # Histogram requests
    # --------------------------------------------------

    def histogram_button_clicked(self):

        button = self.sender()

        for row in range(self.table.rowCount()):

            if self.table.cellWidget(row, 6) is button:

                request = self.get_row_definition(
                    row
                )
                request['type'] = 'histogram'

                self.histogram_requested.emit(
                    request
                )

                return

    # --------------------------------------------------
    # XY requests
    # --------------------------------------------------

    def request_xy_plot(self):

        x_row = self.get_selected_x_row()
        y_row = self.get_selected_y_row()

        if x_row is None:

            QMessageBox.warning(
                self,
                "Selection Error",
                "Select an X row."
            )
            return

        if y_row is None:

            QMessageBox.warning(
                self,
                "Selection Error",
                "Select a Y row."
            )
            return

        request = {
            "x": self.get_row_definition(
                x_row
            ),
            "y": self.get_row_definition(
                y_row
            ),
        }
        request['type'] = 'xy'

        self.xy_requested.emit(request)


class PlotWorkspace(QWidget):
    """
    Reusable plotting workspace.
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

    def add_histogram_plot(self, values, edges, xlabel="", ylabel=""):
        page = HistogramPage(values, edges, xlabel=xlabel, ylabel=ylabel)
        self.add_page(page)

    def add_coincidence_map(self, data, bins=50, range=None, xlabel="first electron", ylabel="second electron", units=None):
        page = CoincmapPage(data, bins=bins, range=range, xlabel=xlabel, ylabel=ylabel, units=units)
        self.add_page(page)

    def add_random_map(self):
        data = np.random.rand(10000, 2)
        self.add_coincidence_map(data)

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



