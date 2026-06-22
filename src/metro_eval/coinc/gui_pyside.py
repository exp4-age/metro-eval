import os
import sys
from PySide6.QtCore import Qt, Signal, QLocale, QObject
from PySide6.QtWidgets import (
    QApplication,
    QTableWidgetItem,
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
    QSplitter,
)

QLocale.setDefault(QLocale(QLocale.C))  # "C" locale = dot decimal

import numpy as np
import logging

from metro_eval.coinc.file_handler import get_keys, read_coinc
from metro_eval.coinc.plot_functions import hist_1D
from metro_eval.coinc.analysis_pages import CoincmapPage, SignalPage, HistogramPage, CalibrationViewPage
from metro_eval.coinc.postprocessing import overlap
from metro_eval.coinc.mask_functions import mask_by_column
from metro_eval.coinc.log_widget import setup_gui_logging, LogWidget
from metro_eval.coinc.calibration_manager import list_calibrations, load_calibration, Calibration



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

        self.calibration_editor=None

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

        center_panel_width = 370
        status_group = self._add_status_group()
        status_group.setFixedWidth(center_panel_width)
        plot_group = self._add_plot_settings_group()
        plot_group.setFixedWidth(center_panel_width)

        # Logging
        self.log_widget = LogWidget()
        setup_gui_logging(self.log_widget)
        self.logger = logging.getLogger(__name__)
        self.log_widget.setFixedWidth(center_panel_width)

        center_panel.addWidget(status_group)
        center_panel.addWidget(plot_group)
        center_panel.addWidget(self.log_widget)
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

        self.logger.info("Application started")

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
        self.calibration_combo.addItems([file.name for file in list_calibrations()])
        self.calibration_combo.setCurrentIndex(0)

        self.new_calib_btn = QPushButton("New")
        self.new_calib_btn.clicked.connect(self.open_new_calibration_in_editor)

        calibration_layout.addWidget(self.calibration_combo,0,0)
        calibration_layout.addWidget(self.new_calib_btn,0,1)

        calib_buttons = QGridLayout()


        self.apply_calib_btn = QPushButton("Apply")
        self.open_calib_btn = QPushButton("Open")
        self.edit_calib_btn = QPushButton("Edit")
        self.remove_calib_btn = QPushButton("Remove")

        self.open_calib_btn.clicked.connect(self.open_existing_calibration)
        self.edit_calib_btn.clicked.connect(self.edit_calibration_in_editor)
        self.apply_calib_btn.clicked.connect(self.apply_calibration)

        calib_buttons.addWidget(self.apply_calib_btn, 0, 0)
        calib_buttons.addWidget(self.open_calib_btn, 0, 1)
        calib_buttons.addWidget(self.edit_calib_btn, 1, 0)
        calib_buttons.addWidget(self.remove_calib_btn, 1, 1)

        calibration_layout.addLayout(calib_buttons, 1,0,1,2)

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

        self.masking_wdgt = MaskSelectionWidget()
        masking_layout.addWidget(self.masking_wdgt)
        
        self.masking_wdgt.masking_requested.connect(self.handle_masking_request)


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
        self.logger.info(f"Coincidence {key} from file(s) {self.file_label.text()} loaded!")

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
        
    def read_manual_overlap_params(self):
        overlap_params = {}
        for key in self.overlap_lineEdit.keys():
            try:
                overlap_params[key] = float(self.overlap_lineEdit[key].text().strip())
            except ValueError:
                print("Select values for all boxes!")
        return overlap_params



    def apply_overlap_core(self, overlap_params=None):
        '''
        Core functionality to apply overlap to data.
        '''
        
        roi_first = (overlap_params['roi_first_min'], overlap_params['roi_first_max'])
        roi_last = (overlap_params['roi_last_min'], overlap_params['roi_last_max'])
        _, p_amount = self.EP_number_from_string(self.status['Coincidence'])
        
        self.data_postproc = overlap(self.data_raw, overlap_params['reptime'], 
                                     roi_first, roi_last, nPhotons=p_amount)
        self.data_current = self.data_postproc
        
        self.set_status("Bunch overlap", True)
        self.set_status("Masks applied", "N/A")
        self.on_array_change()
        self.logger.info("Overlap parameters applied!")

    def apply_overlap(self):
        overlap_params = self.read_manual_overlap_params()
        self.apply_overlap_core(overlap_params)


    def open_existing_calibration(self):
        print(self.calibration_combo.currentText())
        fn = self.calibration_combo.currentText()
        self.calibration = Calibration(load_calibration(
                filename=fn))
        '''self.calibration_view_window = CalibrationView(calibration= self.calibration)
        self.calibration_view_window.show()'''
        self.plot_workspace.add_calibration_view(calib=self.calibration)

        '''
        try:
            fn = self.calibration_combo.currentText()
            self.calibration = Calibration(load_calibration(
                filename=fn))
            self.calibration_view_window = CalibrationView(self.calibration)
        except:
            self.logger.warning("No calibration selected.")
            '''
        
    def open_new_calibration_in_editor(self):
        self.calibration_editor = CalibrationEditor(calibration = None)
        self.calibration_editor.show()

    def edit_calibration_in_editor(self):

        calibration = self.get_selected_calibration()

        self.calibration_editor = CalibrationEditor(
            calibration=calibration
        )
        

        self.calibration_editor.show()

    def apply_calibration(self):
        if self.data_current is None:
            print("No data loaded, nothing to calibrate.")
            return
        
        self.calibration = self.get_selected_calibration()

        overlap_params = self.calibration.bunch_overlap_params
        self.data_postproc = self.apply_overlap_core(overlap_params=overlap_params)
        self.calibrate()
        

    def calibrate(self):
        
        self.data_calibrated = self.calibration.convert(self.data_postproc)
        self.set_status("Calibrated", True)
        self.on_array_change()
        self.logger.info("Data calibrated")


    def get_selected_calibration(self):
        name = self.calibration_combo.currentText()
        if name == "Select calibration":
            self.logger.error("Choose valid calibration!")
            return 
        calibration = Calibration(
            load_calibration(filename=name)
        )
        return calibration
        
        
  
                    

        

    def handle_histogram_request(self, request):
        print("Received histogram request:", request)
        col_idx = request['column']-1
        bins = request['bins']
        range_lo = request['min']
        range_hi = request['max']

        if self.data_current is None:
            self.logger.warning("No data loaded, cannot plot")
            return

        x, y = hist_1D(self.data_current, col_idx, range=(range_lo, range_hi), bins=bins)
        self.plot_workspace.add_histogram_plot(x, y[:-1], xlabel=f"Particle {col_idx+1}", ylabel="Intensity")
        self.logger.info("Histogram plotted")

    def handle_xy_request(self, request):

        if self.data_current is None:
            self.logger.warning("No data loaded, cannot plot")
            return

        col_idx = (request['x']['column']-1, request['y']['column']-1)
        bins = (request['x']['bins'], request['y']['bins'])
        range_1 = (request['x']['min'], request['x']['max'])   
        range_2 = (request['y']['min'], request['y']['max'])
        units = "ns"  #TODO change the units for the case of calibrated data
        
        
        self.plot_workspace.add_coincidence_map(self.data_current[:, [col_idx[0], col_idx[1]]],
                                                bins=bins, range=(range_1, range_2), 
                                                units=units)
        self.logger.info("Coincidence map plotted")

    def handle_masking_request(self, request):
        
        if self.data_current is None:
            self.logger.warning("No data loaded, cannot mask")
            return
        data = self.data_postproc
        filters = []
        for row in request:
            col_idx = row["column"]-1
            lo = row["min"]
            hi = row["max"]
            mask = (lo,hi)
            data = mask_by_column(data, col_idx, mask)
            filters.append((col_idx, mask))
        
        self.data_current=data
        if filters == []:
            filters = [()]
        
        self.set_status("Masks applied", filters)
        self.logger.info(f"Masks applied {filters}")
        self.on_array_change()

        
    # ======================================
    # HELPER FUNCTIONS
    # ======================================

    
    def status_reset_upon_loading(self):
        for key in self.status.keys():
            if key == "File(s)" or key == "Coincidence":
                self.set_status(key, "N/A")
 

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
        self.logger.info("The current data was changed.")
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
        if key == "Masks applied":
            if value == [()]:
                filter_string = "N/A__"
            else:
                filter_string=""
                for entry in value:
                    col_idx, mask = entry
                    filter_string += f"{col_idx+1}: {mask}; "
                
            self.status_labels[key].setText(filter_string[:-2])
        else:
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

    
class CalibrationEditor(QMainWindow):
    def __init__(self, calibration: Calibration | None = None, parent=None):
        super().__init__(parent)

        self.calibration = calibration or Calibration()
        

        self.setWindowTitle("Calibration Editor")
        self.resize(1200, 600)
        '''
        self.points_table = QTableWidget()
        self.points_table.setHorizontalHeaderLabels(
            [
                "TOF",
                "ΔTOF",
                "E",
                "ΔE"
            ]
        )

        
        splitter = QSplitter()


        self.data_info_panel = self._add_data_info_panel()
        splitter.addWidget(self.data_info_panel)
        splitter.addWidget(self._add_points_panel())
        #splitter.addWidget(self._add_build_plot_panel())

        self.setCentralWidget(splitter)
        '''
        central = QWidget()
        self.setCentralWidget(central)

        self.main_layout = QHBoxLayout(central)

        self.panel1 = QVBoxLayout()


        self.data_info_panel = self._add_data_info_panel()
        self.panel1.addWidget(self.data_info_panel)

        self.main_layout.addLayout(self.panel1)
        

        
    def _add_data_info_panel(self):
        info_group = QGroupBox("General information")
        self.fields = {}

        form = QFormLayout()

        for field_name in ["experiment", "setting", "author", "version", "index"]:
            edit = QLineEdit()
            self.fields[field_name] = edit
            form.addRow(field_name + ":", edit)
        
        info_layout = QVBoxLayout()
        info_layout.addLayout(form)

        info_group.setLayout(info_layout)
        return info_group

    def _add_points_table(self):

        n = len(self.calibration.x_values)

        self.points_table.setRowCount(n)

        for row in range(n):

            self.points_table.setItem(
                row, 0,
                QTableWidgetItem(str(self.calibration.x_values[row]))
            )

            self.points_table.setItem(
                row, 1,
                QTableWidgetItem(str(self.calibration.x_err[row]))
            )

            self.points_table.setItem(
                row, 2,
                QTableWidgetItem(str(self.calibration.y_values[row]))
            )

            self.points_table.setItem(
                row, 3,
                QTableWidgetItem(str(self.calibration.y_err[row]))
            )



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
                "#electron",
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

        self.table.setColumnWidth(0, 12)   # X
        self.table.setColumnWidth(1, 12)   # Y
        self.table.setColumnWidth(2, 120)  # Column (will still stretch visually)
        self.table.setColumnWidth(3, 50)   # Min
        self.table.setColumnWidth(4, 50)   # Max
        self.table.setColumnWidth(5, 40)   # Bins
        self.table.setColumnWidth(6, 50)   # Plot

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

class MaskSelectionWidget(QWidget):

    masking_requested = Signal(list)

    def __init__(self, parent=None):
        super().__init__(parent)

        self.table = QTableWidget(0, 4)

        self.table.setHorizontalHeaderLabels(
            [
                "Use",
                "Column",
                "Min",
                "Max",
            ]
        )
        
        self.table.horizontalHeader().setSectionResizeMode(
            QHeaderView.Stretch
        )

        self.add_button = QPushButton("Add Row")
        self.remove_button = QPushButton("Remove Selected Rows")
        self.apply_button = QPushButton("Apply")

        button_layout = QHBoxLayout()
        button_layout.addWidget(self.add_button)
        button_layout.addWidget(self.remove_button)
        button_layout.addStretch()
        button_layout.addWidget(self.apply_button)

        layout = QVBoxLayout(self)
        layout.addWidget(self.table)
        layout.addLayout(button_layout)

        self.add_button.clicked.connect(self.add_row)
        self.remove_button.clicked.connect(
            self.remove_selected_rows
        )
        self.apply_button.clicked.connect(
            self.request_masking
        )

        self.add_row()

    # --------------------------------------------------
    # Row management
    # --------------------------------------------------

    def add_row(self):

        row = self.table.rowCount()
        self.table.insertRow(row)

        use_check = QCheckBox()
        use_check.setText("")
        use_check.setFixedSize(20,20)
        
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

        self.table.setCellWidget(row, 0, use_check)
        self.table.setCellWidget(row, 1, col_combo)
        self.table.setCellWidget(row, 2, min_dsb)
        self.table.setCellWidget(row, 3, max_dsb)

    def remove_selected_rows(self):

        rows = sorted(
            {idx.row() for idx in self.table.selectedIndexes()},
            reverse=True,
        )

        for row in rows:
            self.table.removeRow(row)

    # --------------------------------------------------
    # Checkbox handling
    # --------------------------------------------------

    def get_selected_rows(self):

        selected_rows = []
        for row in range(self.table.rowCount()):

            if self.table.cellWidget(row, 0).isChecked():
                selected_rows.append(row)

        return selected_rows

    # --------------------------------------------------
    # Helpers
    # --------------------------------------------------

    def get_row_definition(self, row):

        column = (
            self.table.cellWidget(row, 1)
            .currentText()
            .strip()
        )

        min_text = (
            self.table.cellWidget(row, 2)
            .text()
            .strip()
        )

        max_text = (
            self.table.cellWidget(row, 3)
            .text()
            .strip()
        )

        return {
            "column": int(column),
            "min": float(min_text)
            if min_text
            else None,
            "max": float(max_text)
            if max_text
            else None,
            "type": "standard"
        }

    # --------------------------------------------------
    # request masking
    # --------------------------------------------------

    def request_masking(self):

        selected_rows = self.get_selected_rows()
        request = []
        if selected_rows == []:
            QMessageBox.information(
                self, 
                "Information",
                "No filter selected. Filters are disapplied from data."
            )
            self.masking_requested.emit(request)
            return
        
        for row in selected_rows:
            request.append(self.get_row_definition(row))

        self.masking_requested.emit(request)

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
        
    def add_calibration_view(self, calib:Calibration):
        page = CalibrationViewPage(calib)
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



