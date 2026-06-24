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

import pyqtgraph as pg
import numpy as np
import logging

from metro_eval.coinc.file_handler import get_keys, read_coinc
from metro_eval.coinc.plot_functions import hist_1D
from metro_eval.coinc.analysis_pages import CoincmapPage, SignalPage, HistogramPage, CalibrationViewPage, ScanAnalysisPage
from metro_eval.coinc.postprocessing import overlap
from metro_eval.coinc.mask_functions import mask_by_column
from metro_eval.coinc.log_widget import setup_gui_logging, LogWidget
from metro_eval.coinc.calibration_manager import (list_calibrations, 
                                                  load_calibration, 
                                                  Calibration, 
                                                  save_calibration,
                                                  get_calibration_filepath, 
                                                  plot_calibration_pg)

from metro_eval.coinc.models import MODELS


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("Analysis GUI")
        self.resize(1400, 800)


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


        test_btn_1d = QPushButton("Create random 1D Plot")
        test_btn_1d.clicked.connect(
            self.plot_workspace.create_random_signal
        )
        test_btn_2d = QPushButton("Create random 2D Plot")
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
        self.remove_calib_btn.clicked.connect(self.remove_calibration)

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
        reset_overlap_btn.clicked.connect(self.reset_overlap)

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

    #### Data selection

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

    #### Manual bunch overlap

    def apply_overlap(self):
        overlap_params = self.read_manual_overlap_params()
        self.apply_overlap_core(overlap_params)

    def reset_overlap(self):
        self.data_current=self.data_raw
        self.data_postproc = self.data_raw
        self.data_calibrated = self.data_raw
        self.on_array_change()
        self.logger.info("Data was changed to raw.")
        self.set_status("Bunch overlap", "N/A")
        self.set_status("Calibrated", "N/A")
        self.set_status("Masks applied", "N/A")

    #### Calibration utility

    def open_existing_calibration(self):
        calibration = self.get_selected_calibration()
        self.plot_workspace.add_calibration_view(calib=calibration)
        
    def open_new_calibration_in_editor(self):
        self.calibration_editor = CalibrationEditor()
        self.calibration_editor.show()
        self.logger.info("Opening new calibration")

    def edit_calibration_in_editor(self):

        calibration = self.get_selected_calibration()

        self.calibration_editor = CalibrationEditor(
            calibration=calibration
        )
        

        self.calibration_editor.show()
        self.logger.info("Editing existing calibration.")

    def apply_calibration(self):
        if self.data_current is None:
            self.logger.warning("No data loaded, nothing to calibrate.")
            return
        
        self.calibration = self.get_selected_calibration()
        self.logger.info(self.calibration.bunch_overlap_params)
        overlap_params = self.calibration.bunch_overlap_params
        self.data_postproc = self.apply_overlap_core(overlap_params=overlap_params)
        self.calibrate()
        self.set_status("Calibrated", "Yes, "+self.calibration.generate_filename())
        
        
        
    def remove_calibration(self):
        '''
        Sets self.calibration=None and changes data_current to data_raw
        '''
        self.data_current = self.data_raw
        self.on_array_change()
        self.logger.info("data is changed to raw data")
                    

        

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
        
        for row in range(self.plot_widget.table.rowCount()):
            combo = self.plot_widget.table.cellWidget(row, 2,)
            combo.clear()
            combo.addItems([f"{i+1}" for i in range(self.data_current.shape[1])])

            


    def set_status(self, key, value):
        '''
        Transfers the contents of status to the display
        '''
        self.status[key] = value
        if key == "Masks applied":
            if value == [()] or value == "N/A":
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
    
    #### Applying bunch overlap

    def read_manual_overlap_params(self):
        overlap_params = {}
        for key in self.overlap_lineEdit.keys():
            try:
                overlap_params[key] = float(self.overlap_lineEdit[key].text().strip())
            except ValueError:
                print("Select values for all boxes!")

        overlap_params["ROI_first"] = [overlap_params["roi_first_min"], overlap_params["roi_first_max"]]
        overlap_params["ROI_last"] = [overlap_params["roi_last_min"], overlap_params["roi_last_max"]]
        overlap_params["repetition_time"] = overlap_params["reptime"]
        return overlap_params



    def apply_overlap_core(self, overlap_params=None):
        '''
        Core functionality to apply overlap to data.
        '''
        
        roi_first = overlap_params["ROI_first"]
        roi_last = overlap_params["ROI_last"]
        _, p_amount = self.EP_number_from_string(self.status['Coincidence'])
        
        self.data_postproc = overlap(self.data_raw, overlap_params['repetition_time'], 
                                     roi_first, roi_last, nPhotons=p_amount)
        self.data_current = self.data_postproc
        
        self.set_status("Bunch overlap", True)
        self.set_status("Masks applied", "N/A")
        self.on_array_change()
        self.logger.info("Overlap parameters applied!")
    
    #### Calibration helper functions

    
    def calibrate(self):
        
        self.data_calibrated = self.calibration.convert(self.data_postproc)
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
    
    #### other methods

    
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
        self.resize(1200, 750)
        
        self.build_ui()
        
    def build_ui(self):
        '''
        Build the main UI for the CalibrationEditor
        
        splitter = QSplitter()
        


        self.data_info_panel = self._add_data_info_panel()
        self.data_point_panel = self._add_points_panel()
        self.cal_plot_panel = self._add_cal_plot_panel()
        splitter.addWidget(self.data_info_panel)
        splitter.addWidget(self.data_point_panel)
        splitter.addWidget(self.cal_plot_panel)
        self.setCentralWidget(splitter)
        '''
        
        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        
        layout_left = QVBoxLayout()

        layout_left_upper = QHBoxLayout()
        
        self.data_info_panel = self._add_data_info_panel()
        self.data_point_panel = self._add_points_panel()

        layout_left_upper.addWidget(self.data_info_panel)
        layout_left_upper.addWidget(self.data_point_panel)

        layout_left_lower = self._add_data_exploration()
        

        layout_left.addLayout(layout_left_upper, 1)
        layout_left.addLayout(layout_left_lower, 1)

        self.cal_plot_panel = self._add_cal_plot_panel()

        main_layout.addLayout(layout_left)
        main_layout.addWidget(self.cal_plot_panel)
        central.setLayout(main_layout)
        
        

        
    def _add_data_info_panel(self):
        '''
        Builds group box containing lineedits to enter the
        general information on the calibration as well as comments 
        and a save button
        '''
        info_group = QGroupBox("General information")
        self.fields = {}

        form = QFormLayout()

        for field_name in ["experiment", "setting", "author", "version", "index"]:
            edit = QLineEdit()
            self.fields[field_name] = edit
            form.addRow(field_name + ":", edit)
        if self.calibration.metadata is not None:
            self.fields["experiment"].setText(str(self.calibration.metadata.experiment))
            self.fields["setting"].setText(str(self.calibration.metadata.setting))
            self.fields["author"].setText(str(self.calibration.metadata.author))
            self.fields["version"].setText(str(self.calibration.metadata.version))
            self.fields["index"].setText(str(self.calibration.metadata.index))
            
        self.fields["Comments"] = QTextEdit()
        form.addRow("Comments:", self.fields["Comments"])
        self.fields["Comments"].setText(self.calibration.comments)

        self.save_btn = QPushButton("Save")
        self.save_btn.clicked.connect(self.save)

        info_layout = QVBoxLayout()
        info_layout.addLayout(form)
        info_layout.addWidget(self.save_btn)


        info_group.setLayout(info_layout)
        return info_group


    def _add_points_panel(self):
        '''
        Returns a QGroupBox() object, which contains a table displaying the calibration points 
        and QLineEdit Widgets + a  Button to enter new calibraiton points to the table.

        Add: Line Removal
        Add: On changing elements in the table, the Calibraiton object is reinitialized with the 
        current entries.
        '''
        points_panel = QGroupBox("Calibration points")

        points_layout = QVBoxLayout()

        self.points_table = QTableWidget()
        self.points_table.setColumnCount(4)
        self.points_table.setHorizontalHeaderLabels(
            [
                "TOF",
                "ΔTOF",
                "E",
                "ΔE"
            ]
        )


        self.populate_points_table()

        add_points_layout = QHBoxLayout()

        self.manual_points_entries = {}
        self.manual_points_entries["x"] = QLineEdit()
        self.manual_points_entries["x"].setPlaceholderText("x")
        self.manual_points_entries["xerr"] = QLineEdit()
        self.manual_points_entries["xerr"].setPlaceholderText("xerr")
        self.manual_points_entries["y"] = QLineEdit()
        self.manual_points_entries["y"].setPlaceholderText("y")
        self.manual_points_entries["yerr"] = QLineEdit()
        self.manual_points_entries["yerr"].setPlaceholderText("yerr")
        
        
        self.add_row_btn = QPushButton("Add point")
        self.add_row_btn.clicked.connect(self._add_points_row)

        for widget in self.manual_points_entries.values():
            add_points_layout.addWidget(widget)
        add_points_layout.addWidget(self.add_row_btn)



        points_layout.addWidget(self.points_table)
        points_layout.addLayout(add_points_layout)
        
        points_panel.setLayout(points_layout)
        return points_panel

    def _add_data_exploration(self):

        layout = QHBoxLayout()
        
        self.plot_tab_widget = PlotWorkspace()
        self.new_scan_tab_btn = QPushButton("New Scan Tab")
        self.new_scan_tab_btn.clicked.connect(self.plot_tab_widget.add_scan_analysis)


        layout.addWidget(self.new_scan_tab_btn)
        layout.addWidget(self.plot_tab_widget, stretch=2)

        return layout

        


    def _add_cal_plot_panel(self):
        
        cal_plot_panel = QGroupBox()
        cal_plot_layout = QVBoxLayout()

        self.plot_widget = pg.PlotWidget()
        plot_calibration_pg(self.calibration, self.plot_widget)


        fit_settings_layout = QHBoxLayout()
        self.method_combo = QComboBox()
        self.method_combo.addItems(["odr", "curve_fit"])
        if self.calibration.method is not None:
            self.method_combo.setCurrentText(self.calibration.method)


        self.models_combo = QComboBox()
        self.models_combo.addItems([model for model in MODELS.keys()])
        if self.calibration.model_func is not None:
            self.method_combo.setCurrentText(self.calibration.calibration_dict["model_type"])
        
        init_params_layout = QHBoxLayout()
        
        number_of_init_parameters = 6


        self.init_params_edits = {}
        for i in range(number_of_init_parameters):
            edit = QLineEdit()
            self.init_params_edits[f"a{i}"] = edit
            init_params_layout.addWidget(edit)

        if self.calibration.p0 is not None:
            for i in range(len(self.calibration.p0)):
                self.init_params_edits[f"a{i}"].setText(str(self.calibration.p0[i]))
        
        
        fitted_params_layout = QHBoxLayout()

        self.fitted_params_edits = {}
        for i in range(number_of_init_parameters):
            edit = QLineEdit()
            edit.setReadOnly(True)
            self.fitted_params_edits[f"a{i}"] = edit
            fitted_params_layout.addWidget(edit)

        if self.calibration.popt is not None:
            for i in range(len(self.calibration.p0)):
                self.fitted_params_edits[f"a{i}"].setText(f"{self.calibration.popt[i]:.2e}")
        


        fit_settings_layout.addWidget(self.method_combo)
        fit_settings_layout.addWidget(self.models_combo)

        cal_plot_layout.addLayout(fit_settings_layout)
        cal_plot_layout.addLayout(init_params_layout)
        cal_plot_layout.addLayout(fitted_params_layout)
        cal_plot_layout.addWidget(self.plot_widget)

        cal_plot_panel.setLayout(cal_plot_layout)
        return cal_plot_panel


    def save(self):
        '''
        Populates the information in the lineedits etc to 
        calibration and saves the calibration to a json file
        '''

        self.populate_calibration_from_edits()

        filepath=get_calibration_filepath(self.calibration.calibration_dict)

        if filepath.exists():
            reply = QMessageBox.question(
                self,
                "Calibration exists",
                f"{filepath.name} already exists.\n\n"
                "Do you want to overwrite it?",
                QMessageBox.Yes | QMessageBox.No,
            )

            if reply == QMessageBox.No:
                return

        save_calibration(self.calibration.calibration_dict)

    def populate_calibration_from_edits(self):
        '''
        Reads the entries in the QLineEdit widgets (and later also other important widgets) to a dicitonary. 
        Then, a Calibration object is initialized with this dictionary. This is set to be the new self.calibration property. 
        '''
        info = {}
        
        for field in self.fields.keys():
            if field == "Comments":
                info[field] = self.fields[field].toPlainText()
            else:
                info[field] = self.fields[field].text()

        info["calibration_points"] = self.get_calibration_points()
        info["method"] = self.method_combo.currentText()
        info["model_type"] = self.models_combo.currentText()

        info["initial_parameters"] = {"p0": self.get_initial_fit_parameters_from_edits()}
        if self.calibration.popt is not None:
            fit_results = {
                "popt" : self.calibration.popt.tolist(),
                "pcov" : self.calibration.pcov.tolist(),
                "perr" : self.calibration.perr.tolist(),
            }
        else:
            fit_results = {
                "popt" : [],
                "pcov" : [],
                "perr" : []
            }
        info["fitted_parameters"] = fit_results
        
        self.calibration = Calibration(info)

    def get_initial_fit_parameters_from_edits(self):
        parameter_list = []
        for i in range(len(self.init_params_edits)):
            if self.init_params_edits[f"a{i}"].text() == "":
                continue
            else:
                parameter_list.append(float(self.init_params_edits[f"a{i}"].text()))
        return parameter_list
    

    def update_calibration_plot(self):
        self.populate_calibration_from_edits()
        plot_calibration_pg(self.calibration, self.plot_widget)
        


    def get_calibration_points(self):
        '''
        Reads the calibration points from the points_table and returns a list in the format:
        [[x, xerr, y, yerr],[...],...]
        This is the format used for initializing a Calibration object from a dicitonary.
        '''

        points = []

        for row in range(self.points_table.rowCount()):

            point = []

            for col in range(4):
                widget = self.points_table.cellWidget(row, col)
                point.append(float(widget.text()) if widget and widget.text() else None)

            points.append(point)

        return points

    def populate_points_table(self):
        '''
        Use the information stored in self.calibration.x_values (x_err, y_values, y_err)
        to populate the points_table.
        '''

        n = len(self.calibration.x_values)

        self.points_table.setRowCount(n)
        

        for row in range(n):
            
            line_edit = QLineEdit()
            line_edit.setText(str(self.calibration.x_values[row]))
            line_edit.editingFinished.connect(self.update_calibration_plot)
            self.points_table.setCellWidget(
                row, 0,
                line_edit)

            
            line_edit = QLineEdit()
            line_edit.setText(str(self.calibration.x_err[row]))
            line_edit.editingFinished.connect(self.update_calibration_plot)
            self.points_table.setCellWidget(
                row, 1,
                line_edit)

            
            line_edit = QLineEdit()
            line_edit.setText(str(self.calibration.y_values[row]))
            line_edit.editingFinished.connect(self.update_calibration_plot)
            self.points_table.setCellWidget(
                row, 2,
                line_edit)
            
            line_edit = QLineEdit()
            line_edit.setText(str(self.calibration.y_err[row]))
            line_edit.editingFinished.connect(self.update_calibration_plot)
            self.points_table.setCellWidget(
                row, 3,
                line_edit)
        

    def _add_points_row(self):
        '''
        Takes the values entered in the self.maual_point_entries QLineEdits 
        and appends them to self.calibration.x_values etc.
        Then, the points_table is populated from self.calibration and the 
        calibration_point plot is replotted.
        '''

        for key, entry in self.manual_points_entries.items():
            print(entry.text())
            if entry.text() == None:
                print(f"Entry {key} is None")
                return

        # Update calibration            
        entries = self.manual_points_entries

        self.calibration.x_values = np.append(self.calibration.x_values, float(entries["x"].text().strip()))
        self.calibration.x_err = np.append(self.calibration.x_err, float(entries["xerr"].text().strip()))
        self.calibration.y_values = np.append(self.calibration.y_values, float(entries["y"].text().strip()))
        self.calibration.y_err = np.append(self.calibration.y_err, float(entries["yerr"].text().strip()))

        # Repopulate points_table
        self.populate_points_table()

        # Empty the QLineEdits
        for entry in self.manual_points_entries.values():
            entry.clear()
        
        plot_calibration_pg(self.calibration, self.plot_widget)
        

            
    



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

    def add_scan_analysis(self):
        page = ScanAnalysisPage()
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



