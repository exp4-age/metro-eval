"""Main PySide6 analysis window for coincidence data workflows.

The application loads raw datasets, chooses a coincidence branch, applies bunch-overlap
correction and optional calibration, filters the data, and displays the results through
interactive histogram and 2D coincidence plots.
"""

import os
import sys

from PySide6.QtGui import QIcon
from PySide6.QtCore import QLocale
from PySide6.QtWidgets import (
    QApplication,
    QFormLayout,
    QMainWindow,
    QWidget,
    QFileDialog,
    QGroupBox,
    QLabel,
    QPushButton,
    QComboBox,
    QLineEdit,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QMessageBox,
    QRadioButton,
)

QLocale.setDefault(QLocale(QLocale.C))  # "C" locale = dot as decimal point

import numpy as np
import logging


from metro_eval.coinc.file_handler import get_keys, read_coinc
from metro_eval.coinc.plot_functions import hist_1D
from metro_eval.coinc.mask_functions import mask_by_column
from metro_eval.coinc.widgets.log_widget import setup_gui_logging, LogWidget
from metro_eval.coinc.calibration_manager import (list_calibrations, 
                                                  load_calibration, 
                                                  Calibration
                                                  )

from metro_eval.coinc.validation import parse_coincidence_key, validate_overlap_params
from metro_eval.coinc.workflow import CoincidenceWorkflow
from metro_eval.coinc.widgets.calibration_editor import CalibrationEditor
from metro_eval.coinc.widgets.plot_definition import PlotDefinitionWidget
from metro_eval.coinc.widgets.mask_selection import MaskSelectionWidget
from metro_eval.coinc.widgets.plot_workspace import PlotWorkspace

class MainWindow(QMainWindow):
    """Main Qt window for loading, processing, and plotting coincidence data.

    The window owns the user interface and orchestration logic only. The scientific
    dataset and its transformation state live in ``CoincidenceWorkflow`` and are
    updated through the workflow object rather than duplicated GUI fields.
    """

    def __init__(self):
        app = QApplication.instance()
        if app is None:
            QApplication([])

        super().__init__()

        self.setWindowTitle("Analysis GUI")
        self.resize(1400, 800)


        # =========================
        # STORING VARIABLES
        # =========================

        self.file_path = None

        self.workflow = CoincidenceWorkflow()
        self.last_directory = ""

        # The CalibrationEditor window
        self.calibration_editor = None

        # Status keys and initial values
        self.status = {
            "File(s)": "N/A",
            "Coincidence": "N/A",
            "Bunch overlap": "N/A",
            "Calibrated": "N/A",
            "Masks applied": "N/A",
            "Data shape (raw)": "N/A",
            "Data shape (current)": "N/A",
        }

        # The coincidence keys, which have priority
        self.pinned_keys = [
            "E", "EE", "EEE", "EEEE",
            "EP", "EEP", "P", "PP"
        ]

        self.build_ui()

    def _sync_workflow_from_gui(self):
        """Keep workflow metadata aligned with the GUI-selected coincidence key."""
        self.workflow.coincidence_key = self.status.get("Coincidence")

    def build_ui(self):
        """Construct the complete main-window layout for the coincidence analysis workflow."""

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

        # =========================
        # CENTER PANEL
        # =========================

        center_panel = QVBoxLayout()

        center_panel_width = 370
        
        status_group = self._add_status_group()
        status_group.setFixedWidth(center_panel_width)
        
        plot_group = self._add_plot_settings_group()
        plot_group.setFixedWidth(center_panel_width)

        # Logging widget
        self.log_widget = LogWidget()
        setup_gui_logging(self.log_widget)
        self.logger = logging.getLogger(__name__)
        self.log_widget.setFixedWidth(center_panel_width)

        center_panel.addWidget(status_group)
        center_panel.addWidget(plot_group)
        center_panel.addWidget(self.log_widget)

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
    # GUI LAYOUT functions
    # =========================

    def _add_data_selection_group(self):
        '''
        Create the file-selection section used to choose one or more input datasets and
        decide which coincidence branch should be loaded.
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
        Create the processing panel that controls automatic/manual overlap handling and
        calibration selection for the currently loaded dataset.
        '''

        post_group = QGroupBox("Postprocessing")
        post_layout = QVBoxLayout()

        # RADIO BUTTON GROUP
        # Make a selection for postprocessing, so that manual and automatic postprocessing is not mixed
        choice_radio_group = QGroupBox()
        choice_radio_layout = QHBoxLayout()

        self.radio_off = QRadioButton("No Postprocessing")
        self.radio_off.toggled.connect(self.enable_postprocessing)
        
        self.radio_manual = QRadioButton("Manual")
        self.radio_manual.toggled.connect(self.enable_postprocessing)

        self.radio_automatic = QRadioButton("automatic")
        self.radio_automatic.setChecked(True)
        self.radio_automatic.toggled.connect(self.enable_postprocessing)

        choice_radio_layout.addWidget(self.radio_off)
        choice_radio_layout.addWidget(self.radio_manual)
        choice_radio_layout.addWidget(self.radio_automatic)

        choice_radio_group.setLayout(choice_radio_layout)
        
        # CALIBRATION BOX

        self.calibration_box = QGroupBox("Calibration")
        calibration_layout = QGridLayout()

        self.calibration_combo = QComboBox()
        self.update_calib_list()

        self.new_calib_btn = QPushButton("")
        self.new_calib_btn.setIcon(QIcon.fromTheme(QIcon.ThemeIcon.DocumentNew))
        self.new_calib_btn.clicked.connect(self.open_new_calibration_in_editor)

        # Update the shown calibrations
        self.update_calib_list_btn = QPushButton()
        self.update_calib_list_btn.setIcon(QIcon.fromTheme(QIcon.ThemeIcon.ViewRefresh))
        self.update_calib_list_btn.clicked.connect(self.update_calib_list)

        calib_buttons = QGridLayout()

        self.apply_calib_btn = QPushButton("Apply")
        self.open_calib_btn = QPushButton("View")
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


        calibration_layout.addWidget(self.calibration_combo,0,0)
        calibration_layout.addWidget(self.new_calib_btn,0,1)
        calibration_layout.addWidget(self.update_calib_list_btn,0,2)
        calibration_layout.addLayout(calib_buttons, 1,0,1,3)

        self.calibration_box.setLayout(calibration_layout)

        # OVERLAP BOX
        self.overlap_box = QGroupBox("Overlap bunches (manual)")
        overlap_layout = QGridLayout()

        # LineEdits
        self.overlap_lineEdit = {}
        
        self.overlap_lineEdit["reptime"] = QLineEdit()
        self.overlap_lineEdit['reptime'].setText("318.76")
        overlap_layout.addWidget(QLabel("Repetition time (ns)"), 0, 0)
        overlap_layout.addWidget(self.overlap_lineEdit['reptime'], 0, 1)

        self.overlap_lineEdit['roi_first_min'] = QLineEdit()
        self.overlap_lineEdit['roi_first_max'] = QLineEdit()
        overlap_layout.addWidget(QLabel("ROI_first (ns)"), 1, 0)

        self.overlap_lineEdit['roi_last_min'] = QLineEdit()
        self.overlap_lineEdit['roi_last_max'] = QLineEdit()
        overlap_layout.addWidget(QLabel("ROI_last (ns)"), 2, 0)

        # Buttons
        apply_overlap_btn = QPushButton("Apply overlap")
        reset_overlap_btn = QPushButton("Reset  overlap")

        apply_overlap_btn.clicked.connect(self.apply_overlap)
        reset_overlap_btn.clicked.connect(self.reset_overlap)

        # Layout
        overlap_layout.addWidget(QLabel("Repetition time (ns)"), 0, 0)
        overlap_layout.addWidget(self.overlap_lineEdit['reptime'], 0, 1)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_first_min'], 1, 1)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_first_max'], 1, 2)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_last_min'], 2, 1)
        overlap_layout.addWidget(self.overlap_lineEdit['roi_last_max'], 2, 2)
        overlap_layout.addWidget(apply_overlap_btn, 3, 0)
        overlap_layout.addWidget(reset_overlap_btn, 3, 1)

        self.overlap_box.setLayout(overlap_layout)
        

        # LAYOUT
        post_layout.addWidget(choice_radio_group, 1)
        post_layout.addWidget(self.overlap_box, 1)
        post_layout.addWidget(self.calibration_box, 1)

        post_group.setLayout(post_layout)

        self.enable_postprocessing()
        return post_group
        
    def _add_masking_group(self):
        '''
        Create the masking panel used to filter the working dataset by value range.
        The current implementation supports column-wise range masks.
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
        Display the current state of the loaded dataset, including file selection,
        calibration status, overlap state, and active mask information.
        '''

        self.status_group = QGroupBox("Data status")
        self.status_widget = QWidget()
        self.status_layout = QFormLayout(self.status_widget)

        self.status_labels = {}

        # Initial population of all status values with "N/A"
        for key in self.status.keys():
            label = QLabel("N/A")
            self.status_labels[key] = label
            self.status_layout.addRow(f"{key}:", label)

        self.status_widget.setLayout(self.status_layout)
        self.status_group.setLayout(QVBoxLayout())
        self.status_group.layout().addWidget(self.status_widget)

        return self.status_group
    
    def _add_plot_settings_group(self):
        '''
        Add the plotting configuration panel used to define histogram and XY plots from
        the active dataset.
        '''

        plot_group = QGroupBox("Plot settings")
        plot_layout = QVBoxLayout()

        self.plot_widget = PlotDefinitionWidget()

        # Connect Signals emitted from self.plot_widget with functions
        self.plot_widget.histogram_requested.connect(self.handle_histogram_request)
        self.plot_widget.xy_requested.connect(self.handle_xy_request)

        plot_layout.addWidget(self.plot_widget)

        plot_group.setLayout(plot_layout)

        return plot_group
    


    # ======================================
    # BUTTON FUNCTIONS
    # ======================================

    #
    # Data selection
    #

    def browse_file(self):
        """Open a file chooser and populate the available coincidence keys."""
        file_path, _ = QFileDialog.getOpenFileNames(
            self,
            "Select file",
            self.last_directory,
            "All Files (*.*)"
        )

        if not file_path:
            self.file_path = None
            self.file_label.setText("No file selected")
            self.dataset_combo.clear()
            return

        self.file_path = list(file_path)
        label = ""
        head = ""
        for file in file_path:
            head, tail = os.path.split(file)
            label += tail + ", "
        label = label[:-2]
        self.file_label.setText(label)

        # Use the directory of the last loaded file as the start directory
        # for the next browsing
        self.last_directory = head

        # Populate the dropdown with the keys from the files
        keys = self.load_keys_from_file()
        self.dataset_combo.clear()
        self.dataset_combo.addItems(keys)

    def load_data(self):
        '''

        Read the selected data key from disk and store it in the canonical workflow
        state. The GUI does not keep separate scientific arrays; it only reflects
        the information exposed by ``CoincidenceWorkflow``.

        '''
        if not self.file_path:
            QMessageBox.warning(
                self,
                "No file selected",
                "Please select at least one file before loading data.",
            )
            return

        key = self.dataset_combo.currentText().strip()
        if not key or key == "Select key":
            QMessageBox.warning(
                self,
                "No coincidence key selected",
                "Please select a valid coincidence key from the dropdown.",
            )
            return

        arrays = []

        # Reads the data from the selected files under key,
        # which is selected in the combobox.
        for path in self.file_path:
            arr = read_coinc(path, key)
            if arr is None:
                self.logger.warning(f"Coincidence key {key!r} not found in {path}.")
                continue
            arrays.append(arr)

        if not arrays:
            QMessageBox.warning(
                self,
                "No usable data found",
                f"The selected key {key!r} could not be read from the chosen files.",
            )
            return

        self.workflow.set_loaded_data(np.concatenate(arrays, axis=0), key)

        # Update status
        self.status_reset_upon_loading()
        self.set_status("File(s)", self.file_label.text())
        self.set_status("Coincidence", key)
        self.on_array_change()

        self._publish_workflow_status("File(s)", self.file_label.text(), 
                        message=f"Coincidence {key} from file(s) {self.file_label.text()} loaded!")
        self._publish_workflow_status("Coincidence", key)

    #
    # Manual bunch overlap
    #

    def apply_overlap(self):
        try:
            overlap_params = self.read_manual_overlap_params()
        except ValueError as exc:
            QMessageBox.warning(
                self,
                "Invalid overlap values",
                str(exc),
            )
            self.logger.warning(f"Manual overlap rejected: {exc}")
            return

        self.apply_overlap_core(overlap_params)

    def reset_overlap(self):
        if self.workflow.raw is None:
            self.logger.warning("No raw data loaded; overlap reset has no effect.")
            return

        self.workflow.reset_to_raw()
        self.on_array_change()
        self._publish_workflow_status("Bunch overlap", "N/A", message="Data was changed to raw.")
        self._publish_workflow_status("Calibrated", "N/A")
        self._publish_workflow_status("Masks applied", "N/A")

    #
    # Calibration utility
    #

    def open_existing_calibration(self):
        '''
        Creates a tab in the PlotWorkspace displaying the major contents of 
        the calibraiton selected in the Combobox
        '''

        calibration = self.get_selected_calibration()
        self.plot_workspace.add_calibration_view(calib=calibration)
        
    def open_new_calibration_in_editor(self):
        '''
        Opens the calibration editor for an empty calibration.
        '''
        
        self.calibration_editor = CalibrationEditor()
        self.calibration_editor.show()
        self.logger.info("Opening new calibration")

    def edit_calibration_in_editor(self):
        '''
        Opens the CalibraitonEditor for the calibration chosen in the ComboBox.
        '''
        
        calibration = self.get_selected_calibration()

        self.calibration_editor = CalibrationEditor(
            calibration=calibration
        )
        self.calibration_editor.show()
        self.logger.info("Editing existing calibration.")

    def apply_calibration(self):
        '''
        Apply the selected calibration to the current workflow state.

        The raw dataset remains intact in the workflow, while the postprocessed and
        calibrated arrays are updated in place before exposing the calibrated result as
        the active working array.
        '''
        if self.workflow.current is None:
            self.logger.warning("No data loaded, nothing to calibrate.")
            return

        calibration = self.get_selected_calibration()
        if calibration is None:
            return

        self.calibration = calibration
        self.logger.info(self.calibration.bunch_overlap_params)
        overlap_params = self.calibration.bunch_overlap_params
        self.apply_overlap_core(overlap_params=overlap_params)
        self.calibrate()
        self._publish_workflow_status("Calibrated", 
                                      "Yes, " + self.calibration.generate_filename(), 
                                      message=f"Calibration applied: {self.calibration.generate_filename()}")
            
    def remove_calibration(self):
        '''
        Reset the active working array back to the raw dataset and clear the
        calibration status in the workflow.
        '''
        if self.workflow.raw is None:
            self.logger.warning("No raw data loaded; no calibration removal applied.")
            return

        self.workflow.postproc = self.workflow.raw.copy()
        self.workflow.calibrated = self.workflow.raw.copy()
        self.workflow.current = self.workflow.raw.copy()
        self.on_array_change()

        self._publish_workflow_status("Calibrated", "N/A", message="data is changed to raw data")
                    
    def handle_histogram_request(self, request) -> None:
        '''
        Is called by the Plot Buttons in the rows of the PlotDefinitionWidget. Reads the entries from
        the row it was called from and plots a 1D histogram in the PlotWorkspace.
        '''
        
        # Identifies the request
        col_idx = request['column']-1
        bins = request['bins']
        range_lo = request['min']
        range_hi = request['max']

        if self.workflow.current is None:
            self.logger.warning("No data loaded, cannot plot")
            return

        # Builds a histogram from dat_current and plots it to a new PlotWorkspace tab
        x, y = hist_1D(self.workflow.current, col_idx, range=(range_lo, range_hi), bins=bins)
        self.plot_workspace.add_histogram_plot(x, y[:-1], xlabel=f"Particle {col_idx+1}", ylabel="Intensity")

        self.logger.info("Histogram plotted.")

    def handle_xy_request(self, request) -> None:
        '''
        Is called by the Plot XY Button in the PlotDefinitionWidget. Reads the entries from
        the rows checked in the X and Y column. Then, it plots an interactive 
        2D Coincidence map tab in the PlotWorkspace.
        '''
        
        if self.workflow.current is None:
            self.logger.warning("No data loaded, cannot plot.")
            return

        # Reads request
        col_idx = (request['x']['column']-1, request['y']['column']-1)
        bins = (request['x']['bins'], request['y']['bins'])
        range_1 = (request['x']['min'], request['x']['max'])   
        range_2 = (request['y']['min'], request['y']['max'])

        if self.status["Calibrated"] == "N/A":
            units = "ns"  
        else:
            units = "eV"
        
        # Plots to the PlotWorkspace
        self.plot_workspace.add_coincidence_map(self.workflow.current[:, [col_idx[0], col_idx[1]]],
                                                bins=bins, range=(range_1, range_2), 
                                                units=units)
        self.logger.info("Coincidence map plotted.")

    def handle_masking_request(self, request) -> None:
        '''
        Apply the selected column filters to the calibrated data and store the result
        in the workflow's active current array.
        '''
        
        if self.workflow.current is None:
            self.logger.warning("No data loaded, cannot mask")
            return
        data = self.workflow.calibrated.copy()
        filters = []
        for row in request:
            # For each checked masking row, the data is filtered.
            col_idx = row["column"]-1
            lo = row["min"]
            hi = row["max"]
            mask = (lo,hi)
            data = mask_by_column(data, col_idx, mask)
            filters.append((col_idx, mask))
        
        # set the current data
        self.workflow.current = data

        # For displaying the filters:
        if filters == []:
            filters = [()]

        self._publish_workflow_status("Masks applied", filters, message=f"Masks applied {filters}")

        # Array changed
        self.on_array_change()

    def update_calib_list(self):
        self.calibration_combo.clear()
        self.calibration_combo.addItems(["Select calibration"])
        self.calibration_combo.addItems([file.name for file in list_calibrations()])
        self.calibration_combo.setCurrentIndex(0)

        
    # ======================================
    # HELPER FUNCTIONS
    # ======================================

    
    def status_reset_upon_loading(self) -> None:
        """Clear the visible status labels before loading a new dataset."""
        for key in self.status:
            self.set_status(key, "N/A")
 

    def enable_postprocessing(self):
        '''
        Depending on the RadioButtons in the Postprocessing group,
        subgroups are dis- or enabled
        '''
        
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
        Refresh the visible status panel whenever the workflow's current dataset changes.
        '''

        if self.workflow.current is None:
            return
        
        self.logger.info("The current data was changed.")

        # Update status labels
        self._publish_workflow_status("Data shape (raw)", self.workflow.raw.shape)
        self._publish_workflow_status("Data shape (current)", self.workflow.current.shape)
        

            


    def _publish_workflow_status(self, key, value, *, message: str | None = None):
        """Update workflow state, GUI status panel, and logger in one place."""
        self.workflow.update_status(key, value)
        self.status[key] = value

        label = self.status_labels.get(key)
        if label is not None:
            if key == "Masks applied":
                if value == [()] or value == "N/A":
                    filter_string = "N/A__"
                else:
                    filter_string = ""
                    for entry in value:
                        col_idx, mask = entry
                        filter_string += f"{col_idx+1}: {mask}; "
                    filter_string = filter_string[:-2]
                label.setText(filter_string)
            else:
                label.setText(str(value))

        if message is not None:
            self.logger.info(message)

    def set_status(self, key, value):
        '''
        Makes a change in the self.status dictionary.
        Transfers the contents of status to the display.
        '''
        self._publish_workflow_status(key, value)

    def load_keys_from_file(self) -> list:
        '''
        Checks the coincidence keys for all selected files. Then, makes
        a unique set of keys, so that there are no doubles. Returns a list,
        whereas the keys defined in self.pinned_keys are have the first indices.
        '''

        key_list = []

        # Check the keys in all runs
        for file in self.file_path:
            key_list.extend(get_keys(file))

        # Remove duplicates
        unique_keys = list(dict.fromkeys(key_list))  # only preserves first-seen order

        # Seperate the pinned keys from all others, merge them in the end
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
    
    #
    # Applying bunch overlap
    #

    def read_manual_overlap_params(self) -> dict:
        '''
        Read the manual overlap values from the GUI fields and convert them to the
        dictionary format expected by the overlap processing function.
        '''

        expected_keys = [
            "reptime",
            "roi_first_min",
            "roi_first_max",
            "roi_last_min",
            "roi_last_max",
        ]

        missing = []
        for key in expected_keys:
            widget = self.overlap_lineEdit.get(key)
            if widget is None:
                missing.append(key)
                continue

            text = widget.text().strip()
            if text == "":
                missing.append(key)
                continue

        if missing:
            raise ValueError(
                "Please fill all overlap fields before applying the correction: "
                + ", ".join(missing)
            )

        overlap_params = {
            "repetition_time": float(self.overlap_lineEdit["reptime"].text().strip()),
            "ROI_first": [
                float(self.overlap_lineEdit["roi_first_min"].text().strip()),
                float(self.overlap_lineEdit["roi_first_max"].text().strip()),
            ],
            "ROI_last": [
                float(self.overlap_lineEdit["roi_last_min"].text().strip()),
                float(self.overlap_lineEdit["roi_last_max"].text().strip()),
            ],
        }

        return validate_overlap_params(overlap_params)



    def apply_overlap_core(self, overlap_params):
        '''
        Apply a bunch-overlap correction to the currently loaded raw data and move the
        working array into the postprocessed state.
        '''
        if self.workflow.raw is None:
            self.logger.warning("No raw data loaded; overlap correction cannot be applied.")
            return

        if not isinstance(overlap_params, dict):
            self.logger.warning("Overlap parameters were not provided in dictionary form.")
            return

        roi_first = overlap_params.get("ROI_first")
        roi_last = overlap_params.get("ROI_last")
        repetition_time = overlap_params.get("repetition_time")

        if not all(isinstance(value, (list, tuple)) and len(value) == 2 for value in (roi_first, roi_last)):
            self.logger.warning("Invalid ROI ranges for overlap correction.")
            return

        if repetition_time is None:
            self.logger.warning("Missing repetition time for overlap correction.")
            return

        # Some coincidence keys include photons, which need special handling in the
        # overlap function. We therefore derive the photon count from the key.
        coincidence_key = self.status.get('Coincidence', 'E')
        try:
            key = parse_coincidence_key(coincidence_key)
            p_amount = key.n_photons
        except ValueError:
            self.logger.warning(f"Invalid coincidence key {coincidence_key!r}; using zero photons for overlap correction.")
            p_amount = 0

        # Set the postprocessed dataset as the active working array.
        self.workflow.coincidence_key = self.status.get("Coincidence", "E")
        self.workflow.apply_overlap(
            {
                "repetition_time": repetition_time,
                "ROI_first": roi_first,
                "ROI_last": roi_last,
            },
            key=self.workflow.coincidence_key,
        )

        # Update status
        self._publish_workflow_status("Bunch overlap", True, message="Overlap parameters applied!")
        self._publish_workflow_status("Masks applied", "N/A")
        self.on_array_change()
    
    #
    # Calibration helper functions
    #
    
    def calibrate(self) -> None:
        '''
        Apply the selected calibration model to the postprocessed data while leaving
        photon columns unchanged and updating the current working array.
        '''
        data = self.workflow.postproc
        ndim = data.ndim
        try:
            _, nphotons = self.EP_number_from_string(self.status['Coincidence'])
        except ValueError:
            QMessageBox.warning(
                            self,                     
                            "Invalid input",          
                            "The coincidence string cannot be used!"
                        )
            return
    
        if nphotons > 0:
            # Photons are part of the coincidence record but should not be calibrated,
            # so they are separated and re-inserted after the electron calibration.
            if ndim == 1:
                electrons = data[:-nphotons]
                photons = data[-nphotons:]
            else:
                electrons = data[:, :-nphotons]
                photons = data[:, -nphotons:]
        else:
            electrons = data
            photons = None
    
        # calibrate electrons
        electrons_cal = self.calibration.convert(electrons)
    
        result = np.empty_like(data)
    
        if nphotons > 0:
            # Restore the uncalibrated photon columns after the electron calibration.
            if ndim == 1:
                result[:-nphotons] = electrons_cal
                result[-nphotons:] = photons
            else:
                result[:, :-nphotons] = electrons_cal
                result[:, -nphotons:] = photons
        else:
            result[:] = electrons_cal

        # Keep a separate calibrated array while exposing the calibrated version as the
        # currently active working dataset.
        self.workflow.calibrated = result
        self.workflow.current = result

        # Update status
        self.on_array_change()

        # Log message
        self.logger.info("Data calibrated")


    def get_selected_calibration(self) -> Calibration | None:
        '''
        Read the selected calibration name from the dropdown and load the matching
        calibration JSON into a Calibration object.
        '''

        name = self.calibration_combo.currentText()
        if name == "Select calibration":
            self.logger.error("Choose valid calibration!")
            QMessageBox.warning(
                self,
                "No calibration selected",
                "Please select a calibration before applying or editing it.",
            )
            return None

        calibration = Calibration(load_calibration(filename=name))
        return calibration
    
    #
    # other methods
    #
    
    @staticmethod
    def EP_number_from_string(string: str) -> tuple:
        '''
        Deduce the number of electron and photon columns implied by a coincidence key
        such as "E", "EE", "2E1P", or "P".
        '''
        key = parse_coincidence_key(string)
        return key.electrons, key.photons




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



