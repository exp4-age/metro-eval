from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTableWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from PySide6.QtGui import QIcon
from metro_eval.coinc.calibration_manager import (Calibration, 
                                                  save_calibration,
                                                  get_calibration_filepath, 
                                                  plot_calibration_pg)
from metro_eval.coinc.file_handler import ScanData, ScanSpectrum, read_scan
from metro_eval.coinc.widgets.plot_workspace import PlotWorkspace
from metro_eval.coinc.models import MODELS
import pyqtgraph as pg
from datetime import datetime
import numpy as np
import os



#-----------------------------------------
# Calibration Editor Class
#-----------------------------------------

class CalibrationEditor(QMainWindow):
    """Editor for calibration metadata, overlap settings, and reference points.

    This widget represents an editing surface for calibration definitions.
    It does not own the loaded analysis data directly.
    """

    def __init__(self, calibration: Calibration | None = None, parent=None):
        super().__init__(parent)

        self.calibration = calibration or Calibration()

        self.setWindowTitle("Calibration Editor")
        self.resize(1300, 750)
        
        self.build_ui()
        
    def build_ui(self) -> None:
        '''
        Build the main UI for the CalibrationEditor
        '''
        
        central = QWidget()
        self.setCentralWidget(central)

        main_layout = QHBoxLayout(central)
        
        layout_left = QVBoxLayout()

        layout_left_upper = QHBoxLayout()
        
        self.data_info_panel = self._add_data_info_panel()
        self.data_info_panel.setMaximumWidth(270)
        self.bunch_overlap_panel = self._add_bunch_overlap_panel()
        self.data_point_panel = self._add_points_panel()

        layout_left_upper.addWidget(self.data_info_panel)
        layout_left_upper.addWidget(self.bunch_overlap_panel)
        layout_left_upper.addWidget(self.data_point_panel)

        layout_left_lower = self._add_data_exploration()
        

        layout_left.addLayout(layout_left_upper, 1)
        layout_left.addLayout(layout_left_lower, 1)

        self.cal_plot_panel = self._add_cal_plot_panel()

        main_layout.addLayout(layout_left, 3)
        main_layout.addWidget(self.cal_plot_panel, 2)
        central.setLayout(main_layout)
        
        

        
    def _add_data_info_panel(self) -> QGroupBox:
        '''
        Build the metadata section for a calibration definition, including experiment
        information, author metadata, comments, and the save action.
        '''
        info_group = QGroupBox("General information")
        self.fields = {}

        form = QFormLayout()

        entry_fields = [
            ("experiment", "beamline_yyyymm"), 
            ("setting", "e.g. acc4, ret32, ..."), 
            ("index", "1"),
            ("author","Your signature (NiGo)"), 
            ("version","v1"), 
            ("last edit","")
            ]

        for field_name, placeholder in entry_fields:
            edit = QLineEdit()
            self.fields[field_name] = edit
            form.addRow(field_name + ":", edit)
            self.fields[field_name].setPlaceholderText(placeholder)
        if self.calibration.metadata is not None:
            self.fields["experiment"].setText(str(self.calibration.metadata.experiment))
            self.fields["setting"].setText(str(self.calibration.metadata.setting))
            self.fields["author"].setText(str(self.calibration.metadata.author))
            self.fields["version"].setText(str(self.calibration.metadata.version))
            self.fields["index"].setText(str(self.calibration.metadata.index))

        self.fields["last edit"].setReadOnly(True)
        self.fields["last edit"].setText(self.calibration.created_date)

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

    def _add_bunch_overlap_panel(self) -> QGroupBox:
        bo_box = QGroupBox("Bunch overlap")

        bo_layout = QGridLayout()

        self.bunch_overlap_edits = {}
        
        self.bunch_overlap_edits["reptime"] = QLineEdit()
        self.bunch_overlap_edits['reptime'].setPlaceholderText("repetition time")

        self.bunch_overlap_edits['roi_first_min'] = QLineEdit()
        self.bunch_overlap_edits['roi_first_min'].setPlaceholderText("ROI1_1")
        self.bunch_overlap_edits['roi_first_max'] = QLineEdit()
        self.bunch_overlap_edits['roi_first_max'].setPlaceholderText("ROI1_2")

        self.bunch_overlap_edits['roi_last_min'] = QLineEdit()
        self.bunch_overlap_edits['roi_last_min'].setPlaceholderText("ROI2_1")
        self.bunch_overlap_edits['roi_last_max'] = QLineEdit()
        self.bunch_overlap_edits['roi_last_max'].setPlaceholderText("ROI2_2")

        self.populate_bunch_overlap_param_edits_from_calibration()

        bo_layout.addWidget(self.bunch_overlap_edits['reptime'], 0,0,1,2)
        bo_layout.addWidget(self.bunch_overlap_edits['roi_first_min'], 1,0)
        bo_layout.addWidget(self.bunch_overlap_edits['roi_first_max'], 1,1)
        bo_layout.addWidget(self.bunch_overlap_edits['roi_last_min'], 2,0)
        bo_layout.addWidget(self.bunch_overlap_edits['roi_last_max'], 2,1)

        # INFO table for repetition times at different facilities:
        reptime_group = QGroupBox("Repetition Times (ns)")

        reptime_form = QFormLayout(reptime_group)

        reptime_form.addRow("MAX IV", QLabel("318.76"))
        reptime_form.addRow("PETRA 3 (40-bunch)", QLabel("191.22"))
        reptime_form.addRow("BESSY II", QLabel("796.8"))
        reptime_form.addRow("BESSY II (4-bunch)", QLabel("199.2"))
        reptime_form.addRow("Soleil", QLabel("1175.66"))


        bo_layout.addWidget(reptime_group, 3,0,1,2)



        bo_box.setLayout(bo_layout)

        return bo_box


    def _add_points_panel(self) -> QGroupBox:
        '''
        Build the calibration points editor. It provides a table of TOF/energy reference
        points and widgets for adding or removing rows from the active calibration.
        '''
        points_panel = QGroupBox("Calibration points")

        points_layout = QVBoxLayout()

        # Create table
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
        col_width=60
        self.points_table.setColumnWidth(0, col_width)
        self.points_table.setColumnWidth(1, col_width)
        self.points_table.setColumnWidth(2, col_width)
        self.points_table.setColumnWidth(3, col_width)
        self.populate_points_table()

        # Add a new point manually
        add_points_layout = QHBoxLayout()

        self.manual_points_entries = {}

        self.manual_points_entries["x"] = QLineEdit()
        self.manual_points_entries["x"].setPlaceholderText("TOF")

        self.manual_points_entries["xerr"] = QLineEdit()
        self.manual_points_entries["xerr"].setPlaceholderText("ΔTOF")
        
        self.manual_points_entries["y"] = QLineEdit()
        self.manual_points_entries["y"].setPlaceholderText("E")
        
        self.manual_points_entries["yerr"] = QLineEdit()
        self.manual_points_entries["yerr"].setPlaceholderText("ΔE")

        self.add_row_btn = QPushButton()
        self.add_row_btn.setIcon(QIcon.fromTheme(QIcon.ThemeIcon.ListAdd))
        self.add_row_btn.clicked.connect(self.points_from_manual_entries)

        self.remove_row_btn = QPushButton()
        self.remove_row_btn.setIcon(QIcon.fromTheme(QIcon.ThemeIcon.ListRemove))
        self.remove_row_btn.clicked.connect(self.remove_points_row)

        # Layout
        for widget in self.manual_points_entries.values():
            widget.setMaximumWidth(60)
            add_points_layout.addWidget(widget)
        add_points_layout.addWidget(self.add_row_btn)
        add_points_layout.addWidget(self.remove_row_btn)

        points_layout.addWidget(self.points_table)
        points_layout.addLayout(add_points_layout)
        
        points_panel.setLayout(points_layout)

        return points_panel

    def _add_data_exploration(self) -> QHBoxLayout:
        """Create the exploratory scan view area used to inspect dummy or imported scans."""

        layout = QHBoxLayout()
        
        self.plot_tab_widget = PlotWorkspace()

        layout_left = QVBoxLayout()

        # Just for testing-----
        self.new_scan_tab_btn = QPushButton("Scan Dummy")
        self.new_scan_tab_btn.clicked.connect(self.add_dummy_scan)
        #-------------------------

        self.new_scan_btn = QPushButton("Browse scan")
        self.new_scan_btn.clicked.connect(self.browse_scans)

        layout_left.addWidget(self.new_scan_tab_btn)
        layout_left.addWidget(self.new_scan_btn)
        layout_left.addStretch()

        layout.addLayout(layout_left)
        layout.addWidget(self.plot_tab_widget, stretch=2)


        return layout


    def _add_cal_plot_panel(self) -> QGroupBox:
        """Create the fit and calibration-curve panel used to visualize the active model."""
        
        cal_plot_panel = QGroupBox("Calibration function")
        cal_plot_layout = QVBoxLayout()

        self.plot_widget = pg.PlotWidget()
        plot_calibration_pg(self.calibration, self.plot_widget)

        # Fit settings: method and model
        fit_settings_layout = QHBoxLayout()

        self.method_combo = QComboBox()
        self.method_combo.addItems(["odr", "curve_fit"])
        self.method_combo.currentTextChanged.connect(self.update_calibration_plot)
        if self.calibration.method is not None:
            self.method_combo.setCurrentText(self.calibration.method)

        self.models_combo = QComboBox()
        self.models_combo.addItems([model for model in MODELS.keys()])
        self.models_combo.currentTextChanged.connect(self.update_calibration_plot)
        if self.calibration.model_func is not None:
            self.method_combo.setCurrentText(self.calibration.calibration_dict["model_type"])
        
        # Initial fit parameters
        number_of_init_parameters = 6
        
        init_params_layout = QHBoxLayout()
        
        self.init_params_edits = {}
        for i in range(number_of_init_parameters):
            edit = QLineEdit()
            edit.editingFinished.connect(self.update_calibration_plot)
            self.init_params_edits[f"a{i}"] = edit
            init_params_layout.addWidget(edit)

        if self.calibration.p0 is not None:
            for i in range(len(self.calibration.p0)):
                self.init_params_edits[f"a{i}"].setText(str(self.calibration.p0[i]))
        
        # Fitted parameters (read-only)
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
        

        # Layout
        fit_settings_layout.addWidget(self.method_combo)
        fit_settings_layout.addWidget(self.models_combo)

        cal_plot_layout.addLayout(fit_settings_layout)
        cal_plot_layout.addLayout(init_params_layout)
        cal_plot_layout.addLayout(fitted_params_layout)
        cal_plot_layout.addWidget(self.plot_widget)

        cal_plot_panel.setLayout(cal_plot_layout)

        return cal_plot_panel


    def save(self) -> None:
        '''
        Convert the current editor fields into a calibration object and save it to disk,
        asking for confirmation before overwriting an existing file.
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

    def populate_calibration_from_edits(self) -> None:
        '''
        Collect the current metadata, fit settings, overlap values, and calibration points
        into a dictionary and rebuild the active Calibration object from it.
        '''
        info = {}
        
        for field in self.fields.keys():
            if field == "Comments":
                info[field] = self.fields[field].toPlainText()
            else:
                info[field] = self.fields[field].text()

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
        info["bunch_overlap"] = self.get_bunch_overlap_params_from_edits()
        info["calibration_points"] = self.get_calibration_points()   
        
        # Save with timestamp
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d, %H:%M:%S")
        info["created_date"] = timestamp

        self.calibration = Calibration(info)

    def get_initial_fit_parameters_from_edits(self) -> list:
        """Return the initial fit parameters currently entered in the calibration editor."""
        parameter_list = []
        for i in range(len(self.init_params_edits)):
            if self.init_params_edits[f"a{i}"].text() == "":
                continue
            else:
                parameter_list.append(float(self.init_params_edits[f"a{i}"].text()))
        return parameter_list

    def get_bunch_overlap_params_from_edits(self)-> dict:
        params = {}

        params["repetition_time"] = float(self.bunch_overlap_edits["reptime"].text())
        params["ROI_first"] = [float(self.bunch_overlap_edits["roi_first_min"].text()),
                               float(self.bunch_overlap_edits["roi_first_max"].text())]
        params["ROI_last"] = [float(self.bunch_overlap_edits["roi_last_min"].text()),
                               float(self.bunch_overlap_edits["roi_last_max"].text())]
        #    end = float(start.text()) if start.text() else None
            
        return params
        
    def populate_bunch_overlap_param_edits_from_calibration(self):
        
        params = self.calibration.bunch_overlap_params
        self.populate_bunch_overlap_parameters_from_dict(params)


    def populate_bunch_overlap_parameters_from_dict(self, params: dict):
        assignments = [(self.bunch_overlap_edits["reptime"], params["repetition_time"]),
                       (self.bunch_overlap_edits["roi_first_min"], params["ROI_first"][0]),
                       (self.bunch_overlap_edits["roi_first_max"], params["ROI_first"][1]),
                       (self.bunch_overlap_edits["roi_last_min"], params["ROI_last"][0]),
                       (self.bunch_overlap_edits["roi_last_max"], params["ROI_last"][1]),
                       ]

        for end, start in assignments:
            end.setText(str(start)) if start != None else None


    def update_calibration_plot(self) -> None:
        self.populate_calibration_from_edits()
        plot_calibration_pg(self.calibration, self.plot_widget)
        
        if self.calibration.popt is not None:
            for i in range(len(self.calibration.p0)):
                self.fitted_params_edits[f"a{i}"].setText(f"{self.calibration.popt[i]:.2e}")
        


    def get_calibration_points(self) -> list:
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

    def populate_points_table(self) -> None:
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
        
    def points_from_manual_entries(self) -> None:
        '''
        Read the manually entered point fields and append the new calibration point to the
        active calibration data.
        '''
        
        for key, entry in self.manual_points_entries.items():
            print(entry.text())
            if entry.text() == None:
                print(f"Entry {key} is None")
                return
        
        self.add_points_row(self.manual_points_entries)
    


    def add_points_row(self, entries: dict) -> None:
        '''
        Append a single calibration point to the active calibration arrays and refresh the
        displayed table and plot.
        '''


        self.calibration.x_values = np.append(self.calibration.x_values, float(entries["x"].text().strip()))
        self.calibration.x_err = np.append(self.calibration.x_err, float(entries["xerr"].text().strip()))
        self.calibration.y_values = np.append(self.calibration.y_values, float(entries["y"].text().strip()))
        self.calibration.y_err = np.append(self.calibration.y_err, float(entries["yerr"].text().strip()))

        # Repopulate points_table
        self.populate_points_table()

        # Empty the QLineEdits
        for entry in self.manual_points_entries.values():
            entry.clear()
        self.update_calibration_plot()
        
    def remove_points_row(self)->None:
        '''
        Remove the rows, which are currently selected
        '''
        
        rows = sorted(
            {idx.row() for idx in self.points_table.selectedIndexes()},
            reverse=True,
        )

        for row in rows:
            self.points_table.removeRow(row)
        
        self.update_calibration_plot()



    def browse_scans(self, verbalize=False):
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select file",
            "",
            "All Files (*.*)"
        )

        filename= os.path.basename(file_path)

        # Enable choice later
        coinc_key = ["1E", "E"]

        col_idx = 0

        scan = read_scan(file_path=file_path)

        for key in coinc_key:
            validKey = True
            for spec in scan.spectra:
                if key in list(spec.data_dict.keys()):
                    continue
                else:
                    validKey = False
                    break
            if validKey:
                for spec in scan.spectra:
                    if verbalize: print("started the loop")
                    data = spec.data_dict[key]
                    h = 0.5  # chosen bin width
                    edges = np.arange(data.min(), data.max() + h, h)

                    counts, edges = np.histogram(data, bins=edges)
                    spec.x = edges[:-1]
                    spec.y = counts
                
                page = self.plot_tab_widget.add_scan_analysis(scan)
                page.point_submitted.connect(self.handle_point_submission_request)
    
    def add_dummy_scan(self):

        spectra = []
        for scan in np.linspace(0,5,10):

            x = np.linspace(0,100,1000)
            y = (np.exp(-(x - (50 - scan*2))**2/(2*3**2))+0.05*np.random.rand(len(x)))

            spectra.append(ScanSpectrum(scan,x=x,y=y))

        scan_data = ScanData(spectra)

        page = self.plot_tab_widget.add_scan_analysis(scan_data)
        page.point_submitted.connect(self.handle_point_submission_request)
    
    def handle_point_submission_request(self, request):
        '''
        Receive a point submitted from the scan-analysis view and add it to the current
        calibration dataset.
        '''
        
        for key, entry in request.items():
            print(entry.text())
            if entry.text() == None:
                print(f"Entry {key} is None")
                return
        
        self.add_points_row(request)
    