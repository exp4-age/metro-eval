import logging
from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QMessageBox,
    QSizePolicy,
    QWidget,
    QHeaderView,
    QTableWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
)

class MaskSelectionWidget(QWidget):
    """Widget for defining per-column value filters applied to the current data.

    Each row represents a mask condition. The widget emits a request that the
    controller applies to the workflow state.
    """

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
    # Selected-mask handling
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
    # Mask request emission
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
