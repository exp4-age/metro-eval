from __future__ import annotations

from PySide6 import QtWidgets, QtCore, QtGui
import pyqtgraph as pg
import numpy as np
from iminuit import Minuit, cost
from iminuit.qtwidget import make_widget


def interactive(x: np.ndarray, y: np.ndarray, yerr: np.ndarray | float) -> int:
    # get the Qt application
    app = QtWidgets.QApplication([])

    # create the main window
    mw = MainWindow(x, y, yerr)
    mw.show()

    # start the event loop
    app.exec()

    # return the fit results
    return mw.fit_results()


def linear(x: np.ndarray, m: float, b: float) -> np.ndarray:
    return m * x + b


def quadratic(x, a, b):
    return a + b * x**2


class MainWindow(QtWidgets.QDialog):
    models = {
        "linear": linear,
        "quadratic": quadratic,
    }

    loss = ["linear", "soft_l1"]

    def __init__(
        self,
        x: np.ndarray,
        y: np.ndarray,
        yerr: np.ndarray | float,
    ) -> None:
        # set up the window
        super().__init__()
        self.setWindowTitle("metro-eval - fitting")
        self.resize(1280, 720)

        # set the font
        font = QtGui.QFont()
        font.setPointSize(11)
        self.setFont(font)

        # create the main layout
        self.layout = QtWidgets.QVBoxLayout(self)

        # store the data
        self.x = x
        self.y = y
        self.yerr = yerr

        # create dummy fit widget
        self.fit_widget = QtWidgets.QWidget()
        self.layout.addWidget(self.fit_widget)

        # create the menu layout
        self.menu_layout = QtWidgets.QHBoxLayout()

        # model selection
        # label
        model_label = QtWidgets.QLabel("model:")
        model_label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(model_label)
        # combo box
        self.model_selection = pg.ComboBox(parent=self, items=self.models)
        self.model_selection.setFixedWidth(200)
        self.model_selection.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.model_selection.currentIndexChanged.connect(self.prepare_fit)
        self.menu_layout.addWidget(self.model_selection)

        # add some spacing
        self.menu_layout.addSpacing(20)

        # loss selection
        # label
        loss_label = QtWidgets.QLabel("loss:")
        loss_label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(loss_label)
        # combo box
        self.loss_selection = pg.ComboBox(parent=self, items=self.loss)
        self.loss_selection.setFixedWidth(200)
        self.loss_selection.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.loss_selection.currentIndexChanged.connect(self.prepare_fit)
        self.menu_layout.addWidget(self.loss_selection)

        # add some spacing
        self.menu_layout.addStretch()

        # dialog buttons
        self.button_box = QtWidgets.QDialogButtonBox(parent=self)
        self.button_box.setOrientation(QtCore.Qt.Orientation.Horizontal)
        self.button_box.setStandardButtons(
            QtWidgets.QDialogButtonBox.StandardButton.Cancel
            | QtWidgets.QDialogButtonBox.StandardButton.Ok
        )
        self.button_box.accepted.connect(self.accept)
        self.button_box.rejected.connect(self.reject)
        QtCore.QMetaObject.connectSlotsByName(self)
        self.menu_layout.addWidget(
            self.button_box, alignment=QtCore.Qt.AlignmentFlag.AlignLeft
        )

        self.layout.addLayout(self.menu_layout)

        # create the initial fit widget
        self.prepare_fit()

    def prepare_fit(self) -> None:
        # get the model
        model = self.model_selection.value()

        # get the loss
        loss = self.loss_selection.value()

        # update the cost function
        c = cost.LeastSquares(self.x, self.y, self.yerr, model, loss=loss)

        # starting values
        vals = [1] * c.npar

        # update the Minuit object
        self.m = Minuit(c, *vals)

        # remove the old fit widget
        self.layout.removeWidget(self.fit_widget)

        # update the visualization
        self.fit_widget = make_widget(
            self.m, self.m._visualize(None), {}, False, False
        )

        # insert the new fit widget at the top of the layout
        self.layout.insertWidget(0, self.fit_widget)

        # perform a first fit
        self.fit_widget.fit_button.click()

    def fit_results(self) -> Minuit:
        return self.m
