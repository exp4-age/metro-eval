from __future__ import annotations

from PySide6 import QtWidgets, QtCore, QtGui
import pyqtgraph as pg
import numpy as np


class CoincWidget(pg.GraphicsLayoutWidget):
    def __init__(
        self,
        data: np.ndarray,
        xe: np.ndarray,
        ye: np.ndarray,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setBackground("white")

        self.data = data
        self.xe = xe
        self.ye = ye

        xs = (xe[-1] - xe[0]) / len(xe)
        ys = (ye[-1] - ye[0]) / len(ye)
        tr = QtGui.QTransform()
        tr.scale(xs, ys)
        tr.translate(xe[0] / xs, ye[0] / ys)

        self.plot_coinc = self.addPlot(row=1, col=0, name="coinc2d")
        self.img = pg.ImageItem()
        self.img.setTransform(tr)
        self.img.setImage(self.data)
        self.plot_coinc.addItem(self.img)
        self.plot_coinc.setXRange(xe[0], xe[-1])
        self.plot_coinc.setYRange(ye[0], ye[-1])
        self.plot_coinc.setLimits(
            xMin=xe[0], xMax=xe[-1], yMin=ye[0], yMax=ye[-1]
        )

        self.plot_x = self.addPlot(row=0, col=0, name="x")
        self.plot_x.hideAxis("bottom")
        self.plot_x.setXLink(self.plot_coinc)
        self.hist_x = pg.BarGraphItem(
            x0=xe[:-1],
            x1=xe[1:],
            height=np.sum(self.data, axis=1),
            pen=pg.mkPen("#0868ac"),
            brush=pg.mkBrush("#f0f9e8"),
        )
        self.plot_x.addItem(self.hist_x)

        self.plot_y = self.addPlot(row=1, col=1, name="y")
        self.plot_y.hideAxis("left")
        self.plot_y.setYLink(self.plot_coinc)
        self.hist_y = pg.BarGraphItem(
            y0=ye[:-1],
            y1=ye[1:],
            x0=0,
            width=np.sum(self.data, axis=0),
            pen=pg.mkPen("#0868ac"),
            brush=pg.mkBrush("#f0f9e8"),
        )
        self.plot_y.addItem(self.hist_y)

        self.plot_cb = self.addPlot(row=0, col=1)
        self.plot_cb.hideAxis("bottom")
        self.plot_cb.hideAxis("left")
        cmap = pg.ColorMap(
            None,
            color=[
                "white",
                "#f0f9e8",
                "#bae4bc",
                "#7bccc4",
                "#43a2ca",
                "#0868ac",
            ],
        )
        self.cb = pg.ColorBarItem(colorMap=cmap)
        self.cb.setImageItem(self.img, insert_in=self.plot_cb)

        layout = self.ci.layout
        layout.setHorizontalSpacing(20)
        layout.setVerticalSpacing(20)
        layout.setRowStretchFactor(0, 1)
        layout.setRowStretchFactor(1, 3)
        layout.setColumnStretchFactor(0, 3)
        layout.setColumnStretchFactor(1, 1)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(
        self,
        data: np.ndarray,
        xe: np.ndarray,
        ye: np.ndarray,
    ) -> None:
        super().__init__()
        self.setWindowTitle("metro-eval - coincidence")
        self.setGeometry(100, 100, 800, 800)
        self.main_widget = QtWidgets.QWidget(self)
        self.setCentralWidget(self.main_widget)
        self.layout = QtWidgets.QVBoxLayout(self.main_widget)

        self.coinc_widget = CoincWidget(data, xe, ye, parent=self.main_widget)
        self.layout.addWidget(self.coinc_widget)
