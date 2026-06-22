from __future__ import annotations

from PySide6 import QtWidgets, QtGui
import pyqtgraph as pg
import numpy as np

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from numpy.typing import ArrayLike, NDArray


def interactive(
    data: NDArray,
    bins: int | tuple[int, int] = 50,
    range: ArrayLike | None = None,  # noqa
    xlabel: str = "first electron",
    ylabel: str = "second electron",
    units: str | None = None,
) -> CoincWidget:
    # get the Qt application
    app = QtWidgets.QApplication.instance()

    if app is None:
        # create a new application if none exists
        app = QtWidgets.QApplication([])

    # create the main window
    coinc = CoincWidget(
        data,
        bins=bins,
        range=range,
        xlabel=xlabel,
        ylabel=ylabel,
        units=units,
    )
    coinc.show()

    # start the event loop
    app.exec()

    return coinc


class CoincWidget(QtWidgets.QWidget):
    def __init__(
        self,
        data: NDArray,
        bins: int = 50,
        range: ArrayLike | None = None,  # noqa
        xlabel: str = "first electron",
        ylabel: str = "second electron",
        units: str | None = None,
    ) -> None:
        super().__init__()
        self.setWindowTitle("metro-eval - coincidence")
        self.resize(800, 800)
        self.layout = QtWidgets.QVBoxLayout(self)

        # set the font
        font = QtGui.QFont()
        font.setPointSize(11)
        self.setFont(font)
        self.css_style = {"font-size": "11pt"}

        # store the data
        self.data = data

        # bin the data
        hist, self.xe, self.ye = np.histogram2d(
            data[:, 0], data[:, 1], bins=bins, range=range
        )

        # create the menu layout
        self.menu_layout = QtWidgets.QHBoxLayout()
        self.layout.addLayout(self.menu_layout)
        self.menu_layout.addStretch()
        self._add_roi_zoom()
        self.menu_layout.addSpacing(40)
        self._add_xbins()
        self.menu_layout.addSpacing(20)
        self._add_ybins()
        self.menu_layout.addStretch()

        # create the plot layout
        self.plot_layout = pg.GraphicsLayoutWidget()
        self.plot_layout.setBackground("white")
        self.layout.addWidget(self.plot_layout)
        self._add_coinc_map(xlabel, ylabel, units)
        self._add_x_projection()
        self._add_y_projection()
        self._add_colorbar()

        # set spacing and stretch of the plots
        layout = self.plot_layout.ci.layout
        layout.setHorizontalSpacing(20)
        layout.setVerticalSpacing(20)
        layout.setRowStretchFactor(0, 1)
        layout.setRowStretchFactor(1, 3)
        layout.setColumnStretchFactor(0, 3)
        layout.setColumnStretchFactor(1, 1)

        # plot the data
        self.update_binning()

        # connect signals
        self.roi_zoom.toggled.connect(self.toggle_roi_zoom)
        self.roi_zoom.setChecked(True)
        self.xbins.sigValueChanged.connect(self.update_binning)
        self.ybins.sigValueChanged.connect(self.update_binning)

    def _add_roi_zoom(self):
        label = QtWidgets.QLabel("roi zoom:")
        label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(label)
        self.roi_zoom = QtWidgets.QPushButton("off", parent=self)
        self.roi_zoom.setCheckable(True)
        self.roi_zoom.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(self.roi_zoom)

    def _add_xbins(self):
        label = QtWidgets.QLabel("x bins:")
        label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(label)
        self.xbins = pg.SpinBox(
            parent=self,
            value=len(self.xe) - 1,
            bounds=(1, None),
            step=10,
            int=True,
            compactHeight=False,
        )
        self.xbins.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(self.xbins)

    def _add_ybins(self):
        label = QtWidgets.QLabel("y bins:")
        label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(label)
        self.ybins = pg.SpinBox(
            parent=self,
            value=len(self.ye) - 1,
            bounds=(1, None),
            step=10,
            int=True,
            compactHeight=False,
        )
        self.ybins.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(self.ybins)

    def _add_coinc_map(self, xlabel: str, ylabel: str, units: str):
        self.plot_coinc = self.plot_layout.addPlot(
            row=1, col=0, name="coinc2d"
        )
        self.plot_coinc.setLabel(
            "bottom", text=xlabel, units=units, **self.css_style
        )
        self.plot_coinc.setLabel(
            "left", text=ylabel, units=units, **self.css_style
        )
        self.plot_coinc.setXRange(self.xe[0], self.xe[-1])
        self.plot_coinc.setYRange(self.ye[0], self.ye[-1])
        self.plot_coinc.setLimits(
            xMin=self.xe[0],
            xMax=self.xe[-1],
            yMin=self.ye[0],
            yMax=self.ye[-1],
        )
        self.img = pg.ImageItem()
        self.plot_coinc.addItem(self.img)

    def _add_x_projection(self):
        self.plot_x = self.plot_layout.addPlot(row=0, col=0, name="x")
        self.plot_x.setLabel("left", text="counts", **self.css_style)
        self.plot_x.setLimits(xMin=self.xe[0], xMax=self.xe[-1], yMin=0)
        self.plot_x.hideAxis("bottom")
        self.plot_x.setXLink(self.plot_coinc)
        self.hist_x = pg.BarGraphItem(
            height=0,
            x0=self.xe[:-1],
            x1=self.xe[1:],
            pen=pg.mkPen("#0868ac"),
            brush=pg.mkBrush("#f0f9e8"),
        )
        self.plot_x.addItem(self.hist_x)

    def _add_y_projection(self):
        self.plot_y = self.plot_layout.addPlot(row=1, col=1, name="y")
        self.plot_y.setLabel("bottom", text="counts", **self.css_style)
        self.plot_y.setLimits(yMin=self.ye[0], yMax=self.ye[-1], xMin=0)
        self.plot_y.hideAxis("left")
        self.plot_y.setYLink(self.plot_coinc)
        self.hist_y = pg.BarGraphItem(
            width=0,
            y0=self.ye[:-1],
            y1=self.ye[1:],
            x0=0,
            pen=pg.mkPen("#0868ac"),
            brush=pg.mkBrush("#f0f9e8"),
        )
        self.plot_y.addItem(self.hist_y)

    def _add_colorbar(self):
        self.plot_cb = self.plot_layout.addPlot(row=0, col=1)
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

    def toggle_roi_zoom(self, checked: bool) -> None:
        if checked:
            self.roi_zoom.setText("on")
            # connect signals
            self.plot_coinc.sigXRangeChanged.connect(self.update_xroi)
            self.plot_coinc.sigYRangeChanged.connect(self.update_yroi)
            self.plot_x.sigXRangeChanged.connect(self.update_xroi)
            self.plot_y.sigYRangeChanged.connect(self.update_yroi)
            # update x and y
            vr = self.plot_coinc.viewRange()
            self.update_xroi(None, vr[0])
            self.update_yroi(None, vr[1])

        else:
            self.roi_zoom.setText("off")
            # disconnect signals
            self.plot_coinc.sigXRangeChanged.disconnect(self.update_xroi)
            self.plot_coinc.sigYRangeChanged.disconnect(self.update_yroi)
            self.plot_x.sigXRangeChanged.disconnect(self.update_xroi)
            self.plot_y.sigYRangeChanged.disconnect(self.update_yroi)
            # update x and y
            self.update_xroi(None, (self.xe[0], self.xe[-1]))
            self.update_yroi(None, (self.ye[0], self.ye[-1]))

    def update_xroi(self, vb, xr: tuple[float, float]) -> None:
        xroi = (self.data[:, 0] > xr[0]) & (self.data[:, 0] < xr[1])
        ydata = self.data[:, 1][xroi]
        hist = np.histogram(ydata, self.ye)[0]
        self.hist_y.setOpts(width=hist, y0=self.ye[:-1], y1=self.ye[1:])

    def update_yroi(self, vb, yr: tuple[float, float]) -> None:
        yroi = (self.data[:, 1] > yr[0]) & (self.data[:, 1] < yr[1])
        xdata = self.data[:, 0][yroi]
        hist = np.histogram(xdata, self.xe)[0]
        self.hist_x.setOpts(height=hist, x0=self.xe[:-1], x1=self.xe[1:])

    def update_binning(self) -> None:
        # get the new binning
        xbins = self.xbins.value()
        ybins = self.ybins.value()

        # update the coincidence map
        hist, self.xe, self.ye = np.histogram2d(
            self.data[:, 0],
            self.data[:, 1],
            bins=(xbins, ybins),
            range=[[self.xe[0], self.xe[-1]], [self.ye[0], self.ye[-1]]],
        )

        # construct the qt transform for the image
        xs = (self.xe[-1] - self.xe[0]) / len(self.xe)
        ys = (self.ye[-1] - self.ye[0]) / len(self.ye)
        tr = QtGui.QTransform()
        tr.scale(xs, ys)
        tr.translate(self.xe[0] / xs, self.ye[0] / ys)

        self.img.setTransform(tr)
        self.img.setImage(hist, autoLevels=True)

        # update the colorbar
        self.cb.setLevels((0, np.nanmax(hist)))

        # update x and y
        if self.roi_zoom.isChecked():
            vr = self.plot_coinc.viewRange()
            self.update_xroi(None, vr[0])
            self.update_yroi(None, vr[1])

        else:
            self.update_xroi(None, (self.xe[0], self.xe[-1]))
            self.update_yroi(None, (self.ye[0], self.ye[-1]))


class SpecWidget(QtWidgets.QWidget):
    def __init__(
        self,
        data: NDArray,
        bins: int = 50,
        range: ArrayLike | None = None,  # noqa
        xlabel: str = "first electron",
        units: str | None = None,
    ) -> None:
        super().__init__()
        self.setWindowTitle("metro-eval - spec")
        self.resize(800, 600)
        self.layout = QtWidgets.QVBoxLayout(self)

        # set the font
        font = QtGui.QFont()
        font.setPointSize(11)
        self.setFont(font)
        self.css_style = {"font-size": "11pt"}

        # store the data
        self.data = data

        # bin the data
        hist, self.xe = np.histogram(data, bins=bins, range=range)

        # create the menu layout
        self.menu_layout = QtWidgets.QHBoxLayout()
        self.layout.addLayout(self.menu_layout)
        self.menu_layout.addStretch()
        self._add_bins()
        self.menu_layout.addStretch()

        # create the plot layout
        self.plot_layout = pg.GraphicsLayoutWidget()
        self.plot_layout.setBackground("white")
        self.layout.addWidget(self.plot_layout)
        self._add_spec(xlabel, units)

        # plot the data
        self.update_binning()

        # connect signals
        self.bins.sigValueChanged.connect(self.update_binning)

    def _add_bins(self):
        label = QtWidgets.QLabel("bins:")
        label.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(label)
        self.bins = pg.SpinBox(
            parent=self,
            value=len(self.xe) - 1,
            bounds=(1, None),
            step=10,
            int=True,
            compactHeight=False,
        )
        self.bins.setSizePolicy(
            QtWidgets.QSizePolicy.Fixed,
            QtWidgets.QSizePolicy.Fixed,
        )
        self.menu_layout.addWidget(self.bins)

    def _add_spec(self, xlabel, units):
        self.plot = self.plot_layout.addPlot(row=0, col=0, name="spec")
        self.plot.setLabel(
            "bottom", text=xlabel, units=units, **self.css_style
        )
        self.plot.setLabel("left", text="counts", **self.css_style)
        self.plot.setXRange(self.xe[0], self.xe[-1])
        self.plot.setLimits(xMin=self.xe[0], xMax=self.xe[-1], yMin=0)
        self.hist = pg.BarGraphItem(
            height=0,
            x0=self.xe[:-1],
            x1=self.xe[1:],
            pen=pg.mkPen("#0868ac"),
            brush=pg.mkBrush("#f0f9e8"),
        )
        self.plot.addItem(self.hist)

    def update_binning(self) -> None:
        # get the new binning
        bins = self.bins.value()
        xr = [self.xe[0], self.xe[-1]]
        # update the histogram
        hist, self.xe = np.histogram(self.data, bins=bins, range=xr)
        self.hist.setOpts(height=hist, x0=self.xe[:-1], x1=self.xe[1:])
