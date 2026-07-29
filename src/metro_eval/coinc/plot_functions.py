'''
Handle files
'''


from typing import TYPE_CHECKING
from numpy.typing import ArrayLike, NDArray

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec


from PySide6 import QtWidgets, QtGui
import pyqtgraph as pg

def generate_array(rows=100000, cols=4, max_value = 3000):
    arr = np.random.rand(rows, cols) * max_value
    arr.sort(axis=1)
    arr = arr[:, ::-1]
    
    # enforce strict decrease
    eps = 1e-8
    for i in range(cols):
        arr[:, i] -= i * eps
    
    return arr


def plot_E1(array, hist_kwargs=None, plot_kwargs=None):
    if not hist_kwargs:
        hist_kwargs = dict(bins=100)
    values, edges = np.histogram(array[:,0], **hist_kwargs)
        
    fig = plt.figure(figsize=(3,2))
    plt.plot(edges[:-1], values)
    plt.xlabel("Electron time of flight")
    
def hist_1D(array, column, bins=None, range=None, density=False):
    hist_kwargs = dict(bins=bins, range=range, density=density)
    values, edges = np.histogram(array[:,column], **hist_kwargs)
    return values, edges

def plot_1D(array, column, bins=None, range=None, color = 'blue', density=False,
            xlabel = "Electron TOF (ns)",
            ylabel = "Intensity (arb. units)",
            hist_kwargs=None, 
            plot_kwargs=None):
    hist_kwargs = hist_kwargs or {}
    hist_kwargs.setdefault('bins', bins)
    if range is not None:
        hist_kwargs['range'] = range
    hist_kwargs.setdefault('density', density)
    
    values, edges = np.histogram(array[:,column], **hist_kwargs)
    
    plot_kwargs = plot_kwargs or {}
    plot_kwargs.setdefault('label', f"Particle {column+1} of {array.shape[1]}")
        
    plt.figure(figsize=(4,2), layout="constrained")
    plt.stairs(values, edges, **plot_kwargs)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()

cmap_UK_blue = None
def bin_2D(array, columns, bins = None, range=None, hist_kwargs=None):
    col1, col2 = columns
    hist_kwargs = hist_kwargs or {}
    hist_kwargs.setdefault('bins', bins)
    if range is not None:
        hist_kwargs['range'] = range
    
    coincmap, xedges, yedges = np.histogram2d(array[:,col1], array[:,col2], 
                                              **hist_kwargs)
    return coincmap, xedges, yedges

def plot_2D(coincmap, xedges, yedges, figsize=(6,4), cmap='YlOrRd',
                   title=None, norm="log", vmin=1, vmax=None, num=None,
                   xlabel="",
                   ylabel="",
                   leftspace=0.12,
                   bottomspace=0.12,
                   topspace=0.95,
                   rightspace=0.95,
                   wspace=0.05,
                   hspace=0.05,
                   width_ratio=[3, 1],
                   height_ratio=[1, 3],
                   fontsize=8,
                   ):
    """Plot a coincedence map and its projections on the x and y axes. Gives
    more control than the standard function from agepy.

    Parameters
    ----------
    coincmap : numpy.ndarray
        2d array of shape (m,n) containing the coincidence map. In most
        cases this will be the output of ``numpy.histogram2d()``.
    xedges : numpy.ndarray
        1d array of shape (m+1) containing the bin edges of the x-axis.
    yedges : numpy.ndarray
        1d array of shape (n+1) containing the bin edges of the y-axis.
    figsize : tuple, optional
        Figure size in inches. Default: None
    cmap : matplotlib.colors.Colormap or str or None, optional
        Colormap passed to ``matplotlib.pyplot.pcolormesh()``.
        Default: 'YlOrRd'
    title : str, optional
        Title of the figure. Default: None
    norm : str or matplotlib.colors.Normalize or None, optional
        Normalization passed to ``matplotlib.pyplot.pcolormesh()``.
        Default: None
    vmin, vmax : float, optional
        Minimum and maximum value for the colormap passed to
        ``matplotlib.pyplot.pcolormesh()``. Default: 1, None
    num: int or str or matplotlib.figure.Figure, optional
        Figure identifier passed to ``matplotlib.pyplot.figure()``.
    xlabel, ylabel : str, optional
        Labels of the x and y axes. Default:
        "early electron kinetic energy", "late electron kinetic energy"

    Returns
    -------
    fig : matplotlib.figure.Figure
        Matplotlib Figure object.
    ax: tuple of matplotlib.axes.Axes
        Tuple of matplotlib Axes objects containing the coincidence map,
        the projection on the x-axis and the projection on the y-axis.

    """
    fig = plt.figure(num=num, figsize=figsize, clear=True)

    # grid with columns=2, row=2
    gs = gridspec.GridSpec(2, 2, width_ratios=width_ratio, height_ratios=height_ratio,
                           wspace=wspace, hspace=hspace,
                           left=leftspace, bottom=bottomspace, right=rightspace, top=topspace,)
    # coinc matrix is subplot 2: lower left
    ax_coinc = plt.subplot(gs[2])
    # spectrum of E0 is subplot 0: upper left
    ax_x = plt.subplot(gs[0], sharex=ax_coinc)
    # spectrum of E1 is subplot 3: lower right
    ax_y = plt.subplot(gs[3], sharey=ax_coinc)
    # colorbar is subplot 1: upper right
    ax_cb = plt.subplot(gs[1])

    # sum spectrum of E0 (top)
    hist_x = np.sum(coincmap, axis=1)
    ax_x.set_xlim(xedges[0], xedges[-1])
    line_x = ax_x.stairs(hist_x, xedges, color='k')

    # sum spectrum of E1 (right)
    hist_y = np.sum(coincmap, axis=0)
    ax_y.set_ylim(yedges[0], yedges[-1])
    line_y = ax_y.stairs(hist_y, yedges, color='k', orientation="horizontal")

    # coinc matrix
    X, Y = np.meshgrid(xedges, yedges)
    pcm = ax_coinc.pcolormesh(X, Y, coincmap.T, cmap=cmap, norm=norm,
                              vmin=vmin, vmax=vmax, rasterized=True)

    # Generate a colorbar for the histogram in the upper right panel
    ax_cb.axis("off")
    ax_cb_inset = ax_cb.inset_axes([0.05, 0.1, 0.3, 0.9])
    cb = fig.colorbar(pcm, cax=ax_cb_inset)

    # Set the labels
    ax_coinc.set_xlabel(xlabel, fontsize = fontsize)
    ax_coinc.set_ylabel(ylabel, fontsize = fontsize)
    ax_coinc.tick_params(axis='both', labelsize = fontsize )
    ax_cb_inset.tick_params(axis='y', labelsize = fontsize )
    # Remove x and y tick labels
    ax_x.tick_params(axis='both', labelleft=False, labelbottom=False)
    ax_y.tick_params(axis='both', labelleft=False, labelbottom=False)

    # Set the title
    if title is not None:
        fig.suptitle(title, fontsize=fontsize)
    
    return fig, (ax_coinc, ax_x, ax_y, ax_cb_inset)





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


if __name__ == "__main__":
    
    array = generate_array(rows=100000)
    plot_1D(array, 1, density=True, bins=100)
    coincmap, xedges, yedges = bin_2D(array, (0,1), bins=(100,100), range=((0,3000), (0,3000)))
    plot_2D(coincmap, xedges, yedges, xlabel="E1 kinetic energy")
    plt.show()
