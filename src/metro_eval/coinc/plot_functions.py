'''
Handle files
'''

import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec

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



if __name__ == "__main__":
    
    array = generate_array(rows=100000)
    plot_1D(array, 1, density=True, bins=100)
    coincmap, xedges, yedges = bin_2D(array, (0,1), bins=(100,100), range=((0,3000), (0,3000)))
    plot_2D(coincmap, xedges, yedges, xlabel="E1 kinetic energy")
    plt.show()
