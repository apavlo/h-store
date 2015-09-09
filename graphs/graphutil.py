import os
import sys
import logging
from options import *

import pylab
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_pdf import PdfPages

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## SAVE GRAPH
## ==============================================
def saveGraph(fig, output, width=OPT_GRAPH_WIDTH, height=OPT_GRAPH_HEIGHT):
    size = fig.get_size_inches()
    dpi = fig.get_dpi()
    LOG.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))

    new_size = (width / float(dpi), height / float(dpi))
    fig.set_size_inches(new_size)
    fig.set_dpi(OPT_GRAPH_DPI)
    new_size = fig.get_size_inches()
    new_dpi = fig.get_dpi()
    LOG.debug("New Size Inches: %s, DPI: %d" % (str(new_size), new_dpi))

    #fig.savefig(os.path.join(dataDir, 'motivation-%s.png' % dataSet), format='png')
    
    pp = PdfPages(output)
    fig.savefig(pp, format='pdf', bbox_inches='tight')
    pp.close()
    LOG.info("OUTPUT: %s", output)
## DEF

## ==============================================
## MAKE GRID
## ==============================================
def makeGrid(ax):
    axes = ax.get_axes()
    axes.yaxis.grid(True,
        linestyle=OPT_GRID_LINESTYLE,
        which=OPT_GRID_WHICH,
        color=OPT_GRID_COLOR
    )
    ax.set_axisbelow(True)
## DEF

