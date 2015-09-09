#!/usr/bin/env python

# Example execution 
# export b=tpcc ; export dataDir=/home/pavlo/Documents/H-Store/Papers/anticache/data/evictions
# ant compile hstore-benchmark -Dproject=$b -Dclient.interval=500 \
#    -Dsite.anticache_enable=true -Dsite.anticache_profiling=true \
#    -Dclient.output_memory=$dataDir/$b-memory.csv \
#    -Dclient.output_csv=$dataDir/$b-throughput.csv \
#    -Dclient.output_anticache_history=$dataDir/$b-evictions.csv

import os
import sys
import re
import logging
import fnmatch
import string
import pylab
import numpy as np
import matplotlib.pyplot as plot
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator
from matplotlib.colors import colorConverter
from pprint import pprint,pformat

from options import *
import graphutil
import datautil

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
## CONFIGURATION
## ==============================================

OPT_FONT_NAME = 'Proxima Nova'
OPT_GRAPH_HEIGHT = 400
OPT_LINE_WIDTH = 5.0
OPT_LINE_STYLE = '-'
OPT_LINE_ALPHA = 0.25
OPT_MARKER_SIZE = 12.0
OPT_COLORS = ['#0A8345'] + list(OPT_COLORS)
OPT_LABEL_WEIGHT = 'bold'

## ==============================================
## CREATE THROUGHPUT GRAPH
## ==============================================
def createThroughputGraph(benchmark, memorySizes, read_pct, hstoreData, mysqlData, memcachedData,
                          y_max=None,
                          linestyles=None,
                          linewidths=None,
                          linecolors=None,
                          markersizes=None,
                          markerfacecolors=None):
    datasets = (hstoreData, mysqlData, memcachedData)
    labels = (OPT_LABEL_HSTORE, OPT_LABEL_MYSQL, OPT_LABEL_MEMCACHED)
    
    # Per Memory Size
    if linestyles is None: linestyles = [OPT_LINE_STYLE]*len(memorySizes)
    if linewidths is None: linewidths = [OPT_LINE_WIDTH]*len(memorySizes)
    if markersizes is None: markersizes = [OPT_MARKER_SIZE]*len(memorySizes)
    if linecolors is None: linecolors = [OPT_COLORS]*len(memorySizes)
    if markerfacecolors is None: markerfacecolors = linecolors
    
    fig = plot.figure()
    ax1 = fig.add_subplot(111)

    x_values = [ ]
    x_labels = [ ]
    for d in hstoreData:
        delta = 0 #  d["throughputs"][0][0]
        labelDelta = 0
        for r in d["throughputs"]:
            x_values.append(r[0] - delta)
            x_labels.append(str(x_values[-1] + labelDelta))
        break
    assert x_values
    
    for memIdx in xrange(len(memorySizes)):
        mem = memorySizes[memIdx]
        for dataIdx in xrange(len(datasets)):
            y_values = [None] * len(x_values)
            for d in datasets[dataIdx]:
                if d["mem"] != mem or d["read_pct"] != read_pct: continue
                y_values = [ ]
                #print benchmark, labels[dataIdx]
                #pprint(d["throughputs"])
                if dataIdx == 0:
                    for i in xrange(len(x_values)):
                        tps = d["throughputs"][i][-1]
                        y_values.append(tps)
                else:
                    for i in xrange(len(x_values)*2):
                        if i % 2 != 0: continue
                        tps = d["throughputs"][i][-1]
                        y_values.append(tps)
                break
            ## FOR
            y_max = max(y_max, max(y_values))
            
            marker = OPT_MARKERS[dataIdx] if markersizes[memIdx] else None
            ax1.plot(x_values, y_values,
                     color=linecolors[memIdx][dataIdx],
                     linewidth=linewidths[memIdx],
                     linestyle=linestyles[memIdx],
                     marker=marker,
                     markeredgecolor='black',#linecolors[memIdx][dataIdx],
                     markersize=markersizes[memIdx],
                     markerfacecolor=markerfacecolors[memIdx][dataIdx],
                     label=labels[dataIdx]
            )
        ## FOR
    ## FOR
    LOG.debug("y_max = %d", y_max)
    
    # GRID
    graphutil.makeGrid(ax1)

    # AXES
    axes = ax1.get_axes()
    axes.set_ylim(0, y_max+10000)
    axes.set_xlim(10, None)
    
    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(#[p[0] for p in bars],
                labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.02, 1.0, 0.10),
                loc=1,
                ncol=len(datasets),
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    
    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_TIME, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(6))
    ax1.set_xticklabels(x_labels)
    #xLabels = map(lambda x: "%d" % (x / 1000), ax1.get_xticks())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    
    return (fig, y_max)
## DEF    

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    
    hstoreData = datautil.loadHStoreData()
    #pprint(hstoreData)
    #print "="*150
    
    mysqlData = datautil.loadMySQLData(memcached=False)
    #pprint(mysqlData)
    #print "="*150
    
    memcachedData = datautil.loadMySQLData(memcached=True)
    #pprint(memcachedData)
    #print "="*150
    
    memorySizes = [ 1, 2 ]
    readPcts = [ 90 ]
    readLabels = dict(map(lambda x: (datautil.OPT_HSTORE_TAGS[x], x), datautil.OPT_HSTORE_TAGS.keys()))
    y_max = None

    linecolors = [OPT_COLORS] + [[colorConverter.to_rgba(rgb, OPT_LINE_ALPHA) for rgb in OPT_COLORS]]*2
    markersizes = [OPT_MARKER_SIZE] + [None]*2
    
    for benchmark in hstoreData.keys():
        for read_pct in readPcts:
            y_max = None
            for memIdx in xrange(len(memorySizes)):
                mem = map(int, reversed(memorySizes[:memIdx+1]))
                fig,y_max = createThroughputGraph(benchmark, mem, read_pct,
                                                  hstoreData[benchmark],
                                                  mysqlData[benchmark],
                                                  memcachedData[benchmark],
                                                  y_max=y_max,
                                                  linecolors=linecolors,
                                                  markersizes=markersizes,
                )
                fileName = "throughput-%s-%s-%02d.pdf" % (benchmark, readLabels[read_pct], memIdx)
                graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
        ## FOR
    ## FOR

## MAIN