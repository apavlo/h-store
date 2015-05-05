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
import argparse
import pylab
import numpy as np
import matplotlib.pyplot as plot
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator
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


## ==============================================
## CREATE WORKLOAD SKEW THROUGHPUT GRAPH
## ==============================================
#def createWorkloadSkewGraphs(benchmark, mem, read_pct, hstoreData, mysqlData, memcachedData):
def createWorkloadSkewGraphs(benchmark, mem, read_pct, noAntiCache, hstoreData, hstoreDataApprox, mysqlData, memcachedData):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    # The x-axis is going to be the skew amount
    x_values = set()
    for d in hstoreData:
        x_values.add(d["skew"])
    x_values = map(float, sorted(x_values, reverse=True))
    x_labels = map(str, x_values)
    #x_labels = ['90/10']*(len(x_values)+1)
#    x_labels = {"1.5", "1.25", "1", "0.75", "0.5"}
    print("x_values = %s", x_values)
    print("x_labels = %s", x_labels)
     
    datasets = (hstoreData, hstoreDataApprox, noAntiCache, mysqlData, memcachedData)
    labels = (OPT_LABEL_HSTORE, OPT_LABEL_HSTORE_APPROX, OPT_LABEL_NO_ANTICACHE, OPT_LABEL_MYSQL, OPT_LABEL_MEMCACHED)

    # For each read_pct, get the y-axis max so that they're all
    # on the same scale
    y_maxes = { }
    for i in xrange(len(datasets)):
        for d in datasets[i]:
            if d["read_pct"] != read_pct: continue
            y_max = y_maxes.get(d["read_pct"], None)
            y_maxes[d["read_pct"]] = max(y_max, d["tps"])
        ## FOR
    ## FOR
    LOG.info("mem=%s / read_pct=%s\n%s", mem, read_pct, pformat(y_maxes))

    if mem == 1:  # configure params for 1 extra line
        datasets = (noAntiCache, hstoreData, hstoreDataApprox, mysqlData, memcachedData)
        labels = (OPT_LABEL_NO_ANTICACHE, OPT_LABEL_HSTORE, OPT_LABEL_HSTORE_APPROX, OPT_LABEL_MYSQL, OPT_LABEL_MEMCACHED)
    else:
        datasets = (hstoreData, hstoreDataApprox, mysqlData, memcachedData)
        labels = (OPT_LABEL_HSTORE, OPT_LABEL_HSTORE_APPROX, OPT_LABEL_MYSQL, OPT_LABEL_MEMCACHED)

    for i in xrange(len(datasets)):
        y_values = [None]*len(x_values)
        for d in datasets[i]:
            if d["mem"] != mem or d["read_pct"] != read_pct: continue
            offset = x_values.index(d["skew"])
            y_values[offset] = d["tps"]
        ## FOR
        LOG.info("%s y_values = %s", labels[i], y_values)

        if mem == 1 and i == 0:
            ax1.plot(x_values, y_values,
                     label=labels[i],
                     color='#8D38C9',
                     linewidth=OPT_LINE_WIDTH,
                     marker='v',
                     markersize=OPT_MARKER_SIZE,
            )
        elif mem == 1:
            ax1.plot(x_values, y_values,
                     label=labels[i],
                     color=OPT_COLORS[i-1],
                     linewidth=OPT_LINE_WIDTH,
                     marker=OPT_MARKERS[i-1],
                     markersize=OPT_MARKER_SIZE,
                     )
        else:
            ax1.plot(x_values, y_values,
                     label=labels[i],
                     color=OPT_COLORS[i],
                     linewidth=OPT_LINE_WIDTH,
                     marker=OPT_MARKERS[i],
                     markersize=OPT_MARKER_SIZE,
                     )
    ## FOR
    
    # GRID
    axes = ax1.get_axes()
    if read_pct == 100:
        axes.set_ylim(0, 140000)
    elif read_pct == 90:
        axes.set_ylim(0, 140000)
    elif read_pct == 50:
        axes.set_ylim(0, 45000)
        
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)


    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    num_col = 4
    if mem == 1:
        num_col = 3
    ax1.legend(labels,
                prop=fp,
                bbox_to_anchor=(0.0, 1.1, 1.0, 0.10),
                loc=1,
                ncol=num_col,
                mode="expand",
                shadow=OPT_LEGEND_SHADOW,
                borderaxespad=0.0,
    )
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
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
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR
    
    return (fig)
## DEF

def createBlockSizeGraph(mem, read_pct, blocksize1024, blocksize2048, blocksize4096):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    
    # The x-axis is going to be the skew amount
    x_values = set()
    for d in blocksize1024:
        x_values.add(d["skew"])
    x_values = map(float, sorted(x_values, reverse=True))
    x_labels = map(str, x_values)
    #x_labels = ['90/10']*(len(x_values)+1)
    #    x_labels = {"1.5", "1.25", "1", "0.75", "0.5"}
    print("x_values = %s", x_values)
    print("x_labels = %s", x_labels)
    
    datasets = (blocksize1024, blocksize2048, blocksize4096)
    labels = ("1MB", "2MB", "4MB")
    
    # For each read_pct, get the y-axis max so that they're all
    # on the same scale
    y_maxes = { }
    for i in xrange(len(datasets)):
        for d in datasets[i]:
            if d["read_pct"] != read_pct: continue
            y_max = y_maxes.get(d["read_pct"], None)
            y_maxes[d["read_pct"]] = max(y_max, d["tps"])
    ## FOR
    ## FOR
    LOG.info("mem=%s / read_pct=%s\n%s", mem, read_pct, pformat(y_maxes))
    
    for i in xrange(len(datasets)):
        y_values = [None]*len(x_values)
        for d in datasets[i]:
            if d["mem"] != mem or d["read_pct"] != read_pct: continue
            offset = x_values.index(d["skew"])
            y_values[offset] = d["tps"]
        ## FOR
        LOG.info("%s y_values = %s", labels[i], y_values)
        
        ax1.plot(x_values, y_values,
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, y_maxes[read_pct]+10000) #  + y_max*0.25)
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)
    
    
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
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
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
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR
    
    return (fig)
## DEF




def createBlockSizeBarGraph(mem, read_pct, blocksize128, blocksize256, blocksize512, blocksize1024):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    datasets = (blocksize128, blocksize256, blocksize512, blocksize1024)
    
    #LOG.info(blocksize128)
    
    x_labels = ['128', '256', '512', '1024']
    x_values = [128, 256, 512, 1024]
    
    LOG.info("x_values = %s", x_values)
    LOG.info("x_labels = %s", x_labels)
    
    y_max = 40000
    
    y_values = [None]*len(x_values)
    for i in xrange(len(datasets)):
        for d in datasets[i]:
            if d["mem"] != mem or d["read_pct"] != read_pct: continue
            y_values[i] = d["tps"]
        ## FOR
        LOG.info("%s y_values = %s", x_labels[i], y_values)
    
    ind = np.arange(4)  # the x locations for the groups
    width = 0.55       # the width of the bars
    
    rects1 = ax1.bar(ind, y_values, width, align='center', color='gray')
    ## FOR
    
    ax1.set_ylim(0, y_max)
    
    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_BLOCK_SIZE, fontproperties=fp)
    ax1.minorticks_off()
    
    ax1.set_xticks(ind)
    ax1.set_xticklabels( x_labels )
    ## FOR
    
    return (fig)
## DEF


def createTupleSizeGraph(mem, read_pct, tuplesize128, tuplesize256, tuplesize512, tuplesize1024):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    # The x-axis is going to be the skew amount
    x_values = set()
    for d in tuplesize1024:
        x_values.add(d["skew"])
    x_values = map(float, sorted(x_values, reverse=True))
    x_labels = map(str, x_values)
    #x_labels = ['90/10']*(len(x_values)+1)
    #    x_labels = {"1.5", "1.25", "1", "0.75", "0.5"}
    print("x_values = %s", x_values)
    print("x_labels = %s", x_labels)
    
    datasets = (tuplesize128, tuplesize256, tuplesize512, tuplesize1024)
    labels = ("128B", "256B", "512B", "1024B")

    # For each read_pct, get the y-axis max so that they're all
    # on the same scale
    y_maxes = { }
    for i in xrange(len(datasets)):
        for d in datasets[i]:
            if d["read_pct"] != read_pct: continue
            y_max = y_maxes.get(d["read_pct"], None)
            y_maxes[d["read_pct"]] = max(y_max, d["tps"])
    ## FOR
    ## FOR
    LOG.info("mem=%s / read_pct=%s\n%s", mem, read_pct, pformat(y_maxes))
    
    for i in xrange(len(datasets)):
        y_values = [None]*len(x_values)
        for d in datasets[i]:
            if d["mem"] != mem or d["read_pct"] != read_pct: continue
            offset = x_values.index(d["skew"])
            y_values[offset] = d["tps"]
        ## FOR
        LOG.info("%s y_values = %s", labels[i], y_values)
        
        ax1.plot(x_values, y_values,
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, y_maxes[read_pct]+10000) #  + y_max*0.25)
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)
    
    
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
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
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
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR
    
    return (fig)
## DEF


def createTPCCThroughputGraph(hstoreData,hstoreApproxTpccData, mysqlData, memcachedData):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)

    LOG.info(hstoreData)

    x_labels = ['1', '2', '4', '8']
    x_values = [1, 2, 3, 4]
    LOG.debug("x_values = %s", x_values)
    LOG.debug("x_labels = %s", x_labels)
    
    datasets = (hstoreData, hstoreApproxTpccData, mysqlData, memcachedData)
    labels = (OPT_LABEL_HSTORE, OPT_LABEL_HSTORE_APPROX, OPT_LABEL_MYSQL, OPT_LABEL_MEMCACHED)

    for i in xrange(len(datasets)):
        y_values = [None]*len(x_values)
        y_values = datasets[i]
        ## FOR
        LOG.info("%s y_values = %s", labels[i], y_values)
        
        ax1.plot(x_values, y_values,
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR


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
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)

    graphutil.makeGrid(ax1)

    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    ylim = (0, 50000)
    ax1.set_ylim(ylim)
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_DATA_SIZE, fontproperties=fp)
#    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
#    xlim = ax1.get_xlim()[::-1]
#    xlim = (xlim[0]-1, xlim[1]+1)
    xlim = (0.75, 4.25)
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR
    
    return (fig)


## DEF

## ==============================================
## CREATE READ RATIO THROUGHPUT GRAPH
## ==============================================
# def createReadRatioThroughputGraph(benchmark, data):
#     print benchmark
#     
#     # The x-axis is going to be the read ratio %
#     x_values = set()
#     for d in data:
#         x_values.add(d["read_pct"])
#     x_values = map(int, sorted(x_values, reverse=True))
#     LOG.info("x_values = %s", x_values)
#     
#     # Then we will have a separate line for each memory size 
#     y_values = { }
#     for d in data:
#         mem = d["mem"]
#         if not mem in y_values:
#             y_values[mem] = [ ]
#         # We don't want to use mcsplit values other than for mem 2.0
#         if "mcsplit" in d:
#             if mem != 2 and d["mcsplit"] > 0: continue
#         y_values[mem].append((int(d["read_pct"]), d["tps"]))
#     #pprint(y_values)
#     
#     # GRAPH
#     fig = plot.figure()
#     ax1 = fig.add_subplot(111)
#     
#     # Throughput Lines
#     line_idx = 0
#     y_max = None
#     for mem in sorted(y_values.iterkeys(), reverse=True):
#         # Make sure that we have a y_value for all x_values
#         missing = set(x_values) - set(map(lambda x: x[0], y_values[mem]))
#         if missing:
#             LOG.info("MISSING: %s ==> X_VALUES=%s / FOUND=%s", missing, x_values, y_values[mem])
#             for x in missing:
#                 y_values[mem].append((x, 0))
#         
#         LOG.debug("mem = %s", mem)
#         y_values[mem] = map(lambda x: x[-1], sorted(y_values[mem], key=lambda y: y[0], reverse=True))
#         print "y_values = ", y_values[mem]
#         try:
#             ax1.plot(x_values, y_values[mem],
#                      label="%.1f" % mem,
#                      color=OPT_COLORS[line_idx],
#                      linewidth=2.0,
#                      marker=OPT_MARKERS[line_idx],
#                      markersize=7.0
#             )
#         except:
#             print "mem =", mem
#             print "x_values =", x_values
#             print "y_values =", y_values[mem]
#             raise
#         line_idx += 1
#     ## FOR
#     
#     # GRID
#     axes = ax1.get_axes()
#     axes.set_ylim(0, None) # y_max + y_max*0.25)
#     axes.yaxis.grid(True, linestyle='-', which='major', color='0.85') # color='lightgrey', alpha=0.5)
#     axes.set_axisbelow(True)
#     graphutil.makeGrid(ax1)
#     
#     # LEGEND
#     fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
#     ax1.legend(loc='upper right', shadow=OPT_LEGEND_SHADOW, prop=fp)
#     
#     # Y-AXIS
#     ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, name=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE)
#     ax1.yaxis.set_major_locator(MaxNLocator(5))
#     ax1.minorticks_on()
#     for tick in ax1.yaxis.get_major_ticks():
#         tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
#         tick.label.set_fontname(OPT_FONT_NAME)
#     
#     # X-AXIS
#     ax1.set_xlim(ax1.get_xlim()[::-1])
#     ax1.set_xlabel(OPT_X_LABEL_READ_RATIO, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE)
#     # ax1.xaxis.set_major_locator(MaxNLocator(6))
#     xLabels = map(lambda x: "%d%%" % x, ax1.get_xticks())
#     ax1.set_xticklabels(xLabels)
#     for tick in ax1.xaxis.get_major_ticks():
#         tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
#         tick.label.set_fontname(OPT_FONT_NAME)
#     
#     return (fig)
# ## DEF

def generateGraphName(benchmark, attributes):
    order = attributes["order"]
    name = benchmark + "-" + \
           "-".join(map(lambda x: str(int(attributes[x])), order)) + \
           ".pdf"
    return (name)
## DEF

def createMergeStrategyGraph(mem, read_pct, mergeTuple, mergeBlock):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    datasets = (mergeTuple, mergeBlock)
    labels = ("tuple-merge", "block-merge")
       
    # The x-axis is going to be the skew amount
    x_values = set()
    for d in mergeTuple:
        x_values.add(d["skew"])
    x_values = map(float, sorted(x_values, reverse=True))
    x_labels = map(str, x_values)
    
    for i in xrange(len(datasets)):
        y_values = [None]*len(x_values)
        for d in datasets[i]:
            if d["mem"] != mem or d["read_pct"] != read_pct: continue
            offset = x_values.index(d["skew"])
            y_values[offset] = d["tps"]
        ## FOR
        LOG.info("%s y_values = %s", labels[i], y_values)
        
        ax1.plot(x_values, y_values,
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR

    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 120000) #  + y_max*0.25)
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)
    
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
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)

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
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR

    return (fig)
## DEF

def createLinkedListGraph(colMap, noLru, single, double):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    datasets = (noLru, single, double)
    labels = ("none", "single", "double")
    
    x_values = (1.5, 1.25, 1.0, .75, .5)
    x_values = map(float, sorted(x_values, reverse=True))
    x_labels = map(str, x_values)

    print("x_values = %s", x_values)
    print("x_labels = %s", x_labels)
    
    for i in xrange(len(datasets)):
        
        y_values = [None]*len(x_values)
        for row in datasets[i]:
            y_values[0] = row[colMap["1.5"]]
            y_values[1] = row[colMap["1.25"]]
            y_values[2] = row[colMap["1.0"]]
            y_values[3] = row[colMap["0.75"]]
            y_values[4] = row[colMap["0.5"]]
        
        print(y_values)
        ax1.plot(x_values, y_values,
                 label=labels[i],
                 color=OPT_COLORS[i],
                 linewidth=OPT_LINE_WIDTH,
                 marker=OPT_MARKERS[i],
                 markersize=OPT_MARKER_SIZE,
                 )
    ## FOR
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 100000) #  + y_max*0.25)
    graphutil.makeGrid(ax1)
    axes.set_xlim(.4, 1.6)

    # LEGEND
            # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(
               labels,
               prop=fp,
               bbox_to_anchor=(0.0, 1.02, 1.0, 0.10),
               loc=1,
               ncol=len(datasets),
               mode="expand",
               shadow=OPT_LEGEND_SHADOW,
               borderaxespad=0.0,
               )
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_TIME_MS, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_WORKLOAD_SKEW, fontproperties=fp)
    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
    xlim = ax1.get_xlim()[::-1]
    xlim = (xlim[0], xlim[1])
    ax1.set_xlim(xlim)
    ax1.minorticks_off()
    ax1.set_xticklabels(x_labels)
    ax1.set_xticks(x_values)
    print(x_labels)
    pprint(ax1.xaxis.get_majorticklocs())
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    ## FOR

    return (fig)
## DEF

def createEvictGraph(colMap, construct, write, fetch, tuplemerge, blockmerge):
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    datasets = (construct, write, fetch, blockmerge, tuplemerge)
    labels = ("construct", "write", "fetch", "block\nmerge", "tuple\nmerge")
    
    x_values = (1, 2, 4, 8, 16)
    x_values = map(int, sorted(x_values, reverse=False))
    x_labels = map(str, x_values)
    
    print("x_values = %s", x_values)
    print("x_labels = %s", x_labels)
        
    ind = np.arange(5)    # the x locations for the groups
    width = 0.35
    offset = width/2
    
    y1 = []
    for row in construct:
        y1.append(row[colMap["1"]])
        y1.append(row[colMap["2"]])
        y1.append(row[colMap["4"]])
        y1.append(row[colMap["8"]])
        y1.append(row[colMap["16"]])
    
    y2 = []
    for row in write:
        y2.append(row[colMap["1"]])
        y2.append(row[colMap["2"]])
        y2.append(row[colMap["4"]])
        y2.append(row[colMap["8"]])
        y2.append(row[colMap["16"]])

    y3 = []
    for row in fetch:
        y3.append(row[colMap["1"]])
        y3.append(row[colMap["2"]])
        y3.append(row[colMap["4"]])
        y3.append(row[colMap["8"]])
        y3.append(row[colMap["16"]])
    
    y4 = []
    for row in blockmerge:
        y4.append(row[colMap["1"]])
        y4.append(row[colMap["2"]])
        y4.append(row[colMap["4"]])
        y4.append(row[colMap["8"]])
        y4.append(row[colMap["16"]])

    y5 = []
    for row in tuplemerge:
        y5.append(row[colMap["1"]])
        y5.append(row[colMap["2"]])
        y5.append(row[colMap["4"]])
        y5.append(row[colMap["8"]])
        y5.append(row[colMap["16"]])

    y6 = []
    for i in range(len(x_values)):
        y6.append(y1[i]+y2[i])

    y7 = []
    for i in range(len(x_values)):
        y7.append(y6[i]+y3[i])
    
    plt1 = ax1.bar(ind-offset, y1, width, align='center', color=OPT_COLORS[0], bottom=0)
    ptt2 = ax1.bar(ind-offset, y2, width, align='center', color=OPT_COLORS[1], bottom=y1)
    ptt3 = ax1.bar(ind-offset, y3, width, align='center', color=OPT_COLORS[2], bottom=y6)
    ptt4 = ax1.bar(ind-offset, y4, width, align='center', color=OPT_COLORS[3], bottom=y7)
    ptt8 = ax1.bar(ind+offset, y5, width, align='center', color='#FFFF00', bottom=y7)


    plt5 = ax1.bar(ind+offset, y1, width, align='center', color=OPT_COLORS[0], bottom=0)
    ptt6 = ax1.bar(ind+offset, y2, width, align='center', color=OPT_COLORS[1], bottom=y1)
    ptt7 = ax1.bar(ind+offset, y3, width, align='center', color=OPT_COLORS[2], bottom=y6)


    ## FOR

    axes = ax1.get_axes()
    graphutil.makeGrid(ax1)
    axes.set_ylim(0, 700)

    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(labels,
               prop=fp,
               bbox_to_anchor=(0.0, 1.02, 1.0, 0.10),
               loc=1,
               ncol=len(datasets),
               mode="expand",
               shadow=OPT_LEGEND_SHADOW,
               borderaxespad=0.0,
               )
    #ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)
    
    # Y-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_ylabel(OPT_Y_LABEL_TIME_MS, fontproperties=fp)
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)

    # X-AXIS
    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
    ax1.set_xlabel(OPT_X_LABEL_BLOCK_SIZE_MB, fontproperties=fp)
    ax1.minorticks_off()

    ax1.set_xticks(ind)
    ax1.set_xticklabels( x_labels )

#    # X-AXIS
#    fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
#    ax1.set_xlabel(OPT_X_LABEL_BLOCK_SIZE_MB, fontproperties=fp)
#    ax1.xaxis.set_major_locator(MaxNLocator(len(x_values)))
##    xlim = ax1.get_xlim()[::-1]
##    xlim = (xlim[0], xlim[1])
##    ax1.set_xlim(xlim)
#    ax1.minorticks_off()
#    ax1.set_xticklabels(x_labels)
#    ax1.set_xticks(x_values)
#    print(x_labels)
#    pprint(ax1.xaxis.get_majorticklocs())
#    for tick in ax1.xaxis.get_major_ticks():
#        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
#        tick.label.set_fontname(OPT_FONT_NAME)
#    ## FOR

    return (fig)
## DEF

def createIndexGraph(colMap, hashData, treeData):
	fig = plot.figure()
	ax1 = fig.add_subplot(111)

	datasets = (hashData, treeData)
	labels = ("hash index", "tree index")

	x_values = (1, 2, 3, 4, 5, 6, 7, 8)
	x_values = map(int, sorted(x_values))
	x_labels = map(str, x_values)

	ind = np.arange(8)    # the x locations for the groups
	width = 0.35
	offset = width/2

	print("x_values = %s", x_values)
	print("x_labels = %s", x_labels)

	for i in xrange(len(datasets)):

		y_values = [None]*len(x_values)
		for row in datasets[i]:
			y_values[0] = row[colMap["1"]]
			y_values[1] = row[colMap["2"]]
			y_values[2] = row[colMap["3"]]
			y_values[3] = row[colMap["4"]]
			y_values[4] = row[colMap["5"]]
			y_values[5] = row[colMap["6"]]
			y_values[6] = row[colMap["7"]]
			y_values[7] = row[colMap["8"]]


		print(y_values)
		ax1.bar(ind-offset, y_values, width, align='center', color=OPT_COLORS[i+1])
		offset = 0 - offset
	## FOR

	axes = ax1.get_axes()
        graphutil.makeGrid(ax1)
	axes.set_ylim(0, 225)

	# LEGEND
	fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
	ax1.legend(
	labels,
	prop=fp,
	bbox_to_anchor=(0.0, 1.02, 1.0, 0.10),
	loc=1,
	ncol=len(datasets),
	mode="expand",
	shadow=OPT_LEGEND_SHADOW,
	borderaxespad=0.0,
	)
	#ax1.legend(loc='center', shadow=OPT_LEGEND_SHADOW, prop=fp)

	# Y-AXIS
	fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_YLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
	ax1.set_ylabel(OPT_Y_LABEL_TIME_MS, fontproperties=fp)
	ax1.yaxis.set_major_locator(MaxNLocator(8))
	ax1.minorticks_on()
	for tick in ax1.yaxis.get_major_ticks():
		tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
		tick.label.set_fontname(OPT_FONT_NAME)

		# X-AXIS
		fp = FontProperties(family=OPT_FONT_NAME+' Condensed', size=OPT_XLABEL_FONT_SIZE, weight=OPT_LABEL_WEIGHT)
		ax1.set_xlabel(OPT_X_LABEL_NUM_INDEXES, fontproperties=fp)
		ax1.minorticks_off()

		ax1.set_xticks(ind)
		ax1.set_xticklabels( x_labels )

		return (fig)
## DEF




## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Throughput Graphs')
    aparser.add_argument('--powerpoint', action='store_true',
                         help='PowerPoint Formatted Graphs')
    args = vars(aparser.parse_args())
    
    
        #if args['powerpoint']:
    OPT_FONT_NAME = 'Proxima Nova'
    OPT_GRAPH_HEIGHT = 300
    OPT_LABEL_WEIGHT = 'bold'
    OPT_COLORS = ['#0A8345'] + list(OPT_COLORS)
    OPT_LINE_WIDTH = 5.0
    OPT_MARKER_SIZE = 12.0
    OPT_GRAPH_HEIGHT = 400

        #else:
        # OPT_LABEL_WEIGHT = None
        #OPT_LINE_WIDTH = 2.0
    #OPT_MARKER_SIZE = 7.0
    
    noAntiCache = datautil.loadHStoreData(OPT_DATA_HSTORE)
    hstoreData = datautil.loadHStoreData(OPT_DATA_HSTORE_ANTICACHE)
    hstoreDataApprox = datautil.loadHStoreData(OPT_DATA_HSTORE_APPROX)
    #pprint(hstoreData)
    #print "="*150
    
    mysqlData = datautil.loadMySQLData(memcached=False)
    #pprint(mysqlData)
    #print "="*150
    
    memcachedData = datautil.loadMySQLData(memcached=True)
    #pprint(memcachedData)
    #print "="*150
    
    
    ## WORKLOAD SKEW GRAPHS
    for benchmark in hstoreData.keys():
        memorySizes = set(map(lambda x: int(x["mem"]), hstoreData[benchmark]))
        readPcts = set(map(lambda x: int(x["read_pct"]), hstoreData[benchmark]))
        readLabels = dict(map(lambda x: (datautil.OPT_HSTORE_TAGS[x], x), datautil.OPT_HSTORE_TAGS.keys()))
        
        for mem in memorySizes:
            for read_pct in readPcts:
#                fig = createWorkloadSkewGraphs(benchmark, mem, read_pct,
#                                               hstoreData[benchmark],
#                                               mysqlData[benchmark],
#                                               memcachedData[benchmark])
                fig = createWorkloadSkewGraphs(benchmark, mem, read_pct, noAntiCache[benchmark], hstoreData[benchmark], hstoreDataApprox[benchmark], mysqlData[benchmark], memcachedData[benchmark])
                fileName = "skew-%s-%dx-%s.pdf" % (benchmark, mem, readLabels[read_pct])
                graphutil.saveGraph(fig, fileName, height=OPT_GRAPH_HEIGHT)
                #break
            ## FOR
            #break
        ## FOR
    ## FOR

	colMap, indexTreeData = datautil.getCSVData(OPT_DATA_INDEX_TREE) 
	colMap, indexHashData = datautil.getCSVData(OPT_DATA_INDEX_HASH) 
	fig = createIndexGraph(colMap, indexHashData, indexTreeData)
	graphutil.saveGraph(fig, "index.pdf")

## LRU graph
    colMap, hstoreNoAnticacheData = datautil.getCSVData(OPT_DATA_LRU_NONE)
    colMap, hstoreSingleListData = datautil.getCSVData(OPT_DATA_LRU_SINGLE)
    colMap, hstoreDoubleListData = datautil.getCSVData(OPT_DATA_LRU_DOUBLE)

    fig = createLinkedListGraph(colMap, hstoreNoAnticacheData, hstoreSingleListData, hstoreDoubleListData)
    graphutil.saveGraph(fig, "lru.pdf")

    colMap, hstoreConstructData = datautil.getCSVData(OPT_DATA_EVICT_CONSTRUCT)
    colMap, hstoreWriteData = datautil.getCSVData(OPT_DATA_EVICT_WRITE)
    colMap, hstoreFetchData = datautil.getCSVData(OPT_DATA_EVICT_FETCH)
    colMap, hstoreBlockMergeData = datautil.getCSVData(OPT_DATA_EVICT_BLOCK_MERGE)
    colMap, hstoreTupleMergeData = datautil.getCSVData(OPT_DATA_EVICT_TUPLE_MERGE)


    fig = createEvictGraph(colMap, hstoreConstructData, hstoreWriteData, hstoreFetchData, hstoreTupleMergeData, hstoreBlockMergeData)
    graphutil.saveGraph(fig, "evict.pdf")

	## TPCC Graph
    hstoreTpccData = datautil.loadHStoreTPCC(OPT_DATA_HSTORE_TPCC)
    hstoreApproxTpccData = datautil.loadHStoreTPCC(OPT_DATA_HSTORE_TPCC_APPROX)
    mysqlTpccData = datautil.loadMySQLTPCC(memcached=False)
    memcachedTpccData = datautil.loadMySQLTPCC(memcached=True)

    fig = createTPCCThroughputGraph(hstoreTpccData, hstoreApproxTpccData, mysqlTpccData, memcachedTpccData)
    graphutil.saveGraph(fig, "tpcc.pdf")


    ## BlockSize graph
#    blocksize128 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_128)
#    blocksize256 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_256)
#    blocksize512 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_512)
    blocksize1024 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_1024)
    blocksize2048 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_2048)
    blocksize4096 = datautil.loadHStoreData(OPT_DATA_BLOCKSIZE_4096)

    for benchmark in blocksize1024.keys():
        fig = createBlockSizeGraph(2, 100, blocksize1024[benchmark], blocksize2048[benchmark], blocksize4096[benchmark])
        graphutil.saveGraph(fig, "blocksize.pdf")


    ## Tuple Size Graph
    tuplesize128 = datautil.loadHStoreData(OPT_DATA_TUPLESIZE_128)
    tuplesize256 = datautil.loadHStoreData(OPT_DATA_TUPLESIZE_256)
    tuplesize512 = datautil.loadHStoreData(OPT_DATA_TUPLESIZE_512)
    tuplesize1024 = datautil.loadHStoreData(OPT_DATA_TUPLESIZE_1024)

    for benchmark in tuplesize1024.keys():
        fig = createTupleSizeGraph(2, 100, tuplesize128[benchmark], tuplesize256[benchmark], tuplesize512[benchmark], tuplesize1024[benchmark])
        graphutil.saveGraph(fig, "tuplesize.pdf")

    mergeBlock = datautil.loadHStoreData(OPT_DATA_MERGE_BLOCK)
    mergeTuple = datautil.loadHStoreData(OPT_DATA_MERGE_TUPLE)

    for benchmark in mergeBlock.keys():
        fig = createMergeStrategyGraph(2, 100, mergeTuple[benchmark], mergeBlock[benchmark])
        graphutil.saveGraph(fig, "merge.pdf")

    ## TPC-C GRAPHS
#    for benchmark in hstoreData.keys():
    
    #for benchmark,data in mysqlData.iteritems():
        #fig = createReadRatioThroughputGraph(benchmark, data)
        #graphutil.saveGraph(fig, "readratios-mysql-%s.pdf" % benchmark)
    ### FOR
    

## MAIN