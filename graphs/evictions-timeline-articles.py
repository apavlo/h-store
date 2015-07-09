#!/usr/bin/env python

# Example execution 
# export b=tpcc ; export OPT_DATA_EVICTIONS=/home/pavlo/Documents/H-Store/Papers/anticache/data/evictions
# ant compile hstore-benchmark -Dproject=$b -Dclient.interval=500 \
#    -Dsite.anticache_enable=true -Dsite.anticache_profiling=true \
#    -Dclient.output_memory=$OPT_DATA_EVICTIONS/$b-memory.csv \
#    -Dclient.output_csv=$OPT_DATA_EVICTIONS/$b-throughput.csv \
#    -Dclient.output_anticache_history=$OPT_DATA_EVICTIONS/$b-evictions.csv

import os
import sys
import csv
import glob
import re
import logging
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plot
import pylab
import numpy as np
import math
import string
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator, MultipleLocator
from pprint import pprint,pformat


from options import *
import graphutil

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
## DATA CONFIGURATION
## ==============================================

OPT_GRAPH_HEIGHT = 450

## ==============================================
## CREATE THROUGHPUT GRAPH
## ==============================================
def createThroughputGraph(benchmark, data):
    y_max = max(data["y_values"])

    tmp = y_max
    order = 1

    while tmp > 10: 
        order = order * 10
        tmp = tmp / 10;
    
    y_max_range = math.ceil(tmp) * order
        
    # INIT
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    # Throughout
    ax1.plot(data["x_values"], data["y_values"],
                color=OPT_COLORS[0],
                linewidth=3.5,
                marker='',
    )
    
#    if len(data["memory"]) > 0:
#        memoryX = map(lambda x: x[0], data["memory"])
#        memoryY = map(lambda x: (x[1] / float(1024)), data["memory"]) # MB
#        ax2 = ax1.twinx()
#        ax2.plot(memoryX, memoryY,
#                    marker='s',
#                    markersize=2.0,
#                    color=OPT_COLORS[1],
#        )
#        ax2.set_ylabel(OPT_Y_LABEL_THROUGHPUT, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE)
#        for tick in ax2.yaxis.get_major_ticks():
#            tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
#            tick.label.set_fontname(OPT_FONT_NAME)
#    else:
#        LOG.warn("Missing memory stats for '%s'" % benchmark)
    
    # Evictions
    if len(data["evictions"]) > 0:
        addEvictionLines(ax1, data["evictions"], y_max_range)
        LOG.info("Adding eviction lines.")
    else:
        LOG.warn("Missing eviction history for '%s'" % benchmark)
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, y_max_range)
    axes.yaxis.grid(True, linestyle='-', which='major', color='0.85') # color='lightgrey', alpha=0.5)
    axes.set_axisbelow(True)
    graphutil.makeGrid(ax1)
    
    # Y-AXIS
    ax1.set_ylabel(OPT_Y_LABEL_THROUGHPUT, name=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE, weight='bold')
    ax1.yaxis.set_major_locator(MaxNLocator(5))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    ax1.set_xlabel(OPT_X_LABEL_TIME, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE, weight='bold')
    ax1.set_xlim(0, 600000)
    ax1.xaxis.set_major_locator(MaxNLocator(6))
    xLabels = map(lambda x: "%d" % (x / 1000), ax1.get_xticks())
    ax1.set_xticklabels(xLabels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    
    return (fig)
## DEF

## ==============================================
## CREATE MEMORY GRAPH
## ==============================================
def createMemoryGraph(benchmark, data):
    x = map(lambda x: x[0], data["memory"])
    inMemory = map(lambda x: x[1], data["memory"]) # In-Memory Data
    anticache = map(lambda x: x[2], data["memory"]) # Anti-Cache Data
    y = np.row_stack((inMemory, anticache))   
    
    # this call to 'cumsum' (cumulative sum), passing in your y data, 
    # is necessary to avoid having to manually order the datasets
    y_stack = np.cumsum(y, axis=0)

    # GRAPH
    fig = plot.figure()
    ax1 = fig.add_subplot(111)
    
    ax1.fill_between(x, 0, y_stack[0,:],
                     color=OPT_COLORS[0],
                     alpha=0.5,
                     label="XXX",
    )
    ax1.fill_between(x, y_stack[0,:], y_stack[1,:],
                     color=OPT_COLORS[1],
                     alpha=0.5,
                     label="YYY",
    )
    ax1.plot(x, map(lambda x: sum(x[1:]), data["memory"]),
             color=OPT_COLORS[1],
             linewidth=3,
             label="Anti-Cache Data"
    )
    ax1.plot(x, inMemory,
             color=OPT_COLORS[0],
             linewidth=3,
             label="In-Memory Data",
    )
    
    # GRID
    axes = ax1.get_axes()
    axes.set_ylim(0, 1250)
    axes.yaxis.grid(True, linestyle='-', which='major', color='0.85') # color='lightgrey', alpha=0.5)
    axes.set_axisbelow(True)
    graphutil.makeGrid(ax1)
    
    # Y-AXIS
    ax1.set_ylabel("Memory (MB)", name=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE)
    yLabels = map(lambda y: "%d" % (y / 1000), ax1.get_yticks())
    #ax1.set_yticklabels(yLabels)
    ax1.yaxis.set_major_locator(MultipleLocator(250))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    ax1.set_xlabel(OPT_X_LABEL_TIME, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE)
    ax1.xaxis.set_major_locator(MaxNLocator(6))
    xLabels = map(lambda x: "%d" % (x / 1000), ax1.get_xticks())
    ax1.set_xticklabels(xLabels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)

    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(loc='upper left', shadow=OPT_LEGEND_SHADOW, prop=fp)
        
    # Evictions
    if len(data["evictions"]) > 0:
        # addEvictionLines(data["evictions"], y_max)
        pass
    else:
        LOG.warn("Missing eviction history for '%s'" % benchmark)
    
    return (fig)
## DEF

## ==============================================
## ADD EVICTION DEMARCATIONS
## ==============================================
def addEvictionLines(ax, evictions, height):
    for x,duration in evictions:
        #width = max(1, duration)
        width = 1
        ax.vlines(x, 0, height,
                  color='black',
                  linewidth=width,
                  linestyles='dashed',
                  alpha=1.0,
        )
    ## FOR
## DEF
        
## ==============================================
## BOTH PLOTS IN ONE
## ==============================================

def plotMemoryAndThroughput(benchmark, data):
    x_max = max(data["x_values"])
    y_max = max(data["y_values"])

    tmp = y_max
    order = 1

    while tmp > 10: 
        order = order * 10
        tmp = tmp / 10;
    
    y_max_range = math.ceil(tmp) * order
    
    x = map(lambda x: x[0], data["memory"])
    inMemory = map(lambda x: x[1], data["memory"]) # In-Memory Data
    anticache = map(lambda x: x[2], data["memory"]) # Anti-Cache Data
    y = np.row_stack((inMemory, anticache))   
    
    # this call to 'cumsum' (cumulative sum), passing in your y data, 
    # is necessary to avoid having to manually order the datasets
    y_stack = np.cumsum(y, axis=0)


    fig = plot.figure()
    fig.set_figheight(10)
    ax1=fig.add_subplot(2,1,1)
   
  #  f, axarr = plot.subplots(2, sharex=True)
   

    ax1.fill_between(x, 0, y_stack[0,:],
                     color=OPT_COLORS[0],
                     alpha=0.5,
                     label="XXX",
    )
    ax1.fill_between(x, y_stack[0,:], y_stack[1,:],
                     color=OPT_COLORS[1],
                     alpha=0.5,
                     label="YYY",
    )
    ax1.plot(x, map(lambda x: sum(x[1:]), data["memory"]),
             color=OPT_COLORS[1],
             linewidth=3,
             label="Anti-Cache Data"
    )
    ax1.plot(x, inMemory,
             color=OPT_COLORS[0],
             linewidth=3,
             label="In-Memory Data",
    )
    
    # GRID
    #mem_max = max((data["memory"])[1])
    print data["memory"]
    mem_max = max(inMemory)
    mem_max = mem_max + max(anticache)
    print mem_max
    tmp = mem_max
    order = 1

    #while tmp > 10: 
    #    order = order * 10
    #    tmp = tmp / 10;
    #    print tmp
    
    mem_max_range = mem_max + mem_max*0.30
    print mem_max_range
    axes = ax1.get_axes()
    axes.set_ylim(0, mem_max_range)
    axes.yaxis.grid(True, linestyle='-', which='major', color='0.85') # color='lightgrey', alpha=0.5)
    axes.set_axisbelow(True)
    graphutil.makeGrid(ax1)

    head = data["run_head"]
    info = data["run_info"]
    # generate title string
    # 0   1         2    3          4                5            6              7             
    # run benchmark tier partitions blocking_clients client_hosts client_threads scaling_factor
    # 8          9              10           11      12    13
    # block_size blocks_evicted threshold_mb runtime merge skew
    title_str="%s %s: %s %s %s %s: %s %s: %s" % (info[2], head[12], info[12], info[3], info[4], head[10], info[10], head[13], info[13])
    
    # Y-AXIS
    
    ax1.set_title(title_str)
    ax1.set_ylabel("Memory (MB)", name=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE)
    yLabels = map(lambda y: "%d" % (y / 1000), ax1.get_yticks())
    #ax1.set_yticklabels(yLabels)
    ax1.yaxis.set_major_locator(MultipleLocator(250))
    ax1.minorticks_on()
    for tick in ax1.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    #ax1.set_xlabel(OPT_X_LABEL_TIME, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE)
    ax1.xaxis.set_major_locator(MaxNLocator(6))
    xLabels = map(lambda x: "%d" % (x / 1000), ax1.get_xticks())
    ax1.set_xticklabels(xLabels)
    for tick in ax1.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)

    # LEGEND
    fp = FontProperties(family=OPT_FONT_NAME, size=OPT_LEGEND_FONT_SIZE)
    ax1.legend(loc='upper left', shadow=OPT_LEGEND_SHADOW, prop=fp)
        
    # Evictions
    if len(data["evictions"]) > 0:
        # addEvictionLines(data["evictions"], y_max)
        pass
    else:
        LOG.warn("Missing eviction history for '%s'" % benchmark)

    ax2 = fig.add_subplot(2,1,2)
    
    # Throughout
    ax2.plot(data["x_values"], data["y_values"],
                color=OPT_COLORS[0],
                linewidth=3.5,
                marker='',
    ) 

    if len(data["evictions"]) > 0:
        addEvictionLines(ax1, data["evictions"], mem_max_range)
        #LOG.info("Adding eviction lines.")
        pass
    else:
        LOG.warn("Missing eviction history for '%s'" % benchmark)

    axes = ax2.get_axes()
    axes.set_ylim(0, y_max_range)
    axes.yaxis.grid(True, linestyle='-', which='major', color='0.85')
    axes.set_axisbelow(True)
    graphutil.makeGrid(ax2)
    
    ax2.set_ylabel(OPT_Y_LABEL_THROUGHPUT, name=OPT_FONT_NAME, size=OPT_YLABEL_FONT_SIZE)
    ax2.yaxis.set_major_locator(MaxNLocator(5))
    ax2.minorticks_on()
    for tick in ax2.yaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    # X-AXIS
    ax2.set_xlabel(OPT_X_LABEL_TIME, name=OPT_FONT_NAME, size=OPT_XLABEL_FONT_SIZE)
    ax2.set_xlim(0, x_max)
    ax2.xaxis.set_major_locator(MaxNLocator(6))
    xLabels = map(lambda x: "%d" % (x / 1000), ax2.get_xticks())
    ax2.set_xticklabels(xLabels)
    for tick in ax2.xaxis.get_major_ticks():
        tick.label.set_fontsize(OPT_YTICKS_FONT_SIZE)
        tick.label.set_fontname(OPT_FONT_NAME)
    
    plot.tight_layout()
    return fig
## DEF

 


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    
    OPT_FONT_NAME = 'DejaVu Sans'
    OPT_LABEL_WEIGHT = 'bold'
    OPT_MARKER_SIZE = 6.0
    OPT_DATA_EVICTIONS = "/home/user/giardino/data-hstore/articles/articles-nvm/k0.05"
    
    ## ----------------------------------------------
    ## LOAD DATA
    ## ----------------------------------------------
    processedData = { }
    for file in glob.glob(os.path.join(OPT_DATA_EVICTIONS, "*-evictions.csv")):
        print file
        m = re.search(r'([\w\-\.]+)-evictions.csv', file)
        assert m
        print m.group(1)
        benchmark = m.group(1)
        
        # Check to see whether we have a throughput+memory files for this benchmark
        throughputFile = os.path.join(OPT_DATA_EVICTIONS, "%s-results.csv" % benchmark)
        if not os.path.exists(throughputFile):
            LOG.warn("Missing throughput file '%s'" % throughputFile)
            continue
        memoryFile = os.path.join(OPT_DATA_EVICTIONS, "%s-memory.csv" % benchmark)
        if not os.path.exists(memoryFile):
            LOG.warn("Missing memory file '%s'" % memoryFile)
    #continue
        
        data = {
            "x_values":   [ ],
            "y_values":   [ ],
            "timestamps": [ ],
            "memory":     [ ],
            "evictions":  [ ],
            "run_head":   [ ],
            "run_info":   [ ],
        }

        
        data["run_head"] = ["run", "benchmark", "tier", "partitions", "blocking_clients",
                            "client_hosts", "client_threads", "scaling_factor", "block_size", "blocks_evicted",
                            "threshold_mb", "runtime", "merge", "skew"]
        data["run_info"] = benchmark.split('-')
               
        i = 0 
        for s in data["run_info"]:
            if i < 4:
                if i == 2:
                    i += 1
                    pass
                i += 1
                continue
            elif i <= len(data["run_info"]) - 2:
                pass
            else:
                data["run_info"][i] = s.translate(None, string.ascii_letters)
                i += 1
        
        ## LOAD THROUGHPUT DATA
        with open(throughputFile, "U") as f:
            reader = csv.reader(f)
            col_xref = None
            first = True
            for row in reader:
                if col_xref is None:
                    col_xref = { }
                    for i in xrange(len(row)):
                        col_xref[row[i]] = i
                    continue
                if first:
                    first = False
                    data["x_values"].append(0)
                    data["y_values"].append(float(row[col_xref['THROUGHPUT']]))
                    data["timestamps"].append(int(row[col_xref['TIMESTAMP']]))

                data["x_values"].append(int(row[col_xref['ELAPSED']]))
                data["y_values"].append(float(row[col_xref['THROUGHPUT']]))
                data["timestamps"].append(int(row[col_xref['TIMESTAMP']]))
            ## FOR
        ## WITH
    
        if os.path.exists(memoryFile):
            ## LOAD MEMORY DATA
            with open(memoryFile, "U") as f:
                reader = csv.reader(f)
                col_xref = None
                memoryData = [ ]
                for row in reader:
                    if col_xref is None:
                        col_xref = { }
                        for i in xrange(len(row)):
                            col_xref[row[i]] = i
                        continue
                    timestamp = int(row[col_xref['ELAPSED']])
                    memorySize = 0
                    anticacheSize = 0
                    for col in ['TUPLE_DATA_MEMORY', 'STRING_MEMORY']:#, 'INDEX_MEMORY']:
                        #for col in ['STRING_MEMORY']:
                        memorySize += long(row[col_xref[col]])
                    for col in ['ANTICACHE_BYTES_EVICTED']:
                        anticacheSize += long(row[col_xref[col]])
#                    for col in ['TUPLE_COUNT']:
#                            #for col in ['STRING_MEMORY']:
#                            memorySize += long(row[col_xref[col]])
#                    for col in ['ANTICACHE_TUPLES_EVICTED']:
#                        anticacheSize += long(row[col_xref[col]])
                    print(memorySize, anticacheSize)
                    memoryData.append((timestamp, memorySize / 1000, anticacheSize / 1000))
                ## FOR
                
                # Repeat the first entry so that we have flushed lines
                memoryData.insert(0, tuple([0] + list(memoryData[0][1:])))
                for i in xrange(len(memoryData)):
                    if memoryData[i][1] == 0: continue
                    for j in xrange(0, i):
                        assert memoryData[j][1] == 0, "%d -> %s" % (j, memoryData[j])
                        memoryData[j] = tuple([memoryData[j][0]] + list(memoryData[i][1:]))
                    break
                ## FOR
                memoryData.append(tuple([data["x_values"][-1]] + list(memoryData[-1][1:])))

                # Sort this mofo
                data["memory"] = sorted(memoryData, key=lambda x: x[0])
            ## WITH
    
        ## LOAD EVICTION DATA
        with open(file, "U") as f:
            reader = csv.reader(f)
            col_xref = None
            evictions = [ ]
            globalStart = min(data["timestamps"])
            LOG.info(globalStart)
            for row in reader:
                if col_xref is None:
                    col_xref = { }
                    for i in xrange(len(row)):
                        col_xref[row[i]] = i
                    continue
                start = int(row[col_xref['START']])
                duration = int(row[col_xref['STOP']]) - start
                assert duration >= 0
                if start < globalStart:
                    continue
                evictions.append((start-globalStart, duration))
            ## FOR
            data["evictions"] = sorted(evictions, key=lambda x: x[0])
        ## WITH
    
        processedData[benchmark] = data
    ## FOR
    
    ## ----------------------------------------------
    ## GENERATE GRAPHS
    ## ----------------------------------------------
    for benchmark,data in processedData.iteritems():
#        fig1 = createThroughputGraph(benchmark, data)
#        fig1.savefig("/home/michaelg/data-hstore/plots/ycsb-bugfinder/png/ycsb-throughput-%s-600s.png" % benchmark)
#        graphutil.saveGraph(fig, "/home/michaelg/data-hstore/plots/ycsb-bugfinder/ycsb-throughput-%s.pdf" % benchmark, height=OPT_GRAPH_HEIGHT)
        
#        fig2 = createMemoryGraph(benchmark, data)
#        fig2.savefig("/home/michaelg/data-hstore/plots/ycsb-bugfinder/png/ycsb-memory-%s-600s.png" % benchmark)
#        graphutil.saveGraph(fig, "/home/michaelg/data-hstore/plots/ycsb-bugfinder/ycsb-memory-%s.pdf" % benchmark, height=OPT_GRAPH_HEIGHT)
        fig3 = plotMemoryAndThroughput(benchmark, data)
        print "making figure for %s" % benchmark
        fig3.savefig("/home/user/giardino/data-hstore/plots/articles/png/articles-comb-%s.png" % benchmark)

    ## FOR

## MAIN
