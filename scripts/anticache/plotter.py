#!/usr/bin/env python

import os
import sys
import csv
import logging
import matplotlib.pyplot as plot
import pylab

OPT_GRAPH_WIDTH = 1200
OPT_GRAPH_HEIGHT = 600
OPT_GRAPH_DPI = 100

## ==============================================
## main
## ==============================================
if __name__ == '__main__':

    plot.xlabel('time')
    plot.ylabel('transactions per second')
    #ax = plot.gca()
    #ax.set_autoscaley_on(False)
    
    input_files = [
        #("VOTER-4p-noevict.csv",   "No Eviction",    "red"),
        #("VOTER-4p-evict-4MB.csv", "Eviction (4MB)", "blue"),
        ("VOTER-4p-evict-8MB.csv", "Eviction (8MB)", "orange"),
    ]
    evictions = set()
    y_max = None

    first = True
    for file, label, color in input_files:
        with open(file, "r") as f:
            reader = csv.reader(f)
            
            col_xref = None
            x_values = [ ]
            y_values = [ ]
            row_idx = 0
            for row in reader:
                if col_xref is None:
                    col_xref = { }
                    for i in xrange(len(row)):
                        col_xref[row[i]] = i
                    continue
                row_idx += 1
                #if row_idx % 2 == 0: continue
                x_values.append(float(row[col_xref['INTERVAL']]))
                y_values.append(float(row[col_xref['THROUGHPUT']]))
                
                y_max = max(y_max, y_values[-1])
                if first and row[col_xref["EVICTING"]] == "true":
                    evictions.add(x_values[-1])
            ## FOR
            plot.plot(x_values, y_values, label=label, color=color)
        ## WITH
        first = len(evictions) == 0
    ## FOR

    if len(evictions) > 0:
        for x in evictions:
            plot.vlines(x, 0, y_max, color='black', linewidth=2.0, linestyles='dashed')
    ## IF
    
    F = pylab.gcf()
    #F.autofmt_xdate()

    # Now check everything with the defaults:
    size = F.get_size_inches()
    dpi = F.get_dpi()
    logging.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))
    
    new_size = (OPT_GRAPH_WIDTH / float(dpi), OPT_GRAPH_HEIGHT / float(dpi))
    F.set_size_inches(new_size)
    F.set_dpi(OPT_GRAPH_DPI)
    
    # Now check everything with the defaults:
    size = F.get_size_inches()
    dpi = F.get_dpi()
    logging.debug("Current Size Inches: %s, DPI: %d" % (str(size), dpi))
    
    new_size = (OPT_GRAPH_WIDTH / float(dpi), OPT_GRAPH_HEIGHT / float(dpi))
    F.set_size_inches(new_size)
    F.set_dpi(OPT_GRAPH_DPI)

    plot.legend(loc='upper right')
    plot.savefig("voter-eviction.png")
## MAIN