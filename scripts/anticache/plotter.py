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
    
    input_files = [
        (4, "#D20040"),
        (8, "#01299F"),
        (16, "#EBBF00"),
        (32, "#5A001B"),
        (64, "#655200"),
    ]
    

    for size, color in input_files:
        evict_size = "%dMB" % size
        label = "VOTER (Eviction Block Size %s)" % evict_size
        
        plot.clf()
        plot.xlabel('time')
        plot.ylabel('transactions per second')
        #ax = plot.gca()
        #ax.set_autoscaley_on(False)
        evictions = { }
        y_max = None

        first = True
        with open("VOTER-4p-evict-%s.csv" % evict_size, "r") as f:
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
                if first and int(row[col_xref["EVICTING"]]) != 0:
                    evictions[x_values[-1]] = int(row[col_xref["EVICTING"]]) / 1000.0
            ## FOR
            plot.plot(x_values, y_values, label=label, color=color)
        ## WITH

        if len(evictions) > 0:
            y_max = max(y_max, 120000)
            for x,width in evictions.iteritems():
                width = max(1, width)
                plot.vlines(x, 0, y_max, color='black', linewidth=width, linestyles='dashed', alpha=1.0)
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
        plot.savefig("voter-eviction-%s.png" % evict_size)
## MAIN