#!/usr/bin/env python

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
import matplotlib.ticker as tkr
def func(x, pos):  # formatter function takes tick label and tick position
   s = '{:0,d}'.format(int(x))
   return s

dictR = {}
dictW = {}
dict = {}
def computeEvictionStats(dataFile):
    colMap, csvData = datautil.getCSVData(dataFile)
    if len(csvData) == 0: return
    pos = dataFile.rfind("/");
    dataFile = dataFile[pos + 3:-10]
    if len(csvData) == 0: return
    if not dictR.has_key(dataFile):
        dictR[dataFile] = []
    if not dictW.has_key(dataFile):
        dictW[dataFile] = []
    
    for row in csvData:
        read = int(row[colMap["ANTICACHE_BYTES_READ"]])
        write = int(row[colMap["ANTICACHE_BYTES_WRITTEN"]])

    dictR[dataFile].append(read)
    dictW[dataFile].append(write)
    
    print dataFile
    print "read: %d" % read
    print "write: %d" % write 
    print
# DEF

def computeThroughputStats(dataFile):
    colMap, csvData = datautil.getCSVData(dataFile)
    rpos = dataFile.rfind("/");
    pos = dataFile.find("/");
    print dataFile
    dataFile = dataFile[rpos + 3:-11]
    if len(csvData) == 0: return

    tp = []
    if not dict.has_key(dataFile):
        dict[dataFile] = []
    
    for row in csvData:
        tp.append(float(row[colMap["THROUGHPUT"]]))
    
    dict[dataFile].append(np.mean(tp))

    print "  Average Throughput: %.2f ms" % np.mean(tp)
    print

def draw_IO_graph(out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
    res1 = []
    res2 = []
    res3 = []
    res4 = []

    for s in skew:
        for tp in dictR:
            if tp.find(s + '-') > 0 and tp.find("lru") > 0 and tp.find("flru") < 0:
                res1.append(np.mean(dictR[tp]) / np.mean(dict[tp]))
                print tp
                print "read: ", np.mean(dictR[tp])
            if tp.find(s + '-') > 0 and tp.find("timestamp") > 0:
                res2.append(np.mean(dictR[tp]) / np.mean(dict[tp]))
                print tp
                print np.mean(dictR[tp])

    for s in skew:
        for tp in dictW:
            if tp.find(s + '-') > 0 and tp.find("lru") > 0 and tp.find("flru") < 0:
                res3.append(np.mean(dictW[tp]) / np.mean(dict[tp]))
                print tp
                print np.mean(dictW[tp])
            if tp.find(s + '-') > 0 and tp.find("timestamp") > 0:
                res4.append(np.mean(dictW[tp]) / np.mean(dict[tp]))
                print tp
                print "write: ", np.mean(dictW[tp])
   
#   \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
#   PMTLM & 9889.48 & 8966.57 & 8483.49 \\ %\hline
#   EUTB & 4932.97 & 4778.50 & 4619.07 \\ %\hline
#   COLD(C=100) & 5200.46 & {\bf 4350.95} & 4394.46 \\
  
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='aLRU-read',hatch='\\',color='#FF6600')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='timestamps-read',hatch='/',color='#99CC00')
    ax.bar( [i+0.00 for i in x] ,res3,width=0.1,label='aLRU-write',hatch='|',color='#FF6600')
    ax.bar( [i+0.1 for i in x] ,res4,width=0.1,label='timestamps-write',hatch='-',color='#99CC00')
    ax.set_ylabel("Disk IO / Transaction (KB)",fontsize=16, weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0,500])
    ax.set_xticklabels(['0.8','1.0','1.1', '1.2'],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16, weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    plot.savefig(out_path)

## ================
## main
## ==============================================
if __name__ == '__main__':
    matches = []
    for root, dirnames, filenames in os.walk("./prime/ycsb-T500-NoLoop"):
        for filename in fnmatch.filter(filenames, '*memory.csv'):
            matches.append(os.path.join(root, filename))
    map(computeEvictionStats, matches)

    matches = []
    for root, dirnames, filenames in os.walk("./prime/ycsb-T500-NoLoop"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    map(computeThroughputStats, matches)

    for tp in dictR:
        print tp
        print "read: %d" % np.mean(dictR[tp])
        print "write: %d" % np.mean(dictW[tp])

    #draw_IO_graph("ycsb-INF-IO.pdf")
    draw_IO_graph("ycsb-T500-NoLoop-prime-IO.pdf")
    #draw_IO_graph("ycsb-T1000-IO.pdf")

## MAIN
