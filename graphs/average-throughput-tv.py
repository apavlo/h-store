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

dict = {}

def computeEvictionStats(dataFile):
    colMap, csvData = datautil.getCSVData(dataFile)
    rpos = dataFile.rfind("/");
    pos = dataFile.find("voter");
    if pos < 0:
        pos = dataFile.find("tpcc")
    print dataFile
    dataFile = dataFile[0:pos] + dataFile[rpos + 3:]
    print dataFile

    if len(csvData) == 0: return

    tp = []
    if not dict.has_key(dataFile):
        dict[dataFile] = []
    
    for row in csvData:
        tp.append(float(row[colMap["THROUGHPUT"]]))

    dict[dataFile].append(np.mean(tp))
    
    print "  Average Throughput: %.2f ms" % np.mean(tp)
    print
# DEF#

def voter_all(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["voter"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("lru") >= 0 and tp.find("prime") < 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append((mean - np.min(dict[tp])))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("timestamps") >= 0 and tp.find("prime") < 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append((mean - np.min(dict[tp])))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("timestamps") >= 0 and tp.find("prime") >= 0:
                res3.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res3_min.append((mean - np.min(dict[tp])))
                res3_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    x = [0.5]
    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.05 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='timestamps',hatch='/',color='#99CC00')
    ax.errorbar([i+0.05 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i+0.1 for i in x],res3,width=0.1,label='prime-timestamps',hatch='\\/',color='#CD0000')
    ax.errorbar([i+0.15 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,0.9])
    ax.set_ylim([0,70000])
    ax.set_xticklabels(["VOTER"], fontsize=16)
    ax.set_xlabel("Workload",fontsize=16,weight='bold')
    ax.set_xticks([0.5])
    y_format = tkr.FuncFormatter(func)  # make formatter

    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    plot.savefig(out_path)

def voter(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["voter"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("lru") >= 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append((mean - np.min(dict[tp])))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("timestamps") >= 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append((mean - np.min(dict[tp])))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    x = [0.5]
    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.05 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='timestamps',hatch='/',color='#99CC00')
    ax.errorbar([i+0.05 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlim([0.3,0.7])
    ax.set_ylim([0,70000])
    ax.set_xticklabels(["VOTER"], fontsize=16)
    ax.set_xlabel("Workload",fontsize=16,weight='bold')
    ax.set_xticks([0.5])
    y_format = tkr.FuncFormatter(func)  # make formatter

    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    plot.savefig(out_path)

def draw_throughput_graph(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["voter", "tpcc"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("lru") >= 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append((mean - np.min(dict[tp])))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("timestamps") >= 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append((mean - np.min(dict[tp])))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    #res1 = [2618.45, 17978.96, 30405.52]
    #res2 =[6123.74, 28654.0766667, 35181.7266667]

  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
          # \hline
    x = [0.5,1]
    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.05 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='timestamps',hatch='/',color='#99CC00')
    ax.errorbar([i+0.05 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlim([0.2,1.2])
    ax.set_ylim([0,70000])
    ax.set_xticklabels(["VOTER", "TPC-C"], fontsize=16)
    ax.set_xlabel("Workload",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1])
    y_format = tkr.FuncFormatter(func)  # make formatter

    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    plot.savefig(out_path)

def draw_throughput_graph_INF(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["voter", "tpcc"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("lru") >= 0 and tp.find("A1000") < 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append((mean - np.min(dict[tp])))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    print res1_min
    print res1_max

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and tp.find("timestamp") >= 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append(mean - np.min(dict[tp]))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') >= 0 and ((tp.find("timestamp") < 0 and tp.find("lru") < 0 and tp.find("tpcc") >= 0) or tp.find("none") >= 0):
                res3.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res3_min.append(mean - np.min(dict[tp]))
                res3_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    #res1 = [2618.45, 17978.96, 30405.52]
    #res2 =[6123.74, 28654.0766667, 35181.7266667]

  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
          # \hline
    x = [0.5,1]
    ax.bar( [i-0.15 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.1 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.05 for i in x],res2,width=0.1,label='timestamps',hatch='/',color='#99CC00')
    ax.errorbar([i-0.0 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i+0.05 for i in x],res3,width=0.1,label='none',hatch='-',color='b')
    ax.errorbar([i+0.1 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,1.2])
    ax.set_ylim([0,70000])
    ax.set_xticklabels(["VOTER", "TPC-C"], fontsize=16)
    ax.set_xlabel("Workload",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1])
    #plt.show()
    plot.savefig(out_path)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    matches = []
    for root, dirnames, filenames in os.walk("./voter/voter-TINF-NoLoop"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    for root, dirnames, filenames in os.walk("./tpcc/tpcc-TINF-NoLoop"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    #for root, dirnames, filenames in os.walk("./prime/tpcc-NoLoop"):
    #    for filename in fnmatch.filter(filenames, '*E50-results.csv'):
    #        matches.append(os.path.join(root, filename))
    map(computeEvictionStats, matches)

    #for tp in dict:
    #    print tp
    #    print np.mean(dict[tp])

    draw_throughput_graph_INF(dict, "tpcc-voter-INF.pdf")
    #draw_throughput_graph(dict, "ycsb-T500.pdf")
    #draw_throughput_graph(dict, "tpcc-voter-NoLoop-prime.pdf")
    #voter(dict, "voter-NoLoop.pdf")
    #voter_all(dict, "voter-NoLoop-prime-all.pdf")

## MAIN
