#!/usr/bin/env python

import os
import sys
import re
import logging
import fnmatch
import string
import argparse
import matplotlib
matplotlib.use('Agg')
import pylab
import numpy as np
import matplotlib.pyplot as plot
import matplotlib.ticker as tkr
import math
import locale
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

dict = {}
def func(x, pos):  # formatter function takes tick label and tick position
       
    s = '{:0,d}'.format(int(x))
    return s

def computeEvictionStats(dataFile):
    colMap, csvData = datautil.getCSVData(dataFile)
    rpos = dataFile.rfind("/");
    pos = dataFile.find("/");
    print dataFile
    dataFile = dataFile[0:pos] + dataFile[rpos + 3:]
    if len(csvData) == 0: return

    tp = []
    txns = []
    time = []
    if not dict.has_key(dataFile):
        dict[dataFile] = []
    
    for row in csvData:
        txn = float(row[colMap["TRANSACTIONS"]])
        t = float(row[colMap["ELAPSED"]])
        txns.append(txn)
        time.append(t)
        tp.append(txn/5) 
    
    dict[dataFile].append(np.mean(tp))

    print "  Average Throughput: %.2f txn/s" % np.mean(tp)
    print
# DEF#

def draw_throughput_graph_all(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2", "S4", "S8"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []
    res4 = []
    res4_min = []
    res4_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("lru") > 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append(mean - np.min(dict[tp]))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("blocking") > 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append(mean - np.min(dict[tp]))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    #for s in skew:
    #    for tp in dict:
    #        if tp.find(s + '-') > 0 and tp.find("flru") > 0:
    #            res2.append(np.mean(dict[tp]))
    #            mean = np.mean(dict[tp])
    #            res2_min.append(mean - np.min(dict[tp]))
    #            res2_max.append(np.max(dict[tp]) - mean)
    #            print tp
    #            print np.mean(dict[tp])

    #for s in skew:
    #    for tp in dict:
    #        if tp.find(s + '-') > 0 and tp.find("timestamps") > 0 and tp.find("prime") >= 0:
    #            res4.append(np.mean(dict[tp]))
    #            mean = np.mean(dict[tp])
    #            res4_min.append(mean - np.min(dict[tp]))
    #            res4_max.append(np.max(dict[tp]) - mean)
    #            print tp
    #            print np.mean(dict[tp])
    #res1 = [2618.45, 17978.96, 30405.52]
    #res2 =[6123.74, 28654.0766667, 35181.7266667]

  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
          # \hline
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.05 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='fLRU',hatch='/',color='#4876FF')
    ax.errorbar([i+0.05 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    #ax.bar( [i+0.0 for i in x],res3,width=0.1,label='rTimestamp',hatch='-',color='#99CC00')
    #ax.errorbar([i+0.05 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    #ax.bar( [i+0.1 for i in x],res4,width=0.1,label='timestamp',hatch='\\/',color='#CD0000')
    #ax.errorbar([i+0.15 for i in x], res4, yerr = [res4_min, res4_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,2.4])
    ax.set_ylim([0,60000])
    ax.set_xticklabels(["0.8", "1.0", "1.1", "1.2"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    plot.savefig(out_path)

def draw_throughput_block_sync_vs_abrt(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1","S1.25"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []
    res4 = []
    res4_min = []
    res4_max = []
    res5 = []
    res5_min = []
    res5_max = []
    res6 = []
    res6_min = []
    res6_max = []

    print dict
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-1c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append(mean - np.min(dict[tp]))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-1c-") > 0 and tp.find("block") > 0 and tp.find("abrt") > 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append(mean - np.min(dict[tp]))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-10c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
                res3.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res3_min.append(mean - np.min(dict[tp]))
                res3_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-10c-") > 0 and tp.find("block") > 0 and tp.find("abrt") > 0:
                res4.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res4_min.append(mean - np.min(dict[tp]))
                res4_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
     
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-50c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
                res5.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res5_min.append(mean - np.min(dict[tp]))
                res5_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-50c-") > 0 and tp.find("block") > 0 and tp.find("abrt") > 0:
                res6.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res6_min.append(mean - np.min(dict[tp]))
                res6_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

     
    x = [0.5,1,1.5,2]
    print res1
    print res2
    print res3
    print res4
    print res5
    print res6

    tmp1 = max(res6)
    tmp2 = max(res5)
    tmp = max(tmp1, tmp2)
    print tmp
    order = 1

    while tmp > 10:
        order = order * 10
        tmp = tmp / 10
    y_max_range = math.ceil(tmp) * order

    print y_max_range

    ax.bar( [i-0.3 for i in x] ,res1,width=0.1,label='1c sync',hatch='\\\\\\',color='#0000CC')
    ax.errorbar([i-0.25 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.2 for i in x],res2,width=0.1,label='1c abrt',hatch='///',color='#FF0000')
    ax.errorbar([i-0.15 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i-0.1 for i in x] ,res3,width=0.1,label='10c sync',hatch='+++',color='#8080E6')
    ax.errorbar([i-0.05 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.bar( [i for i in x],res4,width=0.1,label='10c abrt',hatch='XXX',color='#FF8080')
    ax.errorbar([i+0.05 for i in x], res4, yerr = [res4_min, res4_max], fmt='o')
    ax.bar( [i+0.1 for i in x] ,res5,width=0.1,label='50c sync',hatch='---',color='#CCCCF5')
    ax.errorbar([i+0.15 for i in x], res5, yerr = [res5_min, res5_max], fmt='o')
    ax.bar( [i+0.2 for i in x],res6,width=0.1,label='50c abrt',hatch='...',color='#FFC0C0')
    ax.errorbar([i+0.25 for i in x], res6, yerr = [res6_min, res6_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    lgd = ax.legend(loc=3, bbox_to_anchor=(0.,1, 1, .1),mode="expand", ncol=3)
    ax.set_xlim([0.2,3.2])
    ax.set_ylim([0, y_max_range])
    ax.set_xticklabels(["0.8", "1.01","1.1", "1.25"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1.5,2.5])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    print out_path
    plot.savefig(out_path, bbox_extra_artists=(lgd,), bbox_inches='tight')
                                    
def draw_throughput_sync_tuple_vs_block(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.25", "S4", "S8"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []
    res4 = []
    res4_min = []
    res4_max = []
    res5 = []
    res5_min = []
    res5_max = []
    res6 = []
    res6_min = []
    res6_max = []

    print dict
#    for s in skew:
#        for tp in dict:
#            if tp.find(s + '-') > 0 and tp.find("-1c-") > 0 and tp.find("tuple") > 0 and tp.find("sync") > 0:
#            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("BERKELEY") > 0:
#                res1.append(np.mean(dict[tp]))
#                mean = np.mean(dict[tp])
#                res1_min.append(mean - np.min(dict[tp]))
#                res1_max.append(np.max(dict[tp]) - mean)
#                print tp
#                print np.mean(dict[tp])
    
#    for s in skew:
#        for tp in dict:
#            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("NVM") > 0:
#            if tp.find(s + '-') > 0 and tp.find("-1c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
#                res2.append(np.mean(dict[tp]))
#                mean = np.mean(dict[tp])
#                res2_min.append(mean - np.min(dict[tp]))
#                res2_max.append(np.max(dict[tp]) - mean)
#                print tp
#                print np.mean(dict[tp])
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("-10c-") > 0 and tp.find("tuple") > 0 and tp.find("sync") > 0:
            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("BERKELEY") > 0:
                res3.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res3_min.append(mean - np.min(dict[tp]))
                res3_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("NVM") > 0:
            if tp.find(s + '-') > 0 and tp.find("-10c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
                res4.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res4_min.append(mean - np.min(dict[tp]))
                res4_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
     
#    for s in skew:
#        for tp in dict:
#            if tp.find(s + '-') > 0 and tp.find("-50c-") > 0 and tp.find("tuple") > 0 and tp.find("sync") > 0:
            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("BERKELEY") > 0:
#                res5.append(np.mean(dict[tp]))
#                mean = np.mean(dict[tp])
#                res5_min.append(mean - np.min(dict[tp]))
#                res5_max.append(np.max(dict[tp]) - mean)
#                print tp
#                print np.mean(dict[tp])
    
#    for s in skew:
#        for tp in dict:
#            #if tp.find(s + '-') > 0 and tp.find("aseline") > 0 and tp.find("NVM") > 0:
#            if tp.find(s + '-') > 0 and tp.find("-50c-") > 0 and tp.find("block") > 0 and tp.find("sync") > 0:
#                res6.append(np.mean(dict[tp]))
#                mean = np.mean(dict[tp])
#                res6_min.append(mean - np.min(dict[tp]))
#                res6_max.append(np.max(dict[tp]) - mean)
#                print tp
#                print np.mean(dict[tp])

     
    x = [0.5,1.5,2.5,3.5,4.5]
#    print res1
#    print res2
    print res3
    print res4
#    print res5
#    print res6

    tmp1 = max(res3)
    tmp2 = max(res4)
    tmp = max(tmp1, tmp2)
    print tmp
    order = 1

    while tmp > 10:
        order = order * 10
        tmp = tmp / 10
    y_max_range = math.ceil(tmp) * order

    print y_max_range

#    ax.bar( [i-0.3 for i in x] ,res1,width=0.1,label='1c tuple',hatch='\\\\\\',color='#0000CC')
#    ax.errorbar([i-0.25 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
#    ax.bar( [i-0.2 for i in x],res2,width=0.1,label='1c block',hatch='///',color='#FF0000')
#    ax.errorbar([i-0.15 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i-0.2 for i in x] ,res3,width=0.2,label='10c tuple',hatch='+++',color='#8080E6')
    ax.errorbar([i-0.1 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.bar( [i for i in x],res4,width=0.2,label='10c block',hatch='XXX',color='#FF8080')
    ax.errorbar([i+0.1 for i in x], res4, yerr = [res4_min, res4_max], fmt='o')
#    ax.bar( [i+0.1 for i in x] ,res5,width=0.1,label='50c tuple',hatch='---',color='#CCCCF5')
#    ax.errorbar([i+0.15 for i in x], res5, yerr = [res5_min, res5_max], fmt='o')
#    ax.bar( [i+0.2 for i in x],res6,width=0.1,label='50c block',hatch='...',color='#FFC0C0')
#    ax.errorbar([i+0.25 for i in x], res6, yerr = [res6_min, res6_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    lgd = ax.legend(loc=3, bbox_to_anchor=(0.,1, 1, .1),mode="expand", ncol=3)
    ax.set_xlim([0.2,5.2])
    ax.set_ylim([0, y_max_range])
    ax.set_xticklabels(["0.8", "1.01", "1.25", "4.0", "8.0"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1.5,2.5,3.5,4.5])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    print out_path
    plot.savefig(out_path, bbox_extra_artists=(lgd,), bbox_inches='tight')

def draw_throughput_graph(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
    res1 = []
    res1_min = []
    res1_max = []
    res2 = []
    res2_min = []
    res2_max = []
    res3 = []
    res3_min = []
    res3_max = []
    res4 = []
    res4_min = []
    res4_max = []

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("blocking") > 0 and tp.find("non") < 0 and tp.find("BERKELEY") > 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append(mean - np.min(dict[tp]))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("blocking") > 0 and tp.find("non") < 0 and tp.find("NVM") > 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append(mean - np.min(dict[tp]))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
     
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("nonblocking") > 0 and tp.find("BERKELEY") > 0:
                res3.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res3_min.append(mean - np.min(dict[tp]))
                res3_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])
    
    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("nonblocking") > 0 and tp.find("NVM") > 0:
                res4.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res4_min.append(mean - np.min(dict[tp]))
                res4_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])



   # for s in skew:
   #     for tp in dict:
   #         if tp.find(s + '-') > 0 and tp.find("timestamp") > 0:
   #             res2.append(np.mean(dict[tp]))
   #             mean = np.mean(dict[tp])
   #             res2_min.append(mean - np.min(dict[tp]))
   #             res2_max.append(np.max(dict[tp]) - mean)
   #             print tp
   #             print np.mean(dict[tp])

    #res1 = [2618.45, 17978.96, 30405.52]
    #res2 =[6123.74, 28654.0766667, 35181.7266667]

  #     \#topic ($K$) & 50 & 100 & 150 \\ \hline %\hline
  # TI & 0.7476 & 0.7505  & 0.7349 \\ \hline%\cline{2-4}
  # WTM & \multicolumn{3}{c}{0.7705} \\ \hline%\cline{2-4}
  # COLD(C=100) & 0.8283 & {\bf 0.8397} & 0.8254 \\
          # \hline
    x = [0.5,1,1.5,2]
   # x = [0.8,1.01,1.1,1.2]
    print res1
    print res2
    print res3
    print res4
    ax.bar( [i-0.2 for i in x] ,res1,width=0.1,label='blocking - berkeley',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.15 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.1 for i in x],res2,width=0.1,label='blocking - nvm',hatch='/',color='#99CC00')
    ax.errorbar([i-0.05 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i+0.0 for i in x],res3,width=0.1,label='non-blocking - berkeley',hatch='-',color='#4876FF')
    ax.errorbar([i+0.05 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.bar( [i+0.1 for i in x],res4,width=0.1,label='non-blocking - nvm',hatch='\\/',color='#CD0000')
    ax.errorbar([i+0.15 for i in x], res4, yerr = [res4_min, res4_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=2)
    ax.set_xlim([0.2,2.2])
    ax.set_ylim([0,70000])
    ax.set_xticklabels(["0.8", "1.0", "1.1", "1.2"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    print out_path
    plot.savefig(out_path)

def lru_alru_hstore_evict(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
    res1 = [5000, 18000, 30000, 31000]
    res3 = [5000, 20000, 26000, 25000]

    x = [0.5,1,1.5,2]

    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='LRU',hatch='\\',color='#4876FF')
    ax.bar( [i+0.0 for i in x] ,res3,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,2.2])
    ax.set_ylim([0,50000])
    ax.set_xticklabels(["0.75", "1.0", "1.25", "1.5"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    plot.savefig(out_path)

def lru_alru_hstore(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
    res1 = [8000, 16000, 30000, 31000]
    res2 = [39000, 38000, 39000, 36000]
    res3 = [38000, 37000, 38000, 31000]

    x = [0.5,1,1.5,2]

    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='LRU',hatch='\\',color='#4876FF')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='H-Store',hatch='/',color='#228B22')
    ax.bar( [i+0.1 for i in x] ,res3,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,2.2])
    ax.set_ylim([0,50000])
    ax.set_xticklabels(["0.75", "1.0", "1.25", "1.5"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    plot.savefig(out_path)

def lru_hstore(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
    res1 = [8000, 16000, 30000, 31000]
    res2 = [39000, 38000, 39000, 36000]

    x = [0.5,1,1.5,2]

    ax.bar( [i-0.1 for i in x] ,res1,width=0.1,label='LRU',hatch='\\',color='#4876FF')
    ax.bar( [i-0.0 for i in x],res2,width=0.1,label='H-Store',hatch='/',color='#228B22')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,2.2])
    ax.set_ylim([0,50000])
    ax.set_xticklabels(["0.75", "1.0", "1.25", "1.5"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    y_format = tkr.FuncFormatter(func)  # make formatter
    ax.yaxis.set_major_formatter(y_format) # set formatter to needed axis
    #plt.show()
    plot.savefig(out_path)

def draw_throughput_graph_INF(dict, out_path):
    fig = plot.figure()
    #fig.set_size_inches(8,4.8)
    ax = fig.add_subplot(111)
    skew = ["S0.8", "S1.01", "S1.1", "S1.2"]
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
            if tp.find(s + '-') > 0 and tp.find("lru") > 0:
                res1.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res1_min.append(mean - np.min(dict[tp]))
                res1_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp]), np.min(dict[tp]), np.max(dict[tp])
                print dict[tp]

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("timestamp") > 0:
                res2.append(np.mean(dict[tp]))
                mean = np.mean(dict[tp])
                res2_min.append(mean - np.min(dict[tp]))
                res2_max.append(np.max(dict[tp]) - mean)
                print tp
                print np.mean(dict[tp])

    for s in skew:
        for tp in dict:
            if tp.find(s + '-') > 0 and tp.find("normal") > 0:
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
    x = [0.5,1,1.5,2]
    ax.bar( [i-0.15 for i in x] ,res1,width=0.1,label='aLRU',hatch='\\',color='#FF6600')
    ax.errorbar([i-0.1 for i in x], res1, yerr = [res1_min, res1_max], fmt='o')
    ax.bar( [i-0.05 for i in x],res2,width=0.1,label='timestamps',hatch='/',color='#99CC00')
    ax.errorbar([i-0.0 for i in x], res2, yerr = [res2_min, res2_max], fmt='o')
    ax.bar( [i+0.05 for i in x],res3,width=0.1,label='none',hatch='-',color='b')
    ax.errorbar([i+0.1 for i in x], res3, yerr = [res3_min, res3_max], fmt='o')
    ax.set_ylabel("Transactions per second",fontsize=16,weight='bold')
    #ax.legend(loc='center left', bbox_to_anchor=(1, 0.5))
    ax.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05),ncol=3)
    ax.set_xlim([0.2,2.3])
    ax.set_ylim([0,50000])
    ax.set_xticklabels(["0.8", "1.0", "1.1", "1.2"],fontsize=16)
    ax.set_xlabel("Skew factor (Low -> High)",fontsize=16,weight='bold')
    ax.set_xticks([0.5,1,1.5,2])
    ax.yaxis.get_major_formatter().set_useLocale("d")
    #plt.show()
    plot.savefig(out_path)

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    dict = {}
    matches = []
    for root, dirnames, filenames in os.walk("/home/user/giardino/data-hstore/ycsb/ycsb-nvm/1p"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    #for root, dirnames, filenames in os.walk("ycsb/ycsb-T750-NoLoop"):
    #for root, dirnames, filenames in os.walk("ycsb/ycsb-T500-NoLoop"):
     #   for filename in fnmatch.filter(filenames, '*results.csv'):
     #       matches.append(os.path.join(root, filename))
    map(computeEvictionStats, matches)

    #for tp in dict:
    #    print tp
    #    print np.mean(dict[tp])

    #draw_throughput_graph_INF(dict, "ycsb-INF.pdf")
    #draw_throughput_graph(dict, "ycsb-T500-NoLoop-blocking-vs-nonblocking-6.pdf")
    draw_throughput_sync_tuple_vs_block(dict, "ycsb-1-tier-1p-sync-tuple-vs-block.pdf");

    dict = {}
    matches = []
    for root, dirnames, filenames in os.walk("/home/user/giardino/data-hstore/ycsb/ycsb-nvm/8p"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    #for root, dirnames, filenames in os.walk("ycsb/ycsb-T750-NoLoop"):
    #for root, dirnames, filenames in os.walk("ycsb/ycsb-T500-NoLoop"):
     #   for filename in fnmatch.filter(filenames, '*results.csv'):
     #       matches.append(os.path.join(root, filename))
    map(computeEvictionStats, matches)

    #for tp in dict:
    #    print tp
    #    print np.mean(dict[tp])

    #draw_throughput_graph_INF(dict, "ycsb-INF.pdf")
    #draw_throughput_graph(dict, "ycsb-T500-NoLoop-blocking-vs-nonblocking-6.pdf")
    #draw_throughput_sync_tuple_vs_block(dict, "ycsb-1-tier-8p-sync-tuple-vs-block.pdf");
    #draw_throughput_graph_all(dict, "ycsb-T500-NoLoop-prime-all.pdf")
    #lru_hstore(dict, "lru-hstore.pdf")
    #lru_alru_hstore(dict, "lru-alru-hstore.pdf")
    #lru_alru_hstore_evict(dict, "lru-alru-hstore-evict.pdf")

    dict = {}
    matches = []
    for root, dirnames, filenames in os.walk("/home/user/giardino/data-hstore/ycsb/ycsb-nvm/300s"):
        for filename in fnmatch.filter(filenames, '*results.csv'):
            matches.append(os.path.join(root, filename))
    
    map(computeEvictionStats, matches)

    draw_throughput_block_sync_vs_abrt(dict, "ycsb-1-tier-1p-block-sync-vs-abrt.pdf"); 
## MAIN
