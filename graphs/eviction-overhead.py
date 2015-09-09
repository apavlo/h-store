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


def computeEvictionStats(dataFile):
    colMap, csvData = datautil.getCSVData(dataFile)
    if len(csvData) == 0: return
    
    allTimes = [ ]
    allTuples = [ ]
    allBlocks = [ ]
    allBytes = [ ]
    
    for row in csvData:
        allTimes.append(row[colMap["STOP"]] - row[colMap["START"]])
        allTuples.append(int(row[colMap["TUPLES_EVICTED"]]))
        allBlocks.append(int(row[colMap["TUPLES_EVICTED"]]))
        allBytes.append(int(row[colMap["BYTES_EVICTED"]]))
    
    print dataFile
    print "  Average Time: %.2f ms" % np.mean(allTimes)
    print "  Average Tuples: %.2f" % np.mean(allTuples)
    print "  Average Blocks: %.2f" % np.mean(allBlocks)
    print "  Average Bytes: %.2f MB" % (np.mean(allBytes)/float(1024*1024))
    print
# DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    matches = []
    for root, dirnames, filenames in os.walk(OPT_DATA_HSTORE):
        for filename in fnmatch.filter(filenames, 'evictions.csv'):
            matches.append(os.path.join(root, filename))
    map(computeEvictionStats, matches)

## MAIN