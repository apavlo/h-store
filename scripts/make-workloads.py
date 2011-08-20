#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2011 by H-Store Project
# Brown University
# Massachusetts Institute of Technology
# Yale University
# 
# http://hstore.cs.brown.edu/ 
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
# -----------------------------------------------------------------------
from __future__ import with_statement

import os
import sys
import glob
import re
import commands
import subprocess
import logging
import argparse
import string
from pprint import pprint, pformat

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

HSTORE_OPTS = {
    "client.duration":              180000,
    "client.warmup":                0,
    "client.count":                 1,
    "client.processesperclient":    1,
    "client.txnrate":               10000,
    "client.blocking":              True,
    "client.blocking_concurrent":   1000,
    "client.scalefactor":           100,
}

## ==============================================
## txnCount
## ==============================================
def txnCount(path):
    cmd = "wc -l %s*" % path
    (result, output) = commands.getstatusoutput(cmd)
    assert result == 0, cmd + "\n" + output
    lines = output.split("\n")
    assert lines[-1].find("total") != -1
    return int(lines[-1].strip().split(" ")[0])
## DEF
    
## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Create H-Store transaction trace files')
    aparser.add_argument('benchmark', choices=[ 'tpcc', 'tm1', 'auctionmark', 'locality', 'airline' ],
                         help='Target benchmark')
    aparser.add_argument('--config', type=file,
                         help='Path to H-Store configuration file to use')
    aparser.add_argument('--txn-count', type=int, default=100000,
                         help='The number of transaction records needed')
    aparser.add_argument('--output-path', type=str, default="traces",
                         help='The output directory to store the traces')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    if not os.path.exists(args['output_path']): os.makedirs(args['output_path'])
    
    trace_base = os.path.join(args['output_path'], args['benchmark'])
    HSTORE_OPTS["project"] = args['benchmark']
    hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, HSTORE_OPTS[x]), HSTORE_OPTS.keys()))

    total_cnt = 0
    trace_ctr = 0
    while True:
        trace = "%s-%02d" % (trace_base, trace_ctr)
        cmd = "ant hstore-benchmark -Dtrace=%s %s" % (trace, hstore_opts_cmd)
        subprocess.check_call(cmd, shell=True)
        
        ## After each run check whether we have enough transaction traces
        cnt = txnCount(trace_base)
        assert cnt > 0
        total_cnt += cnt
        logging.info("Number of transactions after round %d: %d" % (trace_ctr, total_cnt))
        if total_cnt >= args['txn_count']: break
        trace_ctr += 1
    ## WHILE
    
## MAIN