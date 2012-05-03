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
import string
from pprint import pprint, pformat

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
import argparse

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

HSTORE_OPTS = {
    "client.duration":              360000,
    "client.warmup":                0,
    "client.count":                 1,
    "client.processesperclient":    200,
    "client.txnrate":               1000,
    "client.blocking":              True,
    "client.blocking_concurrent":   1,
    "client.scalefactor":           1,
}

## ==============================================
## txnCount
## ==============================================
def txnCount(path):
    cmd = "wc -l %s*" % path
    logging.debug("Executing Line Count: " + cmd)
    (result, output) = commands.getstatusoutput(cmd)
    assert result == 0, cmd + "\n" + output
    lines = output.split("\n")
    if len(lines) > 1: assert lines[-1].find("total") != -1
    return int(lines[-1].strip().split(" ")[0])
## DEF
    
## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Create H-Store transaction trace files')
    aparser.add_argument('benchmark', choices=[ 'tpcc', 'tm1', 'auctionmark', 'locality', 'seats' ],
                         help='Target benchmark')
    aparser.add_argument('--config', type=file,
                         help='Path to H-Store configuration file to use')
    aparser.add_argument('--txn-count', type=int, default=100000,
                         help='The number of transaction records needed')
    aparser.add_argument('--output-path', type=str, default="traces",
                         help='The output directory to store the traces')
    aparser.add_argument('--overwrite', action='store_true',
                         help='Overwrite existing trace files')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())

    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    if not os.path.exists(args['output_path']): os.makedirs(args['output_path'])
    
    trace_base = os.path.join(args['output_path'], args['benchmark'])
    HSTORE_OPTS["project"] = args['benchmark']
    hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, HSTORE_OPTS[x]), HSTORE_OPTS.keys()))

    logging.info("Generating Transaction Workload for %s [totalTxns=%d]" % (args['benchmark'].upper(), args['txn_count']))
    total_cnt = 0
    trace_ctr = 0
    while True:
        trace = "%s-%02d" % (trace_base, trace_ctr)
        
        # Check whether it already exists, then we can skip this round
        existing_file = trace + "-0"
        if args['overwrite'] == False and os.path.exists(existing_file) and txnCount(existing_file) > 0:
            cnt = txnCount(existing_file)
            logging.info("Trace file '%s' already exists with %d lines. Skipping..." % (trace, cnt))

        ## Otherwise light 'em up!
        else:
            cmd = "ant hstore-benchmark -Dtrace=%s %s" % (trace, hstore_opts_cmd)
            subprocess.check_call(cmd, shell=True)
            cnt = txnCount(trace_base)

        assert cnt > 0
        total_cnt += cnt
        logging.info("Number of transactions after round %d: %d" % (trace_ctr, total_cnt))
        if total_cnt >= args['txn_count']: break
        trace_ctr += 1
    ## WHILE
    
    # Compute the total again so that we know we are getting all of the existing files
    total_cnt = txnCount(trace_base)
    assert total_cnt > 0
    
    ## Then combine them into a single file
    output = "%s-combined.trace" % args['benchmark']
    logging.info("Combining %d trace records into a single file '%s'" % (total_cnt, output))
    if os.path.exists(output):
        if args['overwrite']:
            logging.warn("Overwriting existing combined output file '%s'" % output)
        else:
            raise Exception("The combined output file '%s' already exists" % output)
    
    HSTORE_OPTS = {
        "project":              args['benchmark'],
        "volt.server.memory":   4096,
        "output":               output,
        "workload":             trace_base + "*",
    }
    hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, HSTORE_OPTS[x]), HSTORE_OPTS.keys()))
    cmd = "ant workload-combine %s" % hstore_opts_cmd
    subprocess.check_call(cmd, shell=True)

    logging.info("OUTPUT FILE: " + HSTORE_OPTS["output"])
## MAIN