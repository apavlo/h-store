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
import json
import logging
import getopt
import string
import math
import types
from pprint import pprint, pformat
from fabric.api import *
from fabric.network import *
from fabric.contrib.files import *

## This has all the functions we can use to invoke experiments on EC2
import fabfile

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION PARAMETERS
## ==============================================

OPT_EXP_TYPE = "motivation"
OPT_EXP_TRIALS = 3
OPT_EXP_SETTINGS = 0
OPT_EXP_FACTOR_START = 0
OPT_EXP_FACTOR_STOP = 110
OPT_REPEAT_FAILED_TRIALS = 3

BASE_SETTINGS = {
    "client.blocking":                  False,
    "client.blocking_concurrent":       250,
    "client.txnrate":                   1000,
    "client.count":                     2,
    "client.processesperclient":        10,
    "client.skewfactor":                -1,
    "client.duration":                  120000,
    "client.warmup":                    60000,
    "client.scalefactor":               100,
    
    "site.count":                       4,
    "site.partitions_per_site":         4,
    "site.status_show_txn_info":        True,
    "site.exec_profiling":              True,
    "site.memory":                      6144,
    
    "benchmark.neworder_only":          True,
    "benchmark.neworder_abort":         False,
    "benchmark.neworder_all_multip":    False,
}



EXPERIMENT_SETTINGS = {
    "motivation": [
        ## Settings #0 - Vary the percentage of multi-partition txns
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         True,
            "site.exec_neworder_cheat":          True,
            "site.exec_neworder_cheat_done_partitions": True,
        },
        ## Settings #1 - Vary the amount of skew of warehouse ids
        {
            "benchmark.neworder_skew_warehouse": True,
            "benchmark.neworder_multip":         False,
        },
        ## Settings #2 - Temporal Skew
        {
            "client.tick_interval":              10000,
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         False,
            "benchmark.temporal_skew":           True,
            "benchmark.temporal_skew_mix":       0,
        },
    ],
    "generate": [
        
        
    ],
}

## ==============================================
## updateEnv
## ==============================================
def updateEnv(env, exp_type, exp_setting, exp_factor):
    if exp_type == "motivation":
        num_warehouses = env["site.count"] * env["site.partitions_per_site"]
        env["benchmark.warehouses"] = num_warehouses
        env["benchmark.loadthreads"] = num_warehouses
        #if exp_factor > 0:
            #env["client.blocking"] = True # To prevent OutOfMemory

        if exp_setting == 0:
            env["benchmark.neworder_multip_mix"] = exp_factor
        elif exp_setting == 1:
            if exp_factor == 0:
                env["benchmark.neworder_skew_warehouse"] = False
                env["client.skewfactor"] =  -1
            else:
                env["client.skewfactor"] = 1.00001 + (0.25 * (exp_factor - 10) / 10.0)
            LOG.info("client.skewfactor = %f [exp_factor=%d]" % (env["client.skewfactor"], exp_factor))
        elif exp_setting == 2:
            if exp_factor == 0:
                env["benchmark.temporal_skew"] = False
            else:
                env["benchmark.temporal_skew"] = True
            env["benchmark.temporal_skew_mix"] = exp_factor
            LOG.info("benchmark.temporal_skew_mix = %d" % env["benchmark.temporal_skew_mix"])
    ## IF
## DEF

#BENCHMARKS = [ 'tpcc', 'tm1', 'airline', 'auctionmark', 'tpce' ]
#DESIGN_ALGORITHMS = {
    #'LNS':  {
        #'designer.partitioner': 'edu.brown.designer.partitioners.LNSPartitioner',
    #}
    ## The same as LNS without vertical partitioning / time intervals
    #'SCH':  {
        #'designer.partitioner': 'edu.brown.designer.partitioners.LNSPartitioner',
    #}
    ## The same as LNS but using a greedy search
    #'GRD':  {
        #'designer.partitioner': 'edu.brown.designer.partitioners.LNSPartitioner',
        #'designer.hints.LIMIT_LOCAL_TIME': -1,
        #'designer.hints.GREEDY_SEARCH': True,
    #}
    #'PKY':  {
        #'designer.partitioner': 'edu.brown.designer.partitioners.PrimaryKeyPartitioner',
    #}
    #'MFA':  {
        #'designer.partitioner': 'edu.brown.designer.partitioners.MostPopularPartitioner',
    #}
#}

## ==============================================
## parseResultsOutput
## ==============================================
def parseResultsOutput(output):
    # We always need to make sure that we clear out ant's [java] prefix
    output = re.sub("[\s]+\[java\] ", "\n", output)
    
    # Find our <json> tag. The results will be inside of there
    regex = re.compile("<json>(.*?)</json>", re.MULTILINE | re.IGNORECASE | re.DOTALL)
    m = regex.search(output)
    if not m: LOG.error("Invalid output:\n" + output)
    assert m

    json_results = json.loads(m.group(1))
    return (json_results)
## DEF

## ==============================================
## generateDesigns
## ==============================================
def generateDesigns():
    # We always need to make sure that we clear out ant's [java] prefix
    output = re.sub("[\s]+\[java\] ", "\n", output)
    
    # Find our <json> tag. The results will be inside of there
    regex = re.compile("<json>(.*?)</json>", re.MULTILINE | re.IGNORECASE | re.DOTALL)
    m = regex.search(output)
    if not m: LOG.error("Invalid output:\n" + output)
    assert m

    json_results = json.loads(m.group(1))
    return (json_results)
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        # Experiment Parameters
        "exp-type=",
        "exp-settings=",
        "exp-trials=",
        "exp-factor-start=",
        "exp-factor-stop=",
        
        "repeat-failed-trials=",
        
        # Enable debug logging
        "debug",
    ])
    
    ## ----------------------------------------------
    ## COMMAND OPTIONS
    ## ----------------------------------------------
    options = { }
    for key, value in _options:
       if key.startswith("--"): key = key[2:]
       if key in options:
          options[key].append(value)
       else:
          options[key] = [ value ]
    ## FOR
    if "debug" in options:
        LOG.setLevel(logging.DEBUG)
        fabfile.LOG.setLevel(logging.DEBUG)
    ## Global Options
    for key in options:
        varname = "OPT_" + key.replace("-", "_").upper()
        if varname in globals() and len(options[key]) == 1:
            orig_type = type(globals()[varname])
            if orig_type == bool:
                val = (len(options[key][0]) == 0 or options[key][0].lower() == "true")
            else: 
                val = orig_type(options[key][0])
            globals()[varname] = val
            LOG.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR

    ## Update Fabric env
    exp_opts = dict(BASE_SETTINGS.items() + EXPERIMENT_SETTINGS[OPT_EXP_TYPE][OPT_EXP_SETTINGS].items())
    assert exp_opts
    for key,val in exp_opts.items():
        if type(val) != types.FunctionType: env[key] = val
    ## FOR

    ## If the OPT_EXP_TYPE is "generate", then we actually don't
    ## want to run on EC2. We instead want to locally create all the design files that 
    ## we need
    
    ## Figure out what keys we need to remove to ensure that one experiment
    ## doesn't contaminate another
    conf_remove = set()
    for other_type in EXPERIMENT_SETTINGS.keys():
        for i in range(len(EXPERIMENT_SETTINGS[other_type])):
            if other_type != OPT_EXP_TYPE or i != OPT_EXP_SETTINGS:
                for key in EXPERIMENT_SETTINGS[other_type][i].keys():
                    if not key in exp_opts: conf_remove.add(key)
                ## FOR
            ## IF
        ## FOR
    ## FOR
    LOG.debug("Configuration Parameters to Remove:\n" + pformat(conf_remove))
    
    client_inst = fabfile.__getClientInstance__()
    LOG.debug("Client Instance: " + client_inst.public_dns_name)
    all_results = [ ]
    stop = False
    
    first = True
    for exp_factor in range(OPT_EXP_FACTOR_START, OPT_EXP_FACTOR_STOP, 10):
        updateEnv(env, OPT_EXP_TYPE, OPT_EXP_SETTINGS, exp_factor)
        
        results = [ ]
        for trial in range(OPT_EXP_TRIALS):
            attempts = OPT_REPEAT_FAILED_TRIALS
            while attempts > 0 and stop == False:
                if first:
                    env["hstore.exec_prefix"] = "compile"
                else:
                    env["hstore.exec_prefix"] = ""
                first = False
                
                if attempts == OPT_REPEAT_FAILED_TRIALS and trial == 0:
                    LOG.debug("%s/%d - Experiment Parameters:\n%s" % (OPT_EXP_TYPE.title(), OPT_EXP_SETTINGS, pformat(env)))
                
                attempts -= 1
                LOG.info("Executing trial #%d for factor %d [attempt=%d/%d]" % (trial, exp_factor, (OPT_REPEAT_FAILED_TRIALS-attempts), OPT_REPEAT_FAILED_TRIALS))
                try:
                    with settings(host_string=client_inst.public_dns_name):
                        output = fabfile.exec_benchmark(project="tpcc", removals=conf_remove, json=True)
                        results.append(parseResultsOutput(output))
                    ## WITH
                    break
                except KeyboardInterrupt:
                    stop = True
                    break
                except SystemExit:
                    LOG.warn("Failed to complete trial succesfully")
                    pass
            ## WHILE
            if stop: break
        ## FOR
        if results: all_results.append((exp_factor, results))
        if stop: break
    ## FOR
    
    try:
        disconnect_all()
    finally:
        for exp_factor, results in all_results:
            print "EXP FACTOR %d" % exp_factor
            for trial in range(len(results)):
                r = results[trial]
                print "   TRIAL #%d: %s" % (trial, r["TXNPERSECOND"])
            ## FOR
            print
        ## FOR
    ## TRY
## MAIN