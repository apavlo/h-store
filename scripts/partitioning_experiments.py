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
OPT_EXP_ATTEMPTS = 3
OPT_START_CLUSTER = False
OPT_TRACE = False
OPT_BENCHMARK = "tpcc"

OPT_BASE_BLOCKING_CONCURRENT = 2000
OPT_BASE_TXNRATE = 2000
OPT_BASE_CLIENT_COUNT = 2

BASE_SETTINGS = {
    "ec2.type":                         "c1.xlarge",
    "ec2.client_type":                  "c1.xlarge",
    "ec2.node_type":                    "m2.4xlarge",
    "ec2.change_type":                  False,
    
    "client.blocking":                  False,
    "client.blocking_concurrent":       OPT_BASE_BLOCKING_CONCURRENT,
    "client.txnrate":                   OPT_BASE_TXNRATE,
    "client.count":                     OPT_BASE_CLIENT_COUNT,
    "client.processesperclient":        10,
    "client.skewfactor":                -1,
    "client.duration":                  120000,
    "client.warmup":                    30000,
    "client.scalefactor":               50,
    
    "site.sites_per_host":              1,
    "site.partitions_per_site":         4,
    "site.exec_profiling":              True,
    "site.memory":                      60020,
    "site.status_show_txn_info":        True,
    "site.status_kill_if_hung":         False,
    "site.status_interval":             None,
    "site.txn_incoming_queue_max_per_partition": 5000,
    "site.txn_incoming_queue_release_factor": 0.75,
    "site.txn_redirect_queue_max_per_partition": 5000,
    "site.txn_redirect_queue_release_factor": 0.75,
    "site.exec_postprocessing_thread":  False,
    
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
            
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         False,
            "benchmark.neworder_all_multip":    False,
            "benchmark.neworder_multip":        False,
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
        env["benchmark.warehouses"] = env["site.partitions"]
        env["benchmark.loadthreads"] = env["site.partitions"]
        #if exp_factor > 0:
            #env["client.blocking"] = True # To prevent OutOfMemory

        if exp_setting == 0:
            env["benchmark.neworder_multip_mix"] = exp_factor
            env["benchmark.neworder_multip"] = (exp_factor > 0)
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
    
    ## The number of concurrent blocked txns should be based on the number of partitions
    if exp_factor == 0:
        delta = OPT_BASE_BLOCKING_CONCURRENT * (env["site.partitions"]/float(32))
        env["client.blocking_concurrent"] = int(OPT_BASE_BLOCKING_CONCURRENT + delta)
        env["client.blocking"] = (exp_factor > 0)
        
        delta = OPT_BASE_TXNRATE * (env["site.partitions"]/float(32))
        env["client.txnrate"] = int(OPT_BASE_TXNRATE + delta)
        
        env["client.count"] = int(OPT_BASE_CLIENT_COUNT * math.ceil(env["site.partitions"]/4.0)) - 2
        
        for key in [ "count", "txnrate", "blocking", "blocking_concurrent" ]:
            key = "client.%s" % key
            LOG.info("%s = %s" % (key, env[key]))
    ## IF
## DEF

## ==============================================
## parseResultsOutput
## ==============================================
def parseResultsOutput(output):
    # We always need to make sure that we clear out ant's [java] prefix
    output = re.sub("[\s]+\[java\] ", "\n", output)
    
    # Find our <json> tag. The results will be inside of there
    regex = re.compile("<json>(.*?)</json>", re.MULTILINE | re.IGNORECASE | re.DOTALL)
    m = regex.search(output)
    if not m: 
        LOG.error("Invalid output:\n" + output)
        raise Exception("Invalid JSON results output")

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
        "exp-attempts=",
        "exp-factor-start=",
        "exp-factor-stop=",
        
        "benchmark=",
        "repeat-failed-trials=",
        "partitions=",
        "start-cluster",
        "trace",
        
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
    if not "partitions" in options:
        raise Exception("Missing 'partitions' parameter")

    ## Update Fabric env
    exp_opts = dict(BASE_SETTINGS.items() + EXPERIMENT_SETTINGS[OPT_EXP_TYPE][OPT_EXP_SETTINGS].items())
    assert exp_opts
    conf_remove = set()
    for key,val in exp_opts.items():
        if val == None: 
            LOG.debug("Parameter to Remove: %s" % key)
            conf_remove.add(key)
            del exp_opts[key]
            assert not key in exp_opts
        elif type(val) != types.FunctionType:
            env[key] = val
    ## FOR
    
    ## Figure out what keys we need to remove to ensure that one experiment
    ## doesn't contaminate another
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
    

    final_results = { }
    totalAttempts = OPT_EXP_TRIALS * OPT_EXP_ATTEMPTS
    first = True
    stop = False
    for partitions in map(int, options["partitions"]):
        LOG.info("%s - %d Partitions - Experiment #%d" % (OPT_EXP_TYPE.upper(), partitions, OPT_EXP_SETTINGS))
        env["site.partitions"] = partitions
        all_results = [ ]
        
        for exp_factor in range(OPT_EXP_FACTOR_START, OPT_EXP_FACTOR_STOP, 20):
            updateEnv(env, OPT_EXP_TYPE, OPT_EXP_SETTINGS, exp_factor)
            LOG.debug("Parameters:\n%s" % pformat(env))
            
            if first:
                if OPT_START_CLUSTER:
                    LOG.info("Starting cluster for experiments")
                    fabfile.start_cluster()
                
                client_inst = fabfile.__getClientInstance__()
                LOG.debug("Client Instance: " + client_inst.public_dns_name)
            ## IF

            results = [ ]
            attempts = 0
            while len(results) < OPT_EXP_TRIALS and attempts < totalAttempts and stop == False:
                ## Only compile for the very first invocation
                env["hstore.exec_prefix"] = "compile" if first else ""
                updateSVN = first
                first = False
                
                attempts += 1
                LOG.info("Executing Trial #%d/%d for Factor %d [attempt=%d/%d]" % (len(results), OPT_EXP_TRIALS, exp_factor, attempts, totalAttempts))
                try:
                    with settings(host_string=client_inst.public_dns_name):
                        output, workloads = fabfile.exec_benchmark(project=OPT_BENCHMARK, \
                                                                   removals=conf_remove, \
                                                                   json=True, \
                                                                   trace=OPT_TRACE, \
                                                                   update=updateSVN)
                        data = parseResultsOutput(output)
                        results.append(float(data["TXNPERSECOND"]))
                        if OPT_TRACE and workloads != None:
                            for f in workloads:
                                LOG.info("Workload File: %s" % f)
                            ## FOR
                        ## IF
                        LOG.info("Throughput: %.2f" % results[-1])
                    ## WITH
                except KeyboardInterrupt:
                    stop = True
                    break
                except SystemExit:
                    LOG.warn("Failed to complete trial succesfully")
                    pass
                except:
                    LOG.warn("Failed to complete trial succesfully")
                    stop = True
                    break
            ## FOR (TRIALS)
            if results: all_results.append((exp_factor, results, attempts))
            stop = stop or (attempts == totalAttempts)
            if stop: break
        ## FOR (EXP_FACTOR)
        if len(all_results) > 0: final_results[partitions] = all_results
        if stop: break
    ## FOR (PARTITIONS)
    
    LOG.info("Disconnecting and dumping results")
    try:
        disconnect_all()
    finally:
        for partitions in sorted(final_results.keys()):
            all_results = final_results[partitions]
            print "%s - Partitions %d" % (OPT_EXP_TYPE.upper(), partitions)
            for exp_factor, results, attempts in all_results:
                print "   EXP FACTOR %d [Attempts:%d/%d]" % (exp_factor, attempts, totalAttempts)
                for trial in range(len(results)):
                    print "      TRIAL #%d: %.4f" % (trial, results[trial])
                ## FOR
            ## FOR
            print
        ## FOR
    ## TRY
## MAIN