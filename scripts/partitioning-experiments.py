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
OPT_EXP_FACTOR_START = "0"
OPT_EXP_FACTOR_STOP = "100"
OPT_EXP_ATTEMPTS = 3
OPT_START_CLUSTER = False
OPT_TRACE = False
OPT_FAST = False
OPT_NO_EXECUTE = False
OPT_NO_COMPILE = False
OPT_NO_JAR = False
OPT_NO_JSON = False
OPT_NO_CONF = False
OPT_NO_UPDATE = False
OPT_NO_SYNC = True
OPT_STOP_ON_ERROR = False
OPT_FORCE_REBOOT = False

OPT_BASE_BLOCKING = True
OPT_BASE_BLOCKING_CONCURRENT = 1
OPT_BASE_TXNRATE_PER_PARTITION = 100000
OPT_BASE_TXNRATE = 12500
OPT_BASE_CLIENT_COUNT = 4
OPT_BASE_CLIENT_PROCESSESPERCLIENT = 400
OPT_BASE_SCALE_FACTOR = 50
OPT_BASE_PARTITIONS_PER_SITE = 6

BASE_SETTINGS = {
    "ec2.client_type":                  "c1.xlarge",
    "ec2.site_type":                  "c1.xlarge",
    #"ec2.site_type":                    "m2.4xlarge",
    #"ec2.client_type":                  "m1.large",
    #"ec2.site_type":                    "m1.xlarge",
    
    "ec2.change_type":                  True,
    
    "client.blocking":                  OPT_BASE_BLOCKING,
    "client.blocking_concurrent":       OPT_BASE_BLOCKING_CONCURRENT,
    "client.txnrate":                   OPT_BASE_TXNRATE,
    "client.count":                     OPT_BASE_CLIENT_COUNT,
    "client.processesperclient":        OPT_BASE_CLIENT_PROCESSESPERCLIENT,
    "client.skewfactor":                -1,
    "client.duration":                  60000,
    "client.warmup":                    00000,
    "client.scalefactor":               OPT_BASE_SCALE_FACTOR,
    "client.txn_hints":                 True,
    "client.throttle_backoff":          50,
    "client.memory":                    6000,
    "client.blocking_loader":           True,
    
    "site.exec_profiling":                              True,
    "site.txn_profiling":                               False,
    "site.pool_profiling":                              False,
    "site.planner_profiling":                           False,
    "site.planner_caching":                             True,
    "site.status_show_txn_info":                        True,
    "site.status_kill_if_hung":                         True,
    "site.status_show_thread_info":                     False,
    "site.status_show_exec_info":                       False,
    "site.status_interval":                             20000,
    "site.txn_incoming_delay":                          1,
    "site.coordinator_init_thread":                     False,
    "site.coordinator_finish_thread":                   False,
    "site.txn_restart_limit":                           5,
    "site.txn_restart_limit_sysproc":                   100,
    
    "site.sites_per_host":                              1,
    "site.partitions_per_site":                         OPT_BASE_PARTITIONS_PER_SITE,
    "site.memory":                                      6002,
    "site.txn_incoming_queue_max_per_partition":        10000,
    "site.txn_incoming_queue_release_factor":           0.90,
    "site.txn_incoming_queue_increase":                 10,
    "site.txn_enable_queue_pruning":                    False,
    "site.exec_postprocessing_thread":                  False,
    "site.pool_localtxnstate_idle":                     20000,
    "site.pool_batchplan_idle":                         10000,
    "site.exec_db2_redirects":                          False,
    "site.cpu_affinity":                                True,
    "site.cpu_affinity_one_partition_per_core":         False,
}

EXPERIMENT_SETTINGS = {
    "motivation": [
        ## Settings #0 - Vary the percentage of multi-partition txns
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         True,
            "site.exec_neworder_cheat":          True,
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
            "benchmark.temporal_skew_rotate":    False,
            
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         False,
            "benchmark.neworder_multip":        False,
        },
    ],
    "throughput": [
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_only":          False,
            "benchmark.neworder_abort":         True,
            "site.exec_neworder_cheat":         True
        }
        
    ],
    "breakdown": [
        {
            "site.exec_speculative_execution": False
        }
        
    ],
}

# Thoughput Experiments
OPT_PARTITION_PLANS = [ 'lns', 'schism', 'popular' ]
OPT_BENCHMARKS = [ 'tm1', 'tpcc', 'tpcc-skewed', 'airline', 'auctionmark' ]
OPT_PARTITION_PLAN_DIR = "files/designplans/vldb-aug2011"

## ==============================================
## updateEnv
## ==============================================
def updateEnv(env, benchmark, exp_type, exp_setting, exp_factor):
    global OPT_BASE_TXNRATE_PER_PARTITION
  
    ## ==============================================
    ## ----------------------------------------------
  
    ## MOTIVATION
    if exp_type == "motivation":
        env["benchmark.neworder_only"] = True
        env["benchmark.neworder_abort"] = False

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
    ## THROUGHPUT
    elif exp_type == "throughput":
        pplan = "%s.%s.pplan" % (benchmark, exp_factor)
        env["hstore.exec_prefix"] = "-Dpartitionplan=%s" % os.path.join(OPT_PARTITION_PLAN_DIR, pplan)
        env["benchmark.neworder_multip_mix"] = -1
        env["benchmark.neworder_multip"] = True
        
        #base_txnrate = int(OPT_BASE_TXNRATE / 2) if benchmark == "airline" else OPT_BASE_TXNRATE
        #env["client.txnrate"] = int(base_txnrate * (env["site.partitions"]/float(4)))
        
        ## Everything but LNS has to use the DB2 redirects
        if exp_factor != "lns":
            env["site.exec_neworder_cheat"] = False
            env["client.txn_hints"] = False
            env["site.exec_db2_redirects"] = True
            env["client.processesperclient"] = OPT_BASE_CLIENT_PROCESSESPERCLIENT / 2
        else:
            env["site.exec_neworder_cheat"] = True
        ## IF
        
    ## BREAKDOWN
    elif exp_type == "breakdown":
        ## FULL DESIGN
        if exp_factor == "full":
            pplan = "%s.%s.pplan" % (benchmark, "lns")
            
        ## WITH SECONDARY INDEX, NO ROUTING
        elif exp_factor == "norouting":
            pplan = "%s.%s.pplan" % (benchmark, "lns")
            env["client.txn_hints"] = False
            env["site.exec_db2_redirects"] = True
            
        ## WITH ROUTING, NO SECONDARY INDEX
        elif exp_factor == "noindexes":
            pplan = "%s.%s.pplan" % (benchmark, "lns")
            env["client.txn_hints"] = True
            env["site.exec_db2_redirects"] = False
            env["hstore.exec_prefix"] += " -Dpartitionplan.nosecondary=true"
        else:
            raise Exception("Unexpected factor '%s'" % exp_factor)
        ## IF
        env["hstore.exec_prefix"] += " -Dpartitionplan=%s" % os.path.join(OPT_PARTITION_PLAN_DIR, pplan)
    ## IF

    ## CUSTOM BENCHMARK TYPE
    if benchmark.startswith("tpcc"):
        env["benchmark.warehouses"] = env["site.partitions"]
        env["benchmark.loadthreads"] = env["site.partitions"]
        if benchmark.endswith("-skewed"):
            env["benchmark.temporal_skew"] = True
            env["benchmark.temporal_skew_rotate"] = False
            env["benchmark.temporal_skew_mix"] = 100
        #else:
            #env["benchmark.temporal_skew"] = False
            #env["benchmark.temporal_skew_rotate"] = False
            #env["benchmark.temporal_skew_mix"] = -1
    elif benchmark == "airline":
        env["client.scalefactor"] = 100
        env["client.txnrate"] = int(OPT_BASE_TXNRATE / 2)

        ## MOTIVATION
        
    env["ec2.force_reboot"] = OPT_FORCE_REBOOT
    env["client.scalefactor"] = OPT_BASE_SCALE_FACTOR
    env["client.txnrate"] = int((OPT_BASE_TXNRATE_PER_PARTITION * env["site.partitions"]) / (env["client.count"] * env["client.processesperclient"]))

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
        
        "benchmarks=",
        "partition-plans=",
        "repeat-failed-trials=",
        "partitions=",
        "start-cluster",
        "fast",
        "no-execute",
        "no-compile",
        "no-update",
        "no-jar",
        "no-conf",
        "no-sync",
        "no-json",
        "force-reboot",
        "stop-on-error",
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
        if varname in globals():
            orig_type = type(globals()[varname])
            if orig_type == bool:
                val = (len(options[key][0]) == 0 or options[key][0].lower() == "true")
            elif orig_type == list:
                if not varname+"_changed" in globals(): ## HACK
                    globals()[varname] = [ ]
                    globals()[varname+"_changed"] = True
                val = globals()[varname] + options[key] # HACK    
            else: 
                val = orig_type(options[key][0])
            globals()[varname] = val
            LOG.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    if OPT_FAST:
        OPT_NO_COMPILE = True
        OPT_NO_UPDATE = True
        OPT_NO_CONF = True
        OPT_NO_JAR = True
        OPT_NO_SYNC = True
    
    if not "partitions" in options:
        raise Exception("Missing 'partitions' parameter")
    if OPT_EXP_TYPE == "motivation":
        OPT_BENCHMARKS = [ 'tpcc' ]

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

    needUpdate = (OPT_NO_UPDATE == False)
    needSync = (OPT_NO_SYNC == False)
    needCompile = (OPT_NO_COMPILE == False)
    forceStop = False
    for benchmark in OPT_BENCHMARKS:
        final_results = { }
        totalAttempts = OPT_EXP_TRIALS * OPT_EXP_ATTEMPTS
        stop = False
        benchmarkType = benchmark.replace("-skewed", "")
        
        for partitions in map(int, options["partitions"]):
            LOG.info("%s - %s - %d Partitions - Experiment #%d" % (OPT_EXP_TYPE.upper(), benchmark.upper(), partitions, OPT_EXP_SETTINGS))
            #env["site.partitions_per_site"] = partitions / OPT_BASE_PARTITIONS_PER_SITE
            env["site.partitions"] = partitions
            all_results = [ ]
                
            if OPT_EXP_TYPE == "motivation":
                # We have to go by 18 because that will get us the right mix percentage at runtime for some reason...
                # range(OPT_EXP_FACTOR_START, OPT_EXP_FACTOR_STOP, 18)
                exp_factors = [ ]
                if OPT_EXP_SETTINGS == 0:
                    values = [ 0, 3, 10, 80, 100 ]
                else:
                    values = range(int(OPT_EXP_FACTOR_START), int(OPT_EXP_FACTOR_STOP), 2)
                for f in values:
                    if f > OPT_EXP_FACTOR_STOP: break
                    if f >= OPT_EXP_FACTOR_START:
                        exp_factors.append(f)
                ## FOR
                    
            elif OPT_EXP_TYPE == "throughput":
                exp_factors = OPT_PARTITION_PLANS
            elif OPT_EXP_TYPE == "breakdown":
                if OPT_EXP_FACTOR_START:
                    exp_factors = [ OPT_EXP_FACTOR_START ]
                else:
                    exp_factors = [ "full", "noindexes", "norouting" ]
                
            else:
                raise Exception("Unexpected experiment type '%s'" % OPT_EXP_TYPE)
                
            if OPT_START_CLUSTER:
                LOG.info("Starting cluster for experiments [noExecute=%s]" % OPT_NO_EXECUTE)
                fabfile.start_cluster()
                if OPT_NO_EXECUTE: sys.exit(0)
            ## IF
            
            ## Synchronize Instance Times
            if needSync: fabfile.sync_time()
            needSync = False
                
            client_inst = fabfile.__getRunningClientInstances__()[0]
            LOG.debug("Client Instance: " + client_inst.public_dns_name)
                
            updateJar = (OPT_NO_JAR == False)
            for exp_factor in exp_factors:
                updateEnv(env, benchmark, OPT_EXP_TYPE, OPT_EXP_SETTINGS, exp_factor)
                LOG.debug("Parameters:\n%s" % pformat(env))
                conf_remove = conf_remove - set(env.keys())
                
                results = [ ]
                attempts = 0
                updateConf = (OPT_NO_CONF == False)
                while len(results) < OPT_EXP_TRIALS and attempts < totalAttempts and stop == False:
                    ## Only compile for the very first invocation
                    if needCompile:
                        if env["hstore.exec_prefix"].find("compile") == -1:
                            env["hstore.exec_prefix"] += " compile"
                    else:
                        env["hstore.exec_prefix"] = env["hstore.exec_prefix"].replace("compile", "")
                    needCompile = False
                    
                    attempts += 1
                    LOG.info("Executing %s Trial #%d/%d for Factor %s [attempt=%d/%d]" % (\
                                benchmark.upper(),
                                len(results),
                                OPT_EXP_TRIALS,
                                exp_factor,
                                attempts,
                                totalAttempts
                    ))
                    try:
                        with settings(host_string=client_inst.public_dns_name):
                            output, workloads = fabfile.exec_benchmark(project=benchmarkType, \
                                                                    removals=conf_remove, \
                                                                    json=(OPT_NO_JSON == False), \
                                                                    trace=OPT_TRACE, \
                                                                    updateJar=updateJar,
                                                                    updateConf=updateConf,
                                                                    updateSVN=needUpdate)
                            if OPT_NO_JSON == False:
                                data = parseResultsOutput(output)
                                txnrate = float(data["TXNPERSECOND"])
                                if int(txnrate) == 0: pass
                                results.append(txnrate)
                                if OPT_TRACE and workloads != None:
                                    for f in workloads:
                                        LOG.info("Workload File: %s" % f)
                                    ## FOR
                                ## IF
                                LOG.info("Throughput: %.2f" % txnrate)
                            ## IF
                        ## WITH
                    except KeyboardInterrupt:
                        stop = True
                        forceStop = True
                        break
                    except SystemExit:
                        LOG.warn("Failed to complete trial succesfully")
                        if OPT_STOP_ON_ERROR:
                            stop = True
                            forceStop = True
                            break
                        pass
                    except:
                        LOG.warn("Failed to complete trial succesfully")
                        stop = True
                        raise
                        break
                    finally:
                        needUpdate = False
                        updateJar = False
                        updateConf = False
                    ## TRY
                    
                ## FOR (TRIALS)
                if results: all_results.append((benchmark, exp_factor, results, attempts))
                stop = stop or (attempts == totalAttempts)
                if stop or forceStop: break
            ## FOR (EXP_FACTOR)
            if len(all_results) > 0: final_results[partitions] = all_results
            if forceStop: break
            stop = False
        ## FOR (PARTITIONS)
        if forceStop: break
        stop = False
    ## FOR (BENCHMARKS)
    
    LOG.info("Disconnecting and dumping results")
    try:
        disconnect_all()
    finally:
        for partitions in sorted(final_results.keys()):
            all_results = final_results[partitions]
            print "%s - Partitions %d" % (OPT_EXP_TYPE.upper(), partitions)
            for benchmark, exp_factor, results, attempts in all_results:
                print "   %s EXP FACTOR %s [Attempts:%d/%d]" % (benchmark.upper(), exp_factor, attempts, totalAttempts)
                for trial in range(len(results)):
                    print "      TRIAL #%d: %.4f" % (trial, results[trial])
                ## FOR
            ## FOR
            print
        ## FOR
    ## TRY
## MAIN