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
from datetime import datetime
from pprint import pprint, pformat
from fabric.api import *
from fabric.network import *
from fabric.contrib.files import *

## This has all the functions we can use to invoke experiments on EC2
import fabfile

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
import codespeed
import argparse

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
OPT_MULTIPLY_SCALEFACTOR = False
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
OPT_RETRY_ON_ZERO = False
OPT_FORCE_REBOOT = False
OPT_SINGLE_CLIENT = False

OPT_BASE_BLOCKING = True
OPT_BASE_BLOCKING_CONCURRENT = 1
OPT_BASE_TXNRATE_PER_PARTITION = 100000
OPT_BASE_TXNRATE = 10000
OPT_BASE_CLIENT_COUNT = 1
OPT_BASE_CLIENT_PROCESSESPERCLIENT = 500
OPT_BASE_SCALE_FACTOR = 50
OPT_BASE_PARTITIONS_PER_SITE = 7

DEBUG_OPTIONS = [
    "site.exec_profiling",
    #"site.txn_profiling",
    #"site.pool_profiling",
    #"site.planner_profiling",
    "site.status_show_txn_info",
    "site.status_show_exec_info",
    #"client.output_basepartitions",
]

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
    "client.processesperclient_per_partition": True,
    "client.interval":                  10000,
    "client.skewfactor":                -1,
    "client.duration":                  120000,
    "client.warmup":                    60000,
    "client.scalefactor":               OPT_BASE_SCALE_FACTOR,
    "client.txn_hints":                 True,
    "client.throttle_backoff":          50,
    "client.memory":                    6000,
    "client.blocking_loader":           False,
    "client.output_basepartitions":     False,
    
    "site.exec_profiling":                              False,
    "site.txn_profiling":                               False,
    "site.pool_profiling":                              False,
    "site.planner_profiling":                           False,
    "site.status_show_txn_info":                        False,
    "site.status_kill_if_hung":                         False,
    "site.status_show_thread_info":                     False,
    "site.status_show_exec_info":                       False,
    "site.status_interval":                             20000,
    "site.txn_incoming_delay":                          1,
    "site.coordinator_init_thread":                     False,
    "site.coordinator_finish_thread":                   False,
    "site.txn_restart_limit":                           5,
    "site.txn_restart_limit_sysproc":                   100,
    
    "site.exec_force_singlepartitioned":                True,
    "site.exec_mispredict_crash":                       False,
    
    "site.sites_per_host":                              1,
    "site.partitions_per_site":                         OPT_BASE_PARTITIONS_PER_SITE,
    "site.memory":                                      6002,
    "site.queue_incoming_max_per_partition":            500,
    "site.queue_incoming_release_factor":               0.90,
    "site.queue_incoming_increase":                     0,
    "site.queue_incoming_throttle":                     False,
    "site.queue_dtxn_max_per_partition":                1000,
    "site.queue_dtxn_release_factor":                   0.90,
    "site.queue_dtxn_increase":                         0,
    "site.queue_dtxn_throttle":                         False,
    
    "site.txn_enable_queue_pruning":                    False,
    "site.exec_postprocessing_thread":                  False,
    "site.pool_localtxnstate_idle":                     20000,
    "site.pool_batchplan_idle":                         10000,
    "site.exec_db2_redirects":                          False,
    "site.cpu_affinity":                                True,
    "site.cpu_affinity_one_partition_per_core":         True,
}

EXPERIMENT_SETTINGS = {
    "motivation": [
        ## Settings #0 - Vary the percentage of multi-partition txns
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         True,
            "benchmark.warehouse_debug":         False,
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
            "benchmark.neworder_only":           False,
            "benchmark.neworder_abort":          True,
            "benchmark.warehouse_debug":         False,
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
OPT_BENCHMARKS = [ 'tm1', 'tpcc', 'tpcc-skewed', 'seats', 'auctionmark' ]
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
        env["hstore.exec_prefix"] += " -Dpartitionplan=%s -Dpartitionplan.ignore_missing=True" % os.path.join(OPT_PARTITION_PLAN_DIR, pplan)
        env["benchmark.neworder_multip_mix"] = -1
        env["benchmark.neworder_multip"] = True
        
        #base_txnrate = int(OPT_BASE_TXNRATE / 2) if benchmark == "seats" else OPT_BASE_TXNRATE
        #env["client.txnrate"] = int(base_txnrate * (env["site.partitions"]/float(4)))
        
        ## Everything but LNS has to use the DB2 redirects
        if exp_factor != "lns":
            env["site.exec_neworder_cheat"] = False
            env["client.txn_hints"] = False
            env["site.exec_db2_redirects"] = True
            #env["client.processesperclient"] = OPT_BASE_CLIENT_PROCESSESPERCLIENT / 2
        else:
            env["site.exec_neworder_cheat"] = (benchmark in ["tpcc", "seats"])
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
        env["benchmark.one_warehouse_per_partition"] = True
        env["benchmark.one_loadthread_per_warehouse"] = True
        if benchmark.endswith("-skewed"):
            env["benchmark.temporal_skew"] = True
            env["benchmark.temporal_skew_rotate"] = False
            env["benchmark.temporal_skew_mix"] = 100
        #else:
            #env["benchmark.temporal_skew"] = False
            #env["benchmark.temporal_skew_rotate"] = False
            #env["benchmark.temporal_skew_mix"] = -1
    #elif benchmark == "seats":
        # env["client.scalefactor"] = 100
        # env["client.txnrate"] = int(OPT_BASE_TXNRATE / 2)

        ## MOTIVATION
        
    env["ec2.force_reboot"] = OPT_FORCE_REBOOT
    #env["client.scalefactor"] = OPT_BASE_SCALE_FACTOR
    # env["client.txnrate"] = int((OPT_BASE_TXNRATE_PER_PARTITION * env["site.partitions"]) / (env["client.count"] * env["client.processesperclient"]))

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
## svnInfo
## ==============================================
def svnInfo(svnRepo, revision = None):
    import pysvn
    
    if revision:
        svnRevision = pysvn.Revision(pysvn.opt_revision_kind.number, revision)
    else:
        svnRevision = pysvn.Revision(pysvn.opt_revision_kind.head)
    
    client = pysvn.Client()
    info = client.info2(svnRepo, revision=svnRevision, recurse=False)[-1][-1]
    last_changed_rev = info['last_changed_rev'].number
    last_changed_date = datetime.fromtimestamp(info['last_changed_date'])
    
    return last_changed_rev, last_changed_date
## DEF


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    BASE_OPTIONS = [
        # Experiment Parameters
        "exp-type=",
        "exp-settings=",
        "exp-trials=",
        "exp-attempts=",
        "exp-factor-start=",
        "exp-factor-stop=",
        
        "benchmarks=",
        "partition-plans=",
        "multiply-scalefactor",
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
        "retry-on-zero",
        "single-client",
        "trace",
        
        "codespeed-url=",
        "codespeed-benchmark=",
        "codespeed-revision=",
        "codespeed-lastrevision=",
        
        # Enable debug logging
        "debug",
        "debug-hstore",
    ]
    for key in BASE_SETTINGS.keys():
        BASE_OPTIONS.append("%s=" % key)
    ## FOR
    
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', BASE_OPTIONS)
    
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
    for param in DEBUG_OPTIONS:
        BASE_SETTINGS[param] = ("debug-hstore" in options)
        
    ## Global Options
    for key in options:
        varname = None
        paramDict = None
        if key in BASE_SETTINGS:
            varname = key
            paramDict = BASE_SETTINGS
        else:
            varname = "OPT_" + key.replace("-", "_").upper()
            if varname in globals():
                paramDict= globals()
        ## IF
        if paramDict is not None:
            orig_type = type(paramDict[varname])
            if orig_type == bool:
                val = (len(options[key][0]) == 0 or options[key][0].lower() == "true")
            elif orig_type == list:
                if not varname+"_changed" in globals(): ## HACK
                    paramDict[varname] = [ ]
                    paramDict[varname+"_changed"] = True
                val = paramDict[varname] + options[key] # HACK    
            else: 
                val = orig_type(options[key][0])
            paramDict[varname] = val
            LOG.debug("%s = %s" % (varname, str(paramDict[varname])))
    ## FOR
    if OPT_FAST:
        OPT_NO_COMPILE = True
        OPT_NO_UPDATE = True
        OPT_NO_CONF = True
        OPT_NO_JAR = True
        OPT_NO_SYNC = True
    if OPT_SINGLE_CLIENT:
        LOG.info("Enabling single-client debug mode!")
        BASE_SETTINGS["client.count"] = 1
        BASE_SETTINGS["client.processesperclient"] = 1
        BASE_SETTINGS["client.processesperclient_per_partition"] = False
        BASE_SETTINGS["client.txnrate"] = 1
    ## IF
    
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
    
    ## Check whether we have already executed this one before
    if "codespeed-lastrevision" in options:
        last_changed_rev, last_changed_date = svnInfo(env["hstore.svn"])
        if int(options["codespeed-lastrevision"][0]) <= last_changed_rev:
            LOG.info("Skipping already executed revision r%d" % last_changed_rev)
            sys.exit(0)
    ## IF
    
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

    # If we get two consecutive intervals with zero results, then stop the benchmark
    if OPT_RETRY_ON_ZERO:
        env["hstore.exec_prefix"] += " -Dkillonzero=true"
    
    needUpdate = (OPT_NO_UPDATE == False)
    needSync = (OPT_NO_SYNC == False)
    needCompile = (OPT_NO_COMPILE == False)
    forceStop = False
    origScaleFactor = BASE_SETTINGS['client.scalefactor']
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
                
            # Increase the client.scalefactor based on the number of partitions
            if OPT_MULTIPLY_SCALEFACTOR:
                BASE_SETTINGS['client.scalefactor'] = int(origScaleFactor / partitions)
                
            if OPT_EXP_TYPE == "motivation":
                # We have to go by 18 because that will get us the right mix percentage at runtime for some reason...
                # range(OPT_EXP_FACTOR_START, OPT_EXP_FACTOR_STOP, 18)
                exp_factors = [ ]
                if OPT_EXP_SETTINGS == 0:
                    values = [ 0, 3, 10, 80, 100 ]
                else:
                    values = range(int(OPT_EXP_FACTOR_START), int(OPT_EXP_FACTOR_STOP), 2)
                LOG.debug("%s Exp Factor Values: %s" % (OPT_EXP_TYPE.upper(), values))
                for f in values:
                    if f > int(OPT_EXP_FACTOR_STOP): break
                    if f >= int(OPT_EXP_FACTOR_START):
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
            LOG.debug("Experimental Factors: %s" % exp_factors)
                
            if OPT_START_CLUSTER:
                LOG.info("Starting cluster for experiments [noExecute=%s]" % OPT_NO_EXECUTE)
                fabfile.start_cluster(updateSync=needSync)
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
                                                                    updateJar=updateJar, \
                                                                    updateConf=updateConf, \
                                                                    updateSVN=needUpdate, \
                                                                    updateLog4j=needUpdate)
                            if OPT_NO_JSON == False:
                                data = parseResultsOutput(output)
                                for key in [ 'TOTALTXNPERSECOND', 'TXNPERSECOND' ]:
                                    if key in data:
                                        txnrate = float(data[key])
                                        break
                                ## FOR
                                minTxnRate = float(data["MINTXNPERSECOND"]) if "MINTXNPERSECOND" in data else None
                                maxTxnRate = float(data["MAXTXNPERSECOND"]) if "MAXTXNPERSECOND" in data else None
                                stddevTxnRate = float(data["STDDEVTXNPERSECOND"]) if "STDDEVTXNPERSECOND" in data else None
                                
                                if int(txnrate) == 0: pass
                                results.append(txnrate)
                                if OPT_TRACE and workloads != None:
                                    for f in workloads:
                                        LOG.info("Workload File: %s" % f)
                                    ## FOR
                                ## IF
                                LOG.info("Throughput: %.2f" % txnrate)
                                
                                if "codespeed-url" in options and options["codespeed-url"][0] and txnrate > 0:
                                    upload_url = options["codespeed-url"][0]
                                    
                                    if "codespeed-revision" in options:
                                        last_changed_rev = int(options["codespeed-revision"][0])
                                        last_changed_rev, last_changed_date = svnInfo(env["hstore.svn"], last_changed_rev)
                                    else:
                                        last_changed_rev, last_changed_date = svnInfo(env["hstore.svn"])
                                    print "last_changed_rev:", last_changed_rev
                                    print "last_changed_date:", last_changed_date
                                    
                                    codespeedBenchmark = options["codespeed-benchmark"][0] if "codespeed-benchmark" in options else benchmark
                                    
                                    LOG.info("Uploading %s results to CODESPEED at %s" % (benchmark, upload_url))
                                    result = codespeed.Result(
                                                commitid=last_changed_rev,
                                                branch=os.path.basename(env["hstore.svn"]),
                                                benchmark=codespeedBenchmark,
                                                project="H-Store",
                                                num_partitions=partitions,
                                                environment="ec2",
                                                result_value=txnrate,
                                                revision_date=last_changed_date,
                                                result_date=datetime.now(),
                                                min_result=minTxnRate,
                                                max_result=maxTxnRate,
                                                std_dev=stddevTxnRate
                                    )
                                    result.upload(upload_url)
                                ## IF
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