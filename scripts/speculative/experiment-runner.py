#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2012 by H-Store Project
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
import subprocess
import csv
from datetime import datetime
from pprint import pprint, pformat
from types import *

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../../tools")))
sys.path.append(os.path.realpath(os.path.join(basedir, "../../third_party/python")))

import hstore
import hstore.codespeed
import hstore.fabric

import argparse
from fabric.api import *
from fabric.network import *
from fabric.contrib.files import *

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

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

OPT_BASE_BLOCKING_CONCURRENT = 1
OPT_BASE_TXNRATE = 100000
OPT_BASE_CLIENT_COUNT = 1
OPT_BASE_CLIENT_THREADS_PER_HOST = 100
OPT_BASE_SCALE_FACTOR = float(1.0)
OPT_BASE_PARTITIONS_PER_SITE = 8
OPT_PARTITION_PLAN_DIR = "files/designplans"
OPT_MARKOV_DIR = "files/markovs"
OPT_GIT_BRANCH = subprocess.check_output("git rev-parse --abbrev-ref HEAD", shell=True).strip()

DEFAULT_OPTIONS = {
    "hstore.git_branch": OPT_GIT_BRANCH
}
DEBUG_OPTIONS = {
    "site.status_enable":             True,
    "site.status_interval":           20000,
    "site.exec_profiling":            True,
    #"site.txn_profiling":             True,    
}
DEBUG_SITE_LOGGING = [
    #"edu.brown.hstore.HStoreSite",
    #"edu.brown.hstore.PartitionExecutor",
    #"edu.brown.hstore.TransactionInitializer",
    #"edu.brown.hstore.specexec.PrefetchQueryPlanner",
    #"edu.brown.hstore.specexec.PrefetchQueryUtil",
    #"edu.brown.hstore.SpecExecScheduler",
    #"edu.brown.hstore.specexec.checkers.MarkovConflictChecker",
    #"edu.brown.hstore.estimators.markov.MarkovEstimator",
    #"edu.brown.hstore.txns.DependencyTracker",
    
    ## CALLBACKS
    #"edu.brown.hstore.callbacks.TransactionPrepareCallback",
    #"edu.brown.hstore.callbacks.TransactionPrepareWrapperCallback",
    #"edu.brown.hstore.callbacks.TransactionInitCallback",
    #"edu.brown.hstore.callbacks.TransactionInitQueueCallback",
    #"edu.brown.hstore.callbacks.BlockingRpcCallback",
]
TRACE_SITE_LOGGING = [
    #"edu.brown.hstore.estimators.markov.MarkovPathEstimator",
]
DEBUG_CLIENT_LOGGING = [
    #"edu.brown.api.BenchmarkComponent",
    #"edu.brown.api.BenchmarkController",
    "edu.brown.api.ControlPipe",
    "edu.brown.api.ControlWorker",
    #"org.voltdb.benchmark.tpcc.TPCCLoader",
]

BASE_SETTINGS = {
    "ec2.site_type":                    "c1.xlarge",
    "ec2.client_type":                  "c1.xlarge",
    "ec2.change_type":                  True,
    "ec2.cluster_group":                "conflictsets", # OPT_GIT_BRANCH,
    
    "hstore.sites_per_host":            1,
    "hstore.partitions_per_site":       OPT_BASE_PARTITIONS_PER_SITE,

    "client.blocking":                  False,
    "client.blocking_concurrent":       OPT_BASE_BLOCKING_CONCURRENT,
    "client.txnrate":                   OPT_BASE_TXNRATE,
    "client.count":                     OPT_BASE_CLIENT_COUNT,
    #"client.threads_per_host":          OPT_BASE_CLIENT_THREADS_PER_HOST,
    "client.interval":                  10000,
    "client.skewfactor":                -1,
    "client.duration":                  300000,
    "client.warmup":                    60000,
    "client.txn_hints":                 True,
    "client.memory":                    6000,
    
    "site.jvm_asserts":                         False,
    "site.log_backup":                          False,
    "site.status_enable":                       False,
    "site.commandlog_enable":                   True,
    "site.commandlog_timeout":                  10,
    "site.txn_restart_limit":                   5,
    "site.txn_restart_limit_sysproc":           100,
    "site.exec_force_singlepartitioned":        True,
    "site.memory":                              6144,
    "site.exec_db2_redirects":                  False,
    "site.exec_readwrite_tracking":             False,
    "site.cpu_affinity":                        True,
    "site.cpu_affinity_one_partition_per_core": True,
}

EXPERIMENT_SETTINGS = [
    # Motivation Experiments
    "motivation-singlepartition",
    "motivation-dtxn-singlenode",
    "motivation-dtxn-multinode",
    "motivation-remotequery",
    
    # Performance Experiments
    "performance-nospec",
    "performance-spec-txn",
    "performance-spec-query",
    "performance-spec-all",
    
    # Conflict Experiments
    "conflicts-table",
    "conflicts-row",
    "conflictsperf-table",
    "conflictsperf-row",
    
    # Hotpsot Experiments
    "hotspots-00-spec",
    "hotspots-00-occ",
    "hotspots-25-spec",
    "hotspots-25-occ",
    "hotspots-50-spec",
    "hotspots-50-occ",
    "hotspots-75-spec",
    "hotspots-75-occ",
    "hotspots-100-spec",
    "hotspots-100-occ",
    
    # Abort Experiments
    "aborts-00-spec",
    "aborts-20-spec",
    "aborts-40-spec",
    "aborts-60-spec",
    "aborts-80-spec",
    "aborts-100-spec",
    "aborts-00-occ",
    "aborts-20-occ",
    "aborts-40-occ",
    "aborts-60-occ",
    "aborts-80-occ",
    "aborts-100-occ",
]

## ==============================================
## updateExperimentEnv
## ==============================================
def updateExperimentEnv(fabric, args, benchmark, partitions):
    targetType = args['exp_type']
  
    ## ----------------------------------------------
    ## CONFLICTS + HOTSPOTS + ABORTS
    ## ----------------------------------------------
    for prefix in ("conflicts", "hotspots", "aborts"):
        if targetType.startswith(prefix):
            targetType = "performance-spec-txn"
            break
    ## FOR
        
    ## ----------------------------------------------
    ## MOTIVATION
    ## ----------------------------------------------
    if targetType.startswith("motivation"):
        fabric.env["site.specexec_enable"] = False
        fabric.env["site.specexec_nonblocking"] = False
        fabric.env["site.markov_enable"] = True
        fabric.env["site.markov_fixed"] = True
        fabric.env["site.exec_force_singlepartitioned"] = False
        fabric.env["client.count"] = 1
        fabric.env["client.txnrate"] = 100000
        fabric.env["client.blocking"] = True
        fabric.env["client.output_response_status"] = True
        fabric.env["client.output_exec_profiling"] = "execprofile.csv"
        fabric.env["client.output_txn_profiling"] = "txnprofile.csv"
        fabric.env["client.output_txn_profiling_combine"] = True
        fabric.env["client.output_txn_counters"] = "txncounters.csv"
        
        ## IF
        if targetType in ('motivation-oneclient', 'motivation-remotequery'):
            fabric.env["client.threads_per_host"] = 1
        else:
            fabric.env["client.threads_per_host"] = partitions * 2  # max(1, int(partitions/2))
        
        if benchmark == "tpcc":
            fabric.env["client.weights"] = "neworder:50,paymentByCustomerId:50,*:0"
            fabric.env["benchmark.payment_only"] = False
            fabric.env["benchmark.neworder_only"] = False
            fabric.env["benchmark.neworder_abort"] = 0
            fabric.env["benchmark.loadthread_per_warehouse"] = False
            fabric.env["benchmark.loadthreads"] = max(16, partitions)
        elif benchmark == "seats":
            fabric.env["client.weights"] = "DeleteReservation:10,NewReservation:75,FindOpenSeats:15,*:0"
        elif benchmark == "smallbank":
            fabric.env["client.weights"] = "SendPayment:100,*:0"
            fabric.env["benchmark.prob_account_hotspot"] = 0
        
        ## ----------------------------------------------
        ## MOTIVATION-SINGLEPARTITION
        ## ----------------------------------------------
        if targetType == "motivation-singlepartition":
            fabric.env["client.weights"] = ""
            
            if benchmark == "tpcc":
                fabric.env["benchmark.neworder_multip"] = False
                fabric.env["benchmark.neworder_multip_remote"] = False
                fabric.env["benchmark.neworder_multip_mix"] = -1
                fabric.env["benchmark.payment_multip"] = False
                fabric.env["benchmark.payment_multip_remote"] = False
                fabric.env["benchmark.payment_multip_mix"] = -1
            elif benchmark == "seats":
                fabric.env["benchmark.force_all_distributed"] = False
                fabric.env["benchmark.force_all_singlepartition"] = True
            elif benchmark == "smallbank":
                fabric.env["client.weights"] = "SendPayment:25,*:15"
                fabric.env["benchmark.prob_multiaccount_dtxn"] = 0
                fabric.env["benchmark.force_multisite_dtxns"] = False
                fabric.env["benchmark.force_singlesite_dtxns"] = False
        ## ----------------------------------------------
        ## MOTIVATION-DTXN-SINGLENODE
        ## ----------------------------------------------
        elif targetType == "motivation-dtxn-singlenode":
            if benchmark == "tpcc":
                fabric.env["benchmark.neworder_multip"] = True
                fabric.env["benchmark.neworder_multip_remote"] = False
                fabric.env["benchmark.neworder_multip_mix"] = 100
                fabric.env["benchmark.payment_multip"] = True
                fabric.env["benchmark.payment_multip_remote"] = False
                fabric.env["benchmark.payment_multip_mix"] = 100
            elif benchmark == "seats":
                fabric.env["benchmark.force_all_distributed"] = True
                fabric.env["benchmark.force_all_singlepartition"] = False
            elif benchmark == "smallbank":
                fabric.env["benchmark.prob_multiaccount_dtxn"] = 100
                fabric.env["benchmark.force_multisite_dtxns"] = False
                fabric.env["benchmark.force_singlesite_dtxns"] = True
        ## ----------------------------------------------
        ## MOTIVATION-DTXN-MULITNODE
        ## ----------------------------------------------
        elif targetType in ("motivation-dtxn-multinode", "motivation-remotequery"):
            if targetType == "motivation-remotequery":
                fabric.env["site.specexec_enable"] = False
                fabric.env["site.specexec_nonblocking"] = True
            
            if benchmark == "tpcc":
                fabric.env["benchmark.neworder_multip"] = True
                fabric.env["benchmark.neworder_multip_remote"] = True
                fabric.env["benchmark.neworder_multip_mix"] = 100
                fabric.env["benchmark.payment_multip"] = True
                fabric.env["benchmark.payment_multip_remote"] = True
                fabric.env["benchmark.payment_multip_mix"] = 100
            elif benchmark == "seats":
                fabric.env["benchmark.force_all_distributed"] = True
                fabric.env["benchmark.force_all_singlepartition"] = False
            elif benchmark == "smallbank":
                fabric.env["benchmark.prob_multiaccount_dtxn"] = 100
                fabric.env["benchmark.force_multisite_dtxns"] = True
                fabric.env["benchmark.force_singlesite_dtxns"] = False

    ## ----------------------------------------------
    ## PERFORMANCE EXPERIMENTS
    ## ----------------------------------------------
    elif targetType.startswith("performance"):
        fabric.env["site.markov_enable"] = True
        fabric.env["site.exec_prefetch_queries"] = False
        fabric.env["site.specexec_enable"] = False
        fabric.env["site.specexec_nonblocking"] = False
        fabric.env["site.specexec_ignore_all_local"] = True
        fabric.env["site.network_incoming_limit_txns"] = 500
        fabric.env["site.txn_profiling_sample"] = 0.01
        
        fabric.env["client.txn_hints"] = True
        fabric.env["client.count"] = 1
        fabric.env["client.txnrate"] = 100000
        fabric.env["client.blocking"] = True
        fabric.env["client.blocking_concurrent"] = 4 * int(partitions/8)
        #if partitions > 16: fabric.env["client.blocking_concurrent"] *= int(partitions/8)
        fabric.env["client.threads_per_host"] = OPT_BASE_CLIENT_THREADS_PER_HOST
        fabric.env["client.scalefactor"] = OPT_BASE_SCALE_FACTOR * int(partitions/8)
        fabric.env["client.output_txn_counters"] = "txncounters.csv"
        fabric.env["client.output_txn_profiling"] = "txnprofile.csv"
        fabric.env["client.output_clients"] = False
        
        if benchmark == "tpcc":
            fabric.env["client.scalefactor"] = OPT_BASE_SCALE_FACTOR
            fabric.env["benchmark.neworder_multip"] = True
            fabric.env["benchmark.neworder_multip_remote"] = False
            fabric.env["benchmark.neworder_multip_mix"] = -1
            fabric.env["benchmark.payment_multip"] = True
            fabric.env["benchmark.payment_multip_remote"] = False
            fabric.env["benchmark.payment_multip_mix"] = -1
        elif benchmark == "seats":
            fabric.env["benchmark.force_all_distributed"] = False
            fabric.env["benchmark.force_all_singlepartition"] = False
            fabric.env["benchmark.prob_multiaccount_dtxn"] = 50
            fabric.env["client.threads_per_host"] = int(OPT_BASE_CLIENT_THREADS_PER_HOST * 0.5)
            fabric.env["client.scalefactor"] = 1.0
            fabric.env["client.weights"] =  "DeleteReservation:1," + \
                                            "FindOpenSeats:65," + \
                                            "NewReservation:30," + \
                                            "UpdateCustomer:2," + \
                                            "UpdateReservation:2," + \
                                            "*:0"
                                        
        elif benchmark == "smallbank":
            fabric.env["client.weights"] = "SendPayment:25,*:15"
            fabric.env["client.scalefactor"] = OPT_BASE_SCALE_FACTOR * int(partitions/4)
            fabric.env["benchmark.force_multisite_dtxns"] = False
            fabric.env["benchmark.force_singlesite_dtxns"] = False
            fabric.env["benchmark.prob_account_hotspot"] = 0
        
        ## ----------------------------------------------
        ## NO SPECULATION
        ## ----------------------------------------------
        if targetType == "performance-nospec":
            #fabric.env["site.markov_enable"] = False
            if partitions>16: fabric.env["client.blocking_concurrent"] = 8 # HACK
            if benchmark == "seats":
                fabric.env["client.blocking_concurrent"] = int(partitions/8)
            
        ## ----------------------------------------------
        ## SPECULATIVE TXNS
        ## ----------------------------------------------
        if targetType in ("performance-spec-txn", "performance-spec-all"):
            fabric.env["site.specexec_enable"] = True
            fabric.env["site.specexec_ignore_stallpoints"] = ""
            fabric.env["site.specexec_scheduler_checker"] = "MARKOV"

            spFactor = 0.65 if targetType == "performance-spec-all" else 0.5
            spThreads = int(fabric.env["client.threads_per_host"] * spFactor)
            fabric.env["client.singlepartition_threads"] = spThreads
            
        ## ----------------------------------------------
        ## SPECULATIVE QUERIES
        ## ----------------------------------------------
        if targetType in ("performance-spec-query", "performance-spec-all"):
            fabric.env["site.exec_prefetch_queries"] = True
            if targetType == "performance-spec-query" and benchmark == "seats":
                fabric.env["client.blocking_concurrent"] = int(partitions/8)
            
    ## ----------------------------------------------
    ## CONFLICTS
    ## ----------------------------------------------
    if args['exp_type'].startswith("conflicts"):
        conflictType = args['exp_type'].split("-")[-1]
        
        fabric.env["client.output_specexec_profiling"] = "specexec.csv"
        fabric.env["site.markov_learning_enable"] = False
        
        # TESTING
        #fabric.env["site.specexec_disable_partitions"] = "1-15"
        #fabric.env["client.scalefactor"] = 0.1
        #fabric.env["client.threads_per_host"] = 80
        #fabric.env["client.blocking_concurrent"] = 2
        #fabric.env["hstore.exec_prefix"] += " -Dmarkov.recompute_end=true"

        ## ----------------------------------------------
        ## PERFORMANCE
        ## ----------------------------------------------
        if args['exp_type'].startswith("conflictsperf"):
            fabric.env["site.specexec_scheduler_policy"] = "FIRST"
            fabric.env["site.specexec_scheduler_window"] = 1
            fabric.env["client.singlepartition_threads"] = 0
        
        ## ----------------------------------------------
        ## ANALYSIS
        ## ----------------------------------------------
        else:
            fabric.env["site.specexec_profiling_sample"] = 1
            fabric.env["site.specexec_ignore_stallpoints"] = "IDLE,SP2_REMOTE_BEFORE,SP3_LOCAL,SP3_REMOTE"
            fabric.env["site.specexec_scheduler_policy"] = "LAST"
            fabric.env["site.specexec_scheduler_window"] = 999999
            fabric.env["site.specexec_ignore_interruptions"] = True
        
        ## ----------------------------------------------
        ## ROW-LEVEL DETECTION
        ## ----------------------------------------------
        if conflictType == "row":
            fabric.env["site.specexec_scheduler_checker"] = "MARKOV"
        ## ----------------------------------------------
        ## TABLE-LEVEL DETECTION
        ## ----------------------------------------------
        elif conflictType == "table":
            fabric.env["site.specexec_scheduler_checker"] = "TABLE"
    ## IF
    
    ## ----------------------------------------------
    ## HOTSPOTS
    ## ----------------------------------------------
    if args['exp_type'].startswith("hotspots"):
        hotspotPcnt = int(args['exp_type'].split("-")[1])
        schedulerType = args['exp_type'].split("-")[2]

        fabric.env["site.specexec_scheduler_policy"] = "FIRST"
        fabric.env["site.specexec_scheduler_window"] = 1
        fabric.env["site.exec_early_prepare"] = True
        fabric.env["client.scalefactor"] = 1
        
        #if partitions == 16:
            #fabric.env["client.blocking_concurrent"] = 8 * int(partitions/8)

        # SMALLBANK
        if benchmark == "smallbank":
            fabric.env["benchmark.hotspot_use_fixed_size"] = False
            fabric.env["benchmark.hotspot_percentage"] = 0.25
            fabric.env["benchmark.hotspot_use_fixed_size"] = False
            fabric.env["benchmark.prob_account_hotspot"] = hotspotPcnt
            fabric.env["benchmark.prob_multiaccount_dtxn"] = 100
            fabric.env["benchmark.force_multisite_dtxns"] = True
        # TPC-C
        elif benchmark == "tpcc":
            fabric.env["benchmark.neworder_multip_remote"] = True
            fabric.env["benchmark.payment_multip_remote"] = True
            fabric.env["benchmark.temporal_skew_mix"] = hotspotPcnt
            fabric.env["benchmark.neworder_skew_warehouse"] = True
        
        ## ----------------------------------------------
        ## HERMES!
        ## ----------------------------------------------
        if schedulerType == "spec":
            fabric.env["site.markov_enable"] = True
            fabric.env["site.specexec_scheduler_checker"] = "MARKOV"
            fabric.env["site.exec_readwrite_tracking"] = False
            fabric.env["site.exec_prefetch_queries"] = False
            #fabric.env["client.singlepartition_threads"] = int(fabric.env["client.threads_per_host"] * 0.75)
            #fabric.env["client.blocking_concurrent"] = 6 * int(partitions/8)
        ## ----------------------------------------------
        ## OCC
        ## ----------------------------------------------
        elif schedulerType == "occ":
            #fabric.env["site.markov_enable"] = False
            fabric.env["site.specexec_scheduler_checker"] = "OPTIMISTIC"
            fabric.env["site.exec_readwrite_tracking"] = True
    ## IF
    
    ## ----------------------------------------------
    ## ABORTS
    ## ----------------------------------------------
    if args['exp_type'].startswith("aborts"):
        abortPercentage = int(args['exp_type'].split("-")[1])
        schedulerType = args['exp_type'].split("-")[2]
        
        fabric.env["site.specexec_scheduler_checker"] = "MARKOV"
        fabric.env["benchmark.neworder_abort"] = abortPercentage
        fabric.env["benchmark.neworder_abort_no_multip"] = True
        fabric.env["benchmark.neworder_abort_no_singlep"] = True
        
        ## ----------------------------------------------
        ## HERMES!
        ## ----------------------------------------------
        if schedulerType == "spec":
            fabric.env["site.specexec_scheduler_checker"] = "MARKOV"
        ## ----------------------------------------------
        ## OCC
        ## ----------------------------------------------
        elif schedulerType == "occ":
            fabric.env["site.specexec_scheduler_checker"] = "OPTIMISTIC"
            fabric.env["site.exec_readwrite_tracking"] = True
    ## IF

    ## ----------------------------------------------
    ## MARKOV MODELS!
    ## ----------------------------------------------
    
    # Make sure we remove the old markov model arg
    newExecPrefix = ""
    for key in map(string.strip, fabric.env["hstore.exec_prefix"].split(" ")):
        if not key.startswith("-Dmarkov"): newExecPrefix += " " + key
    fabric.env["hstore.exec_prefix"] = newExecPrefix
    
    if fabric.env.get('site.markov_enable', False):
        markov = os.path.join(OPT_MARKOV_DIR, "%s-%dp.markov.gz" % (benchmark, partitions))
        fabric.env["hstore.exec_prefix"] += " -Dmarkov=%s" % markov
        #fabric.env["hstore.exec_prefix"] += " -Dmarkov.recompute_end=true"
        
        fabric.env["site.markov_singlep_updates"] = False
        fabric.env["site.markov_dtxn_updates"] = False
        fabric.env["site.markov_path_caching"] = True
        fabric.env["site.markov_endpoint_caching"] = False
        fabric.env["site.markov_fixed"] = False
        fabric.env["site.markov_force_traversal"] = True
        fabric.env["site.network_startup_wait"] = 15000 * 2
    else:
        fabric.env['site.markov_fixed'] = False
        
    for key in ('client.txnrate', 'client.threads_per_host', 'client.scalefactor'):
        if key in args and args[key] and args[key] != fabric.env[key]: 
            LOG.debug("OVERRIDE %s -> %s [orig=%s]", key, args[key], fabric.env[key])
            fabric.env[key] = args[key]
## DEF

## ==============================================
## getCSVOutput
## ==============================================
def getCSVOutput(inst, fabric, args, benchmark, partitions):
    """Find all of the output parameters in the env and retrieve the files from the cluster"""
    for k,v in fabric.env.iteritems():
        if k.startswith("client.output_"):
            LOG.debug("Checking whether '%s' is enabled" % (k))
            if not v is None and isinstance(v, str) and v.endswith(".csv"):
                saveCSVResults(inst, fabric, args, benchmark, partitions, v)
    ## FOR
## DEF

## ==============================================
## saveCSVResults
## ==============================================
def saveCSVResults(inst, fabric, args, benchmark, partitions, filename):
    # Go out and grab the remote file and save it locally in our results dir.
    filename = os.path.join(fabric.hstore_dir, filename)
    LOG.info("Going to retrieve remote CSV file '%s'" % filename)
    contents = fabric.get_file(inst, filename)
    if len(contents) > 0:
        # We'll prefix the name with the number of partitions
        localName = "%s-%02dp-%s" % (benchmark, partitions, os.path.basename(filename))
        resultsDir = os.path.join(args['results_dir'], args['exp_type'])
        localFile = os.path.join(resultsDir, localName)
        with open(localFile, "w") as f:
            f.write(contents)
        LOG.info("Saved CSV results to '%s'" % os.path.realpath(localFile))
    else:
        LOG.warn("The CSV results file '%s' is empty" % filename)
    return
## DEF

## ==============================================
## processResults
## ==============================================
def processResults(inst, fabric, args, partitions, output, workloads):
    data = hstore.parseJSONResults(output)
    
    txnrate = float(data['TXNTOTALPERSECOND'])
    if int(txnrate) == 0: return (data)
    LOG.info("Throughput: %.2f" % txnrate)
    
    txnlatency = float(data['TOTALAVGLATENCY'])
    LOG.info("Latency: %.2f" % txnlatency)
    
    dtxnPercent = float(data['DTXNTOTALCOUNT']) / float(data['TXNTOTALCOUNT'])
    LOG.info("DTXN Percentage: %.4f" % dtxnPercent)
    
    specPercent = float(data['SPECEXECTOTALCOUNT']) / float(data['TXNTOTALCOUNT'])
    LOG.info("Speculative Percentage: %.4f" % specPercent)
    
    minTxnRate = float(data["MINTXNPERSECOND"]) if "MINTXNPERSECOND" in data else None
    maxTxnRate = float(data["MAXTXNPERSECOND"]) if "MAXTXNPERSECOND" in data else None
    stddevTxnRate = float(data["STDDEVTXNPERSECOND"]) if "STDDEVTXNPERSECOND" in data else None
    
    if args['workload_trace'] and not workloads is None:
        for f in workloads:
            LOG.info("Workload File: %s" % f)
        ## FOR
    ## IF
        
    # CODESPEED UPLOAD
    if args["codespeed_url"] and txnrate > 0:
        upload_url = args["codespeed_url"][0]
        
        if args["codespeed_revision"]:
            # FIXME
            last_changed_rev = args["codespeed_revision"][0]
            last_changed_rev, last_changed_date = svnInfo(env["hstore.svn"], last_changed_rev)
        else:
            last_changed_rev, last_changed_date = fabric.get_version(inst)
        LOG.info("last_changed_rev:", last_changed_rev)
        LOG.info("last_changed_date:", last_changed_date)
            
        codespeedBenchmark = benchmark
        if not args["codespeed_benchmark"] is None:
            codespeedBenchmark = args["codespeed_benchmark"]
        
        codespeedBranch = fabric.env["hstore.git_branch"]
        if not args["codespeed_branch"] is None:
            codespeedBranch = args["codespeed_branch"]
            
        LOG.info("Uploading %s results to CODESPEED at %s" % (benchmark, upload_url))
        result = hstore.codespeed.Result(
            commitid=last_changed_rev,
            branch=codespeedBranch,
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
    return (data)
## DEF

## ==============================================
## writeResultsCSV
## ==============================================
def writeResultsCSV(args, benchmark, finalResults, partitions):
    if not partitions in finalResults or not finalResults[partitions]:
        LOG.warn("No results for %s - %s - %d Partitions" % (args['exp_type'].upper(), benchmark.upper(), partitions))
        return
    
    output = getResultsFilename(args, benchmark, partitions)
    with open(output, "w") as fd:
        writer = csv.writer(fd)
        header = None
        for data in finalResults[partitions]:
            if header is None:
                header = data.keys()
                header.remove("TXNRESULTS")
                writer.writerow(header)
            writer.writerow([ data[key] for key in header ])
        ## FOR
    ## WITH
    LOG.info("Wrote %s results to '%s'", benchmark, output)
## DEF

## ==============================================
## getResultsFilename
## ==============================================
def getResultsFilename(args, benchmark, partitions):
    baseName = "%s-%02dp-results.csv" % (benchmark, partitions)
    output = os.path.join(args['results_dir'], args['exp_type'], baseName)
    return output
## DEF

## ==============================================
## createFabricHandle
## ==============================================
def createFabricHandle(name, env):
    fullName = "%sFabric" % name.upper()
    moduleName = "hstore.fabric.%s" % (fullName.lower())
    moduleHandle = __import__(moduleName, globals(), locals(), [fullName])
    klass = getattr(moduleHandle, fullName)
    return klass(env)
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    HSTORE_PARAMS = hstore.getAllParameters()
    
    aparser = argparse.ArgumentParser(description='H-Store Experiment Runner')
    
    aparser.add_argument('fabric', choices=["ec2", "ssh"], help='Fabric Configuration Type')
    aparser.add_argument('--benchmark', choices=hstore.getBenchmarks(), nargs='+',
                         help='Target benchmarks')
    
    # Cluster Parameters
    agroup = aparser.add_argument_group('EC2 Cluster Control Parameters')
    agroup.add_argument("--partitions", type=int, default=4, metavar='P', nargs='+',)
    agroup.add_argument("--start-cluster", action='store_true')
    agroup.add_argument("--stop-cluster", action='store_true')
    agroup.add_argument("--fast-start", action='store_true')
    agroup.add_argument("--force-reboot", action='store_true')
    agroup.add_argument("--single-client", action='store_true')
    agroup.add_argument("--no-execute", action='store_true', help='Do no execute any experiments after starting cluster')
    agroup.add_argument("--no-compile", action='store_true', help='Disable compiling before running benchmark')
    agroup.add_argument("--no-update", action='store_true', help='Disable synching git repository')
    agroup.add_argument("--no-jar", action='store_true', help='Disable constructing benchmark jar')
    agroup.add_argument("--no-conf", action='store_true', help='Disable updating HStoreConf properties file')
    agroup.add_argument("--no-sync", action='store_true', help='Disable synching time between nodes')
    agroup.add_argument("--no-json", action='store_true', help='Disable JSON output results')
    agroup.add_argument("--no-profiling", action='store_true', help='Disable all profiling stats output files')
    agroup.add_argument("--no-shutdown", action='store_true', help='Disable shutting down cluster after a trial run')
    
    ## Experiment Parameters
    agroup = aparser.add_argument_group('Experiment Parameters')
    agroup.add_argument("--exp-type", type=str, choices=EXPERIMENT_SETTINGS, default=EXPERIMENT_SETTINGS[0])
    agroup.add_argument("--exp-trials", type=int, default=3, metavar='N')
    agroup.add_argument("--exp-attempts", type=int, default=3, metavar='N')
    
    ## Benchmark Parameters
    agroup = aparser.add_argument_group('Benchmark Configuration Parameters')
    agroup.add_argument("--multiply-scalefactor", action='store_true')
    agroup.add_argument("--stop-on-error", action='store_true')
    agroup.add_argument("--retry-on-zero", action='store_true')
    agroup.add_argument("--clear-logs", action='store_true')
    agroup.add_argument("--workload-trace", action='store_true')
    agroup.add_argument("--results-dir", type=str, default='results', metavar='D', help='Directory where CSV results are stored')
    agroup.add_argument("--overwrite", action='store_true', help='Overwrite existing results')
    
    ## Codespeed Parameters
    agroup = aparser.add_argument_group('Codespeed Parameters')
    agroup.add_argument("--codespeed-url", type=str, metavar="URL")
    agroup.add_argument("--codespeed-benchmark", type=str, metavar="BENCHMARK")
    agroup.add_argument("--codespeed-revision", type=str, metavar="REV")
    agroup.add_argument("--codespeed-lastrevision", type=str, metavar="REV")
    agroup.add_argument("--codespeed-branch", type=str, metavar="BRANCH")
    
    # And our Fabric environment keys
    agroup = aparser.add_argument_group('Fabric Parameters')
    for key in sorted(hstore.fabric.ENV_DEFAULT):
        keyPrefix = key.split(".")[0]
        if key not in BASE_SETTINGS and keyPrefix in [ "ec2", "hstore" ]:
            confType = type(hstore.fabric.ENV_DEFAULT[key])
            if key in DEFAULT_OPTIONS and not DEFAULT_OPTIONS[key] is None:
                confDefault = DEFAULT_OPTIONS[key]
            else:
                confDefault = hstore.fabric.ENV_DEFAULT[key]
            
            metavar = key.split(".")[-1].upper()
            agroup.add_argument("--"+key, type=confType, default=confDefault, metavar=metavar)
    ## FOR
    
    # Load in all of the possible parameters from our 'build-common.xml' file
    hstoreConfGroups = { }
    for key in sorted(HSTORE_PARAMS):
        keyPrefix = key.split(".")[0]
        if not keyPrefix in hstoreConfGroups:
            groupName = 'HStoreConf %s Parameters' % keyPrefix.title()
            hstoreConfGroups[keyPrefix] = aparser.add_argument_group(groupName)
        
        confType = str
        confDefault = None
        if key in BASE_SETTINGS and not BASE_SETTINGS[key] is None:
            confType = type(BASE_SETTINGS[key])
            confDefault = BASE_SETTINGS[key]
            
        metavar = key.split(".")[-1].upper()
        hstoreConfGroups[keyPrefix].add_argument("--"+key, type=confType, default=confDefault, metavar=metavar)
    ## FOR
    
    # Debug Parameters
    agroup = aparser.add_argument_group('Debug Parameters')
    agroup.add_argument("--debug", action='store_true')
    agroup.add_argument("--debug-hstore", action='store_true')
    agroup.add_argument("--debug-log4j", action='store_true')
    agroup.add_argument("--debug-log4j-site", action='store_true')
    agroup.add_argument("--debug-log4j-client", action='store_true')

    args = vars(aparser.parse_args())
    
    ## ----------------------------------------------
    ## ARGUMENT PROCESSING 
    ## ----------------------------------------------
    
    fabric = createFabricHandle(args["fabric"], env)
    for key in fabric.env.iterkeys():
        if not args.get(key, None) is None:
            fabric.env[key] = args[key]

    for key in args:
        if args[key] is None: continue
        if (key in BASE_SETTINGS or key in HSTORE_PARAMS):
            BASE_SETTINGS[key] = args[key]
    ## FOR
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)
        hstore.fabric.abstractfabric.LOG.setLevel(logging.DEBUG)
        hstore.fabric.sshfabric.LOG.setLevel(logging.DEBUG)
    if args['debug_hstore']:
        for k,v in DEBUG_OPTIONS.iteritems():
            BASE_SETTINGS[k] = v
    if args['debug_log4j']:
        args['debug_log4j_site'] = True
        args['debug_log4j_client'] = True
    if args['fast_start']:
        LOG.info("Enabling fast startup")
        for key in ['compile', 'update', 'conf', 'jar', 'sync']:
            args["no_%s" % key] = True
    if args['single_client']:
        LOG.info("Enabling single-client debug mode!")
        for key in ['count', 'threads_per_host', 'txnrate']:
            BASE_SETTINGS["client.%s" % key] = 1
    
    # If we get two consecutive intervals with zero results, then stop the benchmark
    if args['retry_on_zero']:
        fabric.env["hstore.exec_prefix"] += " -Dkillonzero=true"
    if args['no_shutdown']:
        fabric.env["hstore.exec_prefix"] += " -Dnoshutdown=true"
    
    # Update Fabric env
    conf_remove = set()
    for exp_opts in [ BASE_SETTINGS ]:
        assert exp_opts
        exp_conf_remove = [ ]
        for key,val in exp_opts.iteritems():
            if val == None: 
                LOG.debug("Parameter to Remove: %s" % key)
                exp_conf_remove.append(key)
            elif type(val) != types.FunctionType:
                fabric.env[key] = val
                LOG.debug("env[\"%s\"] = %s", key, val)
        ## FOR
        map(exp_opts.pop, exp_conf_remove)
        conf_remove.update(exp_conf_remove)
    ## FOR
    
    # Figure out what keys we need to remove to ensure that one experiment
    # doesn't contaminate another
    #for other_type in EXPERIMENT_SETTINGS:
        #if other_type != args['exp_type']:
            #for key in EXPERIMENT_SETTINGS[other_type].keys():
                #if not key in exp_opts: conf_remove.add(key)
            ### FOR
        ### IF
    ### FOR
    LOG.debug("Configuration Parameters to Remove:\n" + pformat(conf_remove))
    
    # BenchmarkController Parameters
    controllerParams = { } # { "noshutdown": True }
    
    # Shut 'er down!
    if args['stop_cluster']:
        LOG.info("Stopping cluster now!")
        fabric.stop_cluster()
        sys.exit(0)
        
    if not args['benchmark']:
        raise Exception("Did not specify benchmarks to execute")
    
    # Create local results directory
    if not args.get("results_dir", None) is None:
        resultsDir = os.path.join(args['results_dir'], args['exp_type'])
        if not os.path.exists(resultsDir):
            LOG.info("Creating results directory '%s'" % resultsDir)
            os.makedirs(resultsDir)
    
    needUpdate = (args['no_update'] == False)
    needUpdateLog4j = args['debug_log4j_site'] or args['debug_log4j_client']
    needResetLog4j = not (args['no_update'] or needUpdateLog4j)
    needSync = (args['no_sync'] == False)
    needCompile = (args['no_compile'] == False)
    needClearLogs = (args['clear_logs'] == True)
    
    for benchmark in args['benchmark']:
        finalResults = { }
        totalAttempts = args['exp_trials'] * args['exp_attempts']
        stop = False
        
        for partitions in map(int, args["partitions"]):
            LOG.info("*"*100)
            LOG.info("*"*100)
            LOG.info("** %s - %s - %d Partitions", args['exp_type'].upper(), benchmark.upper(), partitions)
            LOG.info("*"*100)
            LOG.info("*"*100)
            
            resultsOutput = getResultsFilename(args, benchmark, partitions)
            if os.path.exists(resultsOutput):
                if not args['overwrite']:
                    LOG.warn("Results file '%s' already exists. Skipping!\n", resultsOutput)
                    continue
                else:
                    LOG.warn("Results file '%s' already exists. It will be overwritten!", resultsOutput)
            
            try:
                fabric.env["hstore.partitions"] = partitions
                all_results = [ ]
                updateExperimentEnv(fabric, args, benchmark, partitions)
                    
                # Increase the client.scalefactor based on the number of partitions
                if args['multiply_scalefactor']:
                    BASE_SETTINGS["client.scalefactor"] = float(BASE_SETTINGS["client.scalefactor"] * partitions)
                    
                if args['start_cluster']:
                    LOG.info("Starting cluster for experiments [noExecute=%s]" % args['no_execute'])
                    fabric.start_cluster()
                    if args['no_execute']: sys.exit(0)
                ## IF
                
                # Disable all profiling
                if args['no_profiling']:
                    for k,v in fabric.env.iteritems():
                        if re.match("^(client|site)\.[\w\_]*profiling[\w\_]*", k):
                            fabric.env[k] = False if isinstance(v, bool) else ""
                    ## FOR
                
                client_inst = fabric.getRunningInstances()[-1]
                LOG.debug("Client Instance: " + client_inst.public_dns_name)
                
                ## Synchronize Instance Times
                if needSync: fabric.sync_time()
                needSync = False
                    
                ## Clear Log Files
                if needClearLogs: fabric.clear_logs()
                needClearLogs = False
                
                ## Update Log4j
                if needUpdateLog4j:
                    log4jDebug = [ ]
                    log4jTrace = [ ]
                    if args['debug_log4j_site']:
                        log4jDebug += DEBUG_SITE_LOGGING
                        log4jTrace += TRACE_SITE_LOGGING
                    if args['debug_log4j_client']:
                        log4jDebug += DEBUG_CLIENT_LOGGING
                    LOG.debug("Updating log4j.properties:\n  %s", "\n  ".join(log4jDebug))
                    fabric.updateLog4j(reset=True, debug=log4jDebug, trace=log4jTrace)
                    needUpdateLog4j = False
                    needResetLog4j = False
                    
                updateJar = (args['no_jar'] == False)
                LOG.debug("Parameters:\n%s" % pformat(env))
                conf_remove = conf_remove - set(env.keys())
                
                finalResults[partitions] = [ ]
                attempts = 0
                updateConf = (args['no_conf'] == False)
                while len(finalResults[partitions]) < args['exp_trials'] and \
                    attempts < totalAttempts and \
                    stop == False:
                    attempts += 1
                    LOG.info("Executing %s Trial #%d/%d [attempt=%d/%d]",
                                benchmark.upper(),
                                len(finalResults[partitions]),
                                args['exp_trials'],
                                attempts,
                                totalAttempts
                    )
                    try:
                        output, workloads = fabric.exec_benchmark(
                                                client_inst, \
                                                project=benchmark, \
                                                removals=conf_remove, \
                                                json=(args['no_json'] == False), \
                                                trace=args['workload_trace'], \
                                                build=needCompile, \
                                                updateJar=updateJar, \
                                                updateConf=updateConf, \
                                                updateRepo=needUpdate, \
                                                resetLog4j=needResetLog4j, \
                                                extraParams=controllerParams)
                                                
                        # Process JSON Output
                        if args['no_json'] == False:
                            data = processResults(client_inst, fabric, args, partitions, output, workloads)
                            finalResults[partitions].append(data)
                        
                        # CSV RESULT FILES
                        getCSVOutput(client_inst, fabric, args, benchmark, partitions)
                        
                        # Only compile for the very first invocation
                        needCompile = False
                    except KeyboardInterrupt:
                        stop = True
                        break
                    except SystemExit:
                        LOG.warn("Failed to complete trial succesfully")
                        if args['stop_on_error']:
                            stop = True
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
            finally:
                if not args['no_json']:
                    writeResultsCSV(args, benchmark, finalResults, partitions)
            if stop: break
        ## FOR (PARTITIONS)
        if stop: break
    ## FOR (BENCHMARKS)
    
    LOG.info("Disconnecting and dumping results")
    disconnect_all()
## MAIN
