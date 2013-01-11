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
import hstore.fabfile

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
OPT_BASE_TXNRATE_PER_PARTITION = 1000
OPT_BASE_TXNRATE = 1000
OPT_BASE_CLIENT_COUNT = 1
OPT_BASE_CLIENT_THREADS_PER_HOST = 100
OPT_BASE_SCALE_FACTOR = float(1.0)
OPT_BASE_PARTITIONS_PER_SITE = 6
OPT_PARTITION_PLAN_DIR = "files/designplans"
OPT_MARKOV_DIR = "files/markovs/vldb-august2012"

DEFAULT_OPTIONS = {
    "hstore.git_branch": "strangelove"
}
DEBUG_OPTIONS = {
    "site.status_enable":             True,
    "site.status_interval":           20000,
    "site.status_exec_info": True,
    "site.exec_profiling":            True,
    #"site.txn_profiling":             True,    
}
DEBUG_SITE_LOGGING = [
    "edu.brown.hstore.HStoreSite",
    "edu.brown.hstore.PartitionExecutor",
    "edu.brown.hstore.TransactionQueueManager"
]
DEBUG_CLIENT_LOGGING = [
    "edu.brown.api.BenchmarkComponent",
    "edu.brown.api.BenchmarkController",
]

BASE_SETTINGS = {
    "ec2.site_type":                    "m2.4xlarge",
    "ec2.client_type":                  "c1.xlarge",
    #"ec2.site_type":                    "m2.4xlarge",
    #"ec2.client_type":                  "m1.large",
    #"ec2.site_type":                    "m1.xlarge",
    "ec2.change_type":                  True,
    "ec2.cluster_group":                "strangelove",
    
    "hstore.sites_per_host":            1,
    "hstore.partitions_per_site":       OPT_BASE_PARTITIONS_PER_SITE,
    "hstore.num_hosts_round_robin":     None,

    "client.blocking":                  False,
    "client.blocking_concurrent":       OPT_BASE_BLOCKING_CONCURRENT,
    "client.txnrate":                   OPT_BASE_TXNRATE,
    "client.count":                     OPT_BASE_CLIENT_COUNT,
    "client.threads_per_host":          OPT_BASE_CLIENT_THREADS_PER_HOST,
    "client.interval":                  10000,
    "client.skewfactor":                -1,
    "client.duration":                  120000,
    "client.warmup":                    60000,
    "client.scalefactor":               OPT_BASE_SCALE_FACTOR,
    "client.txn_hints":                 True,
    "client.memory":                    6000,
    "client.output_basepartitions":     False,
    
    "site.jvm_asserts":                         False,
    "site.log_backup":                          False,
    "site.status_enable":                       False,
    "site.status_show_thread_info":             False,
    "site.status_exec_info":           False,
    "site.txn_incoming_delay":                  10,
    "site.coordinator_init_thread":             False,
    "site.coordinator_finish_thread":           False,
    "site.txn_restart_limit":                   5,
    "site.txn_restart_limit_sysproc":           100,
    "site.exec_force_singlepartitioned":        True,
    "site.memory":                              61440,
    "site.queue_incoming_max_per_partition":    150,
    "site.queue_incoming_release_factor":       0.90,
    "site.queue_incoming_increase":             10,
    "site.queue_dtxn_max_per_partition":        1000,
    "site.queue_dtxn_release_factor":           0.90,
    "site.queue_dtxn_increase":                 0,
    "site.exec_db2_redirects":                  False,
    "site.cpu_affinity":                        True,
    "site.cpu_affinity_one_partition_per_core": True,
}

EXPERIMENT_SETTINGS = {
    "motivation": {
        # HVM Instances
        # http://cloud-images.ubuntu.com/desktop/precise/current/
        #"ec2.site_ami":                         "ami-efa81d86",
        #"ec2.site_type":                        "cc1.4xlarge",
        #"ec2.client_type":                      "c1.xlarge",
        #"ec2.change_type":                      False,
        #"ec2.cluster_group":                    "hstore-hvm",
        #"hstore.partitions_per_site":           64,
        
        "ec2.site_type":                       "c1.xlarge",
        "site.memory":                          6144,
        "site.txn_incoming_delay":              5,
        "site.specexec_enable":                 False,
        "site.specexec_idle":                   False,
        "site.markov_enable":                   False,
        "client.count":                         1,
        "client.txnrate":                       100000,
        "client.blocking":                      True,
        "client.output_response_status":        True,
        "client.output_exec_profiling":         "execprofile.csv",
        "client.output_queue_profiling":        "queueprofile.csv",
        "client.output_txn_profiling":          "txnprofile.csv",
        "client.output_txn_profiling_combine":  True,
        #"client.output_txn_counters":           "txncounters.csv",
        #"client.output_txn_counters_combine":   True,
        "benchmark.neworder_only":              True,
        "benchmark.neworder_abort":             False,
        "benchmark.neworder_multip_mix":        100,
        "benchmark.loadthread_per_warehouse":   False,
    },
}
EXPERIMENT_SETTINGS['motivation-oneclient'] = dict(EXPERIMENT_SETTINGS['motivation'].items())


## ==============================================
## updateEnv
## ==============================================
def updateEnv(args, env, benchmark, partitions):
    global OPT_BASE_TXNRATE_PER_PARTITION
  
    ## ----------------------------------------------
    ## MOTIVATION
    ## ----------------------------------------------
    if args['exp_type'].startswith("motivation"):
        if env.get('site.markov_enable', False):
            if benchmark == "tpcc":
                markov = "%s-%dp.markov.gz" % (benchmark, partitions)
            else:
                markov = "%s.markov.gz" % (benchmark)
            env["hstore.exec_prefix"] += " -Dmarkov=%s" % os.path.join(OPT_MARKOV_DIR, markov)
        else:
            env['site.markov_fixed'] = True
        ## IF
        if args['exp_type'] == 'motivation-oneclient':
            env["client.threads_per_host"] = 1
        else:
            env["client.threads_per_host"] = int(partitions/2)
        env["benchmark.loadthreads"] = min(16, partitions)
        
        pplan = "%s.lns.pplan" % benchmark
        env["hstore.exec_prefix"] += " -Dpartitionplan=%s" % os.path.join(OPT_PARTITION_PLAN_DIR, pplan)
        env["hstore.exec_prefix"] += " -Dpartitionplan.ignore_missing=True"

## DEF

## ==============================================
## saveCSVResults
## ==============================================
def saveCSVResults(args, partitions, filename):
    # Create local results directory
    resultsDir = os.path.join(args['results_dir'], args['exp_type'])
    if not os.path.exists(resultsDir):
        LOG.info("Creating results directory '%s'" % resultsDir)
        os.makedirs(resultsDir)
    
    # Go out and grab the remote file and save it locally in our results dir.
    filename = os.path.join(hstore.fabfile.HSTORE_DIR, filename)
    LOG.info("Going to retrieve remote CSV file '%s'" % filename)
    contents = hstore.fabfile.get_file(filename)
    if len(contents) > 0:
        # We'll prefix the name with the number of partitions
        localName = "%02dp-%s" % (partitions, os.path.basename(filename))
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
def processResults(args, partitions, output, workloads, results):
    data = hstore.parseJSONResults(output)
    for key in [ 'TXNTOTALPERSECOND' ]:
        if key in data:
            txnrate = float(data[key])
            break
    ## FOR
    assert not txnrate is None, \
        "Failed to extract throughput rate from output\n" + pformat(data)
    
    minTxnRate = float(data["MINTXNPERSECOND"]) if "MINTXNPERSECOND" in data else None
    maxTxnRate = float(data["MAXTXNPERSECOND"]) if "MAXTXNPERSECOND" in data else None
    stddevTxnRate = float(data["STDDEVTXNPERSECOND"]) if "STDDEVTXNPERSECOND" in data else None
    
    if int(txnrate) == 0: return
    LOG.info("Throughput: %.2f" % txnrate)
    
    results.append(txnrate)
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
            last_changed_rev, last_changed_date = hstore.fabfile.get_version()
        LOG.info("last_changed_rev:", last_changed_rev)
        LOG.info("last_changed_date:", last_changed_date)
            
        codespeedBenchmark = benchmark
        if not args["codespeed_benchmark"] is None:
            codespeedBenchmark = args["codespeed_benchmark"]
        
        codespeedBranch = env["hstore.git_branch"]
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
    return
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    HSTORE_PARAMS = hstore.getAllParameters()
    
    aparser = argparse.ArgumentParser(description='H-Store Experiment Runner')
    
    aparser.add_argument('--benchmark', choices=hstore.getBenchmarks(), nargs='+',
                         help='Target benchmarks')
    
    # Cluster Parameters
    agroup = aparser.add_argument_group('EC2 Cluster Control Parameters')
    agroup.add_argument("--partitions", type=int, default=4, metavar='P', nargs='+',)
    agroup.add_argument("--start-cluster", action='store_true')
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
    
    ## Experiment Parameters
    agroup = aparser.add_argument_group('Experiment Parameters')
    agroup.add_argument("--exp-type", type=str, choices=EXPERIMENT_SETTINGS.keys(), default=EXPERIMENT_SETTINGS.keys()[0])
    agroup.add_argument("--exp-trials", type=int, default=3, metavar='N')
    agroup.add_argument("--exp-attempts", type=int, default=3, metavar='N')
    
    ## Benchmark Parameters
    agroup = aparser.add_argument_group('Benchmark Configuration Parameters')
    agroup.add_argument("--multiply-scalefactor", action='store_true')
    agroup.add_argument("--stop-on-error", action='store_true')
    agroup.add_argument("--retry-on-zero", action='store_true')
    agroup.add_argument("--clear-logs", action='store_true')
    agroup.add_argument("--workload-trace", action='store_true')
    agroup.add_argument("--results-dir", type=str, default='results', help='Directory where CSV results are stored')
    
    ## Codespeed Parameters
    agroup = aparser.add_argument_group('Codespeed Parameters')
    agroup.add_argument("--codespeed-url", type=str, metavar="URL")
    agroup.add_argument("--codespeed-benchmark", type=str, metavar="BENCHMARK")
    agroup.add_argument("--codespeed-revision", type=str, metavar="REV")
    agroup.add_argument("--codespeed-lastrevision", type=str, metavar="REV")
    agroup.add_argument("--codespeed-branch", type=str, metavar="BRANCH")
    
    # And our Boto environment keys
    agroup = aparser.add_argument_group('Boto Parameters')
    for key in sorted(hstore.fabfile.ENV_DEFAULT):
        keyPrefix = key.split(".")[0]
        if key not in BASE_SETTINGS and keyPrefix in [ "ec2", "hstore" ]:
            confType = type(hstore.fabfile.ENV_DEFAULT[key])
            if key in DEFAULT_OPTIONS and not DEFAULT_OPTIONS[key] is None:
                confDefault = DEFAULT_OPTIONS[key]
            else:
                confDefault = hstore.fabfile.ENV_DEFAULT[key]
            
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
    
    if not args['benchmark']:
        raise Exception("Did not specify benchmarks to execute")
    
    for key in env.keys():
        if key in args and not args[key] is None:
            env[key] = args[key]
    ## FOR
    for key in args:
        if args[key] is None: continue
        if (key in BASE_SETTINGS or key in HSTORE_PARAMS):
            BASE_SETTINGS[key] = args[key]
    ## FOR
    
    if args['debug']:
        LOG.setLevel(logging.DEBUG)
        hstore.fabfile.LOG.setLevel(logging.DEBUG)
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
        env["hstore.exec_prefix"] += " -Dkillonzero=true"
    
    # Update Fabric env
    conf_remove = set()
    for exp_opts in [ BASE_SETTINGS, EXPERIMENT_SETTINGS[args['exp_type']] ]:
        assert exp_opts
        exp_conf_remove = [ ]
        for key,val in exp_opts.iteritems():
            if val == None: 
                LOG.debug("Parameter to Remove: %s" % key)
                exp_conf_remove.append(key)
            elif type(val) != types.FunctionType:
                env[key] = val
                LOG.debug("env[\"%s\"] = %s", key, val)
        ## FOR
        map(exp_opts.pop, exp_conf_remove)
        conf_remove.update(exp_conf_remove)
    ## FOR
    
    # Figure out what keys we need to remove to ensure that one experiment
    # doesn't contaminate another
    for other_type in EXPERIMENT_SETTINGS.keys():
        if other_type != args['exp_type']:
            for key in EXPERIMENT_SETTINGS[other_type].keys():
                if not key in exp_opts: conf_remove.add(key)
            ## FOR
        ## IF
    ## FOR
    LOG.debug("Configuration Parameters to Remove:\n" + pformat(conf_remove))
    
    # BenchmarkController Parameters
    controllerParams = { }
    
    
    
    needUpdate = (args['no_update'] == False)
    needUpdateLog4j = args['debug_log4j_site'] or args['debug_log4j_client']
    needResetLog4j = not (args['no_update'] or needUpdateLog4j)
    needSync = (args['no_sync'] == False)
    needCompile = (args['no_compile'] == False)
    needClearLogs = (args['clear_logs'] == False)
    origScaleFactor = BASE_SETTINGS['client.scalefactor']
    for benchmark in args['benchmark']:
        final_results = { }
        totalAttempts = args['exp_trials'] * args['exp_attempts']
        stop = False
        
        for partitions in map(int, args["partitions"]):
            LOG.info("%s - %s - %d Partitions" % (args['exp_type'].upper(), benchmark.upper(), partitions))
            env["hstore.partitions"] = partitions
            all_results = [ ]
            updateEnv(args, env, benchmark, partitions)
                
            # Increase the client.scalefactor based on the number of partitions
            if args['multiply_scalefactor']:
                BASE_SETTINGS["client.scalefactor"] = float(BASE_SETTINGS["client.scalefactor"] * partitions)
                
            if args['start_cluster']:
                LOG.info("Starting cluster for experiments [noExecute=%s]" % args['no_execute'])
                hstore.fabfile.start_cluster(updateSync=needSync)
                if args['no_execute']: sys.exit(0)
            ## IF
            
            client_inst = hstore.fabfile.__getRunningClientInstances__()[0]
            LOG.debug("Client Instance: " + client_inst.public_dns_name)
            
            ## Synchronize Instance Times
            if needSync: hstore.fabfile.sync_time()
            needSync = False
                
            ## Clear Log Files
            if needClearLogs: hstore.fabfile.clear_logs()
            needClearLogs = False
            
            ## Update Log4j
            if needUpdateLog4j:
                LOG.info("Updating log4j.properties")
                enableDebug = [ ]
                if args['debug_log4j_site']:
                    enableDebug += DEBUG_SITE_LOGGING
                if args['debug_log4j_client']:
                    enableDebug += DEBUG_CLIENT_LOGGING
                with settings(host_string=client_inst.public_dns_name):
                    hstore.fabfile.enable_debugging(debug=enableDebug)
                ## WITH
                needUpdateLog4j = False
                
            updateJar = (args['no_jar'] == False)
            LOG.debug("Parameters:\n%s" % pformat(env))
            conf_remove = conf_remove - set(env.keys())
            
            results = [ ]
            attempts = 0
            updateConf = (args['no_conf'] == False)
            while len(results) < args['exp_trials'] and attempts < totalAttempts and stop == False:
                ## Only compile for the very first invocation
                if needCompile:
                    if env["hstore.exec_prefix"].find("compile") == -1:
                        env["hstore.exec_prefix"] += " compile"
                else:
                    env["hstore.exec_prefix"] = env["hstore.exec_prefix"].replace("compile", "")
                    
                needCompile = False
                attempts += 1
                LOG.info("Executing %s Trial #%d/%d [attempt=%d/%d]" % (\
                            benchmark.upper(),
                            len(results),
                            args['exp_trials'],
                            attempts,
                            totalAttempts
                ))
                try:
                    with settings(host_string=client_inst.public_dns_name):
                        output, workloads = hstore.fabfile.exec_benchmark(
                                                project=benchmark, \
                                                removals=conf_remove, \
                                                json=(args['no_json'] == False), \
                                                trace=args['workload_trace'], \
                                                updateJar=updateJar, \
                                                updateConf=updateConf, \
                                                updateRepo=needUpdate, \
                                                resetLog4j=needResetLog4j, \
                                                extraParams=controllerParams)
                                                
                        # Process JSON Output
                        if args['no_json'] == False:
                            processResults(args, partitions, output, workloads, results)
                        ## IF
                        
                        # CSV RESULT FILES
                        for key in ["output_txn_profiling", "output_exec_profiling", "output_queue_profiling", "output_txn_counters"]:
                            key = "client.%s" % key
                            LOG.debug("Checking whether '%s' is enabled" % (key))
                            if key in env and not env[key] is None:
                                saveCSVResults(args, partitions, env[key])
                        ## FOR
                        
                    ## WITH
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
            if results: final_results[partitions] = (benchmark, results, attempts)
            if stop: break
            stop = False
        ## FOR (PARTITIONS)
        if stop: break
        stop = False
    ## FOR (BENCHMARKS)
    
    LOG.info("Disconnecting and dumping results")
    try:
        disconnect_all()
    finally:
        for partitions in sorted(final_results.keys()):
            print "%s - Partitions %d" % (args['exp_type'].upper(), partitions)
            pprint(final_results[partitions])
            
            #for benchmark, results, attempts in final_results[partitions]:
                #print "   %s [Attempts:%s/%s]" % (benchmark.upper(), attempts, totalAttempts)
                #for trial in xrange(len(results)):
                    #print "      TRIAL #%d: %.4f" % (trial, results[trial])
                ### FOR
            ### FOR
            #print
        ## FOR
    ## TRY
## MAIN
