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
import tempfile
import shutil
import logging
import getopt
import string
import math
from pprint import pprint, pformat

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## CONFIGURATION PARAMETERS
## ==============================================

EXPERIMENT_SETTINGS = {
    "motivation": [
        ## Trial #0 - Always multi-partition (worst case scenario)
        {
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
        ## Trial #1 - NewOrder Only, Only determines whether multi-p or not
        {
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         True,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
        ## Trial #2 - NewOrder Only, Pick partitions, Mark Done
        {
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         True,
            "site.exec_neworder_cheat_done":    True,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
        },
        ## Trial #2 - NewOrder Only, Pick partitions, Mark Done, No Aborts
        {
            "benchmark.neworder_only":          True,
            "benchmark.neworder_abort":         False,
            "site.exec_no_undo_logging_all":    True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         True,
            "site.exec_neworder_cheat_done":    True,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
        },
    ],
    "markov": [
        ## Trial #0 - Always single-partition, DB2 redirects
        {
            "site.exec_force_singlepartitioned":True,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          True,
            "site.exec_speculative_execution":  False,
        },
        ## Trial #1 - Global Markov Models
        {
            "markov":                           True,
            "markov.global":                    True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
        },
        ## Trial #2 - Partitioned Markov Models
        {
            "markov":                           True,
            "markov.global":                    False,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
        },
        ## Trial #3 - Testing - Always single-partition
        {
            "site.exec_force_singlepartitioned":True,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
    ],
    "thresholds": [
        {
            "thresholds":                       True,
            "markov":                           True,
            "markov.global":                    False,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
        },
    ]
}

HOST_ID_MIN = 1
HOST_ID_MAX = 199
HOSTS_TO_SKIP = [ 20, 21, 45, 77, 114, 101 ] # Busted nodes @ UW-Madison

PARTITIONS = [ 4, 8, 16, 32, 64 ] # , 128 ]

OPT_HOST_FORMAT = "d-%02d.cs.wisc.edu"
OPT_HOSTS_FILE = ""

OPT_COORDINATOR_HOST = ""
OPT_COORDINATOR_DELAY = -1

OPT_SITES_PER_NODE = 1
OPT_PARTITIONS_PER_SITE = 4
OPT_BENCHMARK = "tpcc"
OPT_LOAD_THREADS = 8
OPT_SCALE_FACTOR = 10.0
OPT_TRACE = False
OPT_BLOCKING = False
OPT_TXNRATE = 1000
OPT_DURATION = 120000
OPT_WARMUP = 60000
OPT_CLIENTS_PER_HOST = 4
OPT_CLIENTS_COUNT = -1
OPT_NEWORDER_ONLY = False

OPT_MARKOV_RECOMPUTE_END = False
OPT_MARKOV_RECOMPUTE_WARMUP = False
OPT_MARKOV_DIRECTORY = "files/markovs/vldb-june2011"

OPT_CLUSTER_DIRECTORY = "/tmp/hstore-pavlo/clusters"
OPT_TRACE_DIRECTORY = "traces"

OPT_OUTPUT_LOG = "markov-experiments.log"

OPT_EXP_TYPE = "markov"
OPT_EXP_TRIALS = 3
OPT_EXP_SETTINGS = 0

def formatHostName(f, id):
    name = f
    if f.find('%') != -1:
        name = f % id
    return name
## DEF

def getHostsList():
    hosts = [ ]
    
    if len(OPT_HOSTS_FILE) > 0:
        with open(OPT_HOSTS_FILE, "r") as f:
            for line in f:
                s = map(string.strip, line.split(':'))
                if s[0].startswith("#"): continue
                if len(s[0]) > 0: # Allow dupes
                    hosts.append(s[0])
            ## FOR
        ## WITH
    else:
        for host_id in range(HOST_ID_MIN, HOST_ID_MAX):
            if not host_id in HOSTS_TO_SKIP:
                host = formatHostName(OPT_HOST_FORMAT, host_id)
                hosts.append(host)
        ## FOR
    return (hosts);

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        # Experiment Parameters
        "exp-type=",
        "exp-settings=",
        "exp-trials=",
        
        "blocking=",
        "txnrate=",
        "duration=",
        "warmup=",
        "scale-factor=",
        "clients-per-host=",
        "clients-count=",
        "neworder-only=",
        "sites-per-node=",
        "partitions-per-site=",
        
        "coordinator-host=",
        "coordinator-delay=",
        
        # Benchmark
        "benchmark=",
        # How many partitiosn to use in the experiment
        "partitions=",
        # Enable workload trace dumps
        "trace",

        # Hostname Format
        "host-format=",
        # Hosts File
        "hosts-file=",

        # Whether to recompute Markov models after run
        "markov-recompute-end",
        # Whether to recompute Markov models after warmup period
        "markov-recompute-warmup",
        
        # Thresholds value
        "thresholds=",
        
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
    if "debug" in options: logging.getLogger().setLevel(logging.DEBUG)
    if "partitions" in options: PARTITIONS = map(int, options["partitions"])

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
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    
    if not os.path.exists("%s.jar" % OPT_BENCHMARK):
        logging.info("Building %s project jar" % OPT_BENCHMARK.upper())
        cmd = "ant compile hstore-prepare -Dproject=%s" % OPT_BENCHMARK
        if OPT_OUTPUT_LOG: cmd += " | tee " + OPT_OUTPUT_LOG
        logging.debug(cmd)
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, cmd + "\n" + output
    ## IF
    
    if not os.path.exists(OPT_CLUSTER_DIRECTORY):
        os.makedirs(OPT_CLUSTER_DIRECTORY)
    
    ALL_HOSTS = getHostsList()
    
    for num_partitions in PARTITIONS:
        ## Build Cluster Configuration
        num_sites = num_partitions / OPT_PARTITIONS_PER_SITE
        num_nodes = num_sites / OPT_SITES_PER_NODE
        partition_id = 0
        nodes_added = 0
        site_id = 0
        host_idx = 0
        
        AVAILABLE_HOSTS = ALL_HOSTS[:]
        COORDINATOR_HOST = AVAILABLE_HOSTS.pop(0) if not OPT_COORDINATOR_HOST else OPT_COORDINATOR_HOST
        HSTORE_HOSTS = [ ]
        CLIENT_HOSTS = [ ]
        
        cluster_file = os.path.join(OPT_CLUSTER_DIRECTORY, "%dp.cluster" % num_partitions)
        with open(cluster_file, "w") as fd:
            while len(HSTORE_HOSTS) < num_nodes:
                host = AVAILABLE_HOSTS.pop(0)
                logging.debug("HOST: " + host)
                for i in range(0, OPT_SITES_PER_NODE):
                    logging.debug("   SITE: %d" % site_id)
                    for j in range(0, OPT_PARTITIONS_PER_SITE):
                        logging.debug("      PARTITION: %d" % partition_id)
                        fd.write("%s:%d:%d\n" % (host, site_id, partition_id))
                        partition_id += 1
                    ## FOR
                    site_id += 1
                ## FOR
                HSTORE_HOSTS.append(host)
            ## FOR
        ## WITH
        assert(len(HSTORE_HOSTS) == num_nodes)
        logging.info("Wrote cluster configuration to '%s'" % cluster_file)
        
        ## Clients
        CLIENT_COUNT = num_partitions / 2 if OPT_CLIENTS_COUNT == -1 else OPT_CLIENTS_COUNT
        while len(CLIENT_HOSTS) < CLIENT_COUNT:
            CLIENT_HOSTS.append(AVAILABLE_HOSTS.pop(0))
        ## WHILE
        logging.debug("CLIENT_HOSTS = %s" % CLIENT_HOSTS)
        
        base_opts = {
            "project":                  OPT_BENCHMARK,
            "hosts":                    cluster_file,
        }
        base_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, base_opts[x]), base_opts.keys()))
        cmd = "ant hstore-jar " + base_opts_cmd
        logging.debug(cmd)
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, cmd + "\n" + output
        logging.info("Initialized %s project jar [hosts=%d, sites=%d, partitions=%d]" % (OPT_BENCHMARK.upper(), num_nodes, num_sites, num_partitions))
    
        CLIENT_TXNRATE = int(OPT_TXNRATE)
        
        coordinator_delay = 0 if OPT_COORDINATOR_DELAY == -1 else OPT_COORDINATOR_DELAY
    
        hstore_opts = {
            "coordinator.host":             COORDINATOR_HOST,
            "coordinator.delay":            int(coordinator_delay),
            "client.duration":              OPT_DURATION,
            "client.warmup":                OPT_WARMUP,
            "client.host":                  ",".join(CLIENT_HOSTS),
            "client.count":                 CLIENT_COUNT,
            "client.processesperclient":    OPT_CLIENTS_PER_HOST,
            "client.txnrate":               CLIENT_TXNRATE,
            "client.blocking":              OPT_BLOCKING,
            "client.scalefactor":           OPT_SCALE_FACTOR,
            "benchmark.neworder_only":      False,
            "benchmark.neworder_abort":     True,
            "benchmark.neworder_multip":    True,
            "benchmark.warehouses":         num_partitions,
            "benchmark.loadthreads":        OPT_LOAD_THREADS,
            "benchmark.initial_polling_delay": 10000,
            "markov.recompute_end":         OPT_MARKOV_RECOMPUTE_END,
            "markov.recompute_warmup":      OPT_MARKOV_RECOMPUTE_WARMUP,
        }
        hstore_opts = dict(hstore_opts.items() + EXPERIMENT_SETTINGS[OPT_EXP_TYPE][OPT_EXP_SETTINGS].items())

        if "markov" in hstore_opts and hstore_opts["markov"]:
            markov_type = "global" if "markov.global" in hstore_opts and hstore_opts["markov.global"] else "clustered"
            markov = os.path.join(OPT_MARKOV_DIRECTORY, "%s.%dp.%s.markovs.gz" % (OPT_BENCHMARK.lower(), num_partitions, markov_type))
            assert os.path.exists(markov), "Missing: " + markov
            hstore_opts['markov'] = markov
            
            ## Add a delay in to account for loading the markovs
            ## Eventually we should load them *after* the network connections
            if OPT_COORDINATOR_DELAY == -1:
                hstore_opts["coordinator.delay"] = int(math.log(num_partitions, 4) * OPT_PARTITIONS_PER_SITE * 5000)
            hstore_opts["benchmark.initial_polling_delay"] = hstore_opts["coordinator.delay"]
            
        if "benchmark.neworder_only" in hstore_opts and hstore_opts["benchmark.neworder_only"]:
            del hstore_opts["benchmark.neworder_only"]
            hstore_opts["benchmark.neworder_only"] = True

        if "thresholds" in hstore_opts and hstore_opts["thresholds"]:
            del hstore_opts["thresholds"]
            hstore_opts["markov.thresholds.value"] = float(options["thresholds"][0])

        hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, hstore_opts[x]), hstore_opts.keys()))
        ant_opts_cmd = " ".join([base_opts_cmd, hstore_opts_cmd])

        logging.debug(pformat(hstore_opts))
        
        ## HACK
        with open("properties/default.properties", "r") as f:
            contents = f.read()
        ## WITH
        for e in hstore_opts.items():
            k, v = e
            if k.startswith("benchmark."): continue
            if contents.find(k) != -1:
                contents = re.sub("%s[\s]+=.*" % re.escape(k), "%s = %s" % (k, str(v).lower()), contents)
            else:
                contents += "\n%s = %s" % (k, v)
        with open("properties/default.properties", "w") as f:
            f.write(contents)
        ## WITH
        
        print "%s - EXP %s #%d - PARTITIONS %d" % (OPT_BENCHMARK.upper(), OPT_EXP_TYPE.title(), OPT_EXP_SETTINGS, num_partitions)
        for trial in range(0, OPT_EXP_TRIALS):
            cmd = "ant hstore-benchmark " + ant_opts_cmd
            if OPT_TRACE: 
                trace_dir = os.path.join(OPT_TRACE_DIRECTORY, "%s-%dp-%d" % (OPT_BENCHMARK.lower(), num_partitions, trial))
                cmd += " -Dtrace=" + trace_dir
                logging.debug("Writing workload trace logs to '" + trace_dir + "'")
            if OPT_OUTPUT_LOG: cmd += " | tee " + OPT_OUTPUT_LOG
            if trial == 0: logging.debug(cmd)
            #sys.exit(1)
            (result, output) = commands.getstatusoutput(cmd)
            assert result == 0, cmd + "\n" + output
            
            ## Get the throughput rate from the output
            match = re.search("Transactions per second: ([\d]+\.[\d]+)", output)
            if not match:
                logging.warn("Failed to complete full execution time")
                regex = re.compile("Completed [\d]+ txns at a rate of ([\d]+\.[\d]+) txns/s")
                lines = output.split("\n")
                lines.reverse()
                for line in lines:
                    match = regex.search(line)
                    if match: break
                ## FOR
            txnrate = "XXX"
            if match: txnrate = match.group(1)
            # assert match, "Failed to get throughput:\n" + output
            print "  Trial #%d: %s" % (trial, txnrate)
            
            ## Make sure we kill everything
            #cmd = "pusher --show-host 'pskill java' ./allhosts.txt"
            #(result, output) = commands.getstatusoutput(cmd)
            #assert result == 0, cmd + "\n" + output
        ## FOR (TRIAL)
        print
    ## FOR (PARTITIONS)
    
## MAIN