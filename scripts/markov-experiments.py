#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from pprint import pprint, pformat

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## CONFIGURATION PARAMETERS
## ==============================================

NODE_MAX = 199
NODES_TO_SKIP = [ 20, 21, 45, 77, 114, 101 ] # Busted nodes @ UW-Madison

COORDINATOR_NODE = 1
SITE_NODE_START = COORDINATOR_NODE + 1
SITE_ALL_NODES = range(SITE_NODE_START, NODE_MAX)

PARTITIONS = [ 4, 8, 16, 32, 64 ] # , 128 ]

EXPERIMENT_PARAMS = {
    "motivation": [
        ## Trial #0 - Always multi-partition (worst case scenario)
        {
            "benchmark.neworder_only":          True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
        ## Trial #1 - NewOrder Only, Only determines whether multi-p or not
        {
            "benchmark.neworder_only":          True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         True,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
        ## Trial #2 - NewOrder Only, Pick partitions, Mark Done
        {
            "benchmark.neworder_only":          True,
            "site.exec_force_singlepartitioned":False,
            "site.exec_neworder_cheat":         True,
            "site.exec_neworder_cheat_done":    True,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  False,
        },
    ],
    "markov": [
        ## Trial #0 - Always single-partition, DB2 redirects
        {
            "site.exec_force_singlepartitioned":True,
            "site.exec_neworder_cheat":         False,
            "site.exec_neworder_cheat_done":    False,
            "site.exec_db2_redirects":          False,
            "site.exec_speculative_execution":  True,
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
OPT_NODE_FORMAT = "d-%02d.cs.wisc.edu"
OPT_CLIENT_FORMAT = OPT_NODE_FORMAT
OPT_SITES_PER_NODE = 1
OPT_PARTITIONS_PER_SITE = 2
OPT_BENCHMARK = "tpcc"
OPT_LOAD_THREADS = 8
OPT_SCALE_FACTOR = 10.0
OPT_TRACE = False
OPT_BLOCKING = False
OPT_TXNRATE = -1
OPT_DURATION = 120000
OPT_WARMUP = 60000
OPT_CLIENT_PER_NODE = 4
OPT_CLIENT_COUNT = -1
OPT_NEWORDER_ONLY = False

OPT_MARKOV_RECOMPUTE_END = False
OPT_MARKOV_RECOMPUTE_WARMUP = False
OPT_MARKOV_DIRECTORY = "files/markovs/vldb-june2011"

OPT_CLUSTER_DIRECTORY = "/tmp/hstore/clusters"
OPT_TRACE_DIRECTORY = "traces"

OPT_OUTPUT_LOG = "markov-experiments.log"

OPT_EXP_TYPE = "markov"
OPT_EXP_TRIALS = 3
OPT_EXP_SETTING = 0

## This is needed until I get proper throttling in the clients working...
BASE_TXNRATE = {
    "tpcc": 200,
    "tm1":  500,
    "auctionmark": 200,
}

def formatHostName(f, id):
    name = f
    if f.find('%') != -1:
        name = f % id
    return name
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
        
        "blocking=",
        "txnrate=",
        "duration=",
        "warmup=",
        "scale-factor=",
        "client-per-node=",
        "client-count=",
        "neworder-only=",
        "sites-per-node=",
        "partitions-per-site=",
        # Node Hostname Format
        "node-format=",
        # Client Hostname Format
        "client-format=",
        # Benchmark
        "benchmark=",
        # How many partitiosn to use in the experiment
        "partitions=",
        # Enable workload trace dumps
        "trace",
        
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
    
    for num_partitions in PARTITIONS:
        ## Build Cluster Configuration
        num_sites = num_partitions / OPT_PARTITIONS_PER_SITE
        num_nodes = num_sites / OPT_SITES_PER_NODE
        partition_id = 0
        nodes_added = 0
        site_id = 0
        node_idx = 0
        
        cluster_file = os.path.join(OPT_CLUSTER_DIRECTORY, "%dp.cluster" % num_partitions)
        with open(cluster_file, "w") as fd:
            while nodes_added < num_nodes:
                node_id = SITE_ALL_NODES[node_idx]
                if not node_id in NODES_TO_SKIP:
                    host = formatHostName(OPT_NODE_FORMAT, node_id)
                    for i in range(0, OPT_SITES_PER_NODE):
                        for j in range(0, OPT_PARTITIONS_PER_SITE):
                            fd.write("%s:%d:%d\n" % (host, site_id, partition_id))
                            partition_id += 1
                        ## FOR
                        site_id += 1
                    ## FOR
                    nodes_added += 1
                ## IF
                node_idx += 1
            ## WHILE
        ## WITH
        logging.info("Wrote cluster configuration to '%s'" % cluster_file)
        
        ## Clients
        CLIENT_COUNT = num_partitions / 2 if OPT_CLIENT_COUNT == -1 else OPT_CLIENT_COUNT
        CLIENT_NODES = [ ]
        while len(CLIENT_NODES) < CLIENT_COUNT:
            node_id = SITE_ALL_NODES[node_idx]
            if not node_id in NODES_TO_SKIP:
                CLIENT_NODES.append(node_id)
            node_idx += 1
        ## WHILE
        logging.debug("CLIENT_NODES = %s" % CLIENT_NODES)
        
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
    
        if OPT_TXNRATE == -1:
            CLIENT_TXNRATE = BASE_TXNRATE[OPT_BENCHMARK.lower()]
            if num_partitions == 8:
                CLIENT_TXNRATE *= 0.80
            elif num_partitions == 16:
                CLIENT_TXNRATE *= 0.70
            elif num_partitions == 32:
                CLIENT_TXNRATE *= 0.60
            elif num_partitions == 64:
                CLIENT_TXNRATE *= 0.50
            if OPT_EXP_SETTING == 0:
                if num_partitions >= 8:
                    CLIENT_TXNRATE *= 0.80
        else:
            CLIENT_TXNRATE = OPT_TXNRATE
        CLIENT_TXNRATE = int(CLIENT_TXNRATE)
    
        hstore_opts = {
            "coordinator.host":             formatHostName(OPT_NODE_FORMAT, COORDINATOR_NODE),
            "coordinator.delay":            5000,
            "client.duration":              OPT_DURATION,
            "client.warmup":                OPT_WARMUP,
            "client.host":                  ",".join(map(lambda x: formatHostName(OPT_CLIENT_FORMAT, x), CLIENT_NODES)),
            "client.count":                 CLIENT_COUNT,
            "client.processesperclient":    OPT_CLIENT_PER_NODE,
            "client.txnrate":               CLIENT_TXNRATE,
            "client.blocking":              OPT_BLOCKING,
            "client.scalefactor":           OPT_SCALE_FACTOR,
            "benchmark.neworder_only":      False,
            "benchmark.neworder_abort":     True,
            "benchmark.neworder_multip":    True,
            "benchmark.warehouses":         num_partitions,
            "benchmark.loadthreads":        OPT_LOAD_THREADS,
            "markov.recompute_end":         OPT_MARKOV_RECOMPUTE_END,
            "markov.recompute_warmup":      OPT_MARKOV_RECOMPUTE_WARMUP,
        }

        exp_opts = EXPERIMENT_PARAMS[OPT_EXP_TYPE][OPT_EXP_SETTING]

        if "markov" in exp_opts and exp_opts["markov"]:
            markov_type = "global" if "markov.global" in exp_opts and exp_opts["markov.global"] else "clustered"
            markov = os.path.join(OPT_MARKOV_DIRECTORY, "%s.%dp.%s.markovs.gz" % (OPT_BENCHMARK.lower(), num_partitions, markov_type))
            assert os.path.exists(markov), "Missing: " + markov
            exp_opts['markov'] = markov
            
        if "benchmark.neworder_only" in exp_opts and exp_opts["benchmark.neworder_only"]:
            del exp_opts["benchmark.neworder_only"]
            benchmark_opts["benchmark.neworder_only"] = True

        if "thresholds" in exp_opts and exp_opts["thresholds"]:
            del exp_opts["thresholds"]
            exp_opts["markov.thresholds.value"] = float(options["thresholds"][0])

        hstore_opts = dict(hstore_opts.items() + exp_opts.items())
        hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, hstore_opts[x]), hstore_opts.keys()))
        ant_opts_cmd = " ".join([base_opts_cmd, hstore_opts_cmd])

        logging.debug(pformat(hstore_opts))
        
        ## HACK
        with open("properties/default.properties", "r") as f:
            contents = f.read()
        ## WITH
        for e in exp_opts.items():
            k, v = e
            contents = re.sub("%s[\s]+=.*" % re.escape(k), "%s = %s" % (k, str(v).lower()), contents)
        with open("properties/default.properties", "w") as f:
            f.write(contents)
        ## WITH
        
        print "%s EXP #%d - PARTITIONS %d" % (OPT_BENCHMARK.upper(), OPT_EXP_SETTING, num_partitions)
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