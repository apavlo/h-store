#!/usr/bin/env python
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
from pprint import pprint

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## CONFIGURATION PARAMETERS
## ==============================================

BENCHMARK = "tpcc"

NUM_TRIALS_PER_EXP = 3

CLIENT_NODES_START = 3
CLIENT_COUNT = 4
CLIENT_PER_NODE = 4
CLIENT_BLOCKING = True

SITE_NODE_START = CLIENT_NODES_START + CLIENT_COUNT
SITE_NODE_MAX = 199
SITE_ALL_NODES = range(SITE_NODE_START, SITE_NODE_MAX)
SITES_PER_NODE = 1

PARTITIONS = [ 8, 16, 32, 64, 128 ]
PARTITIONS_PER_SITE = 2
NODES_TO_SKIP = [ 20, 21, 45, 77, 114 ]
NODE_FORMAT = "d-%02d.cs.wisc.edu"

COORDINATOR_NODE = 2

TRIAL_PARAMETERS = [
    ## Trial #1
    {
        "node.force_singlepartition":       True,
        "node.force_neworderinspect":       False,
        "node.force_neworderinspect_done":  False,
    },
    ## Trial #1
    {
        "node.force_singlepartition":       False,
        "node.force_neworderinspect":       True,
        "node.force_neworderinspect_done":  False,
    },
    ## Trial #1
    {
        "node.force_singlepartition":       False,
        "node.force_neworderinspect":       True,
        "node.force_neworderinspect_done":  True,
    },
]
OPT_TRIAL = 0
OPT_LOAD_THREADS = 8
OPT_SCALE_FACTOR = 10

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Which trial to execute
        "trial=",
        ## How many partitiosn to use in the experiment
        "partitions=",
        ## Enable debug logging
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
            globals()[varname] = orig_type(True if type(globals()[varname]) == bool else options[key][0])
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    
    if not os.path.exists("%s.jar" % BENCHMARK):
        logging.info("Building %s project jar" % BENCHMARK.upper())
        cmd = "ant compile hstore-prepare -Dproject=%s" % BENCHMARK
        logging.debug(cmd)
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, cmd + "\n" + output
    ## IF
    
    for num_partitions in PARTITIONS:
        ## Build Cluster Configuration
        num_sites = num_partitions / PARTITIONS_PER_SITE
        num_nodes = num_sites / SITES_PER_NODE
        partition_id = 0
        nodes_added = 0
        site_id = 0
        node_idx = 0
        
        cluster_file = "/tmp/hstore-%dp.cluster" % num_partitions
        with open(cluster_file, "w") as fd:
            while nodes_added < num_nodes:
                node_id = SITE_ALL_NODES[node_idx]
                if not node_id in NODES_TO_SKIP:
                    host = NODE_FORMAT % node_id 
                    for i in range(0, SITES_PER_NODE):
                        for j in range(0, PARTITIONS_PER_SITE):
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
        
        base_opts = {
            "project":      BENCHMARK,
            "cluster":      cluster_file,
        }
        base_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, base_opts[x]), base_opts.keys()))
        cmd = "ant hstore-jar " + base_opts_cmd
        logging.debug(cmd)
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, cmd + "\n" + output
        logging.info("Initialized %s project jar [hosts=%d, sites=%d, partitions=%d]" % (BENCHMARK.upper(), num_nodes, num_sites, num_partitions))
    
        hstore_opts = {
            "coordinator.host":             NODE_FORMAT % COORDINATOR_NODE,
            "coordinator.delay":            10,
            "client.warmup":                20000,
            "client.host":                  ",".join(map(lambda x: NODE_FORMAT % x, range(CLIENT_NODES_START, CLIENT_NODES_START+CLIENT_COUNT))),
            "client.count":                 CLIENT_COUNT,
            "client.processesperclient":    CLIENT_PER_NODE,
            "client.txnrate":               10000,
            "client.blocking":              True,
            "client.scalefactor":           OPT_SCALE_FACTOR,
        }
        benchmark_opts = {
            "benchmark.neworder_only":      True,
            "benchmark.neworder_abort":     True,
            "benchmark.neworder_multip":    True,
            "benchmark.warehouses":         num_partitions,
            "benchmark.loadthreads":        OPT_LOAD_THREADS,
        }
        hstore_opts = dict(hstore_opts.items() + TRIAL_PARAMETERS[OPT_TRIAL].items())
        hstore_opts_cmd = " ".join(map(lambda x: "-Dhstore.%s=%s" % (x, hstore_opts[x]), hstore_opts.keys()))
        benchmark_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, benchmark_opts[x]), benchmark_opts.keys()))
        ant_opts_cmd = " ".join([base_opts_cmd, hstore_opts_cmd, benchmark_opts_cmd])
        
        print "TRIAL #%d - PARTITIONS %d" % (OPT_TRIAL, num_partitions)
        for trial in range(0, NUM_TRIALS_PER_EXP):
            cmd = "ant hstore-benchmark " + ant_opts_cmd + " | tee client.log"
            if trial == 0: logging.debug(cmd)
            #sys.exit(1)
            (result, output) = commands.getstatusoutput(cmd)
            assert result == 0, cmd + "\n" + output
            
            ## Get the throughput rate from the output
            match = re.search("Transactions per second: ([\d]+\.[\d]+)", output)
            assert match, "Failed to get throughput:\n" + output
            print "  Trial #%d: %s" % (trial, match.group(1))
        ## FOR (TRIAL)
        print
    ## FOR (PARTITIONS)
    
## MAIN