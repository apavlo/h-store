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
from pprint import pprint

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

BENCHMARKS = [
    "tm1",
    "tpcc.100w",   
    "tpcc.50w",
    "auctionmark",
    "auctionmark.temporal",
    "tpce",
]

EXPERIMENTS = {
    "LNS":          "lns",
    "PrimaryKey":   "pkey",
    "GreedySearch": "greedy",
    "MostPopular":  "popular",
    
    # Parameter Tuning Experiments
    "LocalTime":    "localtime",
    "BackTracks":   "backtracks",
}
PARAMETER_TUNING_TRIALS = 50
PARAMETER_TUNING_EXPERIMENTS = {
    "LocalTime":    [ ((x+1)*50) for x in range(PARAMETER_TUNING_TRIALS) ],
    "BackTracks":   [ ((x+1)*30) for x in range(PARAMETER_TUNING_TRIALS) ],
}
PARAMETER_TUNING_BENCHMARKS = [
    "tpce",
    "tpcc.100w",
]

COST_PARAMS = [
    "IDLE PARTITIONS",
    "JAVA SKEW",
    "TRANSACTION SKEW",
    "SINGLE-PARTITION",
    "MULTI-PARTITION",
    "TOTAL",
]
COST_REGEX = re.compile("(%s):[\s]+([\d\.]+)" % "|".join(COST_PARAMS), re.IGNORECASE)

LB_PARAMS = [
    "Lower Bounds",
]
LB_REGEX = re.compile("(%s):[\s]+([\d\.]+)" % "|".join(LB_PARAMS), re.IGNORECASE)

OPT_RESUBMIT = False
OPT_FORCE_CALCULATION = False
OPT_SEARCH_TIME = 57600
OPT_LOWER_BOUNDS = True
OPT_PARAMETER_TUNING = True
OPT_SHOW_COMMANDS = False

VLDB_DIR = "files/designs/vldb-nov2010"
CONDOR_DIR = "files/condor"

## ==============================================
## readCostsFile
## ==============================================
def readCostsFile(file):
    logging.debug("Reading costs from '%s'" % file)
    costs = [ ]
    with open(file, "r") as fd:
        ## Read costs until we find our date tag
        for line in reversed(fd.readlines()):
            if line.startswith("-- "):
                break
            data = line.split("\t")
            costs.insert(0, (int(data[0]), float(data[1])))
        ## Read 
    ## WITH
    return costs
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## The benchmarks that we want to look at
        "benchmark=",
        ## The experiments we want to look at
        "experiment=",
        ## Resubmit missing jobs
        "resubmit",
        ## Always calculate costs
        "force-calculation",
        ## Search time to use for LNS/Greedy
        "search-time=",
        ## Calculate Lower Bounds
        "lower-bounds",
        ## Enable parameter tuning experiments
        "parameter-tuning",
        ## Show Condor/Ant commands
        "show-commands",
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

    ## Global Options
    for key in options:
        varname = "OPT_" + key.replace("-", "_").upper()
        if varname in globals() and len(options[key]) == 1:
            orig_type = type(globals()[varname])
            globals()[varname] = orig_type(True if type(globals()[varname]) == bool else options[key][0])
            
    ## FOR
    if "benchmark" in options: BENCHMARKS = options["benchmark"]
    if "experiment" in options: 
        for exp_type in EXPERIMENTS.keys():
            if not exp_type in options["experiment"]:
                del EXPERIMENTS[exp_type]
    ## IF
    
    logging.debug("Completed processing input arguments")


    ## Process each benchmark
    for benchmark in BENCHMARKS:
        benchmark_type = benchmark
        if benchmark_type.find(".") != -1:
            benchmark_type = benchmark_type.split(".")[0]

        condor_files = [ os.path.join(CONDOR_DIR, "base.condor"), os.path.join(CONDOR_DIR, benchmark + ".condor"), "%s" ]
        condor_cmd = "cat " + " ".join(condor_files) + " | condor_submit"

        ant_opts = {
            "jar":          "%s-designer-benchmark.jar" % benchmark_type,
            "project":      benchmark_type,
            "workload":     "files/workloads/%s.trace.gz" % (benchmark),
            "stats":        "files/workloads/%s.stats.gz" % (benchmark_type),
            "correlations": "files/correlations/%s.correlations" % (benchmark_type),
            "hints":        "files/hints/%s.hints" % (benchmark_type),
            "multiplier":   100,
            "limit":        10000,
            "offset":       5000,
            "intervals":    12,
            "include":      "default",
            "costmodel":    "edu.brown.costmodel.TimeIntervalCostModel",
            "volt.client.memory": 3000,
        }
        ant_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, ant_opts[x]), ant_opts.keys()))
        
        cost_cmd = "ant designer-estimate " + ant_opts_cmd
        lb_cmd = "ant designer-lowerbounds " + ant_opts_cmd

        completed = { }
        missing = [ ]

        for exp_type in EXPERIMENTS:
            ## Skip parameter tuning experiments if we don't need it
            tuning_exp = exp_type in PARAMETER_TUNING_EXPERIMENTS
            logging.debug("%s - %s [tuning=%s]" % (benchmark, exp_type, tuning_exp))
            if tuning_exp and not benchmark in PARAMETER_TUNING_BENCHMARKS:
                continue
            
            exp_param = EXPERIMENTS[exp_type]
            exp_dir = os.path.join(CONDOR_DIR, "output", exp_type, benchmark_type)
            base_file = os.path.join(exp_dir, benchmark)

            ## How nice of us!
            if not os.path.exists(exp_dir): os.makedirs(exp_dir)

            ## Tuning Experiments
            if tuning_exp:
                trials = PARAMETER_TUNING_EXPERIMENTS[exp_type]
                logging.debug("%s Number of Trials: %d" % (exp_type, len(trials)))
                missing_trials = [ ]
                for trial in trials:
                    pplan = base_file + ".%d.pplan" % trial
                    error = base_file + ".%d.err" % trial
                    logfile = base_file + ".%d.log" % trial
                    missing.append(pplan)
                    
                    ## Only resubmit if there is an error file and the size is not zero
                    if not os.path.exists(pplan) and \
                       (not os.path.exists(logfile) or
                       (os.path.exists(error) and os.path.getsize(error) > 0)):
                        missing_trials.append(trial)
                    ## IF
                ## FOR
                logging.debug("%s Number of Missing Trials: %d" % (exp_type, len(missing_trials)))

                if OPT_RESUBMIT and len(missing_trials) > 0:
                    exp_file = os.path.join(CONDOR_DIR, "jobs.%s.condor" % EXPERIMENTS[exp_type])
                    assert os.path.exists(exp_file), exp_file
                    
                    ## Get parameter key
                    temp = map(string.upper, re.split("([A-Z][a-z]+)", exp_type))
                    param_key = temp[1] + "_" + temp[3]
                    
                    (fd, fd_path) = tempfile.mkstemp(suffix = "." + exp_param)
                    fd = os.fdopen(fd, "w")
                    fd.write("\n".join(map(lambda x: "HSTORE_%s = %d\nQueue" % (param_key, x), sorted(missing_trials))))
                    fd.close()
                   
                    logging.info("Resubmitting %d %s trials" % (len(missing_trials), exp_type))
                    cmd = condor_cmd % (" ".join([exp_file, fd_path]))
                    if OPT_SHOW_COMMANDS: logging.info(cmd)
                    (result, output) = commands.getstatusoutput(cmd)
                    assert result == 0, output
                    print output
                    
                    os.remove(fd_path)
                
            ## Cost Experiments
            else:
                if exp_param in ["greedy", "lns"]: base_file += "." + str(OPT_SEARCH_TIME)
                pplan = base_file + ".pplan"
                error = base_file + ".err"
                
                ## Need to resubmit
                if not os.path.exists(pplan):
                    missing.append(pplan)
                    
                    ## Only resubmit if there is an error file and the size is not zero
                    if OPT_RESUBMIT and os.path.exists(error) and os.path.getsize(error) > 0:
                        exp_file = os.path.join(CONDOR_DIR, "jobs.%s.condor" % exp_param)
                        cmd = condor_cmd % exp_file
                        if OPT_SHOW_COMMANDS: logging.info(cmd)
                        (result, output) = commands.getstatusoutput(cmd)
                        assert result == 0, output
                        print output
                else:
                    completed[exp_type] = pplan
                ## IF
        ## FOR
        
        ## Calculate the cost for our completed experiments
        if OPT_FORCE_CALCULATION or len(missing) == 0:
            print "".join(map(lambda x: "%-20s" % x, [benchmark] + COST_PARAMS))
            for exp_type in EXPERIMENTS.keys():
                if exp_type in PARAMETER_TUNING_EXPERIMENTS: continue
                costs = { }

                ## Missing
                if not exp_type in completed:
                    for param in COST_PARAMS:
                        costs[param] = "-"

                ## Calculate cost!
                else:
                    pplan = completed[exp_type]
                    cmd = cost_cmd + " -Dpartitionplan=%s" % pplan
                    if OPT_SHOW_COMMANDS: logging.info(cmd)
                    (result, output) = commands.getstatusoutput(cmd)
                    assert result == 0, output

                    for match in COST_REGEX.finditer(output):
                        param = match.group(1)
                        value = match.group(2).strip()  
                        if len(value) > 18: value = value[:18]
                        costs[param] = value
                    ## FOR  
                
                    ## Copy to VLDB directory for posterity
                    vldb_file = os.path.join(VLDB_DIR, "%s.%s.pplan" % (benchmark, EXPERIMENTS[exp_type]))
                    shutil.copyfile(pplan, vldb_file)
                    
                    ## Calculate lowerbounds and write out intermediate costs
                    if OPT_LOWER_BOUNDS and exp_type == "LNS":
                        costs_file = pplan.replace("pplan", "costs")
                        assert os.path.exists(costs_file), "Missing " + costs_file
                        cost_intervals = readCostsFile(costs_file)
                        assert cost_intervals, "Failed to get data from " + costs_file
                        
                        ## Always repeat the last cost so that the graphs look nice...
                        cost_intervals.append(cost_intervals[-1])

                        ## Calculate lower bounds
                        cmd = lb_cmd + " -Dpartitionplan=%s" % pplan
                        if OPT_SHOW_COMMANDS: logging.info(cmd)
                        (result, output) = commands.getstatusoutput(cmd)
                        assert result == 0, cmd + "\n" + output

                        lb_costs = { }
                        for match in LB_REGEX.finditer(output):
                            param = match.group(1)
                            value = match.group(2).strip()  
                            lb_costs[param] = float(value)
                        ## FOR
                        lb_costs["Upper Bounds"] = cost_intervals[0][1] - lb_costs["Lower Bounds"]

                        vldb_costs = os.path.join(VLDB_DIR, "%s.%s.costs" % (benchmark, EXPERIMENTS[exp_type]))
                        with open(vldb_costs, "w") as fd:
                            for time, cost in cost_intervals:
                                time = (time / 1000.0) / 60.0
                                cost = (cost - lb_costs["Lower Bounds"]) / lb_costs["Upper Bounds"]
                                line = "\t".join(map(str, [time, cost, 1-cost]))
                                fd.write(line + "\n")
                            ## FOR
                        ## WITH
                ## IF
                
                print "".join(map(lambda x: "%-20s" % x, [exp_type] + [costs[param] for param in COST_PARAMS]))
            ## FOR
            
            print
            print "-"*150
            print
        ## IF
    ## FOR
## MAIN