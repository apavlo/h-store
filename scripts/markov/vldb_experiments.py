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
    #"auctionmark.temporal",
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
PARAMETER_TUNING_STEPS = {
    "LocalTime":    60,
    "BackTracks":   10,
}
PARAMETER_TUNING_EXPERIMENTS = {
    "LocalTime":    [ ((x+0)*PARAMETER_TUNING_STEPS["LocalTime"]) for x in range(PARAMETER_TUNING_TRIALS) ],
    "BackTracks":   [ ((x+0)*PARAMETER_TUNING_STEPS["BackTracks"]) for x in range(PARAMETER_TUNING_TRIALS) ],
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
OPT_DRY_RUN = False
OPT_FORCE_CALCULATION = False
OPT_SEARCH_TIME = 57600
OPT_LOWER_BOUNDS = False
OPT_PARAMETER_TUNING = True
OPT_LOCALTIME_LAST = 14400
OPT_BACKTRACKS_LAST = 14400
OPT_SHOW_COMMANDS = False
OPT_WORKLOAD_LIMIT = 10000

OPT_VLDB_DIR = "files/designs/vldb-nov2010"
OPT_CONDOR_DIR = "files/condor"
OPT_OUTPUT_DIR = "/home/pavlo/Documents/H-Store/Papers/partitioning/graphs"

## ==============================================
## readCostsFile
## ==============================================
def readCostsFile(file, force = False, scale = None):
    logging.debug("Reading costs from '%s'" % file)
    costs = [ ]
    regex = re.compile("[\s\t]+")
    with open(file, "r") as fd:
        ## Read costs until we find our date tag. We then need to check
        ## whether the next timestamp is less than our last one, meaning that
        ## they loaded a checkpoint and we can keep going
        checkpoint = None
        for line in reversed(fd.readlines()):
            if line.startswith("-- "):
                checkpoint = line[3:].strip()
                continue
            try:
                data = regex.split(line)
                timestamp = int(data[0])/1000
                cost = float(data[1])
            except:
                logging.error("Failed to split: " + line)
                raise
            
            last_timestamp = costs[0][0] if len(costs) > 0 else None
            last_cost = costs[0][1] if len(costs) > 0 else None
            
            ## Checkpoint
            if checkpoint != None and last_cost != None:
                logging.debug("Checkpoint: %s" % checkpoint)
                
                ## If the cost goes back up, then we know that it wasn't a checkpoint
                ## but a complete restart. We'll bail here...
                if cost > last_cost and not force:
                    logging.debug("Hit restart '%s'. Not reading rest of costs file" % checkpoint)
                    break
                ## Otherwise we need to go through all of the times we've seen thus far
                ## and add back the offset from this current timestamp
                logging.debug("Offsetting previous timestamps by %d" % timestamp)
                for i in range(len(costs)):
                    costs[i] = (costs[i][0]+timestamp, costs[i][1])
                last_timestamp = costs[0][0]
                checkpoint = None
            ## IF
            
            ## Fix costs
            if force and last_cost != None and costs[0][1] > cost:
                cost = costs[0][1]
            
            ## If the current cost is the same as the last one, replace it so that 
            ## we have the time come sooner. This happens after restarts
            if last_timestamp != timestamp and last_cost == cost:
                costs[0] = (timestamp, cost)
            else:
                costs.insert(0, (timestamp, cost))
        ## Read 
    ## WITH
    
    if scale:
        start = costs[0][0]
        stop = costs[-1][0]
        diff = stop - start
        
        for i in range(len(costs)):
            costs[i] = ( int(((costs[i][0]-start) / float(diff)) * scale), costs[i][1] )
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
        ## Show the commands that we would execute for resubmitting, but don't
        ## actually resubmit things
        "dry-run",
        ## Always calculate costs
        "force-calculation",
        ## Search time to use for LNS/Greedy
        "search-time=",
        ## Calculate Lower Bounds
        "lower-bounds",
        ## Enable parameter tuning experiments
        "parameter-tuning",
        ## Cut-offs for localsearch parameter data (in seconds)
        "localtime-last=",
        "backtracks-last=",
        ## Number of transactions to use when calculating cost
        "workload-limit=",
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
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    if "benchmark" in options: BENCHMARKS = options["benchmark"]
    if "experiment" in options: 
        for exp_type in EXPERIMENTS.keys():
            if not exp_type in options["experiment"]:
                del EXPERIMENTS[exp_type]
    ## IF
    if OPT_DRY_RUN:
        OPT_RESUBMIT = True
        OPT_SHOW_COMMANDS = True
        logging.info("Enabled dry-run option")
    
    logging.debug("Completed processing input arguments")


    ## Process each benchmark
    for benchmark in BENCHMARKS:
        benchmark_type = benchmark
        if benchmark_type.find(".") != -1:
            benchmark_type = benchmark_type.split(".")[0]

        condor_files = [ os.path.join(OPT_CONDOR_DIR, "base.condor"), os.path.join(OPT_CONDOR_DIR, benchmark + ".condor"), "%s" ]
        condor_cmd = "cat " + " ".join(condor_files) + " | condor_submit"

        ant_opts = {
            "jar":          "%s-designer-benchmark.jar" % benchmark_type,
            "project":      benchmark_type,
            "workload":     "files/workloads/%s.trace.gz" % (benchmark),
            "stats":        "files/workloads/%s.stats.gz" % (benchmark_type),
            "correlations": "files/correlations/%s.correlations" % (benchmark_type),
            "hints":        "files/hints/%s.hints" % (benchmark_type),
            "multiplier":   100,
            "limit":        OPT_WORKLOAD_LIMIT,
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

        ## ----------------------------------------------
        ## EXPERIMENTS
        ## ----------------------------------------------
        for exp_type in EXPERIMENTS:
            ## Skip parameter tuning experiments if we don't need it
            tuning_exp = exp_type in PARAMETER_TUNING_EXPERIMENTS
            logging.debug("%s - %s [tuning=%s]" % (benchmark, exp_type, tuning_exp))
            if tuning_exp and not benchmark in PARAMETER_TUNING_BENCHMARKS:
                continue
            
            exp_param = EXPERIMENTS[exp_type]
            exp_dir = os.path.join(OPT_CONDOR_DIR, "output", exp_type, benchmark_type)
            checkpoints_dir = os.path.join(os.getcwd(), "checkpoints", exp_type)
            base_file = os.path.join(exp_dir, benchmark)
            #if tuning_exp: base_file += "." + exp_type

            ## How nice of us!
            if not os.path.exists(exp_dir): os.makedirs(exp_dir)
            if not os.path.exists(checkpoints_dir): os.makedirs(checkpoints_dir)

            ## ----------------------------------------------
            ## Tuning Experiments
            ## ----------------------------------------------
            if tuning_exp:
                trials = PARAMETER_TUNING_EXPERIMENTS[exp_type]
                logging.debug("%s Number of Trials: %d" % (exp_type, len(trials)))
                missing_trials = [ ]
                for trial in trials:
                    pplan = base_file + ".%d.pplan" % trial
                    error = base_file + ".%d.err" % trial
                    logfile = base_file + ".%d.log" % trial
                    costsfile = base_file + ".%d.costs" % trial
                    missing.append(pplan)
                    
                    ## Only resubmit if there is an error file and the size is not zero
                    if not os.path.exists(pplan):
                        if not os.path.exists(logfile) or (os.path.exists(error) and os.path.getsize(error) > 0):
                            missing_trials.append(trial)
                    else:
                        assert os.path.exists(costsfile), costsfile
                        completed[trial] = pplan
                    ## IF
                ## FOR
                logging.debug("%s Number of Missing Trials: %d" % (exp_type, len(missing_trials)))

                ## Resubmit to Condor
                if OPT_RESUBMIT and len(missing_trials) > 0:
                    exp_file = os.path.join(OPT_CONDOR_DIR, "jobs.%s.condor" % EXPERIMENTS[exp_type])
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
                    if not OPT_DRY_RUN:
                        (result, output) = commands.getstatusoutput(cmd)
                        assert result == 0, output
                        print output
                    ## DRY RUN
                    
                    os.remove(fd_path)
                    
                ## Calculate data files
                elif len(missing_trials) == 0:
                    ## Generate the data files we will need for our graphs
                    ## We can use a cutoff value if we don't just want to take the last
                    varname = "OPT_%s_LAST" % exp_param.upper()
                    cutoff = globals()[varname]
                    logging.debug("%s = %s" % (varname, str(cutoff)))
                    
                    datafile = os.path.join(OPT_OUTPUT_DIR, "localsearchparams-%s.dat" % (exp_param))
                    logging.debug("Writing out graph data to '%s'" % datafile)
                    with open(datafile, 'w') as fd:
                        best_cost = None
                        worst_cost = None
                        all_costs = [ ]
                        for trial in sorted(trials):
                            trial_costs = [ ]
                            
                            ## HACK: Get multiple runs and take the average
                            for run in [ "", "-1", "-2" ]:
                                run_exp_dir = exp_dir + run
                                if not os.path.exists(run_exp_dir):
                                    logging.debug("Missing trial run '%d%s'" % (trial, run))
                                    continue
                                run_base_file = os.path.join(run_exp_dir, benchmark)
                            
                                costsfile = run_base_file + ".%d.costs" % trial
                                if not os.path.exists(costsfile):
                                    logging.warn("Cost file '%s' does not exist" % costsfile)
                                    continue
                                costs = readCostsFile(costsfile)
                                last = None
                                if cutoff == None:
                                    last = costs[-1]
                                else:
                                    for t in costs:
                                        if t[0] >= cutoff:
                                            last = t
                                            break
                                    ## FOR
                                    if last == None: 
                                        logging.debug("Failed to find cost for trial %d after %s. Taking last cost" % (trial, cutoff))
                                        last = costs[-1]
                                ## IF
                                trial_costs.append(last[1])
                            ## FOR (TRIAL RUN)
                            if trial_costs:
                                avg_cost = sum(trial_costs) / float(len(trial_costs))
                                if best_cost == None or avg_cost < best_cost:
                                    best_cost = avg_cost
                                if worst_cost == None or avg_cost > worst_cost:
                                    worst_cost = avg_cost
                                all_costs.append((trial, avg_cost, trial_costs))
                            ## IF
                        ## FOR
                        assert best_cost != None
                        assert worst_cost != None
                        
                        ## Repeat the last for nice looking graphs
                        all_costs.append((all_costs[-1][0] + PARAMETER_TUNING_STEPS[exp_type], \
                                          all_costs[-1][1], all_costs[-1][2]))
                        
                        diff = worst_cost - best_cost
                        for trial, avg_cost, trial_costs in all_costs:
                            ## If this is the localtime experiment, convert the trial seconds into minutes
                            if exp_type == "LocalTime": trial /= float(60)
                            
                            ratio = (worst_cost - avg_cost) / float(diff)
                            fd.write("%d\t%f\t%f\t%s\n" % (trial, ratio, avg_cost, "\t".join(map(str, trial_costs))))
                        ## FOR
                        
                        
                            
                            
                            # fd.write("%d\t%f\n" % (trial, avg_cost))
                        ## (TRIAL)
                    ## WITH
                    logging.debug("Wrote graph data to '%s'" % datafile)
                ## IF
                
            ## ----------------------------------------------
            ## Cost Experiments
            ## ----------------------------------------------
            else:
                if exp_param in ["greedy", "lns"]: base_file += "." + str(OPT_SEARCH_TIME)
                pplan = base_file + ".pplan"
                error = base_file + ".err"
                logfile = base_file + ".log"
                
                ## Need to resubmit
                logging.debug("Checking for '%s'" % pplan)
                if not os.path.exists(pplan):
                    missing.append(pplan)
                    
                    ## Only resubmit if there is an error file and the size is not zero
                    if OPT_RESUBMIT and (not os.path.exists(logfile) or (os.path.exists(error) and os.path.getsize(error) > 0)):
                        logging.debug("Resubmitting %s-%s job" % (exp_type, benchmark))
                        
                        exp_file = os.path.join(OPT_CONDOR_DIR, "jobs.%s.condor" % exp_param)
                        cmd = condor_cmd % exp_file
                        if OPT_SHOW_COMMANDS: logging.info(cmd)
                        if not OPT_DRY_RUN:
                            (result, output) = commands.getstatusoutput(cmd)
                            assert result == 0, output
                            print output
                        ## DRY RUN
                else:
                    completed[exp_type] = pplan
                ## IF
        ## FOR
        
        ## ----------------------------------------------
        ## COST TABLE
        ## ----------------------------------------------
        if len(completed) > 0 and (OPT_FORCE_CALCULATION or len(missing) == 0):
            ## Calculate the cost for our completed experiments
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
                    vldb_file = os.path.join(OPT_VLDB_DIR, "%s.%s.pplan" % (benchmark, EXPERIMENTS[exp_type]))
                    shutil.copyfile(pplan, vldb_file)
                    
                    ## Calculate lowerbounds and write out intermediate costs
                    if OPT_LOWER_BOUNDS and exp_type == "LNS":
                        logging.debug("Calculating lower bounds for %s-%s" % (exp_type, benchmark))
                        costs_file = pplan.replace("pplan", "costs")
                        assert os.path.exists(costs_file), "Missing " + costs_file
                        scale = 28800
                        cost_intervals = readCostsFile(costs_file, force = True, scale = scale)
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
                        if benchmark == "auctionmark":
                            lb_costs["Lower Bounds"] = 0.44
                        lb_costs["Upper Bounds"] = cost_intervals[0][1] - lb_costs["Lower Bounds"]

                        vldb_costs_file = os.path.join(OPT_OUTPUT_DIR, "lowerbounds-%s.dat" % (benchmark))
                        with open(vldb_costs_file, "w") as fd:
                            for time, cost in cost_intervals:
                                time = (time / 60.0)
                                cost = (cost - lb_costs["Lower Bounds"]) / lb_costs["Upper Bounds"]
                                line = "\t".join(map(str, [time, cost, 1-cost]))
                                fd.write(line + "\n")
                            ## FOR
                        ## WITH
                        logging.debug("Wrote lowerbounds cost to '%s'" % vldb_costs_file)
                ## IF
                
                logging.debug("Printing %s costs for %s" % (benchmark, exp_type))
                print "".join(map(lambda x: "%-20s" % x, [exp_type] + [costs[param] for param in COST_PARAMS]))
            ## FOR
            
            print
            print "-"*150
            print
        ## IF
    ## FOR
## MAIN