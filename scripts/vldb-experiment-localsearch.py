#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import glob
import re
import commands
import tempfile
from pprint import pprint 

EXPERIMENTS = [
    "LOCAL_TIME",
    "BACK_TRACKS",
]

BENCHMARKS = [
    "tm1",
    "tpcc.100w",   
    "tpcc.50w",
    "auctionmark",
    "auctionmark.temporal",
    "tpce",
]

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Resubmit missing jobs
        "resubmit",
        ## Always calculate costs
        "force-calculation",
        ## Search time to use for LNS/Greedy
        "search-time=",
        ## Calculate Lower Bounds
        "lower-bounds",
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
    logging.debug("Completed processing input arguments")

    ## Process each benchmark
    for benchmark in BENCHMARKS:
        benchmark_type = benchmark
        if benchmark_type.find(".") != -1:
            benchmark_type = benchmark_type.split(".")[0]

lb_opts = {
    "jar":          project_jar,
    "type":         project,
    "workload":     "files/workloads/%s.trace.gz" % (workload),
    "stats":        "files/workloads/%s.stats.gz" % (project),
    "correlations": "files/correlations/%s.correlations" % (project),
    "hints":        "files/hints/%s.hints" % (project),
    "multiplier":   50,
    "limit":        5000,
    "offset":       0,
    "intervals":    12,
    "include":      procinclude,
    "costmodel":    "edu.brown.costmodel.TimeIntervalCostModel"
}
lb_cmd = "ant designer-lowerbounds " + \
         " ".join(map(lambda x: "-D%s=%s" % (x, lb_opts[x]), lb_opts.keys()))

condor_cmd = "cat files/condor/base.condor " + \
             "files/condor/%s.condor " % (workload) + \
             "%s " + \
             "| condor_submit "

pplan_regex = re.compile(re.escape(workload) + "\.([\d]+)\.out")
lb_regex = re.compile("Actual Cost:[\s]+([\d\.]+)", re.IGNORECASE)

for test_type in [ "LocalSearchTime", "BackTracks", ]:
    completed = { }
    missing = [ ]
    
    dir = "files/condor/output/%s/%s/" % (test_type, project)
    for file in glob.glob(dir + "*.out"):
        match = pplan_regex.search(file)
        if match:
            idx = int(match.group(1))
            pplan = os.path.join(dir, "%s.%d.pplan" % (workload, idx))
            
            ## Success
            if os.path.exists(pplan):
                completed[idx] = pplan
            ## Failed
            else:
                missing.append(idx)
            ## IF
        ## IF
    ## FOR
    #pprint(sorted(completed))
    #pprint(missing)
    
    print test_type
    print "Completed: %d" % len(completed)
    print "Missing:   %d" % len(missing)
    
    ## Re-submit missing ones
    if missing:
        param_type = "BACK_TRACKS" if test_type == "BackTracks" else "LOCAL_TIME"
        condor_type = test_type.lower() if test_type == "BackTracks" else "time"
        job_file = "files/condor/jobs.localsearch.%s.condor" % (condor_type)
        job_contents = [ ]
        
        fd = open(job_file, "r")
        lines = fd.readlines()
        num_lines = len(lines)
        for i in range(num_lines):
            if not lines[i].startswith("Queue") and \
               (i + 1) < num_lines and not lines[i + 1].startswith("Queue"):
                job_contents.append(lines[i])
        ## FOR
        fd.close()
        
        (fd, fd_path) = tempfile.mkstemp(suffix = "." + test_type)
        fd = os.fdopen(fd, "w")
        fd.write("".join(job_contents))
        fd.write("\n".join(map(lambda x: "HSTORE_%s = %d\nQueue" % (param_type, x), missing)))
        fd.close()
        
        cmd = condor_cmd % fd_path
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, output
        print output
        
        os.remove(fd_path)
        
    ## Otherwise, calculate the totals
    elif len(missing) == 0:
        for idx in sorted(completed.keys()):
            pplan = completed[idx]
            cmd = lb_cmd + " -Dpartitionplan=%s" % pplan
                
            (result, output) = commands.getstatusoutput(cmd)
            assert result == 0, output
            
            match = lb_regex.search(output)
            assert match, output
            cost = float(match.group(1))
            print "%-25s%s" % (os.path.basename(pplan), cost)
        ## FOR
    ## IF (missing)
    
    print "-"*50
## FOR