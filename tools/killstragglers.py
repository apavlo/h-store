#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import commands
import logging
import getopt
#from pprint import pprint

LOG_FORMAT = "%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s"
LOG_DATEFMT = "%m-%d-%Y %H:%M:%S"
logging.basicConfig(level = logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATEFMT, stream = sys.stdout)

PS_REGEX = re.compile("([\d]+)[\s]+(.*)")

OPT_DRYRUN      = False
OPT_HSTORESITE  = False
OPT_PROTOENGINE = False
OPT_PROTOCOORD  = False
OPT_CLIENT      = False
OPT_LOGDIR      = "/tmp/hstore/logs"

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        "dryrun",
        "siteid=",
        "hstoresite",
        "protoengine",
        "protocoord",
        "client",
        "debug=",
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
    ## Global Options
    for key in options:
        varname = "OPT_" + key.replace("-", "_").upper()
        if varname in globals() and len(options[key]) == 1:
            orig_type = type(globals()[varname])
            if type(globals()[varname]) == bool:
                globals()[varname] = orig_type(True)
            else:
                globals()[varname] = orig_type(options[key][0])
    ## FOR

    ## SiteId
    siteid = None
    if "siteid" in options: siteid = int(options["siteid"][0])
    
    ## Debug Output
    if "debug" in options:
        logDir = options["debug"][0]
        l = logging.getLogger()
        l.setLevel(logging.DEBUG)
        output = os.path.realpath(os.path.join(logDir, "killstragglers.log"))
        logging.debug("Writing KillStragglers log to '%s'" % output)
        
        if not os.path.exists(logDir): os.makedirs(logDir)
        handler = logging.FileHandler(output)
        handler.setFormatter(l.handlers[0].formatter)
        l.addHandler(handler)

        temp = {
            "HStoreSite":       OPT_HSTORESITE,
            "Dtxn.Engine":      OPT_PROTOENGINE,
            "Dtxn.Coordinator": OPT_PROTOCOORD,
            "Clients":          OPT_CLIENT,
        }
        #logging.debug("Attempting to kill the following components\n" +
                      #"\n".join([ "%-18s %s" % (x+":", "KILL" if temp[x] else "SKIP") for x in sorted(temp.keys()) ]))


    ## Get the list of PIDS -> Commands
    cmd = "ps x -o pid -o command"
    (result, output) = commands.getstatusoutput(cmd)
    assert result == 0, "%s\n%s" % (cmd, output)
    pids = { }
    for match in PS_REGEX.finditer(output):
        if match.group(2).find("<defunct>") == -1:
            pids[int(match.group(1))] = match.group(2)
    ## FOR
    
    to_kill = set()
    for pid in pids.keys():
        cmd = pids[pid]
        
        ## Kill HStoreSites
        if OPT_HSTORESITE and \
           cmd.find("hstore.tag=site") != -1 and \
           cmd.find("java")            != -1 and \
           (siteid == None or (siteid != None and cmd.find("node.siteid=%d" + siteid) != -1)):
            to_kill.add(pid)
            logging.debug("Preparing to kill HStoreSite PID %d\n%s" % (pid, cmd))
        ## IF
        
        ## Kill protodtxnengines
        if OPT_PROTOENGINE and \
           cmd.find("protodtxnengine") != -1 and \
           cmd.find("hstore.tag=site") == -1 and \
           cmd.find("dtxn.conf")       != -1:
            to_kill.add(pid)
            logging.debug("Preparing to kill Dtxn.Engine PID %d\n%s" % (pid, cmd))
        ## IF
        
        ## Kill protodtxncoord
        if OPT_PROTOCOORD and \
           cmd.find("protodtxncoordinator") != -1 and \
           cmd.find("hstore.tag=site")      == -1 and \
           cmd.find("dtxn.conf")            != -1:
            to_kill.add(pid)
            logging.debug("Preparing to kill Dtxn.Coordinator PID %d\n%s" % (pid, cmd))
        ## IF
        
        ## Kill client stuff
        if OPT_CLIENT and \
           cmd.find("-Dhstore.tag=client") != -1:
            to_kill.add(pid)
            logging.debug("Preparing to kill Client PID %d\n%s" % (pid, cmd))
    ## FOR
    
    ## Kill, pussy cat! Kill!
    if len(to_kill) > 0:
        logging.debug("Killing %d PIDs!" % len(to_kill))
        for pid in to_kill:
            cmd = "kill -9 %d" % pid
            if OPT_DRYRUN:
                print cmd
            else:
                commands.getstatusoutput(cmd)
        ## FOR
    else:
        logging.debug("We didn't find any straggler processes to kill!")
## IF