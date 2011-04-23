#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import commands
import getopt
#from pprint import pprint

PS_REGEX = re.compile("([\d]+)[\s]+(.*)")

OPT_DRYRUN      = False
OPT_HSTORESITE  = False
OPT_PROTOENGINE = False
OPT_PROTOCOORD  = False
OPT_CLIENT      = False

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
            globals()[varname] = orig_type(True if type(globals()[varname]) == bool else options[key][0])
    ## FOR

    ## SiteId
    siteid = int(options["siteid"][0]) if "siteid" in options else None

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
           cmd.find("HStoreSite") != -1 and \
           cmd.find("java")       != -1 and \
           (siteid == None or (siteid != None and cmd.find("node.siteid=%d" + siteid) != -1)):
            to_kill.add(pid)
        ## IF
        
        ## Kill protodtxnengines
        if OPT_PROTOENGINE and \
           cmd.find("protodtxnengine") != -1 and \
           cmd.find("hstore.conf")     != -1:
            to_kill.add(pid)
        ## IF
        
        ## Kill protodtxncoord
        if OPT_PROTOCOORD and \
           cmd.find("protodtxncoordinator") != -1 and \
           cmd.find("hstore.conf")     != -1:
            to_kill.add(pid)
        ## IF
        
        ## Kill client stuff
        if OPT_CLIENT and \
            (cmd.find("BLOCKING=true") != -1 or cmd.find("BLOCKING=false") != -1) and \
            cmd.find(".benchmark.") != -1:
            to_kill.add(pid)
    ## FOR
    
    ## Kill, pussy cat! Kill!
    for pid in to_kill:
        cmd = "kill -9 %d" % pid
        if OPT_DRYRUN:
            print cmd
        else:
            commands.getstatusoutput(cmd)
    ##
## IF