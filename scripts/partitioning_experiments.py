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
import json
import logging
import getopt
import string
import math
import types
from pprint import pprint, pformat
from fabric.api import *
from fabric.network import *
from fabric.contrib.files import *

## This has all the functions we can use to invoke experiments on EC2
import fabfile

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

BASE_SETTINGS = {
    "client.blocking":                  True,
    "client.blocking_concurrent":       10,
    "client.txnrate":                   10000,
    "client.count":                     4,
    "client.processesperclient":        10,
    "client.skewfactor":                -1,
    "client.duration":                  120000,
    "client.warmup":                    60000,
    
    "site.count":                       4,
    "site.status_show_txn_info":        True,
    "site.memory":                      12288,
    
    "benchmark.neworder_only":          True,
    "benchmark.neworder_abort":         False,
    "benchmark.neworder_all_multip":    False,
}

## HACK
def motivation0UpdateEnv(env, exp_factor):
    env["benchmark.neworder_multip_mix"] = exp_factor
## DEF
def motivation1UpdateEnv(env, exp_factor):
    if exp_factor == 0:
        env["benchmark.neworder_skew_warehouse"] = False
    else:
        env["client.skewfactor"] = 1.00001 + (0.25 * (exp_factor - 10) / 10.0)
## DEF

EXPERIMENT_SETTINGS = {
    "motivation": [
        ## Trial #0 - Vary the percentage of multi-partition txns
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         True,
            "updateEnvFunc":                     motivation0UpdateEnv,
        },
        ## Trial #1 - Vary the amount of skew of warehouse ids
        {
            "benchmark.neworder_skew_warehouse": False,
            "benchmark.neworder_multip":         False,
            "updateEnvFunc":                     motivation1UpdateEnv,
        },
    ],
}

OPT_EXP_TYPE = "motivation"
OPT_EXP_TRIALS = 3
OPT_EXP_SETTINGS = 0

## ==============================================
## parseResultsOutput
## ==============================================
def parseResultsOutput(output):
    # We always need to make sure that we clear out ant's [java] prefix
    output = re.sub("[\s]+\[java\] ", "\n", output)
    
    # Find our <json> tag. The results will be inside of there
    regex = re.compile("<json>(.*?)</json>", re.MULTILINE | re.IGNORECASE | re.DOTALL)
    m = regex.search(output)
    if not m: LOG.error("Invalid output:\n" + output)
    assert m

    json_results = json.loads(m.group(1))
    return (json_results)
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
    if "debug" in options: LOG.setLevel(logging.DEBUG)

    ## Update Fabric env
    exp_opts = dict(BASE_SETTINGS.items() + EXPERIMENT_SETTINGS[OPT_EXP_TYPE][OPT_EXP_SETTINGS].items())
    assert exp_opts
    for key,val in exp_opts.items():
        if type(val) != types.FunctionType: env[key] = val
    ## FOR
    
    client_inst = fabfile.__getClientInstance__()
    all_results = [ ]
    stop = False
    for exp_factor in range(0, 100, 10):
        if "updateEnvFunc" in exp_opts: exp_opts["updateEnvFunc"](env, exp_factor)
        results = [ ]
        for trial in range(OPT_EXP_TRIALS):
            LOG.info("Executing trial #%d for factor %d" % (trial, exp_factor))
            try:
                with settings(host_string=client_inst.public_dns_name):
                    output = fabfile.exec_benchmark(project="tpcc", json=True)
                    results.append(parseResultsOutput(output))
                ## WITH
            except KeyboardInterrupt:
                stop = True
                break
            except SystemExit:
                LOG.warn("Failed to complete trial succesfully")
        ## FOR
        if results: all_results.append(results)
        if stop: break
    ## FOR
    
    try:
        disconnect_all()
    finally:
        for results in all_results:
            for r in results:
                print r["TXNPERSECOND"]
            print
        ## FOR
    ## TRY
## MAIN