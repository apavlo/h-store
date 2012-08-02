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

# Figure out where we are relative to the root of the repository
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
REPO_ROOT_DIR = os.path.realpath(os.path.join(basedir, "../.."))
        
        
## =============================================================================
## parseJSONResults
## =============================================================================
def parseJSONResults(output):
    # We always need to make sure that we clear out ant's [java] prefix
    output = re.sub("[\s]+\[java\] ", "\n", output)
    
    # Find our <json> tag. The results will be inside of there
    regex = re.compile("<json>(.*?)</json>", re.MULTILINE | re.IGNORECASE | re.DOTALL)
    m = regex.search(output)
    if not m: 
        raise Exception("Invalid JSON results output")
    
    json_results = json.loads(m.group(1))
    return (json_results)
## DEF
        
## =============================================================================
## getAllParameters
## =============================================================================
def getAllParameters():
    """Return a set of all the HStoreConf parameters"""
    global basedir
    buildCommonXml = os.path.join(REPO_ROOT_DIR, "build-common.xml")
    if not os.path.exists(buildCommonXml):
        raise Exception("Unable to load configuration parameters from '%s'" % buildCommonXml)
    
    params = set()
    regex = re.compile("<arg value=\"([\w]+\..*?)=.*?\" />", re.IGNORECASE)
    with open(buildCommonXml, "r") as fd:
        for m in regex.finditer(fd.read()):
            params.add(m.group(1))
    ## WITH
    return params
## DEF

## =============================================================================
## getAllBenchmarks
## =============================================================================
def getBenchmarks():
    """Return a list of all the benchmarks available in H-Store"""
    global basedir
    benchmarkDir = os.path.join(REPO_ROOT_DIR, "properties/benchmarks")
    if not os.path.exists(benchmarkDir):
        raise Exception("Unable to load benchmark information from '%s'" % benchmarkDir)
    
    benchmarks = [ ]
    for file in glob.glob("%s/*.properties" % benchmarkDir):
        benchmarks.append(os.path.basename(file).split(".")[0])
    # FOR
    return benchmarks
## DEF