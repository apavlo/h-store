#!/usr/bin/env python
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
import re
import math
import time
import subprocess
import threading
import logging
import traceback
import paramiko
import socket
import string 
from StringIO import StringIO
from pprint import pformat

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../../third_party/python")))
from fabric.api import *
from fabric.contrib.files import *

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ----------------------------------------------
## benchmark
## ----------------------------------------------
@task
def benchmark():
    """Execute all of the builtin H-Store benchmarks on a cluster that is already running"""
    __getInstances__()
    client_inst = __getRunningClientInstances__()[0]
    with settings(host_string=client_inst.public_dns_name):
        for project in ['tpcc', 'tm1', 'airline', 'auctionmark']:
            exec_benchmark(project=project)
    ## WITH
## DEF

## ----------------------------------------------
## start_cluster
## ----------------------------------------------
@task
def start_cluster(updateSync=True):
    """Deploy a new H-Store cluster on EC2 using the given configuration"""
    # FIXME
## DEF

## ----------------------------------------------
## update_env
## ----------------------------------------------
@task
def update_env():
    """Update the environment on all of the running instances"""
    LOG.info("Updating environment on all running instances")
    __getInstances__()
    for inst in env["ec2.running_instances"]:
        with settings(host_string=inst.public_dns_name):
            setup_env()
        ## WITH
    ## FOR
## DEF

## ----------------------------------------------
## setup_env
## ----------------------------------------------
@task
def setup_env():
    """Configure the execution environmnt for the current instance."""

    return (first_setup)
## DEF

## ----------------------------------------------
## deploy_hstore
## ----------------------------------------------
@task
def deploy_hstore(build=True, update=True):
    # FIXME
## DEF

## ----------------------------------------------
## get_version
## ----------------------------------------------
@task
def get_version():
    """Get the current Git commit id and date in the deployment directory"""
    # FIXME

## DEF

## ----------------------------------------------
## get_running
## ----------------------------------------------
@task
def get_running(exitIfRunning=False):
    """Print a list of all of the running instances"""
    __getInstances__()
    
    if len(env["ec2.running_instances"]) == 0:
        return
        
    print "Found %d Running Instances:" % len(env["ec2.running_instances"])
    i = 0
    for inst in env["ec2.running_instances"]:
        print "  [%02d] %s (%s)" % (i, inst.public_dns_name, __getInstanceType__(inst))
        i += 1
    if exitIfRunning: sys.exit(1)
## DEF

## ----------------------------------------------
## exec_benchmark
## ----------------------------------------------
@task
def exec_benchmark(project="tpcc", removals=[ ], json=False, trace=False, updateJar=True, updateConf=True, updateRepo=False, resetLog4j=False, extraParams={ }):
    # FIXME
## DEF

## ----------------------------------------------
## write_conf
## ----------------------------------------------
@task
def write_conf(project, removals=[ ], revertFirst=False):
    # FIXME
## DEF

## ----------------------------------------------
## get_file
## ----------------------------------------------
@task
def get_file(filePath):
    # FIXME
## DEF

## ----------------------------------------------
## update_conf
## ----------------------------------------------
@task
def update_conf(conf_file, updates={ }, removals=[ ], noSpaces=False):
    # FIXME
## DEF

## ----------------------------------------------
## enable_debugging
## ----------------------------------------------
@task
def enable_debugging(debug=[], trace=[]):
    # FIXME
## DEF

## ----------------------------------------------
## stop
## ----------------------------------------------
@task
def stop_cluster(terminate=False):
    """Stop all instances in the cluster"""
    # FIXME
## DEF

## ----------------------------------------------
## clear_logs
## ----------------------------------------------
@task
def clear_logs():
    """Remove all of the log files on the remote cluster"""
    # FIXME
## DEF

## ----------------------------------------------
## sync_time
## ----------------------------------------------
@task
def sync_time():
    """Invoke NTP synchronization on each instance"""
    __getInstances__()
    for inst in env["ec2.running_instances"]:
        with settings(host_string=inst.public_dns_name):
            __syncTime__()
    ## FOR
## DEF

