# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------
# Copyright (C) 2013 by H-Store Project
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

from abstractfabric import AbstractFabric
from abstractinstance import AbstractInstance

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
basedir = os.path.realpath(os.path.join(basedir, "../../../"))
sys.path.append(os.path.join(basedir, "third_party/python"))
import boto

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

## ==============================================
## SSH NODE CONFIGURATION
## ==============================================

ENV_DEFAULT = {
    # This can either be a path to a file, or a list of hostnames (with duplicates allowed)
    # "ssh.hosts":                [ "host1", "host2", "host2", "host3" ],
    "ssh.hosts":                os.path.realpath(os.path.join(basedir, "hosts.txt")),
    
    # H-Store Options
    "hstore.basedir":           os.path.realpath(os.path.join(basedir, "..")),
}

## ==============================================
## SSHInstance
## ==============================================
class SSHInstance(AbstractInstance):
    
    def __init__(self, hostname):
        super(SSHInstance, self).__init__(hostname)
        self.id = hostname
        self.public_dns_name = hostname
        self.private_dns_name = hostname
    ## DEF
        
## CLASS

## ==============================================
## SSHFabric
## ==============================================
class SSHFabric(AbstractFabric):
    
    def __init__(self, env):
        super(SSHFabric, self).__init__(env, ENV_DEFAULT)
        
        # Create all of our instance handles
        hostnames = set()
        self.unique_hosts = [ ]

        allHostnames = [ ]
        # HOSTS FILE
        if type(self.env["ssh.hosts"]) is str:
            # Check to make sure it exists
            if not os.path.exists(self.env["ssh.hosts"]):
                raise Exception("The ssh.hosts file '%s' does not exist" % self.env["ssh.hosts"])
            with open(self.env["ssh.hosts"]) as fd:
                for line in fd:
                    for host in re.split(r"[\s,]+", line.strip()):
                        if not host.startswith("#"): allHostnames.append(host)
                ## FOR
            ## WITH
        # HOSTS LIST
        else:
            allHostnames = self.env["ssh.hosts"]
        
        LOG.debug("All Hostnames: %s", allHostnames)
        for hostname in allHostnames:
            inst = SSHInstance(hostname)
            self.running_instances.append(inst)
            if not hostname in hostnames:
                self.unique_hosts.append(inst)
                hostnames.add(hostname)
        # self.running_instances.sort(key=lambda inst: inst.name)
        # self.unique_hosts.sort(key=lambda inst: inst.name)
        self.all_instances = self.running_instances
    ## DEF
    
    def start_cluster(self, build=True, update=True):
        for inst in self.unique_hosts:
            self.__setupInstance__(inst, build, update)
    ## DEF
    
    def stop_cluster(self):
        pass
    ## DEF
    
    def sync_time(self):
        pass
    ## DEF
    
    def deploy_hstore(self, build=True, update=True):
        self.start_cluster(build, update)
    ## DEF
    
    def write_conf(self, project, removals=[ ], revertFirst=False):
        for inst in self.unique_hosts:
            self.__writeConf__(inst, project, removals, revertFirst)
    ## DEF
    
    def resetLog4j(self):
        for inst in self.unique_hosts:
            self.__resetDebugging__(inst)
    ## DEF
    
    def updateLog4j(self, reset=False, debug=[], trace=[]):
        if reset: self.resetLog4j()
        for inst in self.unique_hosts:
            self.__updateLog4j__(inst, debug, trace)
    ## DEF
    
    def clear_logs(self):
        for inst in self.unique_hosts:
            self.__clearLogs__(inst)
    ## DEF

    def getAllInstances(self):
        return self.all_instances
    ## DEF

    def getRunningInstances(self):
        return self.running_instances
    ## DEF

    def getRunningSiteInstances(self):
        return self.running_instances[:self.siteCount+1]
    ## DEF

    def getRunningClientInstances(self):
        LOG.debug("Running Instances: %s", map(str, self.running_instances))
        LOG.debug("Client Instances: %s", map(str, self.running_instances[self.siteCount+1:]))
        return self.running_instances[self.siteCount+1:]
    ## DEF

    def getInstance(self, public_dns_name):
        for inst in self.running_instances:
            if inst.public_dns_name.strip() == public_dns_name.strip():
                return (inst)
        return (None)
    ## DEF

## CLASS