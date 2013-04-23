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
    "ssh.hosts":                [ "istc3.csail.mit.edu", "istc4.csail.mit.edu" ],
    "key_filename":             os.path.join(os.environ["HOME"], ".ssh/csail.pem"),
    
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
        for hostname in self.env["ssh.hosts"]:
            self.running_instances.append(SSHInstance(hostname))
        self.running_instances.sort(key=lambda inst: inst.name)
        self.all_instances = self.running_instances
    ## DEF
    
    def start_cluster(self, build=True, update=True):
        for inst in self.running_instances:
            self.__setupInstance__(inst, build, update)
    ## DEF
    
    def deploy_hstore(self, build=True, update=True):
        self.start_cluster(build, update)
    ## DEF
    
    def write_conf(self, project, removals=[ ], revertFirst=False):
        for inst in self.running_instances:
            self.__writeConf__(inst, project, removals, revertFirst)
    ## DEF
    
    def enable_debugging(self, debug=[], trace=[]):
        for inst in self.running_instances:
            self.__enableDebugging__(inst, debug, trace)
    ## DEF
    
    def clear_logs(self):
        for inst in self.running_instances:
            self.__clearLogs__(inst)
    ## DEF

    ## ----------------------------------------------
    ## __getInstances__
    ## ----------------------------------------------        
    def __getInstances__(self):
        pass
    ## DEF

    ## ----------------------------------------------
    ## __getRunningInstances__
    ## ----------------------------------------------        
    def __getRunningInstances__(self, instType):
        instances = [ ]
        for inst in self.env["ec2.running_instances"]:
            if self.__getInstanceType__(inst) == instType:
                instances.append(inst)
        ## FOR
        if len(instances) == 0:
            raise Exception("No running instances with type '%s' were found" % instType)
        return instances
    ## DEF

    ## ----------------------------------------------
    ## __getRunningSiteInstances__
    ## ----------------------------------------------        
    def __getRunningSiteInstances__(self):
        return self.__getRunningInstances__(self.env["ec2.site_type"])
    ## DEF

    ## ----------------------------------------------
    ## __getRunningClientInstances__
    ## ----------------------------------------------        
    def __getRunningClientInstances__(self):
        return self.__getRunningInstances__(self.env["ec2.client_type"])
    ## DEF

    ## ----------------------------------------------
    ## __getInstance__
    ## ----------------------------------------------        
    def __getInstance__(self, public_dns_name):
        LOG.debug("Looking for instance with public_dns_name '%s'" % public_dns_name)
        self.__getInstances__()
        for inst in self.env["ec2.all_instances"]:
            LOG.debug("COMPARE '%s' <=> '%s'", inst.public_dns_name, public_dns_name)
            if inst.public_dns_name.strip() == public_dns_name.strip():
                return (inst)
        return (None)
    ## DEF

    ## ----------------------------------------------
    ## __getInstanceName__
    ## ----------------------------------------------
    def __getInstanceName__(self, inst):
        return inst.name
    ## DEF

    ## ----------------------------------------------
    ## __getInstanceType__
    ## ----------------------------------------------        
    def __getInstanceType__(self, inst):
        attr = inst.get_attribute("instanceType")
        assert attr != None
        assert "instanceType" in attr
        return attr["instanceType"]
    ## DEF

    ## ----------------------------------------------
    ## __getExpectedInstanceType__
    ## ----------------------------------------------        
    def __getExpectedInstanceType__(self, inst):
        self.__getInstances__()
        if not inst in self.env["ec2.running_instances"]:
            inst_offset = 0 if len(self.env["ec2.running_instances"]) < self.siteCount else self.siteCount
        else:
            inst_offset = self.env["ec2.running_instances"].index(inst)
        return self.env["ec2.site_type"] if inst_offset < self.siteCount else self.env["ec2.client_type"]
    ## DEF

    ## ----------------------------------------------
    ## __checkInstanceType__
    ## ----------------------------------------------        
    def __checkInstanceType__(self, inst):
        expectedType = self.__getExpectedInstanceType__(inst)
        LOG.debug("Checking whether the instance type for %s is '%s'" % (self.__getInstanceName__(inst), expectedType))
        attr = inst.get_attribute("instanceType")
        assert attr != None
        assert "instanceType" in attr

        ## Check whether we need to change the instance type before we restart it
        currentType = attr["instanceType"]
        if self.env["ec2.change_type"] == True and currentType != expectedType:
            if inst.update() == 'running':
                raise Exception("Unable to switch instance type from '%s' to '%s' for %s" % (currentType, expectedType, self.__getInstanceName__(inst)))
            LOG.info("Switching instance type from '%s' to '%s' for '%s'" % (currentType, expectedType, self.__getInstanceName__(inst)))
            inst.modify_attribute("instanceType", expectedType)
            return True
        ### IF
        return False
    ## DEF

    ## ----------------------------------------------
    ## __createSecurityGroup__
    ## ----------------------------------------------
    def __createSecurityGroup__(self):
        security_groups = self.ec2_conn.get_all_security_groups()
        for sg in security_groups:
            if sg.name == self.env["ec2.security_group"]:
                return
        ## FOR
        
        LOG.info("Creating security group '%s'" % self.env["ec2.security_group"])
        sg = self.ec2_conn.create_security_group(self.env["ec2.security_group"], 'H-Store Security Group')
        sg.authorize(src_group=sg)
        sg.authorize('tcp', 22, 22, '0.0.0.0/0')
    ## DEF

    ## ----------------------------------------------
    ## __createPlacementGroup__
    ## ----------------------------------------------
    def __createPlacementGroup__(self):
        groupName = self.env["ec2.placement_group"]
        assert groupName
        placement_groups = []
        try:
            placement_groups = self.ec2_conn.get_all_placement_groups(groupnames=[groupName])
        except:
            pass
        if len(placement_groups) == 0:
            LOG.info("Creating placement group '%s'" % groupName)
            self.ec2_conn.create_placement_group(groupName, strategy='cluster')
        return
    ## DEF

    ## ----------------------------------------------
    ## __waitUntilStatus__
    ## ----------------------------------------------        
    def __waitUntilStatus__(self, inst, status):
        tries = 10
        while tries > 0 and not inst.update() == status:
            time.sleep(self.env["ec2.status_wait_time"])
            tries -= 1
        if tries == 0:
            logging.error("Last '%s' status: %s" % (self.__getInstanceName__(inst), inst.update()))
            raise Exception("Timed out waiting for %s to get to status '%s'" % (self.__getInstanceName__(inst), status))
        
        ## Just because it's running doesn't mean it's ready
        ## So we'll wait until we can SSH into it
        if status == 'running':
            # Set the timeout
            original_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(self.env["ec2.reboot_wait_time"])
            host_status = False
            tries = 5
            LOG.info("Testing whether instance '%s' is ready [tries=%d]" % (self.__getInstanceName__(inst), tries))
            while tries > 0:
                host_status = False
                try:
                    transport = paramiko.Transport((inst.public_dns_name, 22))
                    transport.close()
                    host_status = True
                except:
                    pass
                if host_status: break
                time.sleep(self.env["ec2.reboot_wait_time"])
                tries -= 1
            ## WHILE
            socket.setdefaulttimeout(original_timeout)
            if not host_status:
                raise Exception("Failed to connect to '%s'" % self.__getInstanceName__(inst))
    ## DEF
    
    ## ----------------------------------------------
    ## __syncTime__
    ## ----------------------------------------------
    def __syncTime__(self):
        with settings(warn_only=True):
            sudo("ntpdate-debian -b")
        ## WITH
    ## DEF
## CLASS