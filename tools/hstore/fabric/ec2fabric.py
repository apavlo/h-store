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

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
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
## EC2 NODE CONFIGURATION
## ==============================================

## List of packages needed on each instance
ALL_PACKAGES = [
    'git-core',
    'subversion',
    'gcc',
    'g++',
    'openjdk-7-jdk',
    'valgrind',
    'ant',
    'make',
    # Not required, but handy to have
    'htop',
    'realpath',
    'unison',
]
NFSHEAD_PACKAGES = [
    'nfs-kernel-server',
]
NFSCLIENT_PACKAGES = [
    'autofs',
]

TAG_NFSTYPE         = "Type"
TAG_NFSTYPE_HEAD    = "nfs-node"
TAG_NFSTYPE_CLIENT  = "nfs-client"
TAG_CLUSTER         = "Cluster"

ENV_DEFAULT = {
    # Fabric Options
    "user":                     "ubuntu",
    "disable_known_hosts":      True,
    "no_agent":                 True,
    "port":                     22,
    
    # EC2 Options
    "ec2.site_type":            "m2.4xlarge",
    "ec2.site_ami":             "ami-9c78c0f5",
    "ec2.client_type":          "m1.xlarge",
    "ec2.client_ami":           "ami-9c78c0f5",
    "ec2.placement_group":      None,
    "ec2.security_group":       "hstore",
    "ec2.keypair":              "hstore",
    "ec2.region":               "us-east-1b",
    "ec2.access_key_id":        os.getenv('AWS_ACCESS_KEY_ID'),
    "ec2.secret_access_key":    os.getenv('AWS_SECRET_ACCESS_KEY'),
    "ec2.force_reboot":         False,
    "ec2.all_instances":        [ ],
    "ec2.running_instances":    [ ],
    "ec2.reboot_wait_time":     20,
    "ec2.status_wait_time":     20,
    "ec2.cluster_group":        None,
    "ec2.pkg_auto_update":      True,
    
    # H-Store Options
    "hstore.basedir":           "/vol/workspace",
}

## ==============================================
## EC2Fabric
## ==============================================
class EC2Fabric(AbstractFabric):
    
    def __init__(self, env):
        super(EC2Fabric, self).__init__(env, ENV_DEFAULT)
        
        # Setup EC2 Connection
        self.ec2_conn = boto.connect_ec2(self.env["ec2.access_key_id"], self.env["ec2.secret_access_key"])
    ## DEF
    
    ## ----------------------------------------------
    ## start_cluster
    ## ----------------------------------------------
    def start_cluster(self, updateSync=True):
        """Deploy a new H-Store cluster on EC2 using the given configuration"""
        
        ## First make sure that our security group is setup
        self.__createSecurityGroup__()
        
        ## Then create our placement group if they want one
        if self.env["ec2.placement_group"] != None:
            self.__createPlacementGroup__()

        ## Then figure out how many instances we actually need
        instances_needed = self.hostCount + self.clientCount
        instances_count = instances_needed
        if self.env["ec2.cluster_group"]:
            LOG.info("Virtual Cluster: %s" % self.env["ec2.cluster_group"])
        LOG.info("HostCount:%d / SiteCount:%d / PartitionCount:%d / ClientCount:%d", \
                 self.hostCount, self.siteCount, self.partitionCount, self.clientCount)

        ## Retrieve the list of instances that are already deployed (running or not)
        ## and figure out how many we can reuse versus restart versus create
        self.__getInstances__()

        ## These must be running 
        siteInstances = [ ]
        clientInstances = [ ]

        ## The stopped instances that we need to restart
        stoppedInstances = [ ]
        
        ## One instance is the NFS head node. We always need to at least have this guy available.
        nfs_inst = None
        nfs_inst_online = False
        
        for inst in self.all_instances:
            is_running = inst in self.running_instances
            
            ## At least one of the running nodes must be our nfs-node
            if inst.tags[TAG_NFSTYPE] == TAG_NFSTYPE_HEAD:
                if is_running:
                    assert nfs_inst == None, "Multiple NFS instances are running"
                    nfs_inst_online = True
                nfs_inst = inst
            ## IF
            
            ## If it's already running, check its type to see whether we want to make it a client or a site
            if is_running:
                instType = self.__getInstanceType__(inst)
                if instType == self.env["ec2.site_type"] and len(siteInstances) < self.siteCount:
                    siteInstances.append(inst)
                elif instType == self.env["ec2.client_type"]:
                    clientInstances.append(inst)
                ## If it doesn't match any of them, then we'll stop it and change it
                elif self.env["ec2.change_type"]:
                    LOG.info("Stopping %s instance '%s' so that it can be restarted with a different type" % (instType, self.__getInstanceName__(inst)))
                    inst.stop()
                    self.running_instances.remove(inst)
                    stoppedInstances.append(inst)
            else:
                stoppedInstances.append(inst)
        ## FOR
        for inst in stoppedInstances:
            self.__waitUntilStatus__(inst, "stopped")
        ## FOR
        
        ## Make sure that we always start the nfs_inst first
        if nfs_inst != None and nfs_inst in stoppedInstances:
            stoppedInstances.remove(nfs_inst)
            stoppedInstances.insert(0, nfs_inst)
        ## IF
                    
        if nfs_inst == None:
            LOG.info("No '%s' instance is available. Will create a new one" % TAG_NFSTYPE_HEAD)
        elif not nfs_inst_online:
            LOG.info("'%s' instance %s is offline. Will restart" % (TAG_NFSTYPE_HEAD, self.__getInstanceName__(nfs_inst)))
        
        ## Check whether we enough instances already running
        sites_needed = max(0, self.siteCount - len(siteInstances))
        clients_needed = max(0, self.clientCount - len(clientInstances))
        if not nfs_inst_online and sites_needed == 0:
            sites_needed = 1
        orig_running = self.running_instances[:]
        instances_needed = sites_needed + clients_needed
        
        LOG.info("All Instances - Stopped:%d / Running:%d" % (len(all_instances), len(running_instances)))
        LOG.info("SITES   - Running:%d / Needed:%d" % (len(siteInstances), sites_needed))
        LOG.info("CLIENTS - Running:%d / Needed:%d" % (len(clientInstances), clients_needed))
        
        ## See whether we can restart a stopped instance
        if instances_needed > 0 and len(stoppedInstances) > 0:
            ## First roll through and start all these mofos back up
            first = True
            waiting = [ ]
            for inst in stoppedInstances:
                assert not inst in self.running_instances
                currentType = self.__getInstanceType__(inst)
                restart = False
                
                ## Figure out whether this will be a site or a client
                ## HACK: We should really be tagging the instances rather relying 
                ## on an offset to determine whether they are a site or a client 
                if sites_needed > 0:
                    LOG.debug("SITE: %s - Current Type %s <=> %s" % (self.__getInstanceName__(inst), currentType, self.env["ec2.site_type"]))
                    if inst.image_id != self.env["ec2.site_ami"]:
                        LOG.warn("SITE Skipping %s != %s" % (inst.image_id, self.env["ec2.site_ami"]))
                        pass
                    elif currentType != self.env["ec2.site_type"]:
                        if self.env["ec2.change_type"]:
                            inst.modify_attribute("instanceType", self.env["ec2.site_type"])
                            siteInstances.append(inst)
                            restart = True
                    else:
                        restart = True
                    if restart:
                        sites_needed -= 1
                else:
                    LOG.debug("CLIENT: %s - Current Type %s <=> %s" % (self.__getInstanceName__(inst), currentType, self.env["ec2.client_type"]))
                    if inst.image_id != self.env["ec2.client_ami"]:
                        LOG.warn("CLIENT Skipping %s != %s" % (inst.image_id, self.env["ec2.site_ami"]))
                        pass
                    elif currentType != self.env["ec2.client_type"]:
                        if self.env["ec2.change_type"]:
                            inst.modify_attribute("instanceType", self.env["ec2.client_type"])
                            clientInstances.append(inst)
                            restart = True
                    else:
                        restart = True
                    if restart:
                        clients_needed -= 1
                ## IF
                if not restart:
                    LOG.debug("SKIP %s" % self.__getInstanceName__(inst))
                    continue
                    
                LOG.info("Restarting stopped instance '%s' / %s" % (self.__getInstanceName__(inst), currentType))
                inst.start()
                waiting.append(inst)
                instances_needed -= 1
                
                if sites_needed == 0 and clients_needed == 0:
                    break
            ## FOR
            
            if waiting:
                for inst in waiting:
                    self.__waitUntilStatus__(inst, 'running')
                    self.running_instances.append(inst)
                time.sleep(self.env["ec2.reboot_wait_time"])
        ## IF
        
        ## Otherwise, we need to start some new motha truckas
        if instances_needed > 0:
            ## Figure out what the next id should be. Not necessary, but just nice...
            next_id = 0
            for inst in self.all_instances:
                instName = self.__getInstanceName__(inst)
                if instName.startswith("hstore-"):
                    instId = int(instName.split("-")[-1])
                    next_id = max(next_id, instId+1)
            ## FOR

            LOG.info("Deploying %d new instances [next_id=%d]" % (instances_needed, next_id))
            siteInstance_tags = [ ]
            clientInstance_tags = [ ]
            marked_nfs = False
            for i in range(instances_needed):
                tags = { 
                    "Name": "hstore-%02d" % (next_id),
                    TAG_NFSTYPE: TAG_NFSTYPE_CLIENT,
                }
                ## Virtual Cluster
                if self.env["ec2.cluster_group"]:
                    tags[TAG_CLUSTER] = self.env["ec2.cluster_group"]
                
                if nfs_inst == None and not marked_nfs:
                    tags[TAG_NFSTYPE] = TAG_NFSTYPE_HEAD
                    marked_nfs = True
                    
                if sites_needed > 0:
                    siteInstance_tags.append(tags)
                    sites_needed -= 1
                else:
                    clientInstance_tags.append(tags)
                    clients_needed -= 1
                next_id += 1
            ## FOR
            assert sites_needed == 0
            assert clients_needed == 0
            if len(siteInstance_tags) > 0:
                self.__startInstances__(len(siteInstance_tags), 
                                        self.env["ec2.site_ami"], 
                                        self.env["ec2.site_type"],
                                        siteInstance_tags)
                instances_needed -= len(siteInstance_tags)
            if len(clientInstance_tags) > 0:
                self.__startInstances__(len(clientInstance_tags),
                                        self.env["ec2.client_ami"],
                                        self.env["ec2.client_type"],
                                        clientInstance_tags)
                instances_needed -= len(clientInstance_tags)
        ## IF
        assert instances_needed == 0
        assert len(running_instances) >= instances_count, "%d != %d" % (len(running_instances), instances_count)

        ## Check whether we already have an NFS node setup
        for i in range(len(running_instances)):
            inst = self.running_instances[i]
            if TAG_NFSTYPE in inst.tags and inst.tags[TAG_NFSTYPE] == TAG_NFSTYPE_HEAD:
                LOG.debug("BEFORE: %s" % self.running_instances)
                self.running_instances.pop(i)
                self.running_instances.insert(0, inst)
                LOG.debug("AFTER: %s" % self.running_instances)
                break
        ## FOR
            
        first = True
        for inst in self.running_instances:
            LOG.info("Configuring instance '%s'" % (self.__getInstanceName__(inst)))
            with settings(host_string=inst.public_dns_name):
                ## Setup the basic self.environmnt that we need on each node
                ## This will return true if we need to restart it
                first_setup = setup_env()
                need_reboot = first_setup or self.env["ec2.force_reboot"] or not nfs_inst_online
                
                ## The first instance will be our NFS head node
                if first:
                    if need_reboot: self.setup_nfshead(need_reboot)
                    deploy_hstore()
                
                ## Othewise make the rest of the node NFS clients
                else:
                    LOG.info("%s - forceReboot=%s / nfsInstOnline=%s / origRunning=%s" % \
                            (self.__getInstanceName__(inst), self.env["ec2.force_reboot"], nfs_inst_online, (inst in orig_running)))
                    need_reboot = need_reboot or ((nfs_inst_online and inst in orig_running) == False)
                    self.setup_nfsclient(need_reboot)
                first = False
            ## WITH
        ## FOR
        if updateSync: self.sync_time()
    ## DEF

    ## ----------------------------------------------
    ## setup_env
    ## ----------------------------------------------
    def setup_env(self):
        """Configure the execution environmnt for the current instance."""
        
        # Get the release name
        output = run("cat /etc/lsb-release | grep DISTRIB_CODENAME")
        releaseName = output.split("=")[1]
        
        with hide('running', 'stdout'):
            append("/etc/apt/sources.list",
                [ "deb http://archive.canonical.com/ubuntu %s partner" % releaseName,
                    "deb-src http://archive.canonical.com/ubuntu %s partner" % releaseName ], use_sudo=True)
            sudo("apt-get update")
        ## WITH
        sudo("apt-get --yes install %s" % " ".join(ALL_PACKAGES))
        
        # Make sure that we pick openjdk-7-jdk
        sudo("update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java")
        
        __syncTime__()
        
        # Upgrade and clean up packages
        if self.env["ec2.pkg_auto_update"]:
            sudo("apt-get --yes dist-upgrade")
            sudo("apt-get --yes autoremove")
        
        first_setup = False
        with settings(warn_only=True):
            basename = os.path.basename(self.env.key_filename)
            files = [ (self.env.key_filename + ".pub", "/home/%s/.ssh/authorized_keys" % (self.env.user)),
                      (self.env.key_filename + ".pub", "/home/%s/.ssh/id_dsa.pub" % (self.env.user)),
                      (self.env.key_filename,          "/home/%s/.ssh/id_dsa" % (self.env.user)), ]
            for local_file, remote_file in files:
                if run("test -f " + remote_file).failed:
                    put(local_file, remote_file, mode=0600)
                    first_setup = True
        ## WITH
        
        # We may be running a large cluster, in which case we will have a lot of connections
        handlesAllowed = 24000
        for key in [ "soft", "hard" ]:
            update_line = "* %s nofile %d" % (key, handlesAllowed)
            if not contains("/etc/security/limits.conf", update_line):
                append("/etc/security/limits.conf", update_line, use_sudo=True)
        ## FOR
        
        # Bash Aliases
        log_dir = self.env.get("site.log_dir", os.path.join(self.hstore_dir, "obj/logs/sites"))
        aliases = {
            # H-Store Home
            'hh':  'cd ' + self.hstore_dir,
            # H-Store Site Logs
            'hl':  'cd ' + log_dir,
            # General Aliases
            'rmf': 'rm -rf',
            'la':  'ls -lh',
            'h':   'history',
            'top': 'htop',
        }
        aliases = dict([("alias %s" % key, "\"%s\"" % val) for key,val in aliases.items() ])
        update_conf(".bashrc", aliases, noSpaces=True)
        
        # Git Config
        gitConfig = """
            [color]
            diff = auto
            status = auto
            branch = auto

            [color "status"]
            added = yellow
            changed = green
            untracked = cyan
        """
        with settings(warn_only=True):
            if run("test -f " + ".gitconfig").failed:
                for line in map(string.strip, gitConfig.split("\n")):
                    append(".gitconfig", line, use_sudo=False)
        ## WITH
        
        with settings(warn_only=True):
            # Install the real H-Store directory in self.hstore_dir
            install_dir = os.path.dirname(self.hstore_dir)
            if run("test -d %s" % install_dir).failed:
                run("mkdir " + install_dir)
            sudo("chown --quiet %s %s" % (self.env.user, install_dir))
        ## WITH
        
        return (first_setup)
    ## DEF
    
    ## ----------------------------------------------
    ## setup_nfshead
    ## ----------------------------------------------
    def setup_nfshead(rebootInst=True):
        """Deploy the NFS head node"""
        self.__getInstances__()
        
        sudo("apt-get --yes remove %s" % " ".join(NFSCLIENT_PACKAGES))
        sudo("apt-get --yes autoremove")
        sudo("apt-get --yes install %s" % " ".join(NFSHEAD_PACKAGES))
        append("/etc/exports", "%s *(rw,async,no_subtree_check)" % os.path.dirname(self.hstore_dir[:-1]), use_sudo=True)
        sudo("exportfs -a")
        sudo("/etc/init.d/portmap start")
        sudo("/etc/init.d/nfs-kernel-server start")
        
        inst = self.__getInstance__(self.env.host_string)
        assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (self.env.host_string, "\n".join([inst.public_dns_name for inst in self.env["ec2.running_instances"]]))
        self.ec2_conn.create_tags([inst.id], {TAG_NFSTYPE: TAG_NFSTYPE_HEAD})
        
        ## Reboot and wait until it comes back online
        if rebootInst:
            LOG.info("Rebooting " + self.__getInstanceName__(inst))
            reboot(self.env["ec2.reboot_wait_time"])
            __waitUntilStatus__(inst, 'running')
            ## IF
        LOG.info("NFS Head '%s' is online and ready" % self.__getInstanceName__(inst))
    ## DEF

    ## ----------------------------------------------
    ## setup_nfsclient
    ## ----------------------------------------------
    def setup_nfsclient(rebootInst=True):
        """Deploy the NFS client node"""
        self.__getInstances__()
        
        nfs_dir = os.path.dirname(self.hstore_dir[:-1])
        
        ## Update the /etc/hosts files to make it easier for us to point
        ## to different NFS head nodes
        hosts_line = "%s hstore-nfs" % self.env["ec2.running_instances"][0].private_ip_address
        if not contains("/etc/hosts", hosts_line):
            if contains("/etc/hosts", "hstore-nfs"):
                sed("/etc/hosts", ".* hstore-nfs", hosts_line, use_sudo=True)
            else:
                append("/etc/hosts", hosts_line, use_sudo=True)
        
            sudo("apt-get --yes install %s" % " ".join(NFSCLIENT_PACKAGES))
            append("/etc/auto.main", "%s /etc/auto.hstore" % nfs_dir, use_sudo=True)
            append("/etc/auto.hstore", "* hstore-nfs:%s/&" % nfs_dir, use_sudo=True)
            sudo("/etc/init.d/autofs start")
        ## IF
        
        inst = self.__getInstance__(self.env.host_string)
        assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (self.env.host_string, "\n".join([inst.public_dns_name for inst in self.env["ec2.running_instances"]]))
        self.ec2_conn.create_tags([inst.id], {TAG_NFSTYPE: TAG_NFSTYPE_CLIENT})
        
        ## Reboot and wait until it comes back online
        if rebootInst:
            LOG.info("Rebooting " + self.__getInstanceName__(inst))
            reboot(self.env["ec2.reboot_wait_time"])
            __waitUntilStatus__(inst, 'running')
        ## IF
        LOG.info("NFS Client '%s' is online and ready" % self.__getInstanceName__(inst))
        run("cd %s" % self.hstore_dir)
    ## DEF
    
    ## ----------------------------------------------
    ## stop_cluster
    ## ----------------------------------------------
    def stop_cluster(self, terminate=False):
        """Stop all instances in the cluster"""
        self.__getInstances__()
        
        waiting = [ ]
        for inst in self.env["ec2.running_instances"]:
            if self.__getInstanceName__(inst).startswith("hstore-") and inst.state == 'running':
                LOG.info("%s %s" % ("Terminating" if terminate else "Stopping", self.__getInstanceName__(inst)))
                if terminate:
                    inst.terminate()
                else:
                    inst.stop()
                waiting.append(inst)
        ## FOR
        if waiting:
            LOG.info("Halting %d instances" % len(waiting))
            for inst in waiting:
                __waitUntilStatus__(inst, 'terminated' if terminate else 'stopped')
            ## FOR
        else:
            LOG.info("No running H-Store instances were found")
    ## DEF
    
    ## ----------------------------------------------
    ## __startInstances__
    ## ----------------------------------------------        
    def __startInstances__(self, **kwargs):
        instances_count = kwargs["instances_count"]
        ec2_ami = kwargs["ec2_ami"]
        ec2_type = kwargs["ec2_type"]
        instance_tags = kwargs["instance_tags"]
        
        LOG.info("Attemping to start %d instances." % (instances_count))
        try:
            resv = self.ec2_conn.run_instances(ec2_ami,
                                        instance_type=ec2_type,
                                        key_name=env["ec2.keypair"],
                                        min_count=instances_count,
                                        max_count=instances_count,
                                        security_groups=[ self.env["ec2.security_group"] ],
                                        placement=env["ec2.region"],
                                        placement_group=env["ec2.placement_group"])
        except:
            LOG.error("Failed to start %s instances [%s]" % (ec2_type, ec2_ami))
            raise
        LOG.info("Started %d execution nodes. Waiting for them to come online" % len(resv.instances))
        i = 0
        for inst in resv.instances:
            self.env["ec2.running_instances"].append(inst)
            self.env["ec2.all_instances"].append(inst)
            time.sleep(self.env["ec2.reboot_wait_time"])
            try:
                self.ec2_conn.create_tags([inst.id], instance_tags[i])
            except:
                logging.error("BUSTED = %d" % (i))
                logging.error(str(instance_tags))
                raise
            __waitUntilStatus__(inst, 'running')
            LOG.info("New Instance '%s' / %s is ready" % (self.__getInstanceName__(inst), self.env["ec2.site_type"]))
            i += 1
        ## FOR
        time.sleep(self.env["ec2.reboot_wait_time"])
        LOG.info("Started %d instances." % len(resv.instances))
    ## DEF

    ## ----------------------------------------------
    ## __getInstances__
    ## ----------------------------------------------        
    def __getInstances__():
        if self.env["ec2.running_instances"]: return self.env["ec2.running_instances"]
        
        instFilter = { }
        
        ## Virtual Clusters
        if self.env["ec2.cluster_group"]:
            instFilter["tag:" + TAG_CLUSTER] = self.env["ec2.cluster_group"]
        
        self.env["ec2.all_instances"] = [ ]
        self.env["ec2.running_instances"] = [ ]
        
        reservations = self.ec2_conn.get_all_instances(filters=instFilter)
        instances = [i for r in reservations for i in r.instances]
        for inst in instances:
            ## Cluster Groups
            if 'Name' in inst.tags and self.__getInstanceName__(inst).startswith("hstore-"):
                if inst.state != 'terminated': self.env["ec2.all_instances"].append(inst)
                if inst.state == 'running': self.env["ec2.running_instances"].append(inst)
        ## FOR
        sortKey = lambda inst: self.__getInstanceName__(inst)
        self.env["ec2.all_instances"].sort(key=sortKey)
        self.env["ec2.running_instances"].sort(key=sortKey)
        return
    ## DEF

    ## ----------------------------------------------
    ## __getRunningInstances__
    ## ----------------------------------------------        
    def __getRunningInstances__(self, instType):
        self.__getInstances__()
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
        assert inst
        return (inst.tags['Name'] if 'Name' in inst.tags else '???')
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