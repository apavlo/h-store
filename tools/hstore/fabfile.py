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
    ## Not required, but handy to have
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

## Fabric Options
env.key_filename = os.path.join(os.environ["HOME"], ".ssh/hstore.pem")
env.user = 'ubuntu'
env.disable_known_hosts = True
env.no_agent = True
env.port = 22

## Default Environmnt
ENV_DEFAULT = {
    ## EC2 Options
    "ec2.site_type":               "m2.4xlarge",
    "ec2.site_ami":                "ami-39a81d50",
    "ec2.client_type":             "m1.xlarge",
    "ec2.client_ami":              "ami-39a81d50",
    "ec2.placement_group":         None,
    "ec2.security_group":          "hstore",
    "ec2.keypair":                 "hstore",
    "ec2.region":                  "us-east-1b",
    "ec2.access_key_id":           os.getenv('AWS_ACCESS_KEY_ID'),
    "ec2.secret_access_key":       os.getenv('AWS_SECRET_ACCESS_KEY'),
    "ec2.force_reboot":            False,
    "ec2.all_instances":           [ ],
    "ec2.running_instances":       [ ],
    "ec2.reboot_wait_time":        20,
    "ec2.status_wait_time":        20,
    "ec2.cluster_group":           None,
    "ec2.pkg_auto_update":         True,

    ## Client Options
    "client.count":                1,
    "client.threads_per_host":     500,
    
    ## H-Store Options
    "hstore.basedir":               "workspace",
    "hstore.git":                   "git://github.com/apavlo/h-store.git",
    "hstore.git_branch":            "master",
    "hstore.git_options":           "",
    "hstore.clean":                 False,
    "hstore.exec_prefix":           "compile",
    "hstore.partitions":            6,
    "hstore.sites_per_host":        1,
    "hstore.partitions_per_site":   7,
    # "hstore.num_hosts_round_robin": 2,
}

has_rcfile = os.path.exists(env.rcfile)
for k, v in ENV_DEFAULT.items():
    if not k in env:
        env[k] = v
    if v:
        t = type(v)
        LOG.debug("%s [%s] => %s" % (k, t, env[k]))
        env[k] = t(env[k])
## FOR

## H-Store Directory
INSTALL_DIR = os.path.join("/vol", env["hstore.basedir"]) 
HSTORE_DIR = os.path.join(INSTALL_DIR, "h-store")

## Setup EC2 Connection
ec2_conn = boto.connect_ec2(env["ec2.access_key_id"], env["ec2.secret_access_key"])

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
    
    ## First make sure that our security group is setup
    __createSecurityGroup__()
    
    ## Then create our placement group if they want one
    if env["ec2.placement_group"] != None:
        __createPlacementGroup__()

    ## Then figure out how many instances we actually need
    hostCount, siteCount, partitionCount, clientCount = __getInstanceTypeCounts__()
    instances_needed = hostCount + clientCount
    instances_count = instances_needed
    if env["ec2.cluster_group"]: LOG.info("Virtual Cluster: %s" % env["ec2.cluster_group"])
    LOG.info("HostCount:%d / SiteCount:%d / PartitionCount:%d / ClientCount:%d" % (\
             hostCount, siteCount, partitionCount, clientCount))

    ## Retrieve the list of instances that are already deployed (running or not)
    ## and figure out how many we can reuse versus restart versus create
    __getInstances__()

    ## These must be running 
    siteInstances = [ ]
    clientInstances = [ ]

    ## The stopped instances that we need to restart
    stoppedInstances = [ ]
    
    ## One instance is the NFS head node. We always need to at least have this guy available.
    nfs_inst = None
    nfs_inst_online = False
    
    for inst in env["ec2.all_instances"]:
        is_running = inst in env["ec2.running_instances"]
        
        ## At least one of the running nodes must be our nfs-node
        if inst.tags[TAG_NFSTYPE] == TAG_NFSTYPE_HEAD:
            if is_running:
                assert nfs_inst == None, "Multiple NFS instances are running"
                nfs_inst_online = True
            nfs_inst = inst
        ## IF
        
        ## If it's already running, check its type to see whether we want to make it a client or a site
        if is_running:
            instType = __getInstanceType__(inst)
            if instType == env["ec2.site_type"] and len(siteInstances) < siteCount:
                siteInstances.append(inst)
            elif instType == env["ec2.client_type"]:
                clientInstances.append(inst)
            ## If it doesn't match any of them, then we'll stop it and change it
            elif env["ec2.change_type"]:
                LOG.info("Stopping %s instance '%s' so that it can be restarted with a different type" % (instType, __getInstanceName__(inst)))
                inst.stop()
                env["ec2.running_instances"].remove(inst)
                stoppedInstances.append(inst)
        else:
            stoppedInstances.append(inst)
    ## FOR
    for inst in stoppedInstances:
        __waitUntilStatus__(inst, "stopped")
    ## FOR
    
    ## Make sure that we always start the nfs_inst first
    if nfs_inst != None and nfs_inst in stoppedInstances:
        stoppedInstances.remove(nfs_inst)
        stoppedInstances.insert(0, nfs_inst)
    ## IF
                
    if nfs_inst == None:
        LOG.info("No '%s' instance is available. Will create a new one" % TAG_NFSTYPE_HEAD)
    elif not nfs_inst_online:
        LOG.info("'%s' instance %s is offline. Will restart" % (TAG_NFSTYPE_HEAD, __getInstanceName__(nfs_inst)))
    
    ## Check whether we enough instances already running
    sites_needed = max(0, siteCount - len(siteInstances))
    clients_needed = max(0, clientCount - len(clientInstances))
    if not nfs_inst_online and sites_needed == 0:
        sites_needed = 1
    orig_running = env["ec2.running_instances"][:]
    instances_needed = sites_needed + clients_needed
    
    LOG.info("All Instances - Stopped:%d / Running:%d" % (len(env["ec2.all_instances"]), len(env["ec2.running_instances"])))
    LOG.info("SITES   - Running:%d / Needed:%d" % (len(siteInstances), sites_needed))
    LOG.info("CLIENTS - Running:%d / Needed:%d" % (len(clientInstances), clients_needed))
    
    ## See whether we can restart a stopped instance
    if instances_needed > 0 and len(stoppedInstances) > 0:
        ## First roll through and start all these mofos back up
        first = True
        waiting = [ ]
        for inst in stoppedInstances:
            assert not inst in env["ec2.running_instances"]
            currentType = __getInstanceType__(inst)
            restart = False
            
            ## Figure out whether this will be a site or a client
            ## HACK: We should really be tagging the instances rather relying 
            ## on an offset to determine whether they are a site or a client 
            if sites_needed > 0:
                LOG.debug("SITE: %s - Current Type %s <=> %s" % (__getInstanceName__(inst), currentType, env["ec2.site_type"]))
                if inst.image_id != env["ec2.site_ami"]:
                    LOG.warn("SITE Skipping %s != %s" % (inst.image_id, env["ec2.site_ami"]))
                    pass
                elif currentType != env["ec2.site_type"]:
                    if env["ec2.change_type"]:
                        inst.modify_attribute("instanceType", env["ec2.site_type"])
                        siteInstances.append(inst)
                        restart = True
                else:
                    restart = True
                if restart:
                    sites_needed -= 1
            else:
                LOG.debug("CLIENT: %s - Current Type %s <=> %s" % (__getInstanceName__(inst), currentType, env["ec2.client_type"]))
                if inst.image_id != env["ec2.client_ami"]:
                    LOG.warn("CLIENT Skipping %s != %s" % (inst.image_id, env["ec2.site_ami"]))
                    pass
                elif currentType != env["ec2.client_type"]:
                    if env["ec2.change_type"]:
                        inst.modify_attribute("instanceType", env["ec2.client_type"])
                        clientInstances.append(inst)
                        restart = True
                else:
                    restart = True
                if restart:
                    clients_needed -= 1
            ## IF
            if not restart:
                LOG.debug("SKIP %s" % __getInstanceName__(inst))
                continue
                
            LOG.info("Restarting stopped instance '%s' / %s" % (__getInstanceName__(inst), currentType))
            inst.start()
            waiting.append(inst)
            instances_needed -= 1
            
            if sites_needed == 0 and clients_needed == 0:
                break
        ## FOR
        
        if waiting:
            for inst in waiting:
                __waitUntilStatus__(inst, 'running')
                env["ec2.running_instances"].append(inst)
            time.sleep(env["ec2.reboot_wait_time"])
    ## IF
    
    ## Otherwise, we need to start some new motha truckas
    if instances_needed > 0:
        ## Figure out what the next id should be. Not necessary, but just nice...
        next_id = 0
        for inst in env["ec2.all_instances"]:
            instName = __getInstanceName__(inst)
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
            if env["ec2.cluster_group"]:
                tags[TAG_CLUSTER] = env["ec2.cluster_group"]
            
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
            __startInstances__(len(siteInstance_tags), 
                               env["ec2.site_ami"], 
                               env["ec2.site_type"],
                               siteInstance_tags)
            instances_needed -= len(siteInstance_tags)
        if len(clientInstance_tags) > 0:
            __startInstances__(len(clientInstance_tags),
                               env["ec2.client_ami"],
                               env["ec2.client_type"],
                               clientInstance_tags)
            instances_needed -= len(clientInstance_tags)
    ## IF
    assert instances_needed == 0
    assert len(env["ec2.running_instances"]) >= instances_count, "%d != %d" % (len(env["ec2.running_instances"]), instances_count)

    ## Check whether we already have an NFS node setup
    for i in range(len(env["ec2.running_instances"])):
        inst = env["ec2.running_instances"][i]
        if TAG_NFSTYPE in inst.tags and inst.tags[TAG_NFSTYPE] == TAG_NFSTYPE_HEAD:
            LOG.debug("BEFORE: %s" % env["ec2.running_instances"])
            env["ec2.running_instances"].pop(i)
            env["ec2.running_instances"].insert(0, inst)
            LOG.debug("AFTER: %s" % env["ec2.running_instances"])
            break
    ## FOR
        
    first = True
    for inst in env["ec2.running_instances"]:
        LOG.info("Configuring instance '%s'" % (__getInstanceName__(inst)))
        with settings(host_string=inst.public_dns_name):
            ## Setup the basic environmnt that we need on each node
            ## This will return true if we need to restart it
            first_setup = setup_env()
            need_reboot = first_setup or env["ec2.force_reboot"] or not nfs_inst_online
            
            ## The first instance will be our NFS head node
            if first:
                if need_reboot: setup_nfshead(need_reboot)
                deploy_hstore()
            
            ## Othewise make the rest of the node NFS clients
            else:
                LOG.info("%s - forceReboot=%s / nfsInstOnline=%s / origRunning=%s" % \
                        (__getInstanceName__(inst), env["ec2.force_reboot"], nfs_inst_online, (inst in orig_running)))
                need_reboot = need_reboot or ((nfs_inst_online and inst in orig_running) == False)
                setup_nfsclient(need_reboot)
            first = False
        ## WITH
    ## FOR
    if updateSync: sync_time()
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
    if env["ec2.pkg_auto_update"]:
        sudo("apt-get --yes dist-upgrade")
        sudo("apt-get --yes autoremove")
    
    first_setup = False
    with settings(warn_only=True):
        basename = os.path.basename(env.key_filename)
        files = [ (env.key_filename + ".pub", "/home/%s/.ssh/authorized_keys" % (env.user)),
                  (env.key_filename + ".pub", "/home/%s/.ssh/id_dsa.pub" % (env.user)),
                  (env.key_filename,          "/home/%s/.ssh/id_dsa" % (env.user)), ]
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
    log_dir = env.get("site.log_dir", os.path.join(HSTORE_DIR, "obj/logs/sites"))
    aliases = {
        # H-Store Home
        'hh':  'cd ' + HSTORE_DIR,
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
        # Install the real H-Store directory in HSTORE_DIR
        install_dir = os.path.dirname(HSTORE_DIR)
        if run("test -d %s" % install_dir).failed:
            run("mkdir " + install_dir)
        sudo("chown --quiet %s %s" % (env.user, install_dir))
    ## WITH
    
    return (first_setup)
## DEF

## ----------------------------------------------
## setup_nfshead
## ----------------------------------------------
@task
def setup_nfshead(rebootInst=True):
    """Deploy the NFS head node"""
    __getInstances__()
    
    sudo("apt-get --yes remove %s" % " ".join(NFSCLIENT_PACKAGES))
    sudo("apt-get --yes autoremove")
    sudo("apt-get --yes install %s" % " ".join(NFSHEAD_PACKAGES))
    append("/etc/exports", "%s *(rw,async,no_subtree_check)" % os.path.dirname(HSTORE_DIR[:-1]), use_sudo=True)
    sudo("exportfs -a")
    sudo("/etc/init.d/portmap start")
    sudo("/etc/init.d/nfs-kernel-server start")
    
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env["ec2.running_instances"]]))
    ec2_conn.create_tags([inst.id], {TAG_NFSTYPE: TAG_NFSTYPE_HEAD})
    
    ## Reboot and wait until it comes back online
    if rebootInst:
        LOG.info("Rebooting " + __getInstanceName__(inst))
        reboot(env["ec2.reboot_wait_time"])
        __waitUntilStatus__(inst, 'running')
        ## IF
    LOG.info("NFS Head '%s' is online and ready" % __getInstanceName__(inst))
## DEF

## ----------------------------------------------
## setup_nfsclient
## ----------------------------------------------
@task
def setup_nfsclient(rebootInst=True):
    """Deploy the NFS client node"""
    __getInstances__()
    
    nfs_dir = os.path.dirname(HSTORE_DIR[:-1])
    
    ## Update the /etc/hosts files to make it easier for us to point
    ## to different NFS head nodes
    hosts_line = "%s hstore-nfs" % env["ec2.running_instances"][0].private_ip_address
    if not contains("/etc/hosts", hosts_line):
        if contains("/etc/hosts", "hstore-nfs"):
            sed("/etc/hosts", ".* hstore-nfs", hosts_line, use_sudo=True)
        else:
            append("/etc/hosts", hosts_line, use_sudo=True)
    
        sudo("apt-get --yes install %s" % " ".join(NFSCLIENT_PACKAGES))
        append("/etc/auto.master", "%s /etc/auto.hstore" % nfs_dir, use_sudo=True)
        append("/etc/auto.hstore", "* hstore-nfs:%s/&" % nfs_dir, use_sudo=True)
        sudo("/etc/init.d/autofs start")
    ## IF
    
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env["ec2.running_instances"]]))
    ec2_conn.create_tags([inst.id], {TAG_NFSTYPE: TAG_NFSTYPE_CLIENT})
    
    ## Reboot and wait until it comes back online
    if rebootInst:
        LOG.info("Rebooting " + __getInstanceName__(inst))
        reboot(env["ec2.reboot_wait_time"])
        __waitUntilStatus__(inst, 'running')
    ## IF
    LOG.info("NFS Client '%s' is online and ready" % __getInstanceName__(inst))
    run("cd %s" % HSTORE_DIR)
## DEF

## ----------------------------------------------
## deploy_hstore
## ----------------------------------------------
@task
def deploy_hstore(build=True, update=True):
    need_files = False
    
    with settings(warn_only=True):
        if run("test -d %s" % HSTORE_DIR).failed:
            with cd(INSTALL_DIR):
                LOG.debug("Initializing H-Store source code directory for branch '%s'" % env["hstore.git_branch"])
                run("git clone --branch %s %s %s" % (env["hstore.git_branch"], \
                                                     env["hstore.git_options"], \
                                                     env["hstore.git"]))
                update = True
                need_files = True
    ## WITH
    with cd(HSTORE_DIR):
        run("git checkout %s" % env["hstore.git_branch"])
        if update:
            LOG.debug("Pulling in latest changes for branch '%s'" % env["hstore.git_branch"])
            run("git checkout -- properties")
            run("git pull %s" % env["hstore.git_options"])
        
        ## Checkout Extra Files
        with settings(warn_only=True):
            if run("test -d %s" % "files").failed:
                LOG.debug("Initializing H-Store research files directory for branch '%s'" %  env["hstore.git_branch"])
                
                # 2012-08-20 - Create a symlink into /mnt/h-store so that we store 
                #              the larger files out in EBS
                sudo("ant junit-getfiles")
                sudo("chown --quiet -R %s files" % (env.user))
            elif update:
                LOG.debug("Pulling in latest research files for branch '%s'" % env["hstore.git_branch"])
                run("ant junit-getfiles-update")
            ## IF
        ## WITH
            
        if build:
            LOG.debug("Building H-Store from source code")
            if env["hstore.clean"]:
                run("ant clean-all")
            run("ant build")
        ## WITH
    ## WITH
    run("cd %s" % HSTORE_DIR)
## DEF

## ----------------------------------------------
## get_version
## ----------------------------------------------
@task
def get_version():
    """Get the current Git commit id and date in the deployment directory"""
    from datetime import datetime
    
    with cd(HSTORE_DIR):
        output = run("git log --pretty=format:' %h %at ' -n 1")
    data = map(string.strip, output.split(" "))
    rev_id = str(data[1])
    rev_date = datetime.fromtimestamp(int(data[2])) 
    LOG.info("Revision: %s / %s" % (rev_id, rev_date))
    return (rev_id, rev_date)
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
    __getInstances__()
    
    ## Make sure we have enough instances
    hostCount, siteCount, partitionCount, clientCount = __getInstanceTypeCounts__()
    if (hostCount + clientCount) > len(env["ec2.running_instances"]):
        raise Exception("Needed %d host + %d client instances but only %d are currently running" % (\
                        hostCount, clientCount, len(env["ec2.running_instances"])))

    hosts = [ ]
    clients = [ ]
    host_id = 0
    site_id = 0
    partition_id = 0
    partitions_per_site = env["hstore.partitions_per_site"]
    
    ## HStore Sites
    LOG.debug("Partitions Needed: %d" % env["hstore.partitions"])
    LOG.debug("Partitions Per Site: %d" % env["hstore.partitions_per_site"])
    site_hosts = set()
    
    if "hstore.num_hosts_round_robin" in env and env["hstore.num_hosts_round_robin"] != None:
        partitions_per_site = math.ceil(env["hstore.partitions"] / float(env["hstore.num_hosts_round_robin"]))
    
    for inst in __getRunningSiteInstances__():
        site_hosts.add(inst.private_dns_name)
        for i in range(env["hstore.sites_per_host"]):
            firstPartition = partition_id
            lastPartition = min(env["hstore.partitions"], firstPartition + partitions_per_site)-1
            host = "%s:%d:%d" % (inst.private_dns_name, site_id, firstPartition)
            if firstPartition != lastPartition:
                host += "-%d" % lastPartition
            partition_id += partitions_per_site
            site_id += 1
            hosts.append(host)
            if lastPartition+1 == env["hstore.partitions"]: break
        ## FOR (SITES)
        if lastPartition+1 == env["hstore.partitions"]: break
    ## FOR
    assert len(hosts) > 0
    LOG.debug("Site Hosts: %s" % hosts)
    
    ## HStore Clients
    for inst in __getRunningClientInstances__():
        if inst.private_dns_name in site_hosts: continue
        clients.append(inst.private_dns_name)
    ## FOR
    assert len(clients) > 0, "There are no %s client instances available" % env["ec2.client_type"]
    LOG.debug("Client Hosts: %s" % clients)

    ## Make sure the the checkout is up to date
    if updateRepo: 
        LOG.info("Updating H-Store Git checkout")
        deploy_hstore(build=False, update=True)
    ## Update H-Store Conf file
    ## Do this after we update the repository so that we can put in our updates
    if updateConf:
        LOG.info("Updating H-Store configuration files")
        write_conf(project, removals, revertFirst=True)

    ## Construct dict of command-line H-Store options
    hstore_options = {
        "client.hosts":                 ",".join(clients),
        "client.count":                 env["client.count"],
        "client.threads_per_host":      env["client.threads_per_host"],
        "project":                      project,
        "hosts":                        '"%s"' % ";".join(hosts),
    }
    if json: hstore_options["client.output_json"] = True
    if trace:
        import time
        hstore_options["trace"] = "traces/%s-%d" % (project, time.time())
        LOG.debug("Enabling trace files that will be output to '%s'" % hstore_options["trace"])
    LOG.debug("H-Store Config:\n" + pformat(hstore_options))
    
    ## Extra Parameters
    if extraParams:
        hstore_options = dict(hstore_options.items() + extraParams.items())
    
    ## Any other option not listed in the above dict should be written to 
    ## a properties file
    workloads = None
    hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, hstore_options[x]), hstore_options.keys()))
    with cd(HSTORE_DIR):
        prefix = env["hstore.exec_prefix"]
        
        if resetLog4j:
            LOG.info("Reverting log4j.properties")
            run("git checkout %s -- %s" % (env["hstore.git_options"], "log4j.properties"))
        
        if updateJar:
            LOG.info("Updating H-Store %s project jar file" % (project.upper()))
            prefix += " hstore-prepare"
        cmd = "ant %s hstore-benchmark %s" % (prefix, hstore_opts_cmd)
        output = run(cmd)
        
        ## If they wanted a trace file, then we have to ship it back to ourselves
        if trace:
            output = "/tmp/hstore/workloads/%s.trace" % project
            combine_opts = {
                "project":              project,
                "volt.server.memory":   5000,
                "output":               output,
                "workload":             hstore_options["trace"] + "*",
            }
            LOG.debug("Combine %s workload traces into '%s'" % (project.upper(), output))
            combine_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, combine_opts[x]), combine_opts.keys()))
            run("ant workload-combine %s" % combine_opts_cmd)
            workloads = get(output + ".gz")
        ## IF
    ## WITH

    assert output
    return output, workloads
## DEF

## ----------------------------------------------
## write_conf
## ----------------------------------------------
@task
def write_conf(project, removals=[ ], revertFirst=False):
    assert project
    prefix_include = [ 'site', 'client', 'global', 'benchmark' ]
    
    hstoreConf_updates = { }
    hstoreConf_removals = set()
    benchmarkConf_updates = { }
    benchmarkConf_removals = set()
    
    for key in env.keys():
        prefix = key.split(".")[0]
        if not prefix in prefix_include: continue
        if prefix == "benchmark":
            benchmarkConf_updates[key.split(".")[-1]] = env[key]
        else:
            hstoreConf_updates[key] = env[key]
    ## FOR
    for key in removals:
        prefix = key.split(".")[0]
        if not prefix in prefix_include: continue
        if prefix == "benchmark":
            key = key.split(".")[-1]
            assert not key in benchmarkConf_updates, key
            benchmarkConf_removals.add(key)
        else:
            assert not key in hstoreConf_updates, key
            hstoreConf_removals.add(key)
    ## FOR

    toUpdate = [
        ("properties/default.properties", hstoreConf_updates, hstoreConf_removals),
        ("properties/benchmarks/%s.properties" % project, benchmarkConf_updates, benchmarkConf_removals),
    ]
    
    with cd(HSTORE_DIR):
        for _file, _updates, _removals in toUpdate:
            if revertFirst:
                LOG.info("Reverting '%s'" % _file)
                run("git checkout %s -- %s" % (env["hstore.git_options"], _file))
            update_conf(_file, _updates, _removals)
        ## FOR
    ## WITH
## DEF

## ----------------------------------------------
## get_file
## ----------------------------------------------
@task
def get_file(filePath):
    """Retrieve and print the file from the cluster for the given path"""
    sio = StringIO()
    if get(filePath, local_path=sio).failed:
        raise Exception("Failed to retrieve remote file '%s'" % filePath)
    return sio.getvalue()
## DEF

## ----------------------------------------------
## update_conf
## ----------------------------------------------
@task
def update_conf(conf_file, updates={ }, removals=[ ], noSpaces=False):
    LOG.info("Updating configuration file '%s' - Updates[%d] / Removals[%d]", conf_file, len(updates), len(removals))
    contents = get_file(conf_file)
    assert len(contents) > 0, "Configuration file '%s' is empty" % conf_file
    
    first = True
    space = "" if noSpaces else " "
    
    ## Keys we want to update/insert
    for key in sorted(updates.keys()):
        val = updates[key]
        hstore_line = "%s%s=%s%s" % (key, space, space, val)
        regex = "^(?:#)*[\s]*%s[ ]*=[ ]*.*" % re.escape(key)
        m = re.search(regex, contents, re.MULTILINE)
        if not m:
            if first: contents += "\n"
            contents += hstore_line + "\n"
            first = False
            LOG.debug("Added '%s' in %s with value '%s'" % (key, conf_file, val))
        else:
            contents = contents.replace(m.group(0), hstore_line)
            LOG.debug("Updated '%s' in %s with value '%s'" % (key, conf_file, val))
        ## IF
    ## FOR
    
    ## Keys we need to completely remove from the file
    for key in removals:
        if contents.find(key) != -1:
            regex = "%s[ ]*=.*" % re.escape(key)
            contents = re.sub(regex, "", contents)
            LOG.debug("Removed '%s' in %s" % (key, conf_file))
        ## FOR
    ## FOR
    
    sio = StringIO()
    sio.write(contents)
    put(local_path=sio, remote_path=conf_file)
## DEF

## ----------------------------------------------
## enable_debugging
## ----------------------------------------------
@task
def enable_debugging(debug=[], trace=[]):
    conf_file = os.path.join(HSTORE_DIR, "log4j.properties")
    targetLevels = {
        "DEBUG": debug,
        "TRACE": trace,
    }
    
    LOG.info("Updating log4j properties - DEBUG[%d] / TRACE[%d]", len(debug), len(trace))
    contents = get_file(conf_file)
    assert len(contents) > 0, "Configuration file '%s' is empty" % conf_file
    
    # Go through the file and update anything that is already there
    baseRegex = r"(log4j\.logger\.(?:%s))[\s]*=[\s]*(?:INFO|DEBUG|TRACE)(|,[\s]+[\w]+)"
    for level, clazzes in targetLevels.iteritems():
        contents = re.sub(baseRegex % "|".join(map(string.strip, clazzes)),
                          r"\1="+level+r"\2",
                          contents, flags=re.IGNORECASE)
    
    # Then add in anybody that is missing
    first = True
    for level, clazzes in targetLevels.iteritems():
        for clazz in clazzes:
            if contents.find(clazz) == -1:
                if first: contents += "\n"
                contents += "\nlog4j.logger.%s=%s" % (clazz, level)
                first = False
    ## FOR
    
    sio = StringIO()
    sio.write(contents)
    put(local_path=sio, remote_path=conf_file)
## DEF

## ----------------------------------------------
## stop
## ----------------------------------------------
@task
def stop_cluster(terminate=False):
    """Stop all instances in the cluster"""
    __getInstances__()
    
    waiting = [ ]
    for inst in env["ec2.running_instances"]:
        if __getInstanceName__(inst).startswith("hstore-") and inst.state == 'running':
            LOG.info("%s %s" % ("Terminating" if terminate else "Stopping", __getInstanceName__(inst)))
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
## clear_logs
## ----------------------------------------------
@task
def clear_logs():
    """Remove all of the log files on the remote cluster"""
    __getInstances__()
    for inst in env["ec2.running_instances"]:
        if TAG_NFSTYPE in inst.tags and inst.tags[TAG_NFSTYPE] == TAG_NFSTYPE_HEAD:
            print inst.public_dns_name
            ## below 'and' changed from comma by ambell
            with settings(host_string=inst.public_dns_name), settings(warn_only=True):
                LOG.info("Clearning H-Store log files [%s]" % env["hstore.git_branch"])
                log_dir = env.get("site.log_dir", os.path.join(HSTORE_DIR, "obj/logs/sites"))
                run("rm -rf %s/*" % log_dir)
            break
        ## IF
    ## FOR
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

## ----------------------------------------------
## __syncTime__
## ----------------------------------------------
def __syncTime__():
    with settings(warn_only=True):
        sudo("ntpdate-debian -b")
    ## WITH
## DEF

## ----------------------------------------------
## __startInstances__
## ----------------------------------------------        
def __startInstances__(instances_count, ec2_ami, ec2_type, instance_tags):
    LOG.info("Attemping to start %d instances." % (instances_count))
    try:
        resv = ec2_conn.run_instances(ec2_ami,
                                      instance_type=ec2_type,
                                      key_name=env["ec2.keypair"],
                                      min_count=instances_count,
                                      max_count=instances_count,
                                      security_groups=[ env["ec2.security_group"] ],
                                      placement=env["ec2.region"],
                                      placement_group=env["ec2.placement_group"])
    except:
        LOG.error("Failed to start %s instances [%s]" % (ec2_type, ec2_ami))
        raise
    LOG.info("Started %d execution nodes. Waiting for them to come online" % len(resv.instances))
    i = 0
    for inst in resv.instances:
        env["ec2.running_instances"].append(inst)
        env["ec2.all_instances"].append(inst)
        time.sleep(env["ec2.reboot_wait_time"])
        try:
            ec2_conn.create_tags([inst.id], instance_tags[i])
        except:
            logging.error("BUSTED = %d" % (i))
            logging.error(str(instance_tags))
            raise
        __waitUntilStatus__(inst, 'running')
        LOG.info("New Instance '%s' / %s is ready" % (__getInstanceName__(inst), env["ec2.site_type"]))
        i += 1
    ## FOR
    time.sleep(env["ec2.reboot_wait_time"])
    LOG.info("Started %d instances." % len(resv.instances))
## DEF

## ----------------------------------------------
## __waitUntilStatus__
## ----------------------------------------------        
def __waitUntilStatus__(inst, status):
    tries = 10
    while tries > 0 and not inst.update() == status:
        time.sleep(env["ec2.status_wait_time"])
        tries -= 1
    if tries == 0:
        logging.error("Last '%s' status: %s" % (__getInstanceName__(inst), inst.update()))
        raise Exception("Timed out waiting for %s to get to status '%s'" % (__getInstanceName__(inst), status))
    
    ## Just because it's running doesn't mean it's ready
    ## So we'll wait until we can SSH into it
    if status == 'running':
        # Set the timeout
        original_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(env["ec2.reboot_wait_time"])
        host_status = False
        tries = 5
        LOG.info("Testing whether instance '%s' is ready [tries=%d]" % (__getInstanceName__(inst), tries))
        while tries > 0:
            host_status = False
            try:
                transport = paramiko.Transport((inst.public_dns_name, 22))
                transport.close()
                host_status = True
            except:
                pass
            if host_status: break
            time.sleep(env["ec2.reboot_wait_time"])
            tries -= 1
        ## WHILE
        socket.setdefaulttimeout(original_timeout)
        if not host_status:
            raise Exception("Failed to connect to '%s'" % __getInstanceName__(inst))
## DEF

## ----------------------------------------------
## __getInstances__
## ----------------------------------------------        
def __getInstances__():
    if env["ec2.running_instances"]: return env["ec2.running_instances"]
    
    instFilter = { }
    
    ## Virtual Clusters
    if env["ec2.cluster_group"]:
        instFilter["tag:" + TAG_CLUSTER] = env["ec2.cluster_group"]
    
    env["ec2.all_instances"] = [ ]
    env["ec2.running_instances"] = [ ]
    
    reservations = ec2_conn.get_all_instances(filters=instFilter)
    instances = [i for r in reservations for i in r.instances]
    for inst in instances:
        ## Cluster Groups
        if 'Name' in inst.tags and __getInstanceName__(inst).startswith("hstore-"):
            if inst.state != 'terminated': env["ec2.all_instances"].append(inst)
            if inst.state == 'running': env["ec2.running_instances"].append(inst)
    ## FOR
    sortKey = lambda inst: __getInstanceName__(inst)
    env["ec2.all_instances"].sort(key=sortKey)
    env["ec2.running_instances"].sort(key=sortKey)
    return
## DEF

## ----------------------------------------------
## __getRunningInstances__
## ----------------------------------------------        
def __getRunningInstances__(instType):
    __getInstances__()
    instances = [ ]
    for inst in env["ec2.running_instances"]:
        if __getInstanceType__(inst) == instType:
            instances.append(inst)
    ## FOR
    if len(instances) == 0:
        raise Exception("No running instances with type '%s' were found" % instType)
    return instances
## DEF

## ----------------------------------------------
## __getRunningSiteInstances__
## ----------------------------------------------        
def __getRunningSiteInstances__():
    return __getRunningInstances__(env["ec2.site_type"])
## DEF

## ----------------------------------------------
## __getRunningClientInstances__
## ----------------------------------------------        
def __getRunningClientInstances__():
    return __getRunningInstances__(env["ec2.client_type"])
## DEF

## ----------------------------------------------
## __getInstance__
## ----------------------------------------------        
def __getInstance__(public_dns_name):
    LOG.debug("Looking for instance with public_dns_name '%s'" % public_dns_name)
    __getInstances__()
    for inst in env["ec2.all_instances"]:
        LOG.debug("COMPARE '%s' <=> '%s'", inst.public_dns_name, public_dns_name)
        if inst.public_dns_name.strip() == public_dns_name.strip():
            return (inst)
    return (None)
## DEF

## ----------------------------------------------
## __getInstanceTypeCounts__
## ----------------------------------------------        
def __getInstanceTypeCounts__():
    """Return a tuple of the number hosts/sites/partitions/clients that we need"""
    partitionCount = env["hstore.partitions"]
    clientCount = env["client.count"] 
    
    LOG.debug("Partitions Needed: %d" % env["hstore.partitions"])
    LOG.debug("Partitions Per Site: %d" % env["hstore.partitions_per_site"])
    
    if "hstore.num_hosts_round_robin" in env and env["hstore.num_hosts_round_robin"] != None:
        hostCount = int(env["hstore.num_hosts_round_robin"])
        siteCount = hostCount
    else:
        siteCount = int(math.ceil(partitionCount / float(env["hstore.partitions_per_site"])))
        hostCount = int(math.ceil(siteCount / float(env["hstore.sites_per_host"])))
    
    return (hostCount, siteCount, partitionCount, clientCount)
## DEF

## ----------------------------------------------
## __getInstanceName__
## ----------------------------------------------
def __getInstanceName__(inst):
    assert inst
    return (inst.tags['Name'] if 'Name' in inst.tags else '???')
## DEF

## ----------------------------------------------
## __getInstanceType__
## ----------------------------------------------        
def __getInstanceType__(inst):
    attr = inst.get_attribute("instanceType")
    assert attr != None
    assert "instanceType" in attr
    return attr["instanceType"]
## DEF

## ----------------------------------------------
## __getExpectedInstanceType__
## ----------------------------------------------        
def __getExpectedInstanceType__(inst):
    __getInstances__()
    site_offset = __getInstanceTypeCounts__()[0]
    assert site_offset > 0
    if not inst in env["ec2.running_instances"]:
        inst_offset = 0 if len(env["ec2.running_instances"]) < site_offset else site_offset
    else:
        inst_offset = env["ec2.running_instances"].index(inst)
    return env["ec2.site_type"] if inst_offset < site_offset else env["ec2.client_type"]
## DEF

## ----------------------------------------------
## __checkInstanceType__
## ----------------------------------------------        
def __checkInstanceType__(inst):
    expectedType = __getExpectedInstanceType__(inst)
    LOG.debug("Checking whether the instance type for %s is '%s'" % (__getInstanceName__(inst), expectedType))
    attr = inst.get_attribute("instanceType")
    assert attr != None
    assert "instanceType" in attr

    ## Check whether we need to change the instance type before we restart it
    currentType = attr["instanceType"]
    if env["ec2.change_type"] == True and currentType != expectedType:
        if inst.update() == 'running':
            raise Exception("Unable to switch instance type from '%s' to '%s' for %s" % (currentType, expectedType, __getInstanceName__(inst)))
        LOG.info("Switching instance type from '%s' to '%s' for '%s'" % (currentType, expectedType, __getInstanceName__(inst)))
        inst.modify_attribute("instanceType", expectedType)
        return True
    ### IF
    return False
## DEF

## ----------------------------------------------
## __createSecurityGroup__
## ----------------------------------------------
def __createSecurityGroup__():
    security_groups = ec2_conn.get_all_security_groups()
    for sg in security_groups:
        if sg.name == env["ec2.security_group"]:
            return
    ## FOR
    
    LOG.info("Creating security group '%s'" % env["ec2.security_group"])
    sg = ec2_conn.create_security_group(env["ec2.security_group"], 'H-Store Security Group')
    sg.authorize(src_group=sg)
    sg.authorize('tcp', 22, 22, '0.0.0.0/0')
## DEF

## ----------------------------------------------
## __createPlacementGroup__
## ----------------------------------------------
def __createPlacementGroup__():
    groupName = env["ec2.placement_group"]
    assert groupName
    placement_groups = []
    try:
        placement_groups = ec2_conn.get_all_placement_groups(groupnames=[groupName])
    except:
        pass
    if len(placement_groups) == 0:
        LOG.info("Creating placement group '%s'" % groupName)
        ec2_conn.create_placement_group(groupName, strategy='cluster')
    return
## DEF
