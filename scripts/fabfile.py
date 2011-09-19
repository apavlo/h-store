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
import re
import math
import time
import subprocess
import threading
import logging
import traceback
import paramiko
import socket
from StringIO import StringIO
from fabric.api import *
from fabric.contrib.files import *
from pprint import pformat

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## BOTO
cwd = os.getcwd()
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
basename = os.path.basename(realpath)
if not os.path.exists(realpath):
   if os.path.exists(os.path.join(cwd, basename)):
      basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
import boto

## List of packages needed on each node
ALL_PACKAGES = [
    'subversion',
    'gcc',
    'g++',
    'sun-java6-jdk',
    'valgrind',
    'ant',
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

NFSTYPE_TAG         = "Type"
NFSTYPE_TAG_HEAD    = "nfs-node"
NFSTYPE_TAG_CLIENT  = "nfs-client"

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
    "ec2.client_type":             "m1.xlarge",
    "ec2.ami":                     "ami-63be790a",
    "ec2.security_group":          "hstore",
    "ec2.keypair":                 "hstore",
    "ec2.region":                  "us-east-1b",
    "ec2.access_key_id":           os.getenv('AWS_ACCESS_KEY_ID'),
    "ec2.secret_access_key":       os.getenv('AWS_SECRET_ACCESS_KEY'),
    "ec2.force_reboot":            True,
    "ec2.all_instances":           [ ],
    "ec2.running_instances":       [ ],

    ## Site Options
    "site.partitions":             16,
    "site.sites_per_host":         1,
    "site.partitions_per_site":    4,
    
    ## Client Options
    "client.count":                1,
    "client.processesperclient":   2,
    "client.txnrate":              -1,
    "client.scalefactor":          10,
    "client.blocking":             False,
    
    ## H-Store Options
    "hstore.svn":                   "https://database.cs.brown.edu/svn/hstore/branches/partitioning-branch",
    "hstore.svn_options":           "--trust-server-cert --non-interactive --ignore-externals",
    "hstore.clean":                 False,
    "hstore.exec_prefix":           "compile",
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

## Setup EC2 Connection
ec2_conn = boto.connect_ec2(env["ec2.access_key_id"], env["ec2.secret_access_key"])

## ----------------------------------------------
## benchmark
## ----------------------------------------------
@task
def benchmark():
    __getInstances__()
    client_inst = __getClientInstance__()
    with settings(host_string=client_inst.public_dns_name):
        for project in ['tpcc', 'tm1', 'airline', 'auctionmark']:
            exec_benchmark(project=project)
    ## WITH
## DEF

## ----------------------------------------------
## start_cluster
## ----------------------------------------------
@task
def start_cluster():
    """Deploy a new H-Store cluster on EC2 using the given configuration"""
    
    ## First make sure that our security group is setup
    __createSecurityGroup__()

    ## Then figure out how many instances we actually need
    hostCount, siteCount, partitionCount, clientCount = __getInstanceTypeCounts__()
    instances_needed = hostCount + clientCount
    instances_count = instances_needed
    LOG.info("HostCount:%d / SiteCount:%d / PartitionCount:%d / ClientCount:%d" % (\
             hostCount, siteCount, partitionCount, clientCount))

    ## Retrieve the list of instances that are already deployed (running or not)
    ## and figure out how many we can reuse versus restart versus create
    __getInstances__()

    ## At least one of the running nodes must be our nfs-node
    nfs_inst = None
    nfs_inst_online = False
    for inst in env["ec2.all_instances"]:
        if inst.tags[NFSTYPE_TAG] == NFSTYPE_TAG_HEAD:
            if inst in env["ec2.running_instances"]:
                assert nfs_inst == None, "Multiple NFS nodes are running"
                nfs_inst_online = True
            nfs_inst = inst
        ## IF
    ## FOR
    if nfs_inst == None:
        LOG.info("No '%s' instance is available. Will create a new one" % NFSTYPE_TAG_HEAD)
    elif not nfs_inst_online:
        LOG.info("'%s' instance %s is offline. Will restart" % (NFSTYPE_TAG_HEAD, __getInstanceName__(nfs_inst)))
    
    ## Check whether we enough instances already running
    instances_running = len(env["ec2.running_instances"])
    instances_stopped = len(env["ec2.all_instances"]) - instances_running
    instances_needed = max(0, instances_needed - instances_running)
    if not nfs_inst_online and instances_needed == 0:
        instances_needed = 1
    orig_running = env["ec2.running_instances"][:]
    
    LOG.info("AllInstances:%d / Running:%d / Stopped:%d / Needed:%d" % ( \
        len(env["ec2.all_instances"]), \
        len(env["ec2.running_instances"]), \
        instances_stopped, \
        instances_needed \
    ))
    
    ## See whether we can restart a stopped instance
    if instances_needed > 0 and instances_stopped > 0:
        waiting = [ ]
        
        ## If we don't have an NFS node, then we need to make sure that we at least
        ## start that one
        if nfs_inst == None:
            for inst in env["ec2.all_instances"]:
                if inst.tags[NFSTYPE_TAG] == NFSTYPE_TAG_HEAD:
                    nfs_inst = inst
                    break
            ## FOR
        ## IF
        
        ## Put the nfs_inst in teh front so that we make sure that we start it correctly
        first = True
        for inst in [ nfs_inst ] + env["ec2.all_instances"]:
            ## The first one could be empty
            if inst == None: 
                first = False
                continue
            ## If we've found our NFS node, then we're allowed to take as 
            ## many more nodes that we need.
            elif nfs_inst != None and instances_needed == 0: break
            ## If we haven't found our NFS node, then we need to leave one 
            ## more reservation so that we can create it
            elif nfs_inst == None and instances_needed == 1: break
            
            if not inst in env["ec2.running_instances"]:
                ## Check whether we need to change the instance type before we restart it
                __checkInstanceType__(inst)
                
                LOG.info("Restarting stopped node '%s' / %s" % (__getInstanceName__(inst), inst.get_attribute("instanceType")['instanceType']))
                inst.start()
                instances_needed -= 1
                if first or (not first and not inst in waiting): waiting.append(inst)
            first = False
        ## FOR
        if waiting:
            for inst in waiting:
                __waitUntilStatus__(inst, 'running')
                env["ec2.running_instances"].append(inst)
            time.sleep(20)
    ## IF
    
    ## Otherwise, we need to start some new motha truckas
    if instances_needed > 0:
        ## Figure out what the next id should be. Not necessary, but just nice...
        next_id = 0
        for inst in env["ec2.all_instances"]:
            if __getInstanceName__(inst).startswith("hstore-"):
                next_id += 1
        ## FOR

        LOG.info("Deploying %d new instances [next_id=%d]" % (instances_needed, next_id))
        instance_tags = [ ]
        marked_nfs = False
        for i in range(instances_needed):
            tags = { 
                "Name": "hstore-%02d" % (next_id),
                NFSTYPE_TAG: NFSTYPE_TAG_CLIENT,
            }
            if nfs_inst == None and not marked_nfs:
                tags[NFSTYPE_TAG] = NFSTYPE_TAG_HEAD
                marked_nfs = True
            
            instance_tags.append(tags)
            next_id += 1
        ## FOR
        assert instances_needed == len(instance_tags), "%d != %d" % (instances_needed, len(instance_tags))
        __startInstances__(instances_needed, instance_tags)
        instances_needed = 0
    ## IF
    assert instances_needed == 0
    assert len(env["ec2.running_instances"]) >= instances_count, "%d != %d" % (len(env["ec2.running_instances"]), instances_count)

    ## Check whether we already have an NFS node setup
    for i in range(len(env["ec2.running_instances"])):
        inst = env["ec2.running_instances"][i]
        if NFSTYPE_TAG in inst.tags and inst.tags[NFSTYPE_TAG] == NFSTYPE_TAG_HEAD:
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
            setup_env()
            need_reboot = __checkInstanceType__(inst) or env["ec2.force_reboot"]
            
            ## The first instance will be our NFS head node
            if first:
                if need_reboot or not nfs_inst_online: setup_nfshead(need_reboot)
                deploy_hstore()
            
            ## Othewise make the rest of the node NFS clients
            else:
                need_reboot = need_reboot or ((nfs_inst_online and inst in orig_running) == False)
                setup_nfsclient(need_reboot)
            first = False
        ## WITH
    ## FOR
## DEF

## ----------------------------------------------
## update_env
## ----------------------------------------------
@task
def update_env():
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
    # Get the release name
    output = run("cat /etc/lsb-release | grep DISTRIB_CODENAME")
    releaseName = output.split("=")[1]
    
    with hide('running', 'stdout'):
        append("/etc/apt/sources.list",
               [ "deb http://archive.canonical.com/ubuntu %s partner" % releaseName,
                 "deb-src http://archive.canonical.com/ubuntu %s partner" % releaseName ], use_sudo=True)
        sudo("apt-get update")
    ## WITH
    sudo("echo sun-java6-jre shared/accepted-sun-dlj-v1-1 select true | /usr/bin/debconf-set-selections")
    sudo("apt-get --yes install %s" % " ".join(ALL_PACKAGES))
    
    with settings(warn_only=True):
        basename = os.path.basename(env.key_filename)
        files = [ (env.key_filename + ".pub", "/home/%s/.ssh/authorized_keys" % (env.user)),
                  (env.key_filename + ".pub", "/home/%s/.ssh/id_dsa.pub" % (env.user)),
                  (env.key_filename,          "/home/%s/.ssh/id_dsa" % (env.user)), ]
        for local_file, remote_file in files:
            if run("test -f " + remote_file).failed:
                put(local_file, remote_file, mode=0600)
    ## WITH
    
    ## We may be running a large cluster, in which case we will have a lot of connections
    handlesAllowed = 2048
    for key in [ "soft", "hard" ]:
        update_line = "* %s nofile %d" % (key, handlesAllowed)
        if not contains("/etc/security/limits.conf", update_line):
            append("/etc/security/limits.conf", update_line, use_sudo=True)
    ## FOR
    
    # Bash Aliases
    code_dir = os.path.join("$HOME", "hstore", os.path.basename(env["hstore.svn"]))
    log_dir = env.get("site.log_dir", "/tmp/hstore/logs/sites")
    aliases = {
        # H-Store Home
        'hh':  'cd ' + code_dir,
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
    
    hstore_dir = "/home/%s/hstore" % env.user
    with settings(warn_only=True):
        if run("test -d %s" % hstore_dir).failed:
            run("mkdir " + hstore_dir)
    ## WITH
## DEF

## ----------------------------------------------
## setup_nfshead
## ----------------------------------------------
@task
def setup_nfshead(rebootInst=True):
    """Deploy the NFS head node"""
    __getInstances__()
    
    hstore_dir = "/home/%s/hstore" % env.user
    sudo("apt-get --yes install %s" % " ".join(NFSHEAD_PACKAGES))
    append("/etc/exports", "%s *(rw,async,no_subtree_check)" % hstore_dir, use_sudo=True)
    sudo("exportfs -a")
    sudo("/etc/init.d/portmap start")
    sudo("/etc/init.d/nfs-kernel-server start")
    
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env["ec2.running_instances"]]))
    ec2_conn.create_tags([inst.id], {NFSTYPE_TAG: NFSTYPE_TAG_HEAD})
    
    ## Reboot and wait until it comes back online
    if rebootInst:
        LOG.info("Rebooting " + env.host_string)
        reboot(10)
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
    
    ## Update the /etc/hosts files to make it easier for us to point
    ## to different NFS head nodes
    hosts_line = "%s hstore-nfs" % env["ec2.running_instances"][0].private_ip_address
    if not contains("/etc/hosts", hosts_line):
        if contains("/etc/hosts", "hstore-nfs"):
            sed("/etc/hosts", ".* hstore-nfs", hosts_line, use_sudo=True)
        else:
            append("/etc/hosts", hosts_line, use_sudo=True)
    
        sudo("apt-get --yes install %s" % " ".join(NFSCLIENT_PACKAGES))
        append("/etc/auto.master", "/home/%s/hstore /etc/auto.hstore" % env.user, use_sudo=True)
        append("/etc/auto.hstore", "* hstore-nfs:/home/%s/hstore/&" % env.user, use_sudo=True)
        sudo("/etc/init.d/autofs start")
    ## IF
    
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env["ec2.running_instances"]]))
    ec2_conn.create_tags([inst.id], {NFSTYPE_TAG: NFSTYPE_TAG_CLIENT})
    
    ## Reboot and wait until it comes back online
    if rebootInst:
        LOG.info("Rebooting " + env.host_string)
        reboot(10)
        __waitUntilStatus__(inst, 'running')
    ## IF
    LOG.info("NFS Client '%s' is online and ready" % __getInstanceName__(inst))
    
    code_dir = os.path.join("hstore", os.path.basename(env["hstore.svn"]))
    run("cd " + code_dir)
## DEF

## ----------------------------------------------
## deploy_hstore
## ----------------------------------------------
@task
def deploy_hstore(build=True):
    code_dir = os.path.basename(env["hstore.svn"])
    with cd("hstore"):
        with settings(warn_only=True):
            if run("test -d %s" % code_dir).failed:
                run("svn checkout %s %s %s" % (env["hstore.svn_options"], env["hstore.svn"], code_dir))
        with cd(code_dir):
            run("svn update %s" % env["hstore.svn_options"])
            if build:
                LOG.debug("Building H-Store from source code")
                if env["hstore.clean"]:
                    run("ant clean-all")
                run("ant build")
    ## WITH
    code_dir = os.path.join("hstore", os.path.basename(env["hstore.svn"]))
    run("cd " + code_dir)
## DEF

## ----------------------------------------------
## exec_benchmark
## ----------------------------------------------
@task
def exec_benchmark(project="tpcc", removals=[ ], json=False, trace=False, updateJar=True, updateConf=True, updateSVN=False):
    __getInstances__()
    code_dir = os.path.join("hstore", os.path.basename(env["hstore.svn"]))
    
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
    for inst in env["ec2.running_instances"]:
        ## DBMS INSTANCES
        if host_id < hostCount:
            for i in range(env["site.sites_per_host"]):
                firstPartition = partition_id
                lastPartition = min(env["site.partitions"], firstPartition + env["site.partitions_per_site"])-1
                host = "%s:%d:%d" % (inst.private_dns_name, site_id, firstPartition)
                if firstPartition != lastPartition:
                    host += "-%d" % lastPartition
                partition_id += env["site.partitions_per_site"]
                site_id += 1
                hosts.append(host)
            ## FOR (SITES)
        ## CLIENT INSTANCES
        else:
            clients.append(inst.private_dns_name)
        host_id += 1
    ## FOR
    assert len(hosts) > 0

    ## Make sure the the checkout is up to date
    if updateSVN: 
        LOG.info("Updating H-Store SVN checkout")
        deploy_hstore(build=False)
    ## Update H-Store Conf file
    if updateConf:
        LOG.info("Updating H-Store configuration files")
        write_conf(project, removals)

    ## Construct dict of command-line H-Store options
    hstore_options = {
        "coordinator.host":             env["ec2.running_instances"][0].private_dns_name,
        "client.host":                  ",".join(clients),
        "client.count":                 env["client.count"],
        "client.processesperclient":    env["client.processesperclient"],
        "benchmark.warehouses":         partition_id,
        "project":                      project,
        "hosts":                        '"%s"' % ";".join(hosts),
    }
    if json: hstore_options["jsonoutput"] = True
    if trace:
        import time
        hstore_options["trace"] = "traces/%s-%d" % (project, time.time())
        LOG.debug("Enabling trace files that will be output to '%s'" % hstore_options["trace"])
    LOG.debug("H-Store Config:\n" + pformat(hstore_options))
    
    ## Any other option not listed in the above dict should be written to 
    ## a properties file
    workloads = None
    hstore_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, hstore_options[x]), hstore_options.keys()))
    with cd(code_dir):
        prefix = env["hstore.exec_prefix"]
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
def write_conf(project, removals=[ ]):
    assert project
    prefix_include = [ 'site', 'coordinator', 'client', 'global', 'benchmark' ]
    code_dir = os.path.join("hstore", os.path.basename(env["hstore.svn"]))
    
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

    with cd(code_dir):
        update_conf("properties/default.properties", hstoreConf_updates, hstoreConf_removals)
        update_conf("properties/benchmarks/%s.properties" % project, benchmarkConf_updates, benchmarkConf_removals)
    ## WITH
## DEF

## ----------------------------------------------
## update_conf
## ----------------------------------------------
@task
def update_conf(conf_file, updates={ }, removals=[ ], noSpaces=False):
    LOG.info("Updating configuration file '%s' - Updates[%d] / Removals[%d]", conf_file, len(updates), len(removals))
    sio = StringIO()
    if get(conf_file, local_path=sio).failed:
        raise Exception("Failed to retrieve conf file '%s'" % conf_file)
    contents = sio.getvalue()
    assert len(contents) > 0
    
    first = True
    space = "" if noSpaces else " "
    
    ## Keys we want to update/insert
    for key in sorted(updates.keys()):
        val = updates[key]
        hstore_line = "%s%s=%s%s" % (key, space, space, val)
        regex = "%s[ ]*=[ ]*.*" % re.escape(key)
        m = re.search(regex, contents)
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
## stop
## ----------------------------------------------
@task
def stop_cluster(terminate=False):
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
## __startInstances__
## ----------------------------------------------        
def __startInstances__(instances_count, instance_tags):
    LOG.info("Attemping to start %d instances." % (instances_count))
    reservation = ec2_conn.run_instances(env["ec2.ami"],
                                         ## I know that this could be wrong but we will change the types
                                         ## later, so this ok to do for now
                                         instance_type=env["ec2.site_type"],
                                         key_name=env["ec2.keypair"],
                                         min_count=instances_count,
                                         max_count=instances_count,
                                         security_groups=[ env["ec2.security_group"] ],
                                         placement=env["ec2.region"])
    LOG.info("Started %d execution nodes. Waiting for them to come online" % len(reservation.instances))
    i = 0
    for inst in reservation.instances:
        env["ec2.running_instances"].append(inst)
        env["ec2.all_instances"].append(inst)
        time.sleep(5)
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
    time.sleep(20)
    LOG.info("Started %d instances." % len(reservation.instances))
## DEF

## ----------------------------------------------
## __waitUntilStatus__
## ----------------------------------------------        
def __waitUntilStatus__(inst, status):
    tries = 6
    while tries > 0 and not inst.update() == status:
        time.sleep(5)
        tries -= 1
    if tries == 0:
        logging.error("Last '%s' status: %s" % (__getInstanceName__(inst), inst.update()))
        raise Exception("Timed out waiting for %s to get to status '%s'" % (__getInstanceName__(inst), status))
    
    ## Just because it's running doesn't mean it's ready
    ## So we'll wait until we can SSH into it
    if status == 'running':
        # Set the timeout
        original_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(10)
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
            time.sleep(10)
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
    reservations = ec2_conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    for inst in instances:
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
    partitionCount = env["site.partitions"]
    siteCount = int(math.ceil(partitionCount / float(env["site.partitions_per_site"])))
    hostCount = int(math.ceil(siteCount / float(env["site.sites_per_host"])))
    clientCount = env["client.count"] 
    return (hostCount, siteCount, partitionCount, clientCount)
## DEF

## ----------------------------------------------
## __getClientInstance__
## ----------------------------------------------        
def __getClientInstance__():
    __getInstances__()
    site_offset = __getInstanceTypeCounts__()[0]
    assert site_offset > 0
    assert site_offset < len(env["ec2.running_instances"]), "%d < %d" % (site_offset, len(env["ec2.running_instances"])) 
    client_inst = env["ec2.running_instances"][site_offset]
    assert client_inst
    return client_inst
## DEF

## ----------------------------------------------
## __getInstanceName__
## ----------------------------------------------        
def __getInstanceName__(inst):
    assert inst
    return (inst.tags['Name'] if 'Name' in inst.tags else '???')
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
