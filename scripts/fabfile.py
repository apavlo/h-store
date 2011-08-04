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
import argparse
import subprocess
import time
import threading
import logging
import traceback
import paramiko
import socket
from fabric.api import *
from fabric.contrib.files import *
from pprint import pformat

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stderr)

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

## Fabric Options
env.key_filename = os.path.join(os.environ["HOME"], ".ssh/hstore.pem")
env.user = 'ubuntu'
env.disable_known_hosts = True
env.no_agent = True
env.port = 22

## Default Environmnt
ENV_DEFAULT = {
    ## EC2 Options
    "ec2__type":                    "m1.xlarge",
    "ec2__ami":                     "ami-63be790a",
    "ec2__security_group":          "hstore",
    "ec2__keypair":                 "hstore",
    "ec2__region":                  "us-east-1b",
    "ec2__access_key_id":           os.environ['AWS_ACCESS_KEY_ID'],
    "ec2__secret_access_key":       os.environ['AWS_SECRET_ACCESS_KEY'],
    "ec2__all_instances":           [ ],
    "ec2__running_instances":       [ ],

    ## Site Options
    "site__count":                  4,
    "site__partitions_per_site":    4,
    
    ## Client Options
    "client__count":                1,
    "client__processesperclient":   2,
    "client__txnrate":              -1,
    "client__scalefactor":          100,
    "client__blocking":             False,
    
    ## H-Store SVN Options
    "hstore__svn":                   "https://database.cs.brown.edu/svn/hstore/branches/partitioning-branch",
    "hstore__svn_options":           "--trust-server-cert --non-interactive --ignore-externals",
    "hstore__clean":                 False,
}

has_rcfile = os.path.exists(env.rcfile)
for k, v in ENV_DEFAULT.items():
    if not k in env:
        env[k] = v
## FOR

## Setup EC2 Connection
ec2_conn = boto.connect_ec2(env.ec2__access_key_id, env.ec2__secret_access_key)

## ----------------------------------------------
## benchmark
## ----------------------------------------------
@task
def benchmark():
    __getInstances__()
    client_inst = env.ec2__running_instances[env.site__count]
    assert client_inst
    with settings(host_string=client_inst.public_dns_name):
        for project in ['tpcc', 'tm1', 'auctionmark']:
            exec_benchmark(project=project)
        

## ----------------------------------------------
## start_cluster
## ----------------------------------------------
@task
def start_cluster():
    __getInstances__()
    
    ## First make sure that our security group is setup
    __createSecurityGroup__()
    
    instances_needed = env.site__count + env.client__count
    instances_count = instances_needed
    
    ## Check whether we enough instances already running
    instances_stopped = len(env.ec2__all_instances) - len(env.ec2__running_instances)
    
    logging.info("All Instances: %d" % len(env.ec2__all_instances))
    logging.info("Running Instances: %d" % len(env.ec2__running_instances))
    logging.info("Stopped Instances: %d" % instances_stopped)
    logging.info("Needed Instances: %d" % instances_needed)
    
    instances_needed = max(0, instances_needed - len(env.ec2__running_instances))
    
    ## See whether we can restart a stopped instance
    if instances_needed > 0 and instances_stopped > 0:
        waiting = [ ]
        for inst in env.ec2__all_instances:
            if not inst in env.ec2__running_instances:
                logging.info("Restarting stopped node '%s'" % inst)
                inst.start()
                waiting.append(inst)
                instances_needed -= 1
            if instances_needed == 0: break
        if waiting:
            for inst in waiting:
                __waitUntilStatus__(inst, 'running')
                env.ec2__running_instances.append(inst)
            time.sleep(20)
    ## IF
    
    ## Otherwise, we need to start some new motha truckas
    if instances_needed > 0:
        logging.info("Deploying %d new instances" % instances_needed)
        instance_tags = [ ]
        for i in range(instances_needed):
            tags = { 
                "Name": "hstore-%02d" % i,
                "Type": "nfs-client",
            }
            instance_tags.append(tags)
        ## FOR
        assert instances_needed == len(instance_tags), "%d != %d" % (instances_needed, len(instance_tags))
        __startInstances__(instances_needed, env.ec2__type, instance_tags)
        instances_needed = 0
    ## IF
    assert instances_needed == 0
    assert len(env.ec2__running_instances) >= instances_count, "%d != %d" % (len(env.ec2__running_instances), instances_count)

    ## Check whether we already have an NFS node setup
    for i in range(len(env.ec2__running_instances)):
        inst = env.ec2__running_instances[i]
        if 'Type' in inst.tags and inst.tags['Type'] == 'nfs-node':
            logging.debug("BEFORE: %s" % env.ec2__running_instances)
            env.ec2__running_instances.pop(i)
            env.ec2__running_instances.insert(0, inst)
            logging.debug("AFTER: %s" % env.ec2__running_instances)
            break
    ## FOR
        
    first = True
    for inst in env.ec2__running_instances:
        with settings(host_string=inst.public_dns_name):
            ## Setup the basic environmnt that we need on each node
            setup_env()
            
            ## The first instance will be our NFS head node
            if first:
                setup_nfshead()
                deploy_hstore()
            
            ## Othewise make the rest of the node NFS clients
            else:
                setup_nfsclient()
            first = False
        ## WITH
    ## FOR
## DEF

## ----------------------------------------------
## get_env
## ----------------------------------------------
@task
def get_env():
    run("uname -a")

## ----------------------------------------------
## setup_env
## ----------------------------------------------
@task
def setup_env():
    # Get the release name
    output = run("cat /etc/lsb-release | grep DISTRIB_CODENAME")
    releaseName = output.split("=")[1]
    
    append("/etc/apt/sources.list",
           ["deb http://archive.canonical.com/ubuntu %s partner" % releaseName,
            "deb-src http://archive.canonical.com/ubuntu %s partner" % releaseName ], use_sudo=True)
    sudo("apt-get update")
    sudo("echo sun-java6-jre shared/accepted-sun-dlj-v1-1 select true | /usr/bin/debconf-set-selections")
    sudo("apt-get --yes install subversion gcc g++ sun-java6-jdk valgrind ant htop")
    
    with settings(warn_only=True):
        basename = os.path.basename(env.key_filename)
        files = [ (env.key_filename + ".pub", "/home/%s/.ssh/authorized_keys" % (env.user)),
                  (env.key_filename + ".pub", "/home/%s/.ssh/id_dsa.pub" % (env.user)),
                  (env.key_filename,          "/home/%s/.ssh/id_dsa" % (env.user)), ]
        for local_file, remote_file in files:
            if run("test -f " + remote_file).failed:
                put(local_file, remote_file, mode=0600)
    ## WITH
## DEF

## ----------------------------------------------
## setup_nfshead
## ----------------------------------------------
@task
def setup_nfshead():
    """Deploy the NFS head node"""
    __getInstances__()
    
    hstore_dir = "/home/%s/hstore" % env.user
    with settings(warn_only=True):
        if run("test -d %s" % hstore_dir).failed:
            run("mkdir " + hstore_dir)
    sudo("apt-get --yes install nfs-kernel-server")
    append("/etc/exports", "%s *(rw,async,no_subtree_check)" % hstore_dir, use_sudo=True)
    sudo("exportfs -a")
    sudo("/etc/init.d/portmap start")
    sudo("/etc/init.d/nfs-kernel-server start")
    
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env.ec2__running_instances]))
    ec2_conn.create_tags([inst.id], {'Type': 'nfs-node'})
## DEF

## ----------------------------------------------
## setup_nfsclient
## ----------------------------------------------
@task
def setup_nfsclient():
    """Deploy the NFS client node"""
    __getInstances__()
    
    ## Update the /etc/hosts files to make it easier for us to point
    ## to different NFS head nodes
    hosts_line = "%s hstore-nfs" % env.ec2__running_instances[0].private_ip_address
    if contains("/etc/hosts", "hstore-nfs"):
        sed("/etc/hosts", ".* hstore-nfs", hosts_line, use_sudo=True)
    else:
        append("/etc/hosts", hosts_line, use_sudo=True)
    
    sudo("apt-get --yes install autofs")
    append("/etc/auto.master", "/home/%s/hstore /etc/auto.hstore" % env.user, use_sudo=True)
    append("/etc/auto.hstore", "* hstore-nfs:/home/%s/hstore/&" % env.user, use_sudo=True)
    sudo("/etc/init.d/autofs start")
    
    ## Reboot and wait until it comes back online
    inst = __getInstance__(env.host_string)
    assert inst != None, "Failed to find instance for hostname '%s'\n%s" % (env.host_string, "\n".join([inst.public_dns_name for inst in env.ec2__running_instances]))
    logging.info("Rebooting " + env.host_string)
    reboot(10)
    __waitUntilStatus__(inst, 'running')
    logging.info("NFS Client '%s' is online and ready" % inst)
## DEF

## ----------------------------------------------
## deploy_hstore
## ----------------------------------------------
@task
def deploy_hstore():
    code_dir = os.path.basename(env.hstore__svn)
    with cd("hstore"):
        with settings(warn_only=True):
            if run("test -d %s" % code_dir).failed:
                run("svn checkout %s %s %s" % (env.hstore__svn_options, env.hstore__svn, code_dir))
        with cd(code_dir):
            run("svn update %s" % env.hstore__svn_options)
            if env.hstore__clean:
                run("ant clean-all")
            run("ant build")
## DEF

## ----------------------------------------------
## benchmark
## ----------------------------------------------
@task
def exec_benchmark(project="tpcc"):
    __getInstances__()
    code_dir = os.path.join("hstore", os.path.basename(env.hstore__svn))
    
    hosts = [ ]
    clients = [ ]
    host_id = 0
    partition_id = 0
    site_id = 0
    for inst in env.ec2__running_instances:
        if host_id < env.site__count:
            last_partition = (partition_id + env.site__partitions_per_site - 1)
            hosts.append("%s:%d:%d-%d" % (inst.private_dns_name, site_id, partition_id, last_partition))
            partition_id += env.site__partitions_per_site
            site_id += 1
        else:
            clients.append(inst.private_dns_name)
        host_id += 1

    hstore__options = {
        "coordinator.host":             env.ec2__running_instances[0].private_dns_name,
        "coordinator.delay":            10000,
        "client.host":                  ",".join(clients),
        "client.count":                 env.client__count,
        "client.processesperclient":    env.client__processesperclient,
        "client.txnrate":               env.client__txnrate,
        "client.blocking":              env.client__blocking,
        "client.scalefactor":           env.client__scalefactor,
        "benchmark.warehouses":         partition_id,
        "project":                      project,
        "hosts":                        ",".join(hosts),
    }
    logging.info("H-Store Config:\n" + pformat(hstore__options))
    hstore__opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, hstore__options[x]), hstore__options.keys()))
    with cd(code_dir):
        run("ant hstore-prepare hstore-benchmark %s" % (hstore__opts_cmd))
## DEF

## ----------------------------------------------
## stop
## ----------------------------------------------
@task
def stop_cluster(terminate=False):
    __getInstances__()
    
    waiting = [ ]
    for inst in env.ec2__running_instances:
        if inst.tags['Name'].startswith("hstore-") and inst.state == 'running':
            logging.info("%s %s" % ("Terminating" if terminate else "Stopping", inst.public_dns_name))
            if terminate:
                inst.terminate()
            else:
                inst.stop()
            waiting.append(inst)
    ## FOR
    if waiting:
        logging.info("Halting %d instances" % len(waiting))
        for inst in waiting:
            __waitUntilStatus__(inst, 'terminated' if terminate else 'stopped')
        ## FOR
## DEF

## ----------------------------------------------
## __startInstances__
## ----------------------------------------------        
def __startInstances__(instances_count, instance_type, instance_tags):
    logging.info("Attemping to start %d '%s' execution nodes." % (instances_count, instance_type))
    reservation = ec2_conn.run_instances(env.ec2__ami,
                                         instance_type=instance_type,
                                         key_name=env.ec2__keypair,
                                         min_count=instances_count,
                                         max_count=instances_count,
                                         security_groups=[ env.ec2__security_group ],
                                         placement=env.ec2__region)
    logging.info("Started %d execution nodes. Waiting for them to come online" % len(reservation.instances))
    i = 0
    env.ec2__running_instances = [ ]
    for inst in reservation.instances:
        env.ec2__running_instances.append(inst)
        env.ec2__all_instances.append(inst)
        time.sleep(5)
        try:
            ec2_conn.create_tags([inst.id], instance_tags[i])
        except:
            logging.error("BUSTED = %d" % (i))
            logging.error(str(instance_tags))
            raise
        __waitUntilStatus__(inst, 'running')
        logging.info("READY [%s] %s" % (inst, instance_tags[i]))
        i += 1
    ## FOR
    time.sleep(20)
    logging.info("Started %d instances." % len(reservation.instances))
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
        logging.error("Last %s status: %s" % (inst, inst.update()))
        raise Exception("Timed out waiting for %s to get to status '%s'" % (inst, status))
    
    ## Just because it's running doesn't mean it's ready
    ## So we'll wait until we can SSH into it
    if status == 'running':
        # Set the timeout
        original_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(10)
        host_status = False
        tries = 5
        logging.info("Testing whether instance '%s' is ready [tries=%d]" % (inst.public_dns_name, tries))
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
            raise Exception("Failed to connect to '%s'" % inst.public_dns_name)
## DEF


## ----------------------------------------------
## __getInstances__
## ----------------------------------------------        
def __getInstances__():
    if env.ec2__running_instances: return
    reservations = ec2_conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    for inst in instances:
        if 'Name' in inst.tags and inst.tags['Name'].startswith("hstore-"):
            if inst.state != 'terminated': env.ec2__all_instances.append(inst)
            if inst.state == 'running': env.ec2__running_instances.append(inst)
    ## FOR
## DEF

## ----------------------------------------------
## __getInstance__
## ----------------------------------------------        
def __getInstance__(public_dns_name):
    logging.info("Looking for '%s'" % public_dns_name)
    __getInstances__()
    for inst in env.ec2__all_instances:
        logging.debug("COMPARE '%s' <=> '%s'", inst.public_dns_name, public_dns_name)
        if inst.public_dns_name.strip() == public_dns_name.strip():
            return (inst)
    return (None)
## DEF

## ----------------------------------------------
## __createSecurityGroup__
## ----------------------------------------------
def __createSecurityGroup__():
    security_groups = ec2_conn.get_all_security_groups()
    for sg in security_groups:
        if sg.name == env.ec2__security_group:
            return
    ## FOR
    
    logging.info("Creating security group '%s'" % env.ec2__security_group)
    sg = ec2_conn.create_security_group(env.ec2__security_group, 'H-Store Security Group')
    sg.authorize(src_group=sg)
    sg.authorize('tcp', 22, 22, '0.0.0.0/0')
## DEF
