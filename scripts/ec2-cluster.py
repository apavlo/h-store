#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
import argparse
import commands
import logging
import time
import threading
from pprint import pprint

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

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stderr)


NFS_INSTANCE = "i-9bd693f7"
AMI_NFS_NODE = "ami-4c659e25"

EC2_HSTORE_PEM = os.path.join(os.environ["HOME"], ".ssh/hstore.pem")
SSH_OPTIONS = " ".join([ "-i " + EC2_HSTORE_PEM,
                         "-o UserKnownHostsFile=/dev/null",
                         "-o StrictHostKeyChecking=no" ])
SSH_USER = "pavlo"

TEMP_HOSTS_FILE = "/tmp/hstore-ec2.hosts"

OPT_MOUNT_NFS = False
OPT_TYPE = "large"
OPT_LIMIT = -1

AWS_ACCESS_KEY_ID = None # TODO
AWS_SECRET_ACCESS_KEY = None # TODO

## ==============================================
## getInstanceIP
## ==============================================
def getInstanceIPs(instances):
    cmd = "%s %s" % (EC2_DESCRIBE_COMMAND, " ".join(instances))
    logging.debug(cmd)
    (result, output) = commands.getstatusoutput(cmd)
    assert result == 0, "%s\n%s" % (cmd, output)
    logging.debug(output)
    
    ## Parse the output and get the ip addesses
    ret = { }
    for line in output.split("\n"):
        if line.startswith("INSTANCE"):
            data = line.split("\t")
            inst = data[1]
            public_ip = data[3]
            private_ip = data[-6]
            ret[inst] = (public_ip, private_ip)
        ## IF
    ## FOR
    return (ret)
## DEF

## ==============================================
## remoteExec
## ==============================================
def remoteExec(public_ip, remoteDir, remoteCmd):
    cmd = "ssh %s %s@%s \"cd %s && %s\"" % (SSH_OPTIONS, SSH_USER, public_ip, remoteDir, remoteCmd)
    logging.info("RemoteExec: " + cmd)
    (result, output) = commands.getstatusoutput(cmd)
    assert result == 0, "%s\n%s" % (cmd, output)
    print output
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='EC2 Deployment Script')
    aparser.add_argument('--directory', default="Documents/H-Store/SVN-Brown/branches/partitioning-branch",
                         help='Instance type')
    aparser.add_argument('--benchmark', default="tpcc",
                         help='Target benchmark for execution')
    aparser.add_argument('--type', default="m1.large",
                         help='Instance type')
    aparser.add_argument('--numhosts',
                         help='Number of instances to deploy')
    aparser.add_argument('--partitions-per-host', type=int,
                         help='Number of partitions to deploy per host')
    aparser.add_argument('--region', default='us-east-1b',
                         help='The name of the region to place all instances in')
    aparser.add_argument('--keypair', default='cs227',
                         help='The name of the key pair with which to launch instances')
    aparser.add_argument('--securitygroups', nargs='*',
                         help='The security group for these instances')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())
    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)

    ## Create a connection
    conn = boto.connect_ec2(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    images = conn.get_all_images(image_ids=[AMI_NFS_NODE])
    
    print "ami:", images[0].id
    print "instance_type:", args['type']
    print "key_name:", args['keypair']
    print "min_count:", args['numhosts']
    print "max_count:", args['numhosts']
    print "security_groups:", args['securitygroups']
    print "placement:", args['region']
    
    ## Deploy the nodes
    reservation = conn.run_instances(AMI_NFS_NODE,
                                     instance_type=args['type'],
                                     key_name=args['keypair'],
                                     min_count=args['numhosts'],
                                     max_count=args['numhosts'],
                                     security_groups=args['securitygroups'],
                                     placement=args['region'])
    for inst in reservation.instances:
        while not inst.update() == 'running':
            time.sleep(5)
        ## WHILE
        print inst
    ## FOR
    
    ## Update the H-Store on the NFS node
    nfs_node_ip = None # TODO
    remoteExec(nfs_node_ip, args['directory'], "svn up")
    
    ## Build the source code on the first worker node
    hosts = [ ]
    partition_id = 0
    site_id = 0
    for inst in reservation.instances:
        hosts.append("%s:%d:%d-%d", inst.private_dns_name, site_id, partition_id, (partition_id + args['partitions_per_host']))
        partition_id += args['partitions_per_host']
        site_id += 1
    cmd = "ant clean-java build hstore-prepare -Dbenchmark=%s -Dhosts=%s" % (args['benchmark'], ",".join(hosts))
    print cmd
    
    remoteExec(reservation.instances[0].public_dns_name, args['directory'], "ant build")
 
    for inst in reservation.instances:
        inst.stop()
        while not inst.update() == 'terminated':
            time.sleep(5)
            print inst, inst.state
        ## WHILE
        print inst
    ## FOR
    
 
    #assert len(args) > 0
    #command = args[0].strip().lower()
    #assert command in [ "start", "stop", "update" ]
    #instances = args[1:] if len(args) > 1 else ALL_INSTANCES[OPT_TYPE]
    #if OPT_LIMIT > 0: instances = instances[:OPT_LIMIT]
    
    #if command in [ "start", "stop" ]:
        #logging.info("%sing %d instances" % (command.title(), len(instances)))
        #cmd = "%s %s" % (EC2_START_COMMAND if command == "start" else EC2_STOP_COMMAND, " ".join(instances))
        #(result, output) = commands.getstatusoutput(cmd)
        #assert result == 0, "%s\n%s" % (cmd, output)
    
    #if command in [ "start", "update" ]:
        #if command == "start":
            #logging.info("Sleeping for 20 seconds waiting for nodes to start")
            #time.sleep(20)
        #instance_ips = getInstanceIPs(instances + [ NFS_INSTANCE ])
        #pprint(instance_ips)
        
        ### Create Hosts file
        #hosts = """
#127.0.0.1 localhost

## The following lines are desirable for IPv6 capable hosts
#::1 ip6-localhost ip6-loopback
#fe00::0 ip6-localnet
#ff00::0 ip6-mcastprefix
#ff02::1 ip6-allnodes
#ff02::2 ip6-allrouters
#ff02::3 ip6-allhosts
#"""
        
        ### Include the NFS server
        #hosts += "\n# HSTORE CLUSTER\n"
        #hosts += "%s\thstore-nfs\n" % (instance_ips[NFS_INSTANCE][1])
        
        #ctr = 0
        #for inst in instances:
            #hosts += "%s\thstore%d\n" % (instance_ips[inst][1], ctr)
            #ctr += 1
        ### FOR
        
        #with open(TEMP_HOSTS_FILE, "w") as f:
            #f.write(hosts)
        #logging.info("Wrote the new hosts file locally to '%s'" % TEMP_HOSTS_FILE)
        
        ### Upload this mofo to each machine
        ### Note that we always have to update the NFS node first
        #threads = [ ]
        #for inst in [ NFS_INSTANCE ] + instances:
            #public_ip = instance_ips[inst][0]
            #logging.info("Updating %s at %s" % (inst, public_ip))
            #mount_nfs = (inst != NFS_INSTANCE and OPT_MOUNT_NFS)
            #threads.append(threading.Thread(target=deployHostsFile, args=(public_ip, TEMP_HOSTS_FILE, mount_nfs)))
            #threads[-1].start()
        ### FOR
        #for t in threads:
            #t.join()
        #os.unlink(TEMP_HOSTS_FILE)
    ### IF

## IF

