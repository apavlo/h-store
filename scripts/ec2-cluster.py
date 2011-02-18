#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import with_statement

import os
import sys
import getopt
import commands
import logging
import time
import threading
from pprint import pprint

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stderr)

ALL_INSTANCES = [
    "i-8f6227e3",
    "i-b50b4fd9",
    "i-bd6e29d1",
    "i-bf6e29d3",
    "i-b96e29d5",
    "i-bb6e29d7",
    "i-b56e29d9",
    "i-b76e29db",
    "i-b16e29dd",
    "i-b36e29df",
    "i-5f692e33",
]
NFS_INSTANCE = "i-9bd693f7"

EC2_HSTORE_PEM = os.path.join(os.environ["HOME"], ".ssh/hstore.pem")
SSH_OPTIONS = " ".join([ "-i " + EC2_HSTORE_PEM,
                         "-o UserKnownHostsFile=/dev/null",
                         "-o StrictHostKeyChecking=no" ])
SSH_USER = "ubuntu"

EC2_START_COMMAND = "ec2start"
EC2_STOP_COMMAND = "ec2stop"
EC2_DESCRIBE_COMMAND = "ec2din"

TEMP_HOSTS_FILE = "/tmp/hstore-ec2.hosts"

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
## deployHostsFile
## ==============================================
def deployHostsFile(public_ip, hosts_file, mount_nfs = False):
    ## I'm too lazy to install autofs
    extra = "&& sh -c 'if [ `mount | grep -c hstore-nfs` = 0 ]; then sudo mount /opt/nfs ; fi'" if mount_nfs else ""
    
    inst_commands = [
        "scp %s %s %s@%s:~/" % (SSH_OPTIONS, hosts_file, SSH_USER, public_ip), \
        "ssh %s %s@%s \"sudo mv ~/%s /etc/hosts %s\"" % (SSH_OPTIONS, SSH_USER, public_ip, os.path.basename(hosts_file), extra)
    ]

    for cmd in inst_commands:
        logging.debug(cmd)
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, "%s\n%s" % (cmd, output)
    ## FOR 
    
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Mount NFS
        "mount-nfs",
        ## Limit the number of instances to start/stop
        "limit=",
        ## Enable debug logging
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
    if "debug" in options: logging.getLogger().setLevel(logging.DEBUG)

    assert len(args) > 0
    command = args[0].strip().lower()
    assert command in [ "start", "stop", "update" ]
    instances = args[1:] if len(args) > 1 else ALL_INSTANCES
    if "limit" in options: instances = instances[:int(options["limit"][0])]
    
    if command in [ "start", "stop" ]:
        logging.info("%sing %d instances" % (command.title(), len(instances)))
        cmd = "%s %s" % (EC2_START_COMMAND if command == "start" else EC2_STOP_COMMAND, " ".join(instances))
        (result, output) = commands.getstatusoutput(cmd)
        assert result == 0, "%s\n%s" % (cmd, output)
    
    if command in [ "start", "update" ]:
        if command == "start":
            logging.info("Sleeping for 20 seconds waiting for nodes to start")
            time.sleep(20)
        instance_ips = getInstanceIPs(instances + [ NFS_INSTANCE ])
        pprint(instance_ips)
        
        ## Create Hosts file
        hosts = """
127.0.0.1 localhost

# The following lines are desirable for IPv6 capable hosts
::1 ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts
"""
        
        ## Include the NFS server
        hosts += "\n# HSTORE CLUSTER\n"
        hosts += "%s\thstore-nfs\n" % (instance_ips[NFS_INSTANCE][1])
        
        ctr = 0
        for inst in instances:
            hosts += "%s\thstore%d\n" % (instance_ips[inst][1], ctr)
            ctr += 1
        ## FOR
        
        with open(TEMP_HOSTS_FILE, "w") as f:
            f.write(hosts)
        logging.info("Wrote the new hosts file locally to '%s'" % TEMP_HOSTS_FILE)
        
        ## Upload this mofo to each machine
        ## Note that we always have to update the NFS node first
        threads = [ ]
        for inst in [ NFS_INSTANCE ] + instances:
            public_ip = instance_ips[inst][0]
            logging.info("Updating %s at %s" % (inst, public_ip))
            mount_nfs = (inst != NFS_INSTANCE and "mount-nfs" in options)
            threads.append(threading.Thread(target=deployHostsFile, args=(public_ip, TEMP_HOSTS_FILE, mount_nfs)))
            threads[-1].start()
        ## FOR
        for t in threads:
            t.join()
        os.unlink(TEMP_HOSTS_FILE)
    ## IF

## IF

