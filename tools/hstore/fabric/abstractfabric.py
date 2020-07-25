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
import logging
import paramiko
import string 
from datetime import datetime
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

## =====================================================================
## LOGGING CONFIGURATION
## =====================================================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## =====================================================================
## DEPLOYMENT CONFIGURATION
## =====================================================================

ENV_DEFAULT = {
    # Fabric Options
    "key_filename":                     os.path.join(os.environ["HOME"], ".ssh/hstore.pem"),
    "user":                             os.environ["USER"],
    "disable_known_hosts":              True,
    "no_agent":                         True,
    "port":                             22,
    
    # Client Options
    "client.count":                     1,
    "client.threads_per_host":          500,
    
    # H-Store Options
    "hstore.basedir":                   None,
    "hstore.git":                       "git://github.com/apavlo/h-store.git",
    "hstore.git_branch":                "main",
    "hstore.git_options":               "",
    "hstore.clean":                     False,
    "hstore.exec_prefix":               "",
    "hstore.partitions":                6,
    "hstore.sites_per_host":            1,
    "hstore.partitions_per_site":       8,
    "hstore.round_robin_partitions":    True,
}

## =====================================================================
## AbstractFabric
## =====================================================================
class AbstractFabric(object):
    def __init__(self, env, envUpdates):
        self.env = env
        self.updateEnv(ENV_DEFAULT)
        self.updateEnv(envUpdates)
        
        self.hstore_dir = os.path.join(self.env["hstore.basedir"], "h-store")
        LOG.debug("HSTORE DIR: %s", self.hstore_dir)
        
        self.running_instances = [ ]
        self.all_instances = [ ]
        
        self.partitionCount = self.env["hstore.partitions"]
        self.clientCount = self.env["client.count"] 
        if not self.env.get("hstore.num_hosts_round_robin", None) is None:
            self.hostCount = int(self.env["hstore.num_hosts_round_robin"])
            self.siteCount = self.hostCount
        else:
            self.siteCount = int(math.ceil(self.partitionCount / float(self.env["hstore.partitions_per_site"])))
            self.hostCount = int(math.ceil(self.siteCount / float(self.env["hstore.sites_per_host"])))
    ## DEF
    
    def updateEnv(self, envUpdates):
        for k, v in envUpdates.iteritems():
            self.env[k] = v
            if v:
                t = type(v)
                LOG.debug("%s [%s] => %s" % (k, t, self.env[k]))
                self.env[k] = t(self.env[k])
        ## FOR
    ## DEF
    
    ## =====================================================================
    ## IMPLEMENTATION API
    ## =====================================================================
    
    def stop_cluster(self, **kwargs):
        """Stop all instances in the cluster"""
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def __startInstances__(self, **kwargs):
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def updateLog4j(self, reset=False, debug=[], trace=[]):
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def sync_time(self):
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def getInstance(self, public_dns_name):
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def getAllInstances(self):
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def getRunningSiteInstances():
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    def getRunningClientInstances():
        raise NotImplementedError("Unimplemented %s" % self.__init__.im_class)
    ## DEF
    
    ## =====================================================================
    ## MAIN API
    ## =====================================================================
    
    ## ----------------------------------------------
    ## get_version
    ## ----------------------------------------------
    def get_version(self, inst):
        """Get the current Git commit id and date in the deployment directory"""
        with settings(host_string=inst.public_dns_name):
            with cd(self.hstore_dir):
                output = run("git log --pretty=format:' %h %at ' -n 1")
            data = map(string.strip, output.split(" "))
            rev_id = str(data[1])
            rev_date = datetime.fromtimestamp(int(data[2])) 
            LOG.info("Revision: %s / %s" % (rev_id, rev_date))
            return (rev_id, rev_date)
        ## WITH
    ## DEF

    def get_file(self, inst, filePath):
        """Retrieve and print the file from the cluster for the given path"""
        sio = StringIO()
        with settings(host_string=inst.public_dns_name):
            if get(filePath, local_path=sio).failed:
                raise Exception("Failed to retrieve remote file '%s'" % filePath)
        return sio.getvalue()
    ## DEF

    ## ---------------------------------------------------------------------
    ## INTERNAL API
    ## ---------------------------------------------------------------------
    
    def exec_benchmark(self, inst, project, \
                             removals=[ ], json=False, build=True, trace=False, \
                             updateJar=True, updateConf=True, updateRepo=False, resetLog4j=False, \
                             extraParams={ } ):
        ## Make sure we have enough instances
        if (self.hostCount + self.clientCount) > len(self.running_instances):
            raise Exception("Needed %d host + %d client instances but only %d are currently running" % (\
                            self.hostCount, self.clientCount, len(self.running_instances)))

        hosts = [ ]
        clients = [ ]
        host_id = 0
        site_id = 0
        partition_id = 0
        partitions_per_site = self.env["hstore.partitions_per_site"]
        
        ## HStore Sites
        site_hosts = set()
        ## Attempt to assign the same number of partitions to nodes
        if self.env.get("hstore.round_robin_partitions", False):
            sites_needed = math.ceil(self.env["hstore.partitions"] / float(partitions_per_site))
            partitions_per_site = math.ceil(self.env["hstore.partitions"] / float(sites_needed))
        
        LOG.debug("Partitions Needed: %d" % self.env["hstore.partitions"])
        LOG.debug("Partitions Per Site: %d" % partitions_per_site)
        LOG.debug("Sites Per Host: %d" % self.env["hstore.sites_per_host"])
        for siteInst in self.getRunningInstances():
            site_hosts.add(siteInst.private_dns_name)
            for i in range(self.env["hstore.sites_per_host"]):
                firstPartition = partition_id
                lastPartition = min(self.env["hstore.partitions"], firstPartition + partitions_per_site)-1
                host = "%s:%d:%d" % (siteInst.private_dns_name, site_id, firstPartition)
                if firstPartition != lastPartition:
                    host += "-%d" % lastPartition
                partition_id += partitions_per_site
                site_id += 1
                hosts.append(host)
                if lastPartition+1 == self.env["hstore.partitions"]: break
            ## FOR (SITES)
            if lastPartition+1 == self.env["hstore.partitions"]: break
        ## FOR
        LOG.debug("Last Partition: %d", lastPartition)
        LOG.debug("Site Hosts: %s" % site_hosts)
        assert len(hosts) > 0

        ## HStore Clients
        for clientInst in self.getRunningInstances():
            if clientInst.private_dns_name in site_hosts: continue
            clients.append(clientInst.private_dns_name)
        ## FOR
        LOG.debug("Client Hosts: %s" % clients)
        assert len(clients) > 0

        ## Make sure the the checkout is up to date
        if updateRepo: 
            LOG.info("Updating H-Store Git checkout")
            self.deploy_hstore(build=build, update=True)
        ## Update H-Store Conf file
        ## Do this after we update the repository so that we can put in our updates
        if updateConf:
            LOG.info("Updating H-Store configuration files")
            self.write_conf(project, removals, revertFirst=True)

        if resetLog4j:
            LOG.info("Reverting log4j.properties")
            self.resetLog4j()

        ## Construct dict of command-line H-Store options
        hstore_options = {
            "client.hosts":                 ",".join(clients),
            "client.count":                 self.env["client.count"],
            "client.threads_per_host":      self.env["client.threads_per_host"],
            "project":                      project,
            "hosts":                        '"%s"' % ";".join(hosts),
        }
        if json: hstore_options["client.output_results_json"] = True
        if trace:
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
        with settings(host_string=inst.public_dns_name):
            with cd(self.hstore_dir):
                prefix = self.env["hstore.exec_prefix"]
                
                if updateJar:
                    LOG.info("Updating H-Store %s project jar file" % (project.upper()))
                    cmd = "ant %s hstore-prepare %s" % (prefix, hstore_opts_cmd)
                    run(cmd)
                    
                    projectFile = os.path.join(self.hstore_dir, project+".jar")
                    for other in self.running_instances:
                        if other == inst: continue
                        run("scp %s %s:%s" % (projectFile, other.public_dns_name, projectFile))
                ## IF
                    
                LOG.info("Running benchmark on %s", inst)
                cmd = "ant %s hstore-benchmark %s" % (prefix, hstore_opts_cmd)
                output = run(cmd)
                
                ## If they wanted a trace file, then we have to ship it back to ourselves
                if trace:
                    output = "/tmp/hstore/workloads/%s.trace" % project
                    combine_opts = {
                        "project":       project,
                        "global.memory": 5000,
                        "output":        output,
                        "workload":      hstore_options["trace"] + "*",
                    }
                    LOG.debug("Combine %s workload traces into '%s'" % (project.upper(), output))
                    combine_opts_cmd = " ".join(map(lambda x: "-D%s=%s" % (x, combine_opts[x]), combine_opts.keys()))
                    run("ant workload-combine %s" % combine_opts_cmd)
                    workloads = get(output + ".gz")
                ## IF
            ## WITH
        ## WITH

        assert output
        return output, workloads
    ## DEF
    
    ## ----------------------------------------------
    ## __setupInstance__
    ## ----------------------------------------------
    def __setupInstance__(self, inst, build=True, update=True):
        need_files = False
        with settings(host_string=inst.public_dns_name):
            with settings(warn_only=True):
                if run("test -d %s" % self.hstore_dir).failed:
                    with cd(os.path.dirname(self.hstore_dir)):
                        LOG.debug("Initializing H-Store source code directory for branch '%s'" % self.env["hstore.git_branch"])
                        run("git clone --branch %s %s %s" % (self.env["hstore.git_branch"], \
                                                            self.env["hstore.git_options"], \
                                                            self.env["hstore.git"]))
                        update = True
                        need_files = True
            ## WITH
            with cd(self.hstore_dir):
                run("git checkout %s" % self.env["hstore.git_branch"])
                if update:
                    LOG.debug("Pulling in latest changes for branch '%s'" % self.env["hstore.git_branch"])
                    run("git checkout -- properties")
                    run("git pull %s" % self.env["hstore.git_options"])
                
                ## Checkout Extra Files
                with settings(warn_only=True):
                    if run("test -d %s" % "files").failed:
                        LOG.debug("Initializing H-Store research files directory for branch '%s'" %  self.env["hstore.git_branch"])
                        run("ant junit-getfiles")
                    elif update:
                        LOG.debug("Pulling in latest research files for branch '%s'" % self.env["hstore.git_branch"])
                        run("ant junit-getfiles-update")
                    ## IF
                ## WITH
                    
                if build:
                    LOG.debug("Building H-Store from source code")
                    if self.env["hstore.clean"]:
                        run("ant clean-all")
                    run("ant build")
                ## WITH
            ## WITH
            run("cd %s" % self.hstore_dir)
        ## WITH
    ## DEF

    ## ----------------------------------------------
    ## __writeConf__
    ## ----------------------------------------------
    def __writeConf__(self, inst, project, removals=[ ], revertFirst=False):
        prefix_include = [ 'site', 'client', 'global', 'benchmark' ]
        
        hstoreConf_updates = { }
        hstoreConf_removals = set()
        benchmarkConf_updates = { }
        benchmarkConf_removals = set()
        
        with settings(host_string=inst.public_dns_name):
            for key in self.env.keys():
                prefix = key.split(".")[0]
                if not prefix in prefix_include: continue
                if prefix == "benchmark":
                    benchmarkConf_updates[key.split(".")[-1]] = self.env[key]
                else:
                    hstoreConf_updates[key] = self.env[key]
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
            
            with cd(self.hstore_dir):
                for _file, _updates, _removals in toUpdate:
                    if revertFirst:
                        LOG.info("Reverting '%s'" % _file)
                        run("git checkout %s -- %s" % (self.env["hstore.git_options"], _file))
                    self.__updateConf__(inst, _file, _updates, _removals)
                ## FOR
            ## WITH
        ## WITH
    ## DEF
    
    ## ----------------------------------------------
    ## __updateConf__
    ## ----------------------------------------------
    def __updateConf__(self, inst, conf_file, updates={ }, removals=[ ], noSpaces=False):
        LOG.info("Updating configuration file '%s' - Updates[%d] / Removals[%d]", conf_file, len(updates), len(removals))
        
        contents = self.get_file(inst, conf_file)
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
        with settings(host_string=inst.public_dns_name):
            put(local_path=sio, remote_path=conf_file)
        ## WITH
    ## DEF
    
    def __resetDebugging__(self, inst):
        with settings(host_string=inst.public_dns_name):
            with cd(self.hstore_dir):
                run("git checkout %s -- %s" % (self.env["hstore.git_options"], "log4j.properties"))
    ## DEF
    
    def __updateLog4j__(self, inst, debug=[], trace=[]):
        LOG.info("Updating log4j properties - DEBUG[%d] / TRACE[%d]", len(debug), len(trace))
        
        conf_file = os.path.join(self.hstore_dir, "log4j.properties")
        targetLevels = {
            "DEBUG": debug,
            "TRACE": trace,
        }
        with settings(host_string=inst.public_dns_name):
            contents = self.get_file(inst, conf_file)
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
        ## WITH
    ## DEF
    
    ## ----------------------------------------------
    ## __clearLogs__
    ## ----------------------------------------------
    def __clearLogs__(self, inst):
        """Remove all of the log files on the remote cluster"""
        with settings(host_string=inst.public_dns_name):
            with settings(warn_only=True):
                LOG.info("Clearing H-Store log files [%s]" % self.env["hstore.git_branch"])
                log_dir = self.env.get("site.log_dir", os.path.join(self.hstore_dir, "obj/logs/sites"))
                run("rm -rf %s/*" % log_dir)
        ## WITH
    ## DEF

## CLASS