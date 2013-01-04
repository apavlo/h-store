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
import time
import pysvn
import re
import shutil
from datetime import datetime, timedelta
from subprocess import Popen, PIPE, STDOUT
from pprint import pprint

import logging

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
import codespeed
import argparse

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

CODESPEED_PROJECT = "H-Store"
CODESPEED_ENVIRONMENT = "default"
CODESPEED_EXECUTABLE = "default"

SVN_REPO_URL = "https://database.cs.brown.edu/svn/hstore/"
SVN_FILE_URL = "https://database.cs.brown.edu/svn/hstore-files/"

SITES_PER_HOST = 1
PARTITIONS_PER_SITE = 7
DATE_FORMAT = "%Y-%m-%d"

RESULT_REGEX = re.compile("Transactions per second:[\s]+([\d]+\.[\d]+)[\s]*(?:\[min:([\d\.]+) \/ max:([\d\.]+)\])?", re.IGNORECASE)
FAILED_REGEX = re.compile("BUILD FAILED")

## ==============================================
## makeClusterConfiguration
## ==============================================
def makeClusterConfiguration(hostnames, partitions, sites_per_host, partitions_per_site):
    assert type(hostnames) == list
    hosts = [ ]
    host_id = 0
    site_id = 0
    partition_id = 0
    
    ## HStore Sites
    LOG.debug("Partitions Per Site: %d" % partitions_per_site)
    site_hosts = set()
    for host in hostnames:
        site_hosts.add(host)
        for i in range(sites_per_host):
            firstPartition = partition_id
            lastPartition = min(partitions, firstPartition + partitions_per_site)-1
            host = "%s:%d:%d" % (host, site_id, firstPartition)
            if firstPartition != lastPartition:
                host += "-%d" % lastPartition
            partition_id += partitions_per_site
            site_id += 1
            hosts.append(host)
            if lastPartition+1 == partitions: break
        ## FOR (SITES)
        if lastPartition+1 == partitions: break
    ## FOR
    assert len(hosts) > 0
    return hosts
## DEF

## ==============================================
## executeBenchmark
## ==============================================
def executeBenchmark(svnWorkDir, revision, benchmark, args):
    LOG.info("Preparing benchmark '%s' using SVN revision r%d" % (benchmark, revision))

    cmdArgs = " ".join(map(lambda x: "-D%s=%s" % (x, args[x]), args.keys()))
    
    try:
        os.chdir(svnWorkDir)
        cmd = "ant hstore-prepare -Dproject=%s %s" % (benchmark, cmdArgs)
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        output = p.stdout.read()

        if output.find('BUILD FAILED') != -1:
            LOG.warn("Failed to execute command:\n" + cmd)
            LOG.warn("Invalid Output::\n" + output)
            raise Exception("Unexpected error when executing '%s'" % benchmark)

        LOG.info("Executing benchmark '%s' using SVN revision r%d" % (benchmark, revision))
        cmd = "ant hstore-benchmark -Dproject=%s %s" % (benchmark, cmdArgs)
        LOG.info("COMMAND:\n" + cmd)
        p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        output = p.stdout.read()

        if output.find('BUILD FAILED') != -1:
            LOG.warn("Failed to execute command:\n" + cmd)
            LOG.warn("Invalid Output::\n" + output)
            raise Exception("Unexpected error")

        match = RESULT_REGEX.search(output)
        if match == None:
            LOG.warn(output)
            raise Exception("Unable to find transactions per second result")
        
        result = match.group(1)
        min_result = match.group(2)
        max_result = match.group(3)
    
        return (result, min_result, max_result)
    finally:
        os.chdir("..")
## DEF

## ==============================================
## checkoutHStore
## ==============================================
def checkoutHStore(svnRepo, svnDir, rev):
    LOG.info("Checking out '%s' at revision r%d into '%s'" % (os.path.basename(svnRepo), rev, svnDir))
    if not os.path.exists(svnDir):
        client.checkout(svnRepo, svnDir, \
                        revision=pysvn.Revision(pysvn.opt_revision_kind.number, rev), \
                        ignore_externals=True)
    ## IF
    os.chdir(svnDir)
    
    LOG.info("Building '%s' at revision r%d in '%s'" % (os.path.basename(svnRepo), rev, svnDir))
    try:
        p = Popen('ant build', shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        output = p.stdout.read()
        if FAILED_REGEX.search(output):
            print output
            raise Exception("Unexpected error when building SVN revision r%d (%s)" % (rev, startDate))
    finally:
        os.chdir("..")
    
    return True
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Running H-Store benchmark.')
    
    ## H-STORE OPTIONS
    aparser.add_argument('hosts', nargs='+', default=['localhost'],
                         help='HStoreSite hosts')
    aparser.add_argument('--benchmark', nargs='+', choices=[ 'tpcc', 'tm1', 'auctionmark', 'locality', 'airline' ],
                         help='Target benchmark')
    aparser.add_argument('--partitions', nargs='+', default=[2,4,8],
                         help='Total number of partitions in cluster')
    aparser.add_argument('--sites-per-host', default=SITES_PER_HOST,
                         help='Number of sites per host')
    aparser.add_argument('--partitions-per-site', default=PARTITIONS_PER_SITE,
                         help='Number of partitions per site')
                         
    ## SUBVERSION OPTIONS
    aparser.add_argument('--endDate',
                        help='The end date of running H-Store subversions! Eg: 12-31-2010.')
    aparser.add_argument('--startDate',
                        help='The start date of running H-Store subversions! Eg: 12-31-2010. \n' + 
                            'If there is no input of the start date, ' + 
                            'it will automatically run the versions of last 30 days.')
    aparser.add_argument('--svn-repo', default=SVN_REPO_URL,
                        help='The public repository where to download H-Store.')
    aparser.add_argument('--svn-branch', default="trunk",
                        help='The branch you should create for CODESPEED project.')
    
    ## CODESPEED OPTIONS
    aparser.add_argument('--codespeed-url',
                         help='The URL of the CODESPEED site to post benchmark results to.')
    aparser.add_argument('--codespeed-project', default=CODESPEED_PROJECT,
                         help='The name of the CODESPEED project used for these results.')
    aparser.add_argument('--codespeed-environment', default=CODESPEED_ENVIRONMENT,
                         help='The name of the CODESPEED environment used for these results.')

    ## MISCELLANEOUS OPTIONS
    aparser.add_argument('--workDir', default='/tmp', 
                         help='The working directory to use for building and executing H-Store.')
    aparser.add_argument('--clean', action='store_true',
                         help='Remove SVN checkouts after executing benchmarks.')
    aparser.add_argument('--overwrite', action='store_true',
                         help='Remove existing SVN checkouts.')
    aparser.add_argument('--debug', action='store_true',
                         help='Enable debug log messages')
    args = vars(aparser.parse_args())
    
    
    ## ----------------------------------------------
    
    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    
    svnRepo          = args["svn_repo"]
    svnBranch        = args["svn_branch"]
    workDir          = args["workDir"]
    codespeedUrl     = args["codespeed_url"]
    codespeedProject = args["codespeed_project"]
    codespeedEnv     = args["codespeed_environment"]

    if not codespeedUrl:
        raise Exception("Missing CODESPEED URL")
    if not codespeedProject:
        raise Exception("Missing CODESPEED Project")
    if not codespeedEnv:
        raise Exception("Missing CODESPEED Environment")
    
    if svnBranch == "trunk":
        svnRepo = os.path.join(SVN_REPO_URL, svnBranch)
    else:
        svnRepo = os.path.join(SVN_REPO_URL, "branches", svnBranch)
    LOG.debug("Target SVN Repository: " + svnRepo)
    
    ## ----------------------------------------------
    
    if "endDate" in args and args["endDate"]:
        endDate = datetime.strptime(args["endDate"], DATE_FORMAT)
    else:
        now = datetime.now()
        endDate = datetime(now.year, now.month, now.day)
    endDate = endDate + timedelta(days=1) - timedelta(seconds=1)
    LOG.debug("endDate: %s" % endDate)
     
    if "startDate" in args and args["startDate"]:
        startDate = datetime.strptime(args["startDate"], DATE_FORMAT)
    else:
        startDate = endDate - timedelta(days=1)
    LOG.debug("startDate: %s" % startDate)

    if startDate > endDate:
        raise Exception("The start date must be before the end date")
    
    startRev = pysvn.Revision(pysvn.opt_revision_kind.date, time.mktime(startDate.timetuple()))
    endRev = pysvn.Revision(pysvn.opt_revision_kind.date, time.mktime(endDate.timetuple()))
    
    client = pysvn.Client()
    try:
        info = client.log(svnRepo, \
                          revision_start=startRev, \
                          revision_end=endRev )
        LOG.debug("SVN respository '%s' exists" % svnRepo)
    except pysvn.ClientError, ex:
        if 'non-existent' in ex.args[0]:
            raise Exception("SVN repository %s does not exist" % svnRepo)
        else:
            raise Exception("Unexpected error: %s" % ex.args[0])
    if not info:
        LOG.warn("No revisions were found in %s from %s to %s" % (svnRepo, startDate, endDate))
        sys.exit(0)
    assert info
    #print pprint(info[-1].items())
    #print pprint(info)
    #sys.exit(0)

    ## For each revision, check it out of the repository and execute all
    ## the benchmarks that we need to
    created = set()
    try:
        for svnLog in info:
            revision = svnLog["revision"].number
            revisionDate = datetime.fromtimestamp(svnLog["date"])
            
            svnWorkDir = os.path.join(workDir, "%s-r%d" % (svnBranch, revision))
            if os.path.exists(svnWorkDir) and args["overwrite"]:
                LOG.info("Removing existing SVN checkout '%s'" % svnWorkDir)
                shutil.rmtree(svnWorkDir)
            created.add(svnWorkDir)
            checkoutHStore(svnRepo, svnWorkDir, revision)
            
            for num_partitions in args['partitions']:
                cluster = makeClusterConfiguration(args['hosts'], num_partitions, SITES_PER_HOST, PARTITIONS_PER_SITE)
                cmdArgs = {
                    "hosts":                        '"' + (";".join(cluster)) + '"',
                    "client.processesperclient":    500,
                    "client.txnrate":               1000,
                    "client.blocking":              True,
                    "client.blocking_concurrent":   1,
                    "global.temp_dir":              "/tmp/hstore-%s" % os.environ['USER'],
                }
                
                os.chdir(workDir)
                for benchmark in args['benchmark']:
                    # Get PartitionPlan from hstore-files
                    pplan = "%s.%s.pplan" % (benchmark, "lns")
                    
                    if not os.path.exists(pplan):
                        pplanURL = os.path.join(SVN_FILE_URL, "designplans/vldb-aug2011/", pplan)
                        LOG.info("Downloading PartitionPlan '%s'" % pplanURL)
                        with open(pplan, "w") as fd:
                            contents = client.cat(pplanURL)
                            fd.write(contents)
                        ## WITH
                    ## IF
                    cmdArgs["partitionplan"] = os.path.realpath(pplan)
                    
                    try:
                        result, min_result, max_result = executeBenchmark(svnWorkDir, revision, benchmark, cmdArgs)
                        assert result

                        LOG.info("Uploading %s/%s results to CODESPEED at %s" % (codespeedExec, benchmark, codespeedUrl))
                        result = codespeed.Result(
                            commitid=revision,
                            branch=svnBranch,
                            benchmark=benchmark,
                            project=codespeedProject,
                            num_partitions=num_partitions,
                            environment=codespeedEnv,
                            result_value=result,
                            revision_date=revisionDate,
                            result_date=datetime.now(),
                            min_result=min_result,
                            max_result=max_result
                        )
                        result.upload(codespeedUrl)
                        
                    except Exception, ex:
                        #LOG.error("Unexpected error: %s" % ex.args[0])
                        raise
                ## FOR (benchmark)
            ## FOR (partitions)
        # FOR (revisions)
    finally:
        if args["clean"]: map(shutil.rmtree, created)
## MAIN