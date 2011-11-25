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

'''python executeBenchmarks_by_time.py --endDate=10-25-2011 --startDate=10-25-2011 --url='http://localhost:9000/' --svnRepo='https://database.cs.brown.edu/svn/hstore/trunk/' --svnDir=./dyd/test --environment=hstore_test --executable=myrmidon --branch=default --project=H-store'''

import os
import sys
import time
import datetime
import pysvn
import re
from subprocess import Popen, PIPE, STDOUT

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

RESULT_REGEX = re.compile("Transactions per second:[\s]+([\d]+\.[\d]+)", re.IGNORECASE)

## ==============================================
## executeBenchmark
## ==============================================
def executeBenchmark(benchmark, cid, date, branch, ):
    LOG.info("Preparing benchmark '%s' using SVN revision r%d" % (benchmark, cid))
    #overide the temp_dir when preparing benchmarks.
    prep = 'ant hstore-prepare -Dproject=' + benchmark
    p = Popen(prep, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()

    if output.find('BUILD FAILED') != -1:
        print output
        raise Exception("Unexpected error when executing '%s'" % benchmark)

    LOG.info("Executing benchmark '%s' using SVN revision r%d" % (benchmark, cid))
    run = 'ant hstore-benchmark -Dproject=%s' % benchmark
    p = Popen(run, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
    output = p.stdout.read()

    if output.find('BUILD FAILED') != -1:
        LOG.warn(output)
        raise Exception("Unexpected error")

    match = RESULT_REGEX.search(output)    
    if match == None:
        LOG.warn(output)
        raise Exception("Unable to find transactions per second result")
    
    return float(match.group(1))
## DEF

## ==============================================
## checkoutHStore
## ==============================================
def checkoutHStore(startDate, endDate, svnRepo, svnDir, url, benchmark, environment, executable, branch, project):
    #print 'startDate:'      , info['startDate']
    #print 'endDate:'        , info['endDate']
    #print 'url:'            , info['url']
    #print 'svnRepo:'        , info['svnRepo']
    #print 'svnDir:'         , info['svnDir']
    #print 'environment:'    , info['environment']
    #print 'executable:'     , info['executable']
    #print 'branch:'         , info['branch']
    #print 'project:'        , info['project']
    
    client = pysvn.Client();
    err_cnt = 0
    err = ('Benchmark', 'Version', 'Time')
    while startDate <= endDate :
        tmp = startDate + datetime.timedelta(days=1)
        epoch = float(time.mktime(time.strptime(str(tmp), "%Y-%m-%d")))
        info = client.info2(svnRepo, revision=pysvn.Revision(pysvn.opt_revision_kind.date, epoch), recurse=False)
        cid = info[0][1]["rev"].number 

        LOG.info("Checking out H-Store at SVN revision r%d (%s)" % (cid, startDate))
        client.checkout(svnRepo, svnDir, revision=pysvn.Revision(pysvn.opt_revision_kind.date, epoch), ignore_externals=True)
        os.chdir(svnDir)
        
        LOG.info("Building H-Store at SVN reversion r%d (%s)" % (cid, startDate))
        p = Popen('ant build', shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True)
        output = p.stdout.read()
        if output.find('BUILD FAILED') != -1:
            print output
            LOG.error("Unexpected error when building SVN revision r%d (%s)" % (cid, startDate))
            continue
        
        result = None
        try:
            result = executeBenchmark(benchmark, cid, startDate, branch, project, executable, environment, url)
        except:
            err_cnt = err_cnt + 1;
            err = err, (benchmark, cid, startDate)
        
        if result != None:
            LOG.info("Uploading '%s' results to CODESPEED at %s" % (benchmark, url))
            result = codespeed.Result(
                commitid=cid,
                branch=branch,
                project=project,
                executable=executable,
                benchmark=benchmark,
                environment=environment,
                result_value=result_value,
                revision_date=date,
                result_date=date
            )
            result.upload(data)
        ## IF
        
        os.chdir("..")
        startDate = tmp
    ## WHILE
    return err_cnt, err
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Running H-Store benchmark.')
    
    ## H-STORE OPTIONS
    parser.add_argument('benchmark', choices=[ 'tpcc', 'tm1', 'auctionmark', 'locality', 'airline' ],
                         help='Target benchmark')
                         
    ## SUBVERSION OPTIONS
    parser.add_argument('--endDate',
                        help='The end date of running H-Store subversions! Eg: 12-31-2010.')
    parser.add_argument('--startDate',
                        help='The start date of running H-Store subversions! Eg: 12-31-2010. \n' + 
                            'If there is no input of the start date, ' + 
                            'it will automatically run the versions of last 30 days.')
    parser.add_argument('--svnRepo', default = 'https://database.cs.brown.edu/svn/hstore/trunk/',
                        help='The public repository where to download H-Store.')
    #parser.add_argument('--svnDir', default = './svnDir', 
                        #help='The svn file directory and remember that do not forget to add the "./" in the beginning.')
    
    ## CODESPEED OPTIONS
    parser.add_argument('--url', help='The CODESPEED url.')
    parser.add_argument('--project', default=CODESPEED_PROJECT,
                        help='The name you should create for CODESPEED project.')
    parser.add_argument('--environment', default=CODESPEED_ENVIRONMENT,
                        help='The environment you created earlier in the CODESPEED admin website.')
    parser.add_argument('--executable', default=CODESPEED_EXECUTABLE,
                        help='The executable you should create for CODESPEED project.')
    parser.add_argument('--branch', default=None,
                        help='The branch you should create for CODESPEED project.')
    args = parser.parse_args()

    ## ----------------------------------------------
    
    endDate         = args.endDate
    startDate       = args.startDate
    url             = args.url
    svnRepo         = args.svnRepo
    svnDir          = args.svnDir
    environment     = args.environment
    executable      = args.executable
    branch          = args.branch
    project         = args.project

    endDate = time.strptime(endDate, "%m-%d-%Y")
    endDate = datetime.date(endDate[0],endDate[1],endDate[2])

    if endDate == None:
        start = endDate - datetime.timedelta(days=30)
    else:
        startDate = time.strptime(startDate, "%m-%d-%Y")
        startDate = datetime.date(startDate[0],startDate[1],startDate[2])

    if startDate > endDate:
        raise Exception("The start date must be before the end date")
    
    client = pysvn.Client()
    try:
        info = client.info2(svnRepo, revision=pysvn.Revision(pysvn.opt_revision_kind.date, time.time()), recurse=False)
        LOG.debug("SVN respository '%s' exists" % svnRepo)
    except pysvn.ClientError, ex:
        if 'non-existent' in ex.args[0]:
            raise Exception("SVN repository %s does not exist" % svnRepo)
        else:
            raise Exception("Unexpected error: %s" % ex.args[0])

    cnt, err = checkoutHStore(startDate, endDate, \
                              svnRepo, svnDir, \
                              url, environment, executable, branch, project)
    if cnt != 0:
        print 'There were (%d) errors while running H-Store from (%s) to (%s)' % (cnt, str(start), str(end))
        print err
        for x in err:
            print x
## MAIN