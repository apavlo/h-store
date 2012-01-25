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
import os
import urllib
import urllib2
import json
import logging
import time
from pprint import pprint

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

DEFAULT_SLEEP_TIME = 5

## ==============================================
## Codespeed Result
## ==============================================
class Result(object):
    
    OPTIONAL_KEYS = [ 'min', 'max', 'revision_date', 'result_date' ]
    
    def __init__(self, commitid, branch, project, num_partitions, benchmark, environment, result_value, 
                 min_result=None,    # Optional
                 max_result=None,    # Optional
                 std_dev=None,       # Optional
                 revision_date=None, # Optional 
                 result_date=None    # Optional
    ):
        self.commitid = commitid
        self.branch = branch.replace("-branch", "")
        self.project = project
        self.executable = "%02d-partitions" % num_partitions
        self.benchmark = benchmark
        self.environment = environment
        self.result_value = result_value
        self.min = min_result
        self.max = max_result
        self.std_dev = std_dev
        self.revision_date = revision_date
        self.result_date = result_date
    ## DEF

    def upload(self, url, retry=3):
        assert url
        url = os.path.join(url, 'result/add/')
        
        data = { }
        for key in self.__dict__.keys():
            val = self.__dict__[key]
            if val == None:
                if not key in Result.OPTIONAL_KEYS:
                    raise Exception("The field '%s' is None" % key)
            else:
                data[key] = val
        ## FOR
        pprint(data)
        
        params = urllib.urlencode(data)
        LOG.info("Saving result for executable %s, revision %s, benchmark %s" % (
                 self.executable, self.commitid, self.benchmark))
                
        for attempt in range(retry):
            LOG.info("Uploading results to %s [attempt=%d/%d]" % (url, attempt+1, retry))
            try:
                f = urllib2.urlopen(url, params)
                response = f.read()
                f.close()
                break
            except Exception as ex:
                LOG.warn("Unexpected error: %s" % ex)
                if attempt < retry:
                    LOG.warn("Sleeping for %d seconds and then retrying..." % DEFAULT_SLEEP_TIME)
                    time.sleep(DEFAULT_SLEEP_TIME)
                else:
                    raise
            ## TRY
        ## FOR
        LOG.info("Server (%s) response: %s\n" % (url, response))
        return 0
    ## DEF
## CLASS
