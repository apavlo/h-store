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
import urllib, urllib2, json
import logging

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## Codespeed Result
## ==============================================
class Result(object):
    
    OPTIONAL_KEYS = [ 'revision_date', 'result_date' ]
    
    def __init__(self, commitid, branch, project, executable, benchmark, environment, result_value, 
                 revision_date=None, # Optional. Default is taken either from VCS integration or from current date
                 result_date=None # Optional, default is current date
    ):
        self.commitid = commitid
        self.branch = branch
        self.project = project
        self.executable = executable
        self.benchmark = benchmark
        self.environment = environment
        self.result_value = result_value
    ## DEF

    def upload(self, url):
        for key in self.__dict__.keys():
            val = self.__dict__[key]
            if val == None and not key in OPTIONAL_KEYS:
                raise Exception("The field '%s' is None" % key)
        ## FOR
        
        params = urllib.urlencode(self.__dict__)
        LOG.info("Saving result for executable %s, revision %s, benchmark %s" % (
                self.executable, self.commitid, self.benchmark))
                
        f = urllib2.urlopen(url + 'result/add/', params)
        response = f.read()
        f.close()
        LOG.info("Server (%s) response: %s\n" % (url, response))
        return 0
    ## DEF
## CLASS
