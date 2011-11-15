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
import glob
import re
import commands
import subprocess
import logging
import string
from pprint import pprint, pformat

## H-Store Third-Party Libraries
realpath = os.path.realpath(__file__)
basedir = os.path.dirname(realpath)
if not os.path.exists(realpath):
    cwd = os.getcwd()
    basename = os.path.basename(realpath)
    if os.path.exists(os.path.join(cwd, basename)):
        basedir = cwd
sys.path.append(os.path.realpath(os.path.join(basedir, "../third_party/python")))
import argparse

logging.basicConfig(level = logging.INFO,
                    format="PREPROCESSOR [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

def preprocess(path, input, output):
    os.chdir(path)
    for f in map(os.path.realpath, glob.glob("*")):
        if os.path.isdir(f) and f.endswith(".svn") == False:
            preprocess(f, input, output)
        elif f.endswith(".java"):
            logging.debug("Examining '%s'" % f)
            contents = None
            with open(f, "r") as fd:
                contents = fd.readlines()
            dirty = False
            basename = os.path.basename(f)
            for i in range(len(contents)):
                if contents[i].find("__FILE__") != -1:
                    contents[i] = contents[i].replace("__FILE__", basename)
                    dirty = True
                if contents[i].find("__LINE__") != -1:
                    contents[i] = contents[i].replace("__LINE__", str(i+1))
                    dirty = True
            ## FOR
            if dirty:
                logging.debug("INPUT: " + input)
                logging.debug("OUTPUT: " + output)
                logging.debug("ORIG: " + f)
                
                output_path = os.path.join(output, f.replace(input, "")[1:])
                output_dir = os.path.dirname(output_path)
                if not os.path.exists(output_dir): os.makedirs(output_dir)
                
                logging.debug("NEW PATH: " + output_path)
                logging.debug("NEW_DIR: " + output_dir)
                
                #if not os.path.exists(output_path) or os.path.getmtime(output_path) < os.path.getmtime(f):
                logging.debug("Saving updated %s file to %s" % (basename, output_dir))
                with open(output_path, "w") as fd:
                    fd.write("".join(contents))
            ## IF
    ## FOR
    os.chdir("..")
    return
## DEF

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Update static logging prefixes')
    aparser.add_argument('input', help='Source code input directory')
    aparser.add_argument('output', help='Modified source code output directory')
    aparser.add_argument('--debug', action='store_true', help='Enable debug log messages')
    args = vars(aparser.parse_args())
    
    if args['debug']: logging.getLogger().setLevel(logging.DEBUG)
    if not os.path.exists(args['output']): os.makedirs(args['output'])
    
    preprocess(args['input'], os.path.realpath(args['input']), os.path.realpath(args['output']))
## MAIN
    