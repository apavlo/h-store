#!/usr/bin/env python

import os
import sys
import shutil
import subprocess
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
import argparse

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
                                  datefmt='%m-%d-%Y %H:%M:%S')
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## DEFAULT CONFIGURATION
## ==============================================
SVN_REPO = "https://database.cs.brown.edu/svn/hstore-files/"
SVN_OPTS = "--non-interactive --trust-server-cert"

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    aparser = argparse.ArgumentParser(description='Install H-Store Research Files')
    aparser.add_argument('path', help='Installation path')
    aparser.add_argument('--svn-repo', default=SVN_REPO, help='SVN repository')
    aparser.add_argument('--svn-options', default=SVN_OPTS, help='SVN checkout options')
    aparser.add_argument('--overwrite', action='store_true', help='Overwrite existing directory')
    aparser.add_argument('--update', action='store_true', help='Pull down the latest version of files')
    aparser.add_argument('--copy', default=None, help='Copy from existing local copy')
    aparser.add_argument('--symlink', default=None, help='Create a symlink for the directory')
    args = vars(aparser.parse_args())

    copy = False
    if args['copy'] and not args['copy'].startswith("${"):
        if not os.path.exists(args['copy']):
            LOG.warn("Unable to copy from local cache. The directory '%s' does not exist" % args['copy'])
        else:
            LOG.warn("Copying from local cache directory '%s'" % args['copy'])
            copy = True
            
            
    # Symlink
    if args['symlink'] and not args['symlink'].startswith("${"):
        tmp = args['symlink']
        args['symlink'] = args['path']
        args['path'] = tmp
    else:
        args['symlink'] = None
    
    if copy:
        cmd = "cp -rvl %(copy)s %(path)s" % args
    else:
        cmd = "svn %(svn_options)s checkout %(svn_repo)s %(path)s" % args

    if os.path.exists(args['path']) and args['overwrite']:
        LOG.warn("Deleting directory '%s' and reinstalling" % args['path'])
        shutil.rmtree(args['path'])
        
    if not os.path.exists(args['path']):
        # Bombs away!
        LOG.info(cmd)
        subprocess.check_call(cmd, shell=True)
        if not args['symlink'] is None and not os.path.islink(args['symlink']):
            LOG.info("Creating symlink from %(path)s to %(symlink)s" % args)
            os.symlink(args["path"], args["symlink"])
    else:
        LOG.info("Installation directory '%s' already exists. Not overwriting" % os.path.realpath(args['path']))
        
    ## Update
    if args['update']:
        os.chdir(args["path"])
        cmd = "svn %(svn_options)s update" % args
        LOG.info(cmd)
        try:
            output = subprocess.check_output(cmd, shell=True)
            print output
        except:
            # Check whether we need to upgrade the repo
            if output.find("You need to upgrade the working copy") != -1:
                LOG.info("Upgrading repository...")
                upgrade_cmd = "svn %(svn_options)s upgrade" % args
                subprocess.check_call(upgrade_cmd, shell=True)
                LOG.info(upgrade_cmd)
        
                LOG.info(cmd)
                subprocess.check_call(cmd, shell=True)
            else:
                raise
    ## IF

        
## MAIN