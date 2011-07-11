#!/usr/bin/env python
# -*- coding: utf-8 -*-

## Usage:
## ant catalog-export -Dproject=tm1
## gunzip -c ./files/workloads/tm1.trace.gz | ./scripts/json-to-mysql.py --debug --catalog=tm1.json | gzip --best -c > tm1.csv.gz
##
## To load in MySQL:
## LOAD DATA INFILE '/tmp/tm1.csv' INTO TABLE tm1_log FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 LINES;



from __future__ import with_statement

import os
import sys
import re
import json
import logging
import getopt
import string
import time
import csv
from datetime import *
from pprint import pprint

from hstoretraces import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stderr)

## ==============================================
## GLOBAL CONFIGURATION PARAMETERS
## ==============================================

SCHEMA = [
    "event_time",
    "user_host",
    "thread_id",
    "server_id",
    "command_type",
    "argument",
]

COMMAND_TYPE_QUERY      = "Query"
COMMAND_TYPE_CONNECT    = "Connect"
COMMAND_TYPE_PREPARE    = "Prepare"
COMMAND_TYPE_EXECUTE    = "Execute"

OPT_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
OPT_USER_HOST   = "localhost"
OPT_THREAD_ID   = 1000
OPT_SERVER_ID   = 0
OPT_DEBUG       = False
OPT_CATALOG     = None

## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Input trace file (default is stdin)
        "trace=",
        ## MySQL CSV Output File
        "output=",
        ## JSON Catalog File
        "catalog=",
        ## MySQL Defaults
        "thread-id=",
        "user-host=",
        "server-id=",
        ## Take all threads greater than ones provided with --thread=
        "thread-greater",
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
    ## Global Options
    for key in options:
        varname = "OPT_" + key.replace("-", "_").upper()
        if varname in globals() and len(options[key]) == 1:
            orig_val = globals()[varname]
            orig_type = type(orig_val) if orig_val != None else str
            if orig_type == bool:
                val = (options[key][0].lower == "true")
            else: 
                val = orig_type(options[key][0])
            globals()[varname] = val
            logging.debug("%s = %s" % (varname, str(globals()[varname])))
    ## FOR
    if OPT_DEBUG: logging.getLogger().setLevel(logging.DEBUG)

    ## ----------------------------------------------
    ## Load in catalog JSON
    ## ----------------------------------------------
    assert OPT_CATALOG
    CATALOG_PROCEDURES, CATALOG_TABLES = loadCatalog(OPT_CATALOG, cleanSQL = True)

    ## ----------------------------------------------
    ## Setup Output
    ## ----------------------------------------------
    if "output" in options:
        OUTPUT_FILE = options["output"][0]
        OUTPUT_FD = open(OUTPUT_FILE, "w")
    else:
        OUTPUT_FILE = "stdout"
        OUTPUT_FD = sys.stdout
    ## IF
    output = csv.writer(OUTPUT_FD, quoting=csv.QUOTE_ALL)
    logging.debug("Writing MySQL CSV to '%s'" % OUTPUT_FILE)
    output.writerow(SCHEMA)

    ## ----------------------------------------------
    ## Open Workload Trace
    ## ----------------------------------------------
    trace_file = options["trace"][0] if "trace" in options else "-"
    base_time = datetime.now()
    with open(trace_file, "r") if trace_file != "-" else sys.stdin as fd:
        line_ctr, abort_ctr, commit_ctr = (0, 0, 0)
        for line in map(string.strip, fd):
            json_data = json.loads(line)
            txn = TransactionTrace().fromJSON(json_data)
            assert txn, "Failure on line %d" % line_ctr
            txn_start = (base_time + timedelta(microseconds=txn.start*0.001)).strftime(OPT_TIMESTAMP_FORMAT)
            base_row = [
                txn_start,
                OPT_USER_HOST,
                OPT_THREAD_ID,
                OPT_SERVER_ID,
            ]
            
            ## ----------------------------------------------
            ## Connection Setup
            ## We do this here just so that we get the first timestamp of the txn
            ## ----------------------------------------------
            if line_ctr == 0:
                ## Connect
                output.writerow(base_row + [ COMMAND_TYPE_CONNECT, "xyz@%s on %s" % (OPT_USER_HOST, OPT_USER_HOST)])
                
                ## Write SQL Prepare Statements
                for catalog_proc in CATALOG_PROCEDURES.values():
                    for sql in catalog_proc.values():
                        output.writerow(base_row + [ COMMAND_TYPE_PREPARE, sql ])
                    ## FOR
                ## FOR
            ## IF
              
            catalog_proc = CATALOG_PROCEDURES[txn.name]
            assert catalog_proc, "Invalid procedure %s" % (txn.name)
                
            ## Start Transaction
            output.writerow(base_row + [ COMMAND_TYPE_EXECUTE, "SET TRANSACTION ISOLATION LEVEL READ COMMITTED" ])
                
            for query in txn.getQueries():
                catalog_stmt = catalog_proc[query.name]
                assert catalog_stmt, "Invalid query %s.%s" % (txn.name, query.name)
                base_row[0] = (base_time + timedelta(microseconds=query.start*0.001)).strftime(OPT_TIMESTAMP_FORMAT)
                output.writerow(base_row + [ COMMAND_TYPE_EXECUTE, query.populateSQL(catalog_stmt) ])
            ## FOR
            
            ## Stop Transaction
            base_row[0] = (base_time + timedelta(microseconds=txn.stop*0.001)).strftime(OPT_TIMESTAMP_FORMAT)
            if txn.aborted:
                output.writerow(base_row + [ COMMAND_TYPE_QUERY, "rollback" ])
                abort_ctr += 1
            else:
                output.writerow(base_row + [ COMMAND_TYPE_QUERY, "commit" ])
                commit_ctr += 1
            line_ctr += 1
            if line_ctr > 0 and line_ctr % 10000 == 0: logging.info("Transaction #%05d [commit=%d, abort=%d]" % (line_ctr, commit_ctr, abort_ctr))
        ## FOR
        
    ## WITH
    OUTPUT_FD.close()
## MAIN