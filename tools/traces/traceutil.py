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
import re
import json
import logging
import getopt
import string
import time
import random
from pprint import pprint

from hstoretraces import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stderr)

## ==============================================
## GLOBAL CONFIGURATION PARAMETERS
## ==============================================


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Input trace file (default is stdin)
        "trace=",
        ## Parameter mapping
        "param-map=",
        ## Transaction Offset
        "offset=",
        ## Transaction Limit
        "limit=",
        ## Don't format txns. Just write them out "raw"
        "raw",
        ## Enable debug logging
        "debug",
        ## Commands Output
        "help",
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

    args = map(string.strip, args)
    trace_file = options["trace"][0] if "trace" in options else "-"
    command = args.pop(0).lower()
    search_key = args[0] if len(args) > 0 else None
    if search_key != None and search_key.isdigit(): search_key = int(search_key)
    
    offset = int(options["offset"][0]) if "offset" in options else None
    # You can't assume the search_key is the same as the offset, since we may reordered these
    # if offset == None and type(search_key) == int: offset = search_key
    offset_first = True
    
    limit = int(options["limit"][0]) if "limit" in options else None
    write_raw = ("raw" in options)
    
    txn_ctr = -1
    txn_abort_ctr = 0
    limit_ctr = 0
    procedure_counts = { }
    query_counts = { }
    current_txn = None
    
    ## Parameter Mapping
    param_mappings = None
    if "param-map" in options:
        json_file = options["param-map"][0]
        with open(json_file, "r") as fd:
            param_mappings = json.load(fd)
        ## WITH
    ## IF

    #logging.debug("Trace: %s" % trace_file)
    logging.debug("Command:    %s" % command)
    logging.debug("Parameters: [%s]" % ",".join(args))
    logging.debug("Options:    [offset=%s, limit=%s, searchKey=%s]" % (str(offset), str(limit), str(search_key)))
    with open(trace_file, "r") if trace_file != "-" else sys.stdin as fd:
        for line in map(string.strip, fd):
            txn_ctr += 1
            if txn_ctr > 0 and txn_ctr % 10000 == 0: logging.debug("Transaction #%05d" % txn_ctr)
            
            ## Offset
            if offset != None and txn_ctr < offset: continue
            elif offset != None and offset_first:
                offset_first = False
                logging.info("Transaction #%05d [offset=%d]" % (txn_ctr, offset))
                
            if limit != None and limit_ctr >= limit: break
            json_data = json.loads(line)
            catalog_name = json_data["NAME"]
            txn_id = int(json_data["TXN_ID"])
            
            ## ----------------------------------------------
            ## GET
            ## ----------------------------------------------
            if command == "get":
                if search_key == None or search_key in [ catalog_name, txn_id ]:
                    txn = TransactionTrace().fromJSON(json_data)
                    assert txn
                    
                    if len(args) > 1 and len(txn.getQueries(args[1])) == 0: continue
                    if write_raw:
                        print line
                    else:
                        print "[%05d] %s" % (txn_ctr, txn.name)
                        pprint(txn.toJSON());
                        #print json.dumps(txn.toJSON(), indent=2)
                    limit_ctr += 1
            ## ----------------------------------------------
            ## FIX TPC-E MarketFeed
            ## ----------------------------------------------
            elif command == "fixmarketfeed":
                txn = TransactionTrace().fromJSON(json_data)
                if current_txn == None:
                    current_txn = txn
                    current_txn_ctr = random.randint(10, 20) - 1
                    continue

                ## Copy parameters
                for txn_param_idx in range(len(txn.params)):
                    if type(txn.params[txn_param_idx]) == list: 
                        for val in txn.params[txn_param_idx]:
                            current_txn.params[txn_param_idx].append(val)
                    ## IF
                ## Copy queries
                for query in txn.getQueries():
                    current_txn.addQuery(query)
                
                current_txn_ctr -= 1
                if current_txn_ctr == 0:
                    if write_raw:
                        print json.dumps(current_txn.toJSON())
                    else:
                        print json.dumps(current_txn.toJSON(), indent=2)
                    current_txn = None
                    limit_ctr += 1
            ## ----------------------------------------------
            ## FIX TM1 GetNewDestination
            ## ----------------------------------------------
            elif command == "fixgetnewdestination":
                txn = TransactionTrace().fromJSON(json_data)
                
                if catalog_name == "GetNewDestination":
                    ## Duplicate the S_ID for the only query
                    query = txn.getQueries()[0]
                    query.params.insert(0, query.params[0])
                ## IF
                    
                if write_raw:
                    print json.dumps(txn.toJSON())
                else:
                    print json.dumps(txn.toJSON(), indent=2)
                limit_ctr += 1
            ## ----------------------------------------------
            ## FIX TXN IDS
            ## ----------------------------------------------
            elif command == "fixtxnids":
                txn = TransactionTrace().fromJSON(json_data)
                if type(txn.txn_id) in (str, unicode) and \
                   not txn.txn_id.isdigit():
                    txn.txn_id = TransactionTrace.NEXT_TXN_ID - 1
                    #print "FIX:", txn.txn_id
                #else:
                    #print "txn_id:", txn.txn_id
                    #print "type:", type(txn.txn_id)
                    #print "digit:", txn.txn_id.isdigit()
                    
                ## IF
                if write_raw:
                    print json.dumps(txn.toJSON())
                else:
                    print json.dumps(txn.toJSON(), indent=2)
                limit_ctr += 1
            ## ----------------------------------------------
            ## FIX
            ## ----------------------------------------------
            elif command == "fixparams":
                assert param_mappings
                if True or search_key in [ catalog_name ]:
                    txn = TransactionTrace().fromJSON(json_data)
                    assert txn
                    
                    txn_param_map = param_mappings[catalog_name]
                    updated = False
                    for txn_param_idx in range(len(txn.params)):
                        if txn.params[txn_param_idx] != None or txn_param_map[txn_param_idx][1] == None: continue
                        if type(txn_param_map[txn_param_idx][1]) != list:
                            txn_param_map[txn_param_idx][1] = [ txn_param_map[txn_param_idx][1] ]
                            txn_param_map[txn_param_idx][2] = [ txn_param_map[txn_param_idx][2] ]
                        for ii in range(len(txn_param_map[txn_param_idx][1])):
                            query_name = txn_param_map[txn_param_idx][1][ii]
                            query_param_idx = txn_param_map[txn_param_idx][2][ii]
                            
                            for query in txn.getQueries(query_name):
                                if query.params[query_param_idx] != None:
                                    logging.debug("Fixed %s parameter #%d using parameter %d from %s" % (catalog_name, txn_param_idx, query_param_idx, query_name))
                                    txn.params[txn_param_idx] = query.params[query_param_idx]
                                    updated = True
                                    break
                            ## FOR (Query Parameters)
                        ## FOR (Txn Parameter Query Mapping)
                    ## FOR (Txn Parameters)
                    if updated:
                        writeJSON(txn.toJSON(), sys.stdout)
                        if not catalog_name in procedure_counts: procedure_counts[catalog_name] = 0
                        procedure_counts[catalog_name] += 1
                        #print "[%05d] %s" % (txn_ctr, txn.name)
                        #print json.dumps(txn.toJSON(), indent=2)
                    else:
                        print line
                else:
                    print line
                limit_ctr += 1
            ## ----------------------------------------------
            ## MARKOV PAPER NEWORDER RESTRUCTURE
            ## ----------------------------------------------
            elif command == "markov":
                if catalog_name != "neworder": continue

                txn = TransactionTrace().fromJSON(json_data)
                assert txn
                w_id = txn.params[0]
    
                ## We want to remove these queries
                remove = [ "getItemInfo", "getDistrict", "getCustomer", "createNewOrder", "incrementNextOrderId" ]
                to_remove = [ ]
                
                max_num_items = 2
                num_partitions = 2
                
                ## And then move createOrder to be right before the first updateStock
                to_move = None
                to_move_before = None
                
                counters = { }
                
                num_items = 2 # random.randint(1, max_num_items)
                
                ## Randomly let some of them abort
                abort_idx = None
                if random.randint(0, 100) == 1:
                    abort_idx = random.randint(0, num_items-1)
                    txn_abort_ctr += 1
                    logging.debug("ABORT: %d / %d" % (txn_abort_ctr, txn_ctr))
                aborted = False

                ## 10% of txns need to use a remote warehouse
                remote_idx = None
                if random.randint(0, 9) == 0:
                    remote_idx = random.randint(0, num_items-1)
                    remote_w_id = w_id + random.randint(1, num_partitions)

                queries = txn.getQueries()
                
                for i in range(len(queries)):
                    q = queries[i]
                    assert q
                    
                    if q.name in remove:
                        to_remove.append(q)
                    elif q.name == "createOrder":
                        assert to_move == None
                        to_move = q
                    elif q.name == "updateStock" and to_move_before == None:
                        to_move_before = q
                    ## All getStockInfo## should be set to getStockInfo01
                    elif q.name.startswith("getStockInfo"):
                        q.name = "getStockInfo01"
                    ## IF

                    if not q.name in counters: counters[q.name] = 0
                    
                    if abort_idx != None and q.name == "updateStock" and counters[q.name] == abort_idx:
                        txn.aborted = True
                        aborted = True
                    if q.name in [ "updateStock", "getStockInfo01" ]:
                        ## Need to make remote
                        if remote_idx != None and counters[q.name] == remote_idx:
                            q.params[-1] = remote_w_id
                        else:
                            q.params[-1] = w_id
                    ## IF
                    
                    counters[q.name] += 1
                    if aborted or (q.name in ["getStockInfo01", "updateStock", "createOrderLine"] and counters[q.name] > num_items):
                        to_remove.append(q)
                    ## IF
                ## FOR
                
                # Update queries
                for q in to_remove:
                    queries.remove(q)
                ## FOR
                if to_move != None:
                    if to_move_before == None: continue
                    queries.remove(to_move)
                    try:
                        idx = queries.index(to_move_before)
                        queries.insert(idx, to_move)
                    except:
                        queries.append(to_move)
                ## IF
                if len(queries) == 1: continue
                
                txn.setQueries(queries)
                
                if write_raw:
                    print json.dumps(txn.toJSON())
                else:
                    pprint(txn.toJSON())
                limit_ctr += 1

            ## ----------------------------------------------
            ## EXTRACT
            ## ----------------------------------------------
            elif command == "extract":
                if catalog_name in args:
                    print line
                    limit_ctr += 1
            ## ----------------------------------------------
            ## FILTER
            ## ----------------------------------------------
            elif command == "filter":
                if not catalog_name in args:
                    print line
                    limit_ctr += 1
            ## ----------------------------------------------
            ## COUNT
            ## ----------------------------------------------
            elif command == "count":
                if len(args) == 0 or catalog_name in args:
                    txn = TransactionTrace().fromJSON(json_data)
                    assert txn
                    
                    procedure_counts[catalog_name] = procedure_counts.get(catalog_name, 0) + 1
                    query_counts[catalog_name] = query_counts.get(catalog_name, 0) + txn.getQueryCount()
                    limit_ctr += 1
                ## IF
            ## ----------------------------------------------
            ## INVALID!
            ## ----------------------------------------------
            else:
                logging.fatal("Invalid command '%s'" % command.upper())
                sys.exit(1)
            ## IF
        ## FOR
    ## WITH
    if procedure_counts:
        if command == "fixparams":
            logging.debug(str(procedure_counts))
        else:
            print "%-20s%10s%10s" % ("Procedure", "Txns", "Queries")
            print "-"*45
            proc_total = 0
            query_total = 0
            line_format = "%-20s%10d%10d"
            
            for key in sorted(procedure_counts.keys()):
                print line_format % (key, procedure_counts[key], query_counts[key])
                proc_total += procedure_counts[key]
                query_total += query_counts[key]
            ## FOR
            print "-"*45
            print line_format % ("TOTAL", proc_total, query_total)
## MAIN