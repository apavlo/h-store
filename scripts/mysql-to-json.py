#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import json
import logging
import getopt
import string
import time
from pprint import pprint
import MySQLdb

from hstoretraces import *

logging.basicConfig(level = logging.INFO,
                    format="%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s",
                    datefmt="%m-%d-%Y %H:%M:%S",
                    stream = sys.stdout)

## ==============================================
## GLOBAL CONFIGURATION PARAMETERS
## ==============================================

DB_HOST = None
DB_USER = None
DB_PASSWORD = None
DB_PORT = 3306
DB_NAME = "tpce"
DB_TABLE = "metarelcloud_general_log"

OUTPUT_FILE = "-"
OUTPUT_FD = None

## Catalog Information
CATALOG_PROCEDURES = { }
CATALOG_TABLES = { }

PROCEDURES = { }
PROCEDURE_COUNT = { }

## Procedures that we want to print out debug statements
DEBUG_PROCEDURES = [ ]



STRICT_MATCHING = False

NESTED_SELECT_MARKER = "XYZ"

NESTED_SELECT_REGEX = re.compile("(?:FROM|IN)[\s]+\([\s]*(SELECT .*?)[\s]*\) (?:AS|ORDER)", re.IGNORECASE)

## If a query contains one of these keywords, then just skip it
SKIP_KEYWORDS = map(string.lower, [
    "LAST_INSERT_ID",
    "CONCAT(ex_desc, ",
    
    ## CustomerPosition
    "FROM (%s) AS t, trade, trade_history, status_type" % NESTED_SELECT_MARKER,
    
    ## MarketWatch
    "COALESCE(",
    
    ## DataMaintenance
    "AND s_symb NOT IN",
    "UPDATE watch_item, watch_list",
    "SET ex_desc = INSERT(ex_desc",
])

## ==============================================
## splitNestedSelect
## ==============================================
def splitNestedSelect(sql):
    match = NESTED_SELECT_REGEX.search(sql)
    assert match, "Failed to extract nested SELECT statement from '%s'" % sql
    
    inner = match.group(1).strip()
    outer = sql.replace(inner, NESTED_SELECT_MARKER)
    
    logging.info("Successfully extracted nested SELECT statement")
    logging.info("INNER: %s" % inner)
    logging.info("OUTER: %s" % outer)
    return [outer, inner]
## DEF

## ==============================================
## Auto-Reconnecting DB Wrapper
## ==============================================
class DB:
    conn = None
    
    def __init__(self, host, user, passwd, name, port):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.name = name
        self.port = int(port)

    def connect(self):
        self.conn = MySQLdb.connect(host=self.host, user=self.user, passwd=self.passwd, db=self.name, port=self.port)

    def query(self, sql):
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
        except (AttributeError, MySQLdb.OperationalError):
            self.connect()
            cursor = self.conn.cursor()
            cursor.execute(sql)
        return cursor
## CLASS


## ==============================================
## main
## ==============================================
if __name__ == '__main__':
    _options, args = getopt.gnu_getopt(sys.argv[1:], '', [
        ## Database Options
        "host=",
        "user=",
        "password=",
        "port=",
        "name=",
        "table=",
        ## JSON file for mapping txn params from query params
        "params-map=",
        ## Limit the number of queries fetched per thread
        "limit=",
        ## Trace Output File
        "output=",
        ## Separate Output File per Thread
        "output-per-thread",
        ## JSON Catalog File
        "catalog=",
        ## Force Thread Id
        "thread=",
        ## Take all threads greater than ones provided with --thread=
        "thread-greater",
        ## When strict matching is enabled, the script will fail if a query/txn can't be matched
        "strict",
        ## Enable debug logging
        "debug",
        ## Enable debug logging for a specific procedure
        "debug-proc=",
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
    if "debug-proc" in options: DEBUG_PROCEDURES = list(options["debug-proc"])
    if "strict" in options: STRICT_MATCHING = True

    for key in [ "host", "user", "password", "port", "name", "table" ]:
        varname = "DB_" + key.upper()
        if key in options:
            value = options[key][0]
            assert value
            locals()[varname] = value
        ## IF
        if not locals()[varname]:
            logging.fatal("Missing parameter '%s'" % key)
            sys.exit(1)
    ## FOR

    ## ----------------------------------------------
    ## Load in catalog JSON
    ## ----------------------------------------------
    CATALOG_PROCEDURES, CATALOG_TABLES = loadCatalog(options["catalog"][0])
    
    ## Create all the StoredProcedure objects
    for proc_name in CATALOG_PROCEDURES.keys():
        debug = (proc_name in DEBUG_PROCEDURES)
        PROCEDURES[proc_name] = StoredProcedure(proc_name, CATALOG_PROCEDURES[proc_name], CATALOG_TABLES, debug)
        PROCEDURE_COUNT[proc_name] = 0
    ## FOR

    ## ----------------------------------------------
    ## Load in parameters mapping
    ## ----------------------------------------------
    if "params-map" in options:
        loadParameterMapping(options["params-map"][0], PROCEDURES)

    ## ----------------------------------------------
    ## Connect to DB
    ## ----------------------------------------------
    logging.info("Connecting to %s@%s:%s" % (DB_USER, DB_HOST, DB_PORT))
    db = DB(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_PORT)
    
    if not "thread" in options or "thread-greater" in options:
        sql = "SELECT DISTINCT thread_id FROM %s" % DB_TABLE
        if "thread-greater" in options and "thread" in options:
            options["thread"] = map(int, options["thread"])
            sql += " WHERE thread_id > %d" % max(options["thread"])
        cursor = db.query(sql)
        thread_ids = [ ]
        for row in cursor.fetchall():
            thread_ids.append(int(row[0]))
        ## FOR
    else:
        thread_ids = map(int, options["thread"])
    ## IF
    logging.info("Number of threads: %d" % len(thread_ids))

    ## ----------------------------------------------
    ## Process Thread Workload Logs
    ## ----------------------------------------------
    query_ctr = 0
    for thread_id in thread_ids:
        logging.info("Grabbing queries for thread #%d" % thread_id)
        sql = """
        SELECT event_time, command_type, argument
          FROM %s
         WHERE thread_id = %d
         ORDER BY event_time ASC""" % (DB_TABLE, thread_id)
        if "limit" in options: sql += " LIMIT %d" % int(options["limit"][0])
        cursor = db.query(sql)

        ## ----------------------------------------------
        ## Open output file
        ## ----------------------------------------------
        if "output" in options:
            OUTPUT_FILE = options["output"][0]
           
            if "output-per-thread" in options:
                if OUTPUT_FD: OUTPUT_FD.close()
                OUTPUT_FILE += ".%d" % thread_id
                OUTPUT_FD = open(OUTPUT_FILE, "w")
            elif not OUTPUT_FD:
                OUTPUT_FD = open(OUTPUT_FILE, "w")
        else:
            OUTPUT_FILE = "stdout"
            OUTPUT_FD = sys.stdout
        ## IF
        logging.info("Writing out traces to '%s'" % OUTPUT_FILE)


        ctr = -1
        current_procedure = None    ## StoredProcedure
        current_txn = None          ## TransactionTrace
        need_to_fail = None
        for row in cursor.fetchall():
            ctr += 1
            queries = [ row ]
            
            while len(queries) > 0:
                next_row = queries.pop(0)
                event_time = next_row[0]
                command = next_row[1]
                query = next_row[2]
                query_lower = query.lower()
                
                debug_str = "[%d] %s" % (ctr, " - ".join(map(str, [command, query, event_time])))
                logging.debug(debug_str)
                
                if need_to_fail != None:
                    need_to_fail -= 1
                    if need_to_fail == 0: sys.exit(1)
                    continue

                ## ----------------------------------------------
                ## SKIP
                ## ----------------------------------------------
                if len([ keyword for keyword in SKIP_KEYWORDS if query_lower.find(keyword) != -1]) > 0:
                    logging.info("Query [%d] contains skip keywords. Ignoring..." % ctr)
                    #logging.info(debug_str)
                    continue

                ## ----------------------------------------------
                ## SPECIAL CASE: Nested SELECT
                ## ----------------------------------------------
                elif NESTED_SELECT_REGEX.search(query):
                    for query in splitNestedSelect(query):
                        queries.append([ event_time, command, query ])
                        logging.debug("Queued up extracted query from nested select: %s" % query)
                    ## FOR
                    assert queries
                    continue

                ## ----------------------------------------------
                ## PREPARE
                ## ----------------------------------------------
                elif command == "Prepare":
                    ## Figure out what stored procedure they are executing
                    if query.find("SET TRANSACTION") != -1: continue
                    
                    ## Check whether this is the start of a new procedure
                    #logging.debug("Current Procedure: %s" % str(current_procedure))
                    current_procedure = findCatalogProcedureMatch(query, PROCEDURES)
                    if not current_procedure:
                        msg = "Failed to identify procedure: %s" % debug_str
                        if STRICT_MATCHING: assert current_procedure, msg
                        logging.warn(msg)
                        continue
                    ## IF
                    assert current_procedure
                    current_procedure.addQuery(query)
                    current_procedure = None
                    
                ## ----------------------------------------------
                ## QUERY
                ## ----------------------------------------------
                elif command == "Query":
                    if query in ["commit", "rollback"]:
                        ## If we don't have a current_txn, then that means that we
                        ## weren't able to figure out what procedure the txn was
                        ## We'll just ignore everything
                        if not current_txn.catalog_name:
                            logging.debug("Got commit message but we weren't able to identify the procedure. Ignoring...")
                            query_traces = current_txn.getQueries()
                            assert not query_traces, "Unknown transaction has queries\n" + "\n".join([ "[%d] %s" % (i, query_traces[i].orig_query) for i in range(len(query_traces)) ])
                            
                            if not UNKNOWN_PROCEDURE in PROCEDURE_COUNT: PROCEDURE_COUNT[UNKNOWN_PROCEDURE] = 0
                            PROCEDURE_COUNT[UNKNOWN_PROCEDURE] += 1
                        else:
                            num_queries = len(current_txn.getQueries())
                            assert num_queries > 0, "Txn #%d %s does not have any queries in it" % (current_txn.txn_id, current_txn.catalog_name)
                            query_ctr += num_queries
                            logging.info("Marking txn #%d %s as completed with %d queries [thread=%d]" % (current_txn.txn_id, current_txn.catalog_name, num_queries, thread_id))
                                #debug_str = "\n"
                                #for tuple in current_txn.queries:
                                    #debug_str += str(tuple) + "\n"
                                ### FOR
                                #logging.info(debug_str)
                                #logging.info("-"*32)
                                
                                #debug_str = "\n"
                                #for i in range(len(current_procedure.orig_queries)):
                                    #debug_str += "[%d] %s\n" % (i, current_procedure.orig_queries[i])
                                #logging.info(debug_str)
                                #logging.debug("="*100)
                                #sys.exit(1)
                            ### IF
                            PROCEDURE_COUNT[current_txn.catalog_name] += 1
                            
                            ## Set the final attributes for this txn
                            aborted = (query == "rollback")
                            current_txn.finish(current_procedure, event_time, aborted = aborted)
                            
                            #if aborted or current_txn.catalog_name == "BrokerVolume":
                                #print "-"*100
                                #print
                                #print json.dumps(current_txn.toJSON(), indent=2)
                                #sys.exit(0)
                            
                            
                            ## And then write it out to the trace file and reset ourselves
                            writeJSON(current_txn.toJSON(), OUTPUT_FD)
                        ## IF
                        
                        logging.debug("Reseting current txn and procedure variables")
                        current_txn = None
                        current_procedure = None
                    ## IF
                ## ----------------------------------------------
                ## EXECUTE
                ## ----------------------------------------------
                elif command == "Execute":
                    
                    ## Start of a new txn
                    if query.find("SET TRANSACTION") != -1:
                        assert not current_txn
                        
                        ## Create a new TransactionTrace object, even though at this
                        ## point we don't know what procedure it is
                        logging.debug("Starting a new transaction. Reseting current procedure variable")
                        current_txn = TransactionTrace(nextTraceId(),
                                                       event_time,
                                                       False,
                                                       STRICT_MATCHING)
                        current_procedure = None
                        
                    ## Executing a query in this txn
                    else:
                        # logging.debug(debug_str)
                        assert current_txn, "Executing a query before the txn started: %s" % debug_str
                        
                        ## Figure out what Stored Procedure they have if this is the first query
                        exclude_procs = set()
                        while True:
                            debug = False
                            if exclude_procs:
                                logging.info("Trying to re-match first query in transaction: %s [excluded=%s]" % (query, str(exclude_procs)))
                                debug = True
                                
                            if not current_procedure:
                                logging.debug("Trying to match first query in transaction. Excluded = %s" % str(exclude_procs))
                                current_procedure = findCatalogProcedureMatch(query, PROCEDURES, exclude_procs, debug = debug)
                                
                                if not current_procedure:
                                    logging.warn("Failed to match first query for new txn: %s" % debug_str)
                                    if STRICT_MATCHING:
                                        query_traces = current_txn.getQueries()
                                        assert current_procedure, "Unknown Query for '%s'\n" % (current_txn.catalog_name) + "\n".join([ "[%03d] %s" % (i, query_traces[i].orig_query) for i in range(len(query_traces)) ])
                                    break
                                elif current_txn.catalog_name and current_txn.catalog_name != current_procedure.proc_name:
                                    logging.info("Switched Transaction from %s to %s" % (current_txn.catalog_name, current_procedure.proc_name))
                                    
                                    ## Rebuild the txn
                                    orig_txn = current_txn
                                    current_txn = TransactionTrace(
                                                        orig_txn.id,
                                                        orig_txn.start_timestamp,
                                                        current_procedure.debug,
                                                        STRICT_MATCHING)
                                    current_txn.catalog_name = current_procedure.proc_name
                                    for orig_query_trace in orig_txn.getQueries():
                                        (catalog_name, params) = current_procedure.parseQuery(orig_query_trace.orig_query)
                                        if catalog_name == WRONG_PROCEDURE_MATCH:
                                            logging.fatal("Conflicting Procedure queries:\n %s.%s: %s\n %s.%s: %s" % (orig_txn.name, orig_query_trace.catalog_name, orig_query_trace.orig_query, current_txn.catalog_name, "XXX", query))
                                            sys.exit(1)
                                        logging.info("%s.%s -> %s.%s: %s" % (orig_txn.catalog_name, orig_query_trace.catalog_name, current_txn.catalog_name, catalog_name, orig_query_trace.orig_query))
                                        current_txn.addQuery(QueryTrace(
                                                orig_query_trace.id,
                                                orig_query_trace.orig_query,
                                                catalog_name,
                                                current_txn.catalog_name,
                                                params,
                                                orig_query_trace.start_timestamp))
                                    ## FOR
                                    assert len(orig_txn.getQueries()) == len(current_txn.getQueries())
                                    logging.info("Copied %d queries into new Transaction instance '%s'" % (len(orig_txn.getQueries()), current_txn.catalog_name))
                                logging.debug("Identified query as part of '%s'" % current_procedure.proc_name)
                            ## IF
                            assert current_procedure
                            if not current_txn.catalog_name and current_procedure:
                                current_txn.catalog_name = current_procedure.proc_name
                                logging.debug("Selected current transaction procedure as '%s'" % current_txn.catalog_name)
                            assert current_txn.catalog_name, "Unknown Procedure: %s" % debug_str
                            

                            ## This will retrieve the original parameterized query and extract its 
                            ## parameters from the query that we just executed. We then need to
                            ## take this information and figure out what query it corresponds
                            ## with in our catalog
                            logging.debug("Extracting query catalog name and parameters from query in '%s'" % current_procedure)
                            (catalog_name, params) = current_procedure.parseQuery(query)
                            
                            ## If somebody up above says that we're in the wrong procedure, 
                            ## then we'll loop back around and try to correct our mistake
                            if catalog_name == WRONG_PROCEDURE_MATCH:
                                logging.info("Incorrectly labeled txn as '%s' because query doesn't match anything. Retrying: %s" % (current_txn.catalog_name, query))
                                current_procedure = None
                                exclude_procs.add(current_txn.catalog_name)
                                continue
                            ## IF
                            
                            current_query = QueryTrace(nextTraceId(),
                                                query,
                                                catalog_name,
                                                current_txn.catalog_name,
                                                params,
                                                event_time)
                            current_txn.addQuery(current_query)
                            #if catalog_name == "get" and current_txn.catalog_name == "BrokerVolume":
                                #print "-"*60
                                #print current_procedure.catalog_proc[catalog_name]
                                #print
                                #print current_query.toJSON()
                                #sys.exit(1)
                            
                            break
                        ## WHILE
                    ## IF
                ## ----------------------------------------------
                ## Not needed?
                ## ----------------------------------------------
                else:
                    logging.debug("Unused event: %s" % debug_str)
                    continue
                
                ## IF
            ## WHILE (queries)
        ## FOR (cursor)
        logging.info("Procedure count after thread #%d" % thread_id)
        pprint(PROCEDURE_COUNT)
        print "Total # of Queries:", query_ctr
        if UNKNOWN_PROCEDURE in PROCEDURE_COUNT: logging.warn("Unknown procedures in thread #%d" % thread_id)
    ## FOR

    
## MAIN