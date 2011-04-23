# -*- coding: utf-8 -*-

import os
import sys
import re
import logging
import json
import string
from pprint import pprint

## ==============================================
## GLOBAL CONFIGURATION PARAMETERS
## ==============================================

## Procedure Name Markers
WRONG_PROCEDURE_MATCH = "*WRONG*"
UNKNOWN_PROCEDURE = "*UNKNOWN*"

## Query types
SQL_TYPES = [ "SELECT", "INSERT", "UPDATE", "DELETE" ]
for i in range(len(SQL_TYPES)):
    key = "SQL_" + SQL_TYPES[i]
    globals()[key] = i
## FOR


## ==============================================
## writeJSON
## ==============================================
def writeJSON(json_data, output):
    logging.debug("Writing out TransactionTrace #%d" % json_data["ID"])
    output.write(json.dumps(json_data) + "\n")
## DEF

## ==============================================
## loadCatalog
## ==============================================
def loadCatalog(json_file, cleanSQL = False):
    with open(json_file, "r") as fd:
        json_data = json.load(fd)
        procs = json_data["PROCEDURES"]
        tables = json_data["TABLES"]
    logging.debug("Read in %d procedures and %d tables from catalog json file '%s'" % (len(procs), len(tables), json_file))
    
    if cleanSQL:
        regex = re.compile("([\s][\s]+)")
        for catalog_proc in procs.values():
            for stmt_name in catalog_proc.keys():
                catalog_proc[stmt_name] = regex.sub(" ", catalog_proc[stmt_name])
            ##
        ## FOR
    ## IF
    return (procs, tables)
## DEF

## ==============================================
## loadParameterMapping
## ==============================================
def loadParameterMapping(json_file, procedures):
    ctr = 0
    with open(json_file, "r") as fd:
        json_data = json.load(fd)
        for proc_name in json_data.keys():
            if not proc_name in procedures:
                logging.warn("Unknown procedure from parameter mapping file: %s" % proc_name)
                continue
            procedures[proc_name].setParameterMapping(json_data[proc_name])
            ctr += 1
        ## FOR
    logging.debug("Read in %d parameter mapping entries from json file '%s'" % (ctr, json_file))
    return
## DEF

## ==============================================
## findCatalogProcedureMatch
## ==============================================
def findCatalogProcedureMatch(query, procedures, exclude_procs = [ ], debug = False):
    logging.debug("Trying to match first query in transaction. Excluded = %s" % str(exclude_procs))
    best_score = 0
    best_matches = [ ]
    for proc_name in procedures.keys():
        if proc_name in exclude_procs: continue
        proc = procedures[proc_name]
        is_match, score =  proc.isProcedure(query)
        
        if is_match and score >= best_score:
            if score > best_score:
                best_matches = [ ]
            ## IF
            best_matches.append(proc_name)
            best_score = score
            if debug: logging.info("Transaction Match: %s (score=%d)" % (proc_name, score))
        ## IF
    ## FOR
    if debug: logging.info("Matches = %s" % str(best_matches))
    
    # assert best_match, "Failed to find StoredProcedure match for query: %s" % query
    if len(best_matches) > 1 and debug:
        logging.debug("Multiple Procedure matches found query: %s" % str(best_matches))
    ## IF
    return procedures[best_matches[0]] if best_matches else None
    
## DEF

## ==============================================
## findCatalogStatementMatch
## ==============================================
def findCatalogStatementMatch(proc_name, catalog_proc, catalog_tables, query, debug = False):
    assert proc_name
    assert catalog_proc
    
    logging.debug("Trying to match query to Statement in procedure '%s'" % proc_name)
    
    ## First grab the information we will need about the query
    query_accessinfo = getAccessInfo(query, catalog_tables)
    query_type = getQueryType(query)
    query_tables = set(query_accessinfo.keys())

    ## Now go through all of Statements for this Procedure and try to find a match
    catalog_match = [ ]
    catalog_match_ctr = 0
    catalog_match_ctr_max = sum([ len(query_accessinfo[key]) for key in query_accessinfo.keys() ])
    
    catalog_queries_by_type = getCatalogStatementsByType(catalog_proc)
    if not query_type in catalog_queries_by_type:
        logging.debug("There are no '%s' queries in procedure '%s'" % (SQL_TYPES[query_type], proc_name))
        return WRONG_PROCEDURE_MATCH, -1
    ## IF

    if debug: logging.info("Number of %s queries in %s: %d" % (SQL_TYPES[query_type], proc_name, len(catalog_queries_by_type[query_type])))
    for catalog_query in catalog_queries_by_type[query_type]:
        query_catalog_sql = catalog_proc[catalog_query]
        query_catalog_accessinfo = getAccessInfo(query_catalog_sql, catalog_tables)
        query_catalog_tables = set(query_catalog_accessinfo.keys())
        
        ## If there are no parameters in the original catalog query, then check whether it's an exact match
        if query_catalog_sql.find("?") == -1 and query_catalog_sql.lower() == query.lower():
            logging.info("Found no-parameter exact match %s.%s" % (proc_name, catalog_query))
            catalog_match = [ catalog_query ]
            catalog_match_ctr = 100
            break
        ## IF
        
        if debug:
            logging.info("-"*32)
            logging.info("%s.%s" % (proc_name, catalog_query))
            logging.info(query + "\n")
            logging.info(query_catalog_sql + "\n")
            logging.info("catalog_tables =  " + str(sorted(query_catalog_tables)))
            logging.info("query_tables   = " + str(sorted(query_tables)))
        
        ## At the very least we expect the matching query to have the same
        ## tables in it. This might not always work
        if query_tables == query_catalog_tables:
            columns_matched = 0
            for table in query_tables:
                if query_accessinfo[table] == query_catalog_accessinfo[table]:
                    columns_matched += len(query_accessinfo[table])
                    if debug: logging.info("Columns Matched = " + str(query_accessinfo[table] & query_catalog_accessinfo[table]))
            ## FOR
            
            if columns_matched > 0 and columns_matched >= catalog_match_ctr:
                if columns_matched == catalog_match_ctr:
                    catalog_match.append(catalog_query)
                else:
                    catalog_match = [ catalog_query ]
                    catalog_match_ctr = columns_matched
                if debug:
                    logging.info("Match: catalog=%s, columns_matched=%d, best=%d" % (catalog_query, columns_matched, catalog_match_ctr))
            ## IF
        ## IF
        if debug: logging.info("")
    ## FOR
    if debug: logging.info("# of Catalog Matches: %d [score=%d]" % (len(catalog_match), catalog_match_ctr))
    
    ## If we have multiple matches, see whether we can find an exact match on the SQL
    if len(catalog_match) > 1:
        logging.debug("Found %d catalog matches. Looking for exact SQL match: %s" % (len(catalog_match), query))
        best_match = None
        best_match_ctr = 0
        query_pieces = map(string.strip, query.lower().replace(",", "").split(" "))
        for catalog_query in catalog_queries_by_type[query_type]:
            catalog_pieces = map(string.strip, catalog_proc[catalog_query].lower().replace(",", "").split(" "))
            ctr = 0
            for i in range(len(query_pieces)):
                if i >= len(query_pieces) or i >= len(catalog_pieces): break
                ctr += 1 if query_pieces[i] == catalog_pieces[i] else 0
            if ctr > best_match_ctr:
                best_match_ctr = ctr
                best_match = catalog_query
        ## FOR
        if best_match != None:
            logging.debug("Found exact SQL match: %s" % best_match)
            catalog_match = [ best_match ]
    ## IF
            
    ## Now we need to figure out what our best match is (if we even got one)
    if len(catalog_match) != 1:
        msg  = "Failed to find catalog match for query in procedure '%s': %s" % (proc_name, query)
        debug = ""
        debug += "  Query:        %s\n" % query
        debug += "  QueryType:    %s\n" % query_type
        debug += "  QueryAccessI: %s\n" % str(query_accessinfo)

        debug += ("-"*32) + "\n"
        debug += "Catalog Statements:\n"
        for catalog_query in catalog_queries_by_type[query_type]:
            debug += "  %-20s%s\n" % (catalog_query + ":", catalog_proc[catalog_query]) 
            debug += "  %s\n\n" % getAccessInfo(catalog_proc[catalog_query], catalog_tables)
            ## FOR
        
        #debug += ("-"*32) + "\n"
        #debug += "Prepared Statements:\n"
        #for i in range(len(catalog_proc.orig_queries)):
            #debug += "  [%d] %s\n\n" % (i, catalog_proc.orig_queries[i])

        ## If no matches, then just return None
        if not catalog_match:
            logging.debug(msg)
            return (WRONG_PROCEDURE_MATCH, -1)
        
        ## Otherwise print all of the matches out so that we can figure it out
        debug += ("-"*32) + "\n"
        debug += "Found Matches:\n"
        for i in range(len(catalog_match)):
            debug += "  [%d] %s\n" % (i, catalog_match[i])
            debug += "      %s\n\n" % catalog_proc[catalog_match[i]]
        logging.warn(msg)
        logging.debug(debug)
        return (None, -1)
    ## IF
    
    if debug: logging.info("Returning Catalog Match: %s.%s [score=%d]" % (proc_name, catalog_match[0], catalog_match_ctr))
    return (catalog_match[0], catalog_match_ctr)
## DEF

## ==============================================
## nextTraceId
## ==============================================
NEXT_TRACE_ID = 1
def nextTraceId():
    global NEXT_TRACE_ID
    ret = NEXT_TRACE_ID
    NEXT_TRACE_ID += 1
    return (ret)
## DEF

## ==============================================
## convertTimestamp
## ==============================================
def convertTimestamp(sql_time):
    if sql_time != None and type(sql_time) != int:
        epoch = int(sql_time.strftime('%s'))
        usec = sql_time.microsecond
        seconds = epoch + (usec / 1000000.0)
        
        ## Convert to nanoseconds
        return (seconds * 1000000000)
    return sql_time
## DEF

## ==============================================
## getQueryType
## ==============================================
def getQueryType(sql):
    base = sql.split(" ")[0].strip().upper()
    name = "SQL_%s" % base
    assert name in globals(), "Invalid SQL Command '%s'" % base
    return globals()[name]
## DEF

## ==============================================
## getAccessInfo
## ==============================================
def getAccessInfo(sql, catalog_tables):
    accessinfo = { }
    sql = sql.lower()
    for table in catalog_tables:
        for table_name in [ " " + table, table + ",", ]:
            if sql.find(table_name.lower()) != -1:
                accessinfo[table] = set()
                for column in catalog_tables[table]["COLUMNS"]:
                    if sql.find(column.lower()) != -1: accessinfo[table].add(column)
                ## FOR
            ## IF
        ## FOR
    ## FOR
    return (accessinfo)
## DEF

## ==============================================
## getCatalogStatementsByType
## ==============================================
def getCatalogStatementsByType(catalog_proc):
    assert catalog_proc
    
    catalog_queries_by_type = { }
    for catalog_query in catalog_proc.keys():
        catalog_sql = catalog_proc[catalog_query]
        query_type = getQueryType(catalog_sql)
        if not query_type in catalog_queries_by_type:
            catalog_queries_by_type[query_type] = [ ]
        catalog_queries_by_type[query_type].append(catalog_query)
    ## FOR
    return (catalog_queries_by_type)
## DEF