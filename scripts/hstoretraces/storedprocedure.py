# -*- coding: utf-8 -*-

import os
import sys
import re
import logging
import string
from pprint import pprint

from common import *

## ==============================================
## StoredProcedure
## ==============================================
class StoredProcedure:

    COL_NAME_REGEX = "[\d\w\_]+"
    VALUE_REGEX = "\d\w\-\_\.\:\,\!\&\(\)\@\*\/"

    # COL_NAME = (COL_NAME +)? PARAMETER
    BASE_PREDICATE = "%s[\)]?[\s]+(?:[=<>]{1,2}|LIKE|IN)[\s]+(?:%s[\s]+\+[\s]+)?%s"
    
    BASE_PARAM_REGEX_STR = BASE_PREDICATE % ("(" + COL_NAME_REGEX + ")", COL_NAME_REGEX, "(?:\?|INTERVAL)")
    BASE_PARAM_REGEX = re.compile(BASE_PARAM_REGEX_STR)
    
    PARAM_VALUE_REGEX = "(?:['\"]([%s\s]*)['\"]|([%s]*))" % (VALUE_REGEX, VALUE_REGEX)
    
    INSERT_REGEX = re.compile("VALUES[\s]*(\()", re.IGNORECASE)

    def __init__(self, proc_name, catalog_proc, catalog_tables, debug = False):
        self.proc_name = proc_name
        self.catalog_proc = catalog_proc
        self.catalog_tables = catalog_tables
        
        ## The original parameterized query string passed in by PREPARE
        self.orig_queries = [ ]
        
        ## Which Statement in the catalog is each query mapped to
        self.query_catalog = [ ]

        ## Whether the query is a SELECT/INSERT/UPDATE/DELETE
        self.query_type = [ ]

        ## Regex to match a query to one of the original parameterized queries
        self.query_regex = [ ]
        self.query_debug = [ ]

        ## Mappings to extract the parameter values. Each entry in the map
        ## will be a list of regexes to pick apart the values
        self.query_params_regex = [ ]
        self.query_params_debug = [ ]
        
        ## Mapping from query parameters to txn parameters
        ## We have to do this because we don't know what the original params were
        self.txn_params = [ ]
        
        ## The first queries are the strings we use to identify that we are starting
        ## a new transaction for this StoredProcedure
        self.first_queries = [ ]
        self.first_queries_type = [ ]
        for sql in map(string.lower, catalog_proc.values()):
            self.first_queries.append(sql[:20])
            self.first_queries_type.append(getQueryType(sql))
        ## FOR
        
        ## For each query in the catalog, we want to pull out what tables/columns
        ## they access so that we can try to find our corresponding match in the
        ## workload trace (which may have slightly varying SQL)
        self.catalog_accessinfo = { }
        self.catalog_types = { }
        self.catalog_queries_by_type = { }
        
        ## Whether to enable debug output for this StoredProcedure
        self.debug = debug
    ## DEF
    
    def __str__(self):
        return str(self.__unicode__())
    
    def __unicode__(self):
        return self.proc_name
    
    def addQuery(self, query):
        params_regex = [ ]
        params_debug = [ ]

        ## Can we can multiple prepares for the same query? I guess so...
        if query in self.orig_queries:
            logging.debug("Duplicate Query for %s.%s: %s" % (self.proc_name, self.query_catalog[self.orig_queries.index(query)], query))
            return
        ## IF

        ## Extract basic information about this query
        self.orig_queries.append(query)
        self.query_debug.append(re.escape(query).replace("\\?", "['\"]?.*?['\"]?"))
        try:
            self.query_regex.append(re.compile(self.query_debug[-1], re.IGNORECASE))
        except:
            logging.error("Invalid regex for procedure '%s': %s\n%s" % (self.proc_name, self.query_debug[-1], query))
            raise
        self.query_type.append(getQueryType(query))

        ## Figure out query from the catalog it belongs to
        ## It's hack but we will base it on the query type plus the number
        ## of columns and tables that are referenced in the table 
        assert self.proc_name
        catalog_query, score = findCatalogStatementMatch(self.proc_name, self.catalog_proc, self.catalog_tables, query)
        #assert catalog_query, "No catalog match for query in '%s'" % self.proc_name
        self.query_catalog.append(catalog_query)
        catalog_sql = self.catalog_proc[catalog_query]
        assert catalog_sql

        ## SELECT/DELETE/UPDATE
        if self.query_type[-1] != SQL_INSERT:
            ## Find everything that looks like "COLUMN = ?"
            ## This probably isn't going to pick everything up, so we are going
            ## to need to refine it
            for match in StoredProcedure.BASE_PARAM_REGEX.finditer(catalog_sql):
                ## We are going to create a new regex using the found column name
                ## so that we can extract the value portion of the predicate
                col = re.escape(match.group(1))
                param_regex = StoredProcedure.BASE_PREDICATE % (col, col, StoredProcedure.PARAM_VALUE_REGEX)
                params_regex.append(re.compile(param_regex, re.IGNORECASE))
                params_debug.append(param_regex)
            ## FOR
        ## INSERT
        else:
            idx = StoredProcedure.INSERT_REGEX.search(catalog_sql).end(1)
            assert idx != -1
            ctr = 0
            for value in catalog_sql[idx:].split(","):
                value = value.strip()
                if value.startswith("?"):
                    params_regex.append(ctr)
                    ctr += 1
            ## FOR
            
            #if self.debug: 
                #logging.info("OrigQuery = %s" % catalog_sql[idx:])
                #logging.info("Params = %s" % str(value_keys))
                #logging.info("%s.%s: %s" % (self.proc_name, catalog_query, str(params_regex)))
        ## FOR
        self.query_params_regex.append(params_regex)
        self.query_params_debug.append(params_debug)

        if self.debug: logging.debug("Add parameterized query for %s: %s" % (self.proc_name, query))
        if self.debug: logging.debug("Parameterized: %s" % catalog_sql)
    ## DEF
    
    def parseQuery(self, query, force_query_idx = None, retry = True):
        if self.debug: logging.info("Parsing query [force_query_idx=%s, retry=%s]: %s" % (str(force_query_idx), str(retry), query))
        params = [ ]
        orig_query = None
        query_type = getQueryType(query)
        
        ## HACK to fix escaped apostrophes
        query = query.replace("\\'", "-")
        
        ## If there are no parameters, then we want to do an exact match
        #if query != SQL_INSERT and query.find("?") == -1:
            #for query_idx in range(len(self.orig_queries)):
                #if self.orig_queries[query_idx].lower() == query.lower():
                    #logging.debug("Matched no-parameter query '%s'" % query)
                    #force_query_idx = query_idx
                    #break
                ### IF
            ### FOR
        ### FOR
        
        if force_query_idx != None:
            if not self.query_catalog[force_query_idx]:
                self.addQuery(query)
            if self.debug: logging.info("Parsing query forced as %s.%s" % (self.proc_name, self.query_catalog[force_query_idx]))
        
        ## First figure out what kind of query it is
        matched_query_idx = None
        for query_idx in range(len(self.orig_queries)):
            if force_query_idx != None and force_query_idx != query_idx: continue
            if self.query_regex[query_idx].search(query) or force_query_idx != None:
                if not self.query_catalog[query_idx] and STRICT_MATCHING:
                    logging.warn("Unexpected query for %s: %s" % (self.proc_name, query))
                    break

                matched_query_idx = query_idx
                if self.debug: logging.info("Query matched with %s.%s: %s" % (self.proc_name, self.query_catalog[query_idx], self.orig_queries[query_idx]))
                if self.query_type[query_idx] != SQL_INSERT:
                    start_idx = 0

                    #print proc_query
                    #print query
                    #print self.query_params[proc_query]
                    #print self.query_params_debug[proc_query]
                    
                    for i in range(len(self.query_params_regex[query_idx])):
                        assert start_idx <= len(query)
                        
                        param_regex = self.query_params_regex[query_idx][i]
                        param_str = self.query_params_debug[query_idx][i]
                        
                        if self.debug:
                            logging.info("%s.%s Parameter #%d: %s" % (self.proc_name, self.query_catalog[matched_query_idx], i, param_str))
                            logging.info("Remaining Query[%d]: %s" % (start_idx, query[start_idx:]))
                        match = param_regex.search(query[start_idx:])
                        if not match:
                            logging.warn("Failed to match parameter in %s.%s: %s => %s" % (self.proc_name, self.query_catalog[query_idx], param_str, query))
                            params.append(None)
                            continue
                        assert match

                        match_idx = 2 if match.group(2) else 1
                        value = match.group(match_idx)
                        end_idx = start_idx + match.end(match_idx)
                        query_segment = query[start_idx:end_idx]
                        
                        if self.debug: logging.info("Query Segment[%d:%d]: %s" % (start_idx, end_idx, query_segment))
                        
                        ## HACK: We have to manually pick apart IN clauses
                        if query_segment.endswith("IN ("):
                            query_segment = query[end_idx:]
                            assert query_segment.count(')') == 1, "Too many )'s in remaining query for IN clause: %s" % query_segment
                            
                            value = [ ]
                            for item in query_segment.split(')')[0].split(','):
                                item = item.strip()
                                if item[0] == "'" and item[-1] == "'": item = item[1:-1]
                                value.append(item)
                            #end_idx += len(query_segment)
                            if self.debug: logging.info("Extract IN clause values: %s" % str(value))
                        
                        ## HACK: HStore expects dates for parameters, but MySQL can use DATE
                        ## functions to make things easier. So if we see something uses a 
                        ## data function on the column, then just ignore the parameter
                        ## Example: DataMaintenance.updateDailyMarket
                        elif query_segment.find("EXTRACT(DAY FROM") != -1 or query_segment.endswith("+ INTERVAL"):
                            logging.warn("Parameter #%d uses date function. Ignoring value..." % i)
                            value = None

                        ## HACK: Assume that any string "NULL" should really be None
                        elif value == "NULL":
                            value = None

                        elif value == None:
                            logging.warn("Failed to extract parameter #%d [match_idx=%d]: %s\n%s\n%s" % (i, match_idx, query, param_str, match.groups()))
                            break
                        start_idx = end_idx
                        assert start_idx != -1
                        
                        #logging.debug(param_str)
                        if self.debug: logging.info("Param[%d]: %s [start_idx=%d]" % (i, value, start_idx))
                        params.append(value)
                    ## FOR
                else:
                    ## Assume that the values don't have commas for now
                    start_idx = StoredProcedure.INSERT_REGEX.search(query).end(1)
                    values = [ ]
                    for value in query[start_idx:].split(","):
                        values.append(value.replace("\"", "").replace("'", "").strip())
                    ## FOR
                    for param_idx in self.query_params_regex[query_idx]:
                        value = values[param_idx]
                        ## HACK: Assume that any string "NULL" should really be None
                        if value == "NULL": value = None
                        params.append(value)
                    ## FOR
                    
                    ## Check whether the last parameter includes the ')' char
                    if params[-1] != None and params[-1][-1] == ')':
                        params[-1] = params[-1][:-1]
                    
                    if self.debug:
                        print "query  =", query[start_idx:].split(",")
                        print "values =", values
                        print "params =", params
                        print "param_idx=", self.query_params_regex[query_idx]

                ## IF
                break
            ## IF
        ## FOR
        
        ## If we didn't find a match, then it may be that this query is being called
        ## without first calling prepare first, so let's see if we can pick it apart from
        ## something in the catalog that we haven't seen yet
        if matched_query_idx == None and retry:
            logging.debug("No match was found for query in '%s'. Try to see if we can match it to a catalog item" % self.proc_name)
            catalog_query, score = findCatalogStatementMatch(self.proc_name, self.catalog_proc, self.catalog_tables, query, debug = self.debug)
            
            if catalog_query == WRONG_PROCEDURE_MATCH:
                return (WRONG_PROCEDURE_MATCH, None)
            elif catalog_query:
                self.addQuery(self.catalog_proc[catalog_query])
                force_query_idx = self.query_catalog.index(catalog_query)
                return self.parseQuery(query, force_query_idx = force_query_idx, retry = False)
            ## IF
        elif matched_query_idx == None:
            print "???????????"
            
        #elif matched_query_idx == None:
            #query_idx = -1
            #catalog_name = self.query_catalog[query_idx]
            #print catalog_name, self.query_regex[query_idx].search(query)
            #print self.query_debug[query_idx]
            ### FOR
            
        ## If this procedure doesn't have this type of query, 
        if matched_query_idx == None:
            print "DEBUG =", self.debug
            print "RETRY =", retry
            print "CATALOG_QUERY =", catalog_query
            print "WRONG_PROCEDURE_MATCH =", WRONG_PROCEDURE_MATCH
        assert matched_query_idx != None, "Unexpected %s query: %s\n%s" % (self.proc_name, query, "\n".join(["[" + str(self.query_catalog[i]) + "] " + self.orig_queries[i] for i in range(len(self.orig_queries))]))
        
        ## Return the index offset of the original query so that we are not storing
        ## the same string all over the place
        return (self.query_catalog[matched_query_idx], params)
    ## DEF
    
    def isProcedure(self, query):
        logging.debug("Trying to determine whether query is part of %s" % self.proc_name)
        query = query.lower()
        query_type = getQueryType(query)
        
        ## Grab the catalog match
        catalog_match, score = findCatalogStatementMatch(self.proc_name, self.catalog_proc, self.catalog_tables, query)
        
        ## And then check to make sure that the first part of the query looks the same
        if catalog_match != None and catalog_match != WRONG_PROCEDURE_MATCH:
            for query_idx in range(len(self.first_queries)):
                if query.startswith(self.first_queries[query_idx]):
                    logging.debug("Matched procedure %s with first query %s: %s" % (self.proc_name, catalog_match, self.first_queries[query_idx]))
                    return True, score
            ## FOR
            logging.debug("Found catalog match '%s.%s' but failed to find query prefix match" % (self.proc_name, catalog_match))
            # logging.info("Query Prefixes:\n" + "\n".join(["[%d] %s" % (i, self.first_queries[i]) for i in range(len(self.first_queries))]))
        ## FOR
        return False, score
    ## DEF
    
    def setParameterMapping(self, mapping):
        ## Turn any entries that are not a list into lists
        for m in mapping:
            param_name = m[0]
            query_names = m[1]
            param_idxs = m[2]
            param_is_array = m[3] if len(m) == 4 else False
                
            if type(query_names) != list:
                query_names = [ query_names ] if query_names != None else [ ]
                param_idxs = [ param_idxs ] if param_idxs != None else [ ]
            assert len(query_names) == len(param_idxs), "Mismatch %s: %s <=> %s" % (self.proc_name, str(query_names), str(param_idxs))
                
            self.txn_params.append((param_name, query_names, param_idxs, param_is_array))
        ## FOR
        if mapping: logging.debug("Set Txn Parameter Mapping: %s" % self.proc_name)
    ## DEF
    
## CLASS