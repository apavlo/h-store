# -*- coding: utf-8 -*-

import sys
import json
import logging
from common import *

## ==============================================
## AbstractTraceElement
## ==============================================
class AbstractTraceElement(object):
    def __init__(self, catalog_name = None, params = [ ], output = [ ], start_timestamp = None, stop_timestamp = None, aborted = False):
        self.name = catalog_name
        self.params = list(params)
        self.output = list(output)
        self.start = start_timestamp
        self.stop = stop_timestamp
        self.aborted = aborted
    ## DEF
    
    def toJSON(self):
        #data = { }
        #for key in self.__dict__.keys():
            #val = self.__dict__[key]
            #if key in ['start', 'stop']:
                #val = convertTimestamp(val)
            #data[key.upper()] = val
        ### FOR
        #return (data)
        
        return ({
            "NAME":     self.name,
            "START":    convertTimestamp(self.start),
            "STOP":     convertTimestamp(self.stop),
            "ABORTED":  self.aborted,
            "PARAMS":   self.params,
            "OUTPUT":   self.output,
            
        })
    ## DEF
    
    def fromJSON(self, data):
        for key in self.__dict__.keys():
            if key.upper() in data: self.__dict__[key] = data[key.upper()]
        ## FOR
    ## DEF
## CLASS

## ==============================================
## TransactionTrace
## ==============================================
class TransactionTrace(AbstractTraceElement):
    NEXT_TXN_ID = 1000
    
    def __init__(self, start_timestamp = None, debug = False, strict_matching = False):
        super(TransactionTrace, self).__init__(start_timestamp = start_timestamp)
        #assert self.id
        #assert self.start
        assert not self.params
        
        self.__queries = [ ]
        self.next_batch_id = 0
        self.cur_batch_id = 0

        self.debug = debug
        self.strict_matching = strict_matching
        
        self.txn_id = TransactionTrace.NEXT_TXN_ID
        TransactionTrace.NEXT_TXN_ID += 1
    ## DEF
    
    def finish(self, proc, stop_timestamp, aborted = False):
        assert self.name
        if self.debug: logging.info("Finishing txn '%s' with %d queries..." % (self.name, len(self.__queries)))

        ## Make sure all of our queries have the same procedure as the txn
        for query_trace in self.__queries:
            assert self.name == query_trace.proc_name, \
                "Mismatched procedure name: %s != %s.%s" % (self.name, query_trace.proc_name, query_trace.name)
            assert query_trace.name in proc.catalog_proc.keys(), \
                "Unexpected query '%s' in transaction '%s'" % (query_trace.name, self.name)
        ## FOR

        ## Figure out our parameters
        #print "proc.txn_params=", proc.txn_params
        for i in range(len(proc.txn_params)):
            param_name = proc.txn_params[i][0]
            query_names = proc.txn_params[i][1]
            param_idxs = proc.txn_params[i][2]
            param_is_array = proc.txn_params[i][3]
            param_values = [ ]

            #print "param_name=", param_name
            #print "query_names=", query_names
            #print "param_idxs=", param_idxs
            #print "param_is_array=", param_is_array
            
            ## Loop through and find the invocations of the query we're looking for
            for ii in range(len(query_names)):
                query_name = query_names[ii]
                param_idx = param_idxs[ii]
                queries = self.getQueries(query_name)
                
                if self.debug: logging.info("Looking for parameter #%d (%s) from %s.%s:%d [num_queries=%d]" % (i, proc.txn_params[i][0], self.name, query_name, param_idx, len(queries)))
                
                for query_trace in queries:
                    assert query_trace.name == query_name, "Unexpected query trace name: %s <-> %s\n%s" % (query_name, query_trace.name, query_trace.toJSON())
                    if param_idx >= len(query_trace.params):
                        logging.error("Parameter Index %d exceeds length of parameters for %s.%s" % (param_idx, self.name, query_name))
                        logging.info(query_trace.orig_query)
                        logging.info("Parameters: %s" % str(query_trace.params))
                        logging.info(query_trace.toJSON())
                        sys.exit(1)
                        continue
                    ## IF
                    param_values.append(query_trace.params[param_idx])
                ## FOR
            ## FOR
            
            ## If we have no values, then just store None
            if not param_values:
                self.params.append(None)
            ## If we have multiple values but we're not array, just store the first
            elif not param_is_array:
                self.params.append(param_values[0])
            ## If we're suppose to be an array, then jam the whole thing in there
            else:
                self.params.append(param_values)
            ## IF
            if self.debug: logging.info("%s.%s [%d] = %s" % (self.name, proc.txn_params[i][0], i, str(self.params[-1])))
        ## FOR
        
        ## Make sure that it doesn't have more params than it should 
        assert len(self.params) == len(proc.txn_params), "Too many parameters for %s: %d != %d\n%s" % (self.name, len(self.params), len(proc.txn_params), str(self.params))
        
        ## And it would be nice if all the params weren't null
        non_null_param_ctr = len([self.params[i] for i in range(len(self.params)) if self.params[i] != None])
        parameterized_queries = len([self.__queries[i] for i in range(len(self.__queries)) if self.__queries[i].orig_query.find("?") != -1])
        if parameterized_queries > 0:
            assert non_null_param_ctr > 0, \
                "All the txn parameters are null for '%s': %s\n%s" % (self.name, self.params, self.debugQueries())
        
        self.stop = stop_timestamp
        self.aborted = self.getQueries()[-1].aborted = aborted
    ## DEF
    
    def nextBatchId(self):
        ret = self.next_batch_id
        self.next_batch_id += 1
        self.cur_batch_id = ret
        return (ret)
    ## DEF
    
    def getQueries(self, name = None):
        return [ q for q in self.__queries if name == None or q.name == name ]
    ## DEF
    
    def getQueryCount(self):
        return len(self.__queries)
    ## DEF
    
    def debugQueries(self):
        for query_trace in self.__queries:
            print query_trace.orig_query
        ## FOR
        
        return "\n".join(["[%d] %-20s %s" % (i, self.__queries[i].name, self.__queries[i].orig_query) for i in range(len(self.__queries))])
    ## DEF
    
    def addQuery(self, query_trace):
        if self.strict_matching: assert query_trace.name, "Mising query catalog name for '%s': %s" % (self.name, query_trace.orig_query)
        if not query_trace.name:
            logging.debug("QueryTrace does not have catalog name. Skipping...")
            return

        # logging.debug("New QueryTrace: %s.%s" % (self.name, query_trace.name))
        query_trace.batch_id = self.cur_batch_id
        self.__queries.append(query_trace)
            
        ## HACK: Set the stop timestamp of the last query
        ## to be the start timestamp of this query[\s]*
        ## But doesn't it matter whether the queries are in
        ## the same batch or not??
    ## DEF
    
    def removeQuery(self, query_trace):
        self.__queries.remove(query_trace)
    ## DEF
    
    def setQueries(self, queries):
        self.__queries = queries[:]
    ## DEF
    
    def toJSON(self):
        data = AbstractTraceElement.toJSON(self)
        data["QUERIES"] = [ q.toJSON() for q in self.__queries ]
        data["TXN_ID"] = self.txn_id
        return (data)
        
    def fromJSON(self, data):
        super(TransactionTrace, self).fromJSON(data)
        self.txn_id = data["TXN_ID"]
        for query_json in data["QUERIES"]:
            self.addQuery(QueryTrace().fromJSON(query_json))
        ## FOR
        return (self)
    ## DEF
        
## CLASS

## ==============================================
## QueryTrace
## ==============================================
class QueryTrace(AbstractTraceElement):
    
    def __init__(self, orig_query = None, name = None, proc_name = None, params = [], output = [], start_timestamp = None):
        super(QueryTrace, self).__init__(catalog_name = name, params = params, start_timestamp = start_timestamp)
        self.proc_name = proc_name
        self.batch_id = None
        self.orig_query = orig_query
        
        #if self.name == "get" and self.proc_name == "BrokerVolume":
            #print "-"*60
            #print self.orig_query
            #print
            #print self.toJSON()
            #sys.exit(1)
    ## DEF
    
    def populateSQL(self, sql):
        new_sql = ""
        split = sql.split("?")
        for i in range(0, len(split)-1):
            new_sql += str(split[i]) + str(self.params[i])
        ## FOR
        new_sql += str(split[-1])
        return (new_sql)
    ## DEF
    
    def toJSON(self):
        data = AbstractTraceElement.toJSON(self)
        #data["PROC_NAME"] = self.proc_name
        data["BATCH_ID"] = self.batch_id
        return (data)
    ## DEF
    
    def fromJSON(self, data):
        super(QueryTrace, self).fromJSON(data)
        #self.proc_name = data["PROC_NAME"]
        self.batch_id = data["BATCH_ID"]
        return (self)
    ## DEF
    
## CLASS