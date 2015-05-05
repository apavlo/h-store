
import os
import sys
import csv
import glob
import re
import logging
import fnmatch
import string
import numpy as np
from pprint import pprint

from options import *

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION
## ==============================================
OPT_HSTORE_TAGS = {
    'read-heavy':   90, # read_pct
    'write-heavy':  50, # read_pct
    'read-only':    100, # read_pct
}
OPT_MYSQL_MEMORY = {
    1.0:   1.0,
    0.5:   2.0,
    0.250: 4.0,
    0.125: 8.0,
}
OPT_MYSQL_WARMUP = 60 # seconds

## ==============================================
## getCSVData
## ==============================================
def getCSVData(dataFile):
    csvData = [ ]
    colMap = None
    with open(dataFile, "U") as f:
        for row in csv.reader(f):
            row = map(string.strip, row)
            if colMap is None:
                colMap = { }
                for i in xrange(len(row)):
                    colName = row[i].split("(")[0].strip().upper()
                    colMap[colName] = i
                continue
            for i in xrange(len(row)):
                if row[i].isdigit(): row[i] = float(row[i])
            csvData.append(row)
        ## FOR
            
    ## WITH
    assert not colMap is None, "No column mapping retrieved from '%s'" % dataFile
    return (colMap, csvData)
## DEF

def loadTPCCData():
    matches = []
    for root, dirnames, filenames in os.walk(OPT_DATA_HSTORE):
        for filename in fnmatch.filter(filenames, 'results.csv'):
            matches.append(os.path.join(root, filename))
    
    data = { }
    for dataFile in matches:
        tags = dataFile.replace(OPT_DATA_HSTORE, "").split("/")[1:-1]
        benchmark = tags.pop(0)
        attrs = {"file": dataFile}
        
        # Read/Write Ratio
        readRatio = tags.pop(0)
        assert readRatio in OPT_HSTORE_TAGS
        attrs["read_pct"] = OPT_HSTORE_TAGS[readRatio]
        
        # Memory
        memory = int(tags.pop(0))
        assert memory > 0
        attrs["mem"] = memory
        
        # Skew
#        skew = map(int, tags.pop(0).split("-"))
#        assert len(skew) == 2
#        attrs["hot_access"] = skew[0]
#        attrs["hot_data"] = skew[1]
        
        skew = float(tags.pop(0))
        attrs["skew"] = skew
        
        # Now load in this mess and figure out what it is
        colMap, csvData = getCSVData(dataFile)
        
        # Skip empty files
        if not csvData:
            LOG.warn("No data was found in '%s'", dataFile)
            continue

        # Get the last elapsed time
        elapsed = csvData[-1][colMap["ELAPSED"]] / 1000.0
        assert elapsed
        elapsedDelta = (csvData[1][colMap["ELAPSED"]] - csvData[0][colMap["ELAPSED"]]) / 1000.0
        txnTotal = sum(map(lambda x: x[colMap["TRANSACTIONS"]], csvData))
        attrs["tps"] = txnTotal / elapsed
        attrs["throughputs"] = [ ]
        attrs["timestamps"] = { }
        for row in csvData:
            interval = int(row[colMap["ELAPSED"]]/1000)
            attrs["throughputs"].append((interval, row[colMap["TRANSACTIONS"]]/elapsedDelta))
            attrs["timestamps"][interval] = int(row[colMap["TIMESTAMP"]])
        ## FOR
        
        # Eviction Data
        evictionFile = dataFile.replace("results.csv", "evictions.csv")
        attrs["evictions"] = [ ]
        if os.path.exists(evictionFile):
            pass
        ## 
        
        assert not data is None
        if not benchmark in data:
            data[benchmark] = [ ]
        data[benchmark].append(attrs)
        #pprint(attrs)
        
    ## FOR
    return (data)
## DEF
## ==============================================
## LOAD H-STORE DATA
## ==============================================
def loadHStoreData(datapath):
    matches = []
    for root, dirnames, filenames in os.walk(datapath):
        for filename in fnmatch.filter(filenames, 'results.csv'):
            matches.append(os.path.join(root, filename))
    
    data = { }
    for dataFile in matches:
        
        #LOG.info("processing data file: " + dataFile)
        
        tags = dataFile.replace(datapath, "").split("/")[1:-1]
        benchmark = tags.pop(0)
        attrs = {"file": dataFile}
        
        # Read/Write Ratio
        readRatio = tags.pop(0)
        assert readRatio in OPT_HSTORE_TAGS
        attrs["read_pct"] = OPT_HSTORE_TAGS[readRatio]
        
        # Memory
        memory = int(tags.pop(0))
        assert memory > 0
        attrs["mem"] = memory
        
        # Skew
#        skew = map(int, tags.pop(0).split("-"))
#        assert len(skew) == 2
#        attrs["hot_access"] = skew[0]
#        attrs["hot_data"] = skew[1]
        
        skew = float(tags.pop(0))
        attrs["skew"] = skew
        
        # Now load in this mess and figure out what it is
        colMap, csvData = getCSVData(dataFile)
        
        # Skip empty files
        if not csvData:
            LOG.warn("No data was found in '%s'", dataFile)
            continue

        # Get the last elapsed time
        elapsed = csvData[-1][colMap["ELAPSED"]] / 1000.0
        assert elapsed
        elapsedDelta = (csvData[1][colMap["ELAPSED"]] - csvData[0][colMap["ELAPSED"]]) / 1000.0
        txnTotal = sum(map(lambda x: x[colMap["TRANSACTIONS"]], csvData))
        attrs["tps"] = txnTotal / elapsed
        attrs["throughputs"] = [ ]
        attrs["timestamps"] = { }
        for row in csvData:
            #LOG.info("processing row.")
            interval = int(row[colMap["ELAPSED"]]/1000)
            attrs["throughputs"].append((interval, row[colMap["TRANSACTIONS"]]/elapsedDelta))
            attrs["timestamps"][interval] = int(row[colMap["TIMESTAMP"]])
        ## FOR
        
        # Eviction Data
        evictionFile = dataFile.replace("results.csv", "evictions.csv")
        attrs["evictions"] = [ ]
        if os.path.exists(evictionFile):
            pass
        ## 
        
        assert not data is None
        if not benchmark in data:
            data[benchmark] = [ ]
        data[benchmark].append(attrs)
        #pprint(attrs)
        
    ## FOR
    return (data)
## DEF

def loadCSVData():
    matches = []
    for root, dirnames, filenames in os.walk(datapath):
        for filename in fnmatch.filter(filenames, 'results.csv'):
            matches.append(os.path.join(root, filename))
    
    data = { }
    for dataFile in matches:
                

        attrs = {"file": dataFile}
        
        # Now load in this mess and figure out what it is
        colMap, csvData = getCSVData(dataFile)
                

    ## FOR
    return (data)


def loadHStoreTPCC(datapath):

    dataSizes = [1, 2, 4, 8]
    
    data = []
    for data_size in dataSizes:
        dataFile = os.path.realpath(os.path.join(datapath, str(data_size)))
        dataFile = os.path.realpath(os.path.join(dataFile, "results.csv"))
		# Now load in this mess and figure out what it is
        colMap, csvData = getCSVData(dataFile)
           
        attrs = { }
        
		# Skip empty files
        if not csvData:
			LOG.warn("No data was found in '%s'", dataFile)
    
        times = [ ]
        for row in csvData:
            # Get the last elapsed time
            elapsed = csvData[-1][colMap["ELAPSED"]] / 1000.0
            assert elapsed
            elapsedDelta = (csvData[1][colMap["ELAPSED"]] - csvData[0][colMap["ELAPSED"]]) / 1000.0
            txnTotal = sum(map(lambda x: x[colMap["TRANSACTIONS"]], csvData))
            attrs["tps"] = txnTotal / elapsed
            attrs["datasize"] = data_size
            times.append(txnTotal / elapsed)

                #if not data_size in data:
                ##data[data_size] = [ ]
                #data[data_size].append(attrs)
                
        data.append(np.mean(times))

	## FOR


    return (data)
# DEF

def loadMySQLTPCC(memcached = False):

    dataSizes = [1, 2, 4, 8]
	
    data = [ ]
    for data_size in dataSizes:
        
        if memcached:
            dataFile = os.path.realpath(os.path.join(OPT_DATA_MEMCACHED_TPCC, str(data_size)))
        else:
            dataFile = os.path.realpath(os.path.join(OPT_DATA_MYSQL_TPCC, str(data_size)))
        
        dataFile = os.path.realpath(os.path.join(dataFile, "results.res"))
    
		# Now load in this mess and figure out what it is
        colMap, csvData = getCSVData(dataFile)
		
        attrs = { }
        attrs["datasize"] = data_size
        attrs["throughputs"] = [ ]
        times = [ ]
        for row in csvData:
            timestamp = float(row[colMap['TIME']])
            if timestamp < OPT_MYSQL_WARMUP:
                continue
            throughput = float(row[colMap['THROUGHPUT']])
            interval = int(row[colMap['TIME']])
            times.append(throughput)
            attrs["throughputs"].append((interval, throughput))
        ## FOR

        # Always discard the last entry because of some funkiness in framework
        times.pop(-1)
        attrs["throughputs"].pop(-1)
        attrs["tps"] = np.mean(times)

        data.append(np.mean(times))

            #if not data_size in data:
    #data[data_size] = [ ]
#data[data_size].append(attrs)
	## FOR

    return (data)
# DEF

## ==============================================
## LOAD MYSQL DATA
## ==============================================
def loadMySQLData(memcached = False):
   
    matches = []
    for root, dirnames, filenames in os.walk(OPT_DATA_MYSQL):
        for filename in fnmatch.filter(filenames, '*.res'):
            matches.append(os.path.join(root, filename))
    
    data = { }
    for dataFile in matches:
        tags = dataFile.replace(OPT_DATA_MYSQL, "").split("/")[1:-1]
        benchmark = tags.pop(0)
        attrs = {"file": dataFile}
        
        # Process path to get attrs
        i = 0
        attrs = { }
        attributeList = [ ]
        while i < len(tags):
            if tags[i] == "hot_access": # rename to skew
                tags[i] = "skew"
            attrs[tags[i]] = float(tags[i+1])
            attributeList.append(tags[i])
            i += 2
        ## WHILE
        attrs["order"] = attributeList
        
        # Check whether this is Memcache data
        if memcached:
            if attrs["mcsplit"] == 0: continue
            # HACK
            if attrs["read_pct"] == 95: attrs["read_pct"] = 90
        else:
            if attrs["mcsplit"] != 0: continue
            
        # Convert memory size
        if not attrs["mem"] in OPT_MYSQL_MEMORY:
            LOG.debug("Skipping unexpected memory size for '%s'", dataFile)
            continue
        attrs["mem"] = OPT_MYSQL_MEMORY[attrs["mem"]]
        
        # Load CSV data file and calculate avg throughput
        LOG.debug("READING OLTP-BENCH DATA: %s", dataFile)
        colMap,csvData = getCSVData(dataFile)
        attrs["throughputs"] = [ ]
        times = [ ]
        for row in csvData:
            timestamp = float(row[colMap['TIME']])
            if timestamp < OPT_MYSQL_WARMUP:
                continue
            throughput = float(row[colMap['THROUGHPUT']])
            interval = int(row[colMap['TIME']])
            times.append(throughput)
            attrs["throughputs"].append((interval, throughput))
        ## FOR

        # Always discard the last entry because of some funkiness in framework
        times.pop(-1)
        attrs["throughputs"].pop(-1)
        attrs["tps"] = np.mean(times)
    
        if not benchmark in data:
            data[benchmark] = [ ]
        data[benchmark].append(attrs)
        #pprint(attrs)
    ## FOR
    
    return (data)
## DEF