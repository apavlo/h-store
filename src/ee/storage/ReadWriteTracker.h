/* Copyright (C) 2013 by H-Store Project
 * Brown University
 * Massachusetts Institute of Technology
 * Yale University
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#ifndef HSTORE_READWRITETRACKER_H
#define HSTORE_READWRITETRACKER_H

#include <string>
#include "boost/unordered_set.hpp"
#include "boost/unordered_map.hpp"
#include "common/tabletuple.h"
#include "common/TupleSchema.h"
#include "storage/table.h"

typedef boost::unordered_set<uint32_t> RowOffsets;

namespace voltdb {
    
class ExecutorContext;
class TableTuple;
class TupleSchema;
class Table;
    
/**
 * Read/Write Tuple Tracker for a single transaction
 */
class ReadWriteTracker {
    
    friend class ReadWriteTrackerManager;
    
    public:
        ReadWriteTracker(int64_t txnId);
        ~ReadWriteTracker();
        
        void markTupleRead(Table *table, TableTuple *tuple);
        void markTupleWritten(Table *table, TableTuple *tuple);
        
        void clear();
        
        std::vector<std::string> getTablesRead();
        std::vector<std::string> getTablesWritten();
        
    private:
        void insertTuple(boost::unordered_map<std::string, RowOffsets*> *map, Table *table, TableTuple *tuple);
        std::vector<std::string> getTableNames(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        int64_t txnId;
        
        // TableName -> RowOffsets
        boost::unordered_map<std::string, RowOffsets*> reads;
        boost::unordered_map<std::string, RowOffsets*> writes;
        
}; // CLASS

/**
 * ReadWriteTracker Manager 
 */
class ReadWriteTrackerManager {
    public:
        ReadWriteTrackerManager(ExecutorContext *ctx);
        ~ReadWriteTrackerManager();
    
        ReadWriteTracker* enableTracking(int64_t txnId);
        ReadWriteTracker* getTracker(int64_t txnId);
        void removeTracker(int64_t txnId);
        
        Table* getTuplesRead(ReadWriteTracker *tracker);
        Table* getTuplesWritten(ReadWriteTracker *tracker);
        
    private:
        void getTuples(boost::unordered_map<std::string, RowOffsets*> *map) const;
        
        ExecutorContext *executorContext;
        TupleSchema *resultSchema;
        Table *resultTable;
        boost::unordered_map<int64_t, ReadWriteTracker*> trackers;
}; // CLASS

}
#endif
