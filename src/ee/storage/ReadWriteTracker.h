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

#include <vector>
#include "boost/unordered_map.hpp"

typedef std::vector<int32_t> rowOffsets;

namespace voltdb {
    
/**
 * Read/Write Tuple Tracker
 */
class ReadWriteTracker {
    
    public:
        ReadWriteTracker(int64_t txnId);
        ~ReadWriteTracker();
        
        void markTupleRead(const int32_t tableId, const int32_t rowOffset);
        void markTupleWritten(const int32_t tableId, const int32_t rowOffset);
        void clear();
        
        
    private:
        int64_t txnId;
        // TableId -> RowOffsets
        boost::unordered_map<int32_t, rowOffsets> reads;
        // TableId -> RowOffsets
        boost::unordered_map<int32_t, rowOffsets> writes;
        
}; // CLASS


}
#endif












