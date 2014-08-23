/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.SystemStatsCollector;

public class MemoryStats extends StatsSource {
    
    public static final VoltTable.ColumnInfo COLUMNS[] = {
        new VoltTable.ColumnInfo("RSS", VoltType.BIGINT),
        new VoltTable.ColumnInfo("JAVA_USED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("JAVA_UNUSED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("TUPLE_COUNT", VoltType.BIGINT),
        new VoltTable.ColumnInfo("TUPLE_ALLOCATED_MEMORY", VoltType.BIGINT),
        new VoltTable.ColumnInfo("TUPLE_DATA_MEMORY", VoltType.BIGINT),
        new VoltTable.ColumnInfo("INDEX_MEMORY", VoltType.INTEGER),
        new VoltTable.ColumnInfo("STRING_MEMORY", VoltType.INTEGER),
        new VoltTable.ColumnInfo("POOLED_MEMORY", VoltType.BIGINT),
        // ACTIVE
        new VoltTable.ColumnInfo("ANTICACHE_TUPLES_EVICTED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_EVICTED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_EVICTED", VoltType.BIGINT),
        // GLOBAL WRITTEN
        new VoltTable.ColumnInfo("ANTICACHE_TUPLES_WRITTEN", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_WRITTEN", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_WRITTEN", VoltType.BIGINT),
        // GLOBAL READ
        new VoltTable.ColumnInfo("ANTICACHE_TUPLES_READ", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_READ", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_READ", VoltType.BIGINT),
    };
    
    static class PartitionMemRow {
        long tupleCount = 0;
        int tupleDataMem = 0;
        int tupleAllocatedMem = 0;
        int indexMem = 0;
        int stringMem = 0;
        long pooledMem = 0;
        
        // ACTIVE
        long tuplesEvicted = 0;
        long blocksEvicted = 0;
        long bytesEvicted = 0;
        
        // GLOBAL WRITTEN
        long tuplesWritten = 0;
        long blocksWritten = 0;
        long bytesWritten = 0;
        
        // GLOBAL READ
        long tuplesRead = 0;
        long blocksRead = 0;
        long bytesRead = 0;
    }
    Map<Long, PartitionMemRow> m_memoryStats = new TreeMap<Long, PartitionMemRow>();

    public MemoryStats() {
        super("MEMORY", false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            boolean returnRow = true;

            @Override
            public boolean hasNext() {
                return returnRow;
            }

            @Override
            public Object next() {
                if (returnRow) {
                    returnRow = false;
                    return new Object();
                } else {
                    return null;
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void populateColumnSchema(ArrayList<ColumnInfo> columns) {
        super.populateColumnSchema(columns);
        for (VoltTable.ColumnInfo col : COLUMNS) {
            columns.add(col);
        } // FOR
    }

    @Override
    protected synchronized void updateStatsRow(Object rowKey, Object[] rowValues) {
        // sum up all of the site statistics
        PartitionMemRow totals = new PartitionMemRow();
        for (PartitionMemRow pmr : m_memoryStats.values()) {
            totals.tupleCount += pmr.tupleCount;
            totals.tupleDataMem += pmr.tupleDataMem;
            totals.tupleAllocatedMem += pmr.tupleAllocatedMem;
            totals.indexMem += pmr.indexMem;
            totals.stringMem += pmr.stringMem;
            totals.pooledMem += pmr.pooledMem;
            
            // ACTIVE
            totals.tuplesEvicted += pmr.tuplesEvicted;
            totals.blocksEvicted += pmr.blocksEvicted;
            totals.bytesEvicted += pmr.bytesEvicted;
            
            // GLOBAL WRITTEN
            totals.tuplesWritten += pmr.tuplesWritten;
            totals.blocksWritten += pmr.blocksWritten;
            totals.bytesWritten += pmr.bytesWritten;
            
            // GLOBAL READ
            totals.tuplesRead += pmr.tuplesRead;
            totals.blocksRead += pmr.blocksRead;
            totals.bytesRead += pmr.bytesRead;
            
        }

        // get system statistics
        int rss = 0; int javaused = 0; int javaunused = 0;
        SystemStatsCollector.Datum d = SystemStatsCollector.getRecentSample();
        if (d != null) {
            rss = (int) (d.rss / 1024);
            double javausedFloat = d.javausedheapmem + d.javausedsysmem;
            javaused = (int) (javausedFloat / 1024);
            javaunused = (int) ((d.javatotalheapmem + d.javatotalsysmem - javausedFloat) / 1024);
        }

        rowValues[columnNameToIndex.get("RSS")] = rss;
        rowValues[columnNameToIndex.get("JAVA_USED")] = javaused;
        rowValues[columnNameToIndex.get("JAVA_UNUSED")] = javaunused;
        rowValues[columnNameToIndex.get("TUPLE_COUNT")] = totals.tupleCount;
        rowValues[columnNameToIndex.get("TUPLE_ALLOCATED_MEMORY")] = totals.tupleAllocatedMem;
        rowValues[columnNameToIndex.get("TUPLE_DATA_MEMORY")] = totals.tupleDataMem;
        rowValues[columnNameToIndex.get("INDEX_MEMORY")] = totals.indexMem;
        rowValues[columnNameToIndex.get("STRING_MEMORY")] = totals.stringMem;
        rowValues[columnNameToIndex.get("POOLED_MEMORY")] = totals.pooledMem / 1024;
        
        // ACTIVE
        rowValues[columnNameToIndex.get("ANTICACHE_TUPLES_EVICTED")] = totals.tuplesEvicted;
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_EVICTED")] = totals.blocksEvicted;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_EVICTED")] = totals.bytesEvicted / 1024;
        
        // GLOBAL WRITTEN
        rowValues[columnNameToIndex.get("ANTICACHE_TUPLES_WRITTEN")] = totals.tuplesWritten;
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_WRITTEN")] = totals.blocksWritten;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_WRITTEN")] = totals.bytesWritten / 1024;
        
        // GLOBAL READ
        rowValues[columnNameToIndex.get("ANTICACHE_TUPLES_READ")] = totals.tuplesRead;
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_READ")] = totals.blocksRead;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_READ")] = totals.bytesRead / 1024;
        
        super.updateStatsRow(rowKey, rowValues);
    }

    public synchronized void eeUpdateMemStats(long partitionId,
                                              long tupleCount,
                                              int tupleDataMem,
                                              int tupleAllocatedMem,
                                              int indexMem,
                                              int stringMem,
                                              long pooledMemory,
                                              
                                              // ACTIVE
                                              long tuplesEvicted, long blocksEvicted, long bytesEvicted,
                                              // GLOBAL WRITTEN 
                                              long tuplesWritten, long blocksWritten, long bytesWritten,
                                              // GLOBAL READ
                                              long tuplesRead, long blocksRead, long bytesRead
                                              ) {
        PartitionMemRow pmr = new PartitionMemRow();
        pmr.tupleCount = tupleCount;
        pmr.tupleDataMem = tupleDataMem;
        pmr.tupleAllocatedMem = tupleAllocatedMem;
        pmr.indexMem = indexMem;
        pmr.stringMem = stringMem;
        pmr.pooledMem = pooledMemory;
        
        // ACTIVE
        pmr.tuplesEvicted = tuplesEvicted;
        pmr.blocksEvicted = blocksEvicted;
        pmr.bytesEvicted = bytesEvicted;
        
        // GLOBAL WRITTEN
        pmr.tuplesWritten = tuplesWritten;
        pmr.blocksWritten = blocksWritten;
        pmr.bytesWritten = bytesWritten;
        
        // GLOBAL READ
        pmr.tuplesRead = tuplesRead;
        pmr.blocksRead = blocksRead;
        pmr.bytesRead = bytesRead;
        
        
        m_memoryStats.put(partitionId, pmr);
    }
}
