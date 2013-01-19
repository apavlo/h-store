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
        new VoltTable.ColumnInfo("TUPLES_EVICTED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("BLOCKS_EVICTED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("BYTES_EVICTED", VoltType.BIGINT),
    };
    
    static class PartitionMemRow {
        long tupleCount = 0;
        int tupleDataMem = 0;
        int tupleAllocatedMem = 0;
        int indexMem = 0;
        int stringMem = 0;
        long pooledMem = 0;
        long tuplesEvicted = 0;
        long blocksEvicted = 0;
        long bytesEvicted = 0;
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
            totals.tuplesEvicted += pmr.tuplesEvicted;
            totals.blocksEvicted += pmr.blocksEvicted;
            totals.bytesEvicted += pmr.bytesEvicted;
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
        rowValues[columnNameToIndex.get("TUPLES_EVICTED")] = totals.tuplesEvicted;
        rowValues[columnNameToIndex.get("BLOCKS_EVICTED")] = totals.blocksEvicted;
        rowValues[columnNameToIndex.get("BYTES_EVICTED")] = totals.bytesEvicted;
        super.updateStatsRow(rowKey, rowValues);
    }

    public synchronized void eeUpdateMemStats(long siteId,
                                              long tupleCount,
                                              int tupleDataMem,
                                              int tupleAllocatedMem,
                                              int indexMem,
                                              int stringMem,
                                              long pooledMemory,
                                              long tuplesEvicted,
                                              long blocksEvicted,
                                              long bytesEvicted) {
        PartitionMemRow pmr = new PartitionMemRow();
        pmr.tupleCount = tupleCount;
        pmr.tupleDataMem = tupleDataMem;
        pmr.tupleAllocatedMem = tupleAllocatedMem;
        pmr.indexMem = indexMem;
        pmr.stringMem = stringMem;
        pmr.pooledMem = pooledMemory;
        pmr.tuplesEvicted = tuplesEvicted;
        pmr.blocksEvicted = blocksEvicted;
        pmr.bytesEvicted = bytesEvicted;
        m_memoryStats.put(siteId, pmr);
    }
}
