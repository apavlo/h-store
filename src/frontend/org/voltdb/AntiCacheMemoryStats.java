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

public class AntiCacheMemoryStats extends StatsSource {
    
    public static final VoltTable.ColumnInfo COLUMNS[] = {
        new VoltTable.ColumnInfo("ANTICACHE_ID", VoltType.INTEGER),
        new VoltTable.ColumnInfo("ANTICACHEDB_TYPE", VoltType.INTEGER),

        // ACTIVE
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_EVICTED", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_EVICTED", VoltType.BIGINT),
        // GLOBAL WRITTEN
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_WRITTEN", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_WRITTEN", VoltType.BIGINT),
        // GLOBAL READ
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_READ", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_READ", VoltType.BIGINT),
        // FREE
        new VoltTable.ColumnInfo("ANTICACHE_BLOCKS_FREE", VoltType.BIGINT),
        new VoltTable.ColumnInfo("ANTICACHE_BYTES_FREE", VoltType.BIGINT),

    };
    
    static class PartitionMemRow {
        int anticacheID;
        int anticacheType;

        // ACTIVE
        long blocksEvicted = 0;
        long bytesEvicted = 0;
        
        // GLOBAL WRITTEN
        long blocksWritten = 0;
        long bytesWritten = 0;
        
        // GLOBAL READ
        long blocksRead = 0;
        long bytesRead = 0;

        // FREE 
        long blocksFree= 0;
        long bytesFree = 0;
    }
    Map<Long, Map<Long, PartitionMemRow> > m_memoryStats = new TreeMap<Long, Map<Long, PartitionMemRow> >();

    public AntiCacheMemoryStats() {
        super("ANTICACHEMEMORY", false);
    }

    @Override
    protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
        return new Iterator<Object>() {
            int numDBs = m_memoryStats.size();
            int key = 0;

            @Override
            public boolean hasNext() {
                if (key < numDBs)
                    return true;
                else
                    return false;
            }

            @Override
            public Object next() {
                key += 1;
                return new Long(key - 1);
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
        Map<Long, PartitionMemRow> tier = m_memoryStats.get(rowKey);
            // sum up all of the site statistics
        PartitionMemRow totals = new PartitionMemRow();
        for (PartitionMemRow pmr : tier.values()) {
            totals.anticacheID = pmr.anticacheID;
            totals.anticacheType = pmr.anticacheType;

            // ACTIVE
            totals.blocksEvicted += pmr.blocksEvicted;
            totals.bytesEvicted += pmr.bytesEvicted;

            // GLOBAL WRITTEN
            totals.blocksWritten += pmr.blocksWritten;
            totals.bytesWritten += pmr.bytesWritten;

            // GLOBAL READ
            totals.blocksRead += pmr.blocksRead;
            totals.bytesRead += pmr.bytesRead;

            totals.blocksFree += pmr.blocksFree;
            totals.bytesFree += pmr.bytesFree;
        }

        //System.out.println("From updateStatsRow: " + totals.anticacheID + " " + totals.anticacheType);

        rowValues[columnNameToIndex.get("ANTICACHE_ID")] = totals.anticacheID;
        rowValues[columnNameToIndex.get("ANTICACHEDB_TYPE")] = totals.anticacheType;

        // ACTIVE
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_EVICTED")] = totals.blocksEvicted;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_EVICTED")] = totals.bytesEvicted / 1024;

        // GLOBAL WRITTEN
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_WRITTEN")] = totals.blocksWritten;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_WRITTEN")] = totals.bytesWritten / 1024;

        // GLOBAL READ
        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_READ")] = totals.blocksRead;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_READ")] = totals.bytesRead / 1024;

        rowValues[columnNameToIndex.get("ANTICACHE_BLOCKS_FREE")] = totals.blocksFree;
        rowValues[columnNameToIndex.get("ANTICACHE_BYTES_FREE")] = totals.bytesFree / 1024;
        super.updateStatsRow(rowKey, rowValues);
    }

    public synchronized void eeUpdateMemStats(long partitionId,
            int anticacheID,
            int anticacheType,

            // ACTIVE
            long blocksEvicted, long bytesEvicted,
            // GLOBAL WRITTEN 
            long blocksWritten, long bytesWritten,
            // GLOBAL READ
            long blocksRead, long bytesRead,
            // Free
            long blocksFree, long bytesFree
            ) {
        PartitionMemRow pmr = new PartitionMemRow();
        pmr.anticacheID = anticacheID;
        pmr.anticacheType = anticacheType;

        // ACTIVE
        pmr.blocksEvicted = blocksEvicted;
        pmr.bytesEvicted = bytesEvicted;

        // GLOBAL WRITTEN
        pmr.blocksWritten = blocksWritten;
        pmr.bytesWritten = bytesWritten;

        // GLOBAL READ
        pmr.blocksRead = blocksRead;
        pmr.bytesRead = bytesRead;

        pmr.blocksFree = blocksFree;
        pmr.bytesFree = bytesFree;

        if (m_memoryStats.get(new Long(anticacheID)) == null)
            m_memoryStats.put(new Long(anticacheID), new TreeMap<Long, PartitionMemRow>());

        //System.out.println("From AntiCacheMemoryStats: " + partitionId + " " + anticacheID);

        m_memoryStats.get(new Long(anticacheID)).put(partitionId, pmr);
    }
}
