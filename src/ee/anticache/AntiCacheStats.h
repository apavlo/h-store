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

#ifndef ANTICACHESTATS_H_
#define ANTICACHESTATS_H_

#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "storage/table.h"
#include "anticache/AntiCacheDB.h"
#include "anticache/AntiCacheEvictionManager.h"
#include <vector>
#include <string>

namespace voltdb {

/**
 * StatsSource extension for tables.
 */
class AntiCacheStats : public voltdb::StatsSource {
public:
    /**
     * Static method to generate the column names for the tables which
     * contain stats for multiple anticaching levels
     */
    static std::vector<std::string> generateAntiCacheStatsColumnNames();

    /**
     * Static method to generate the remaining schema information for
     * the tables which contain stats from the multiple anticache levels.
     */
    static void populateAntiCacheStatsSchema(std::vector<voltdb::ValueType>& types,
                                         std::vector<int32_t>& columnLengths,
                                         std::vector<bool>& allowNull);

    /**
     * Return an empty AntiCacheStats table
     */
    static Table* generateEmptyAntiCacheStatsTable();

    /*
     * Constructor caches reference to the table that will be generating the statistics
     */
    AntiCacheStats(Table* table, AntiCacheDB* acdb);

    /**
     * Configure a StatsSource superclass for a set of statistics. Since this class is only used in the EE it can be assumed that
     * it is part of an Execution Site and that there is a site Id.
     * @parameter name Name of this set of statistics
     * @parameter hostId id of the host this partition is on
     * @parameter hostname name of the host this partition is on
     * @parameter siteId this stat source is associated with
     * @parameter partitionId this stat source is associated with
     * @parameter databaseId Database this source is associated with
     */
    virtual void configure(
            std::string name,
            voltdb::CatalogId hostId,
            std::string hostname,
            voltdb::CatalogId siteId,
            voltdb::CatalogId partitionId,
            voltdb::CatalogId databaseId);

protected:

    /**
     * Update the stats tuple with the latest statistics available to this StatsSource.
     */
    virtual void updateStatsTuple(voltdb::TableTuple *tuple);

    /**
     * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
     * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
     * contributing to the end of the list.
     */
    virtual std::vector<std::string> generateStatsColumnNames();

    /**
     * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
     * end of a list.
     */
    virtual void populateSchema(std::vector<voltdb::ValueType> &types, std::vector<int32_t> &columnLengths, std::vector<bool> &allowNull);

    ~AntiCacheStats();

private:
    /**
     * AntiCacheDB whose stats are being collected.
     */
    AntiCacheDB * m_acdb;

    // AntiCacheID 
    int16_t m_acid;

    // Blocks/bytes evicted in last interval
    int32_t m_lastBlocksEvicted;
    int64_t m_lastBytesEvicted;

    // blocks/bytes unevicted in last inverval
    int32_t m_lastBlocksUnevicted;
    int64_t m_lastBytesUnevicted;

    // blocks/bytes evicted/unevicted in total
    int32_t m_totalBlocksEvicted;
    int64_t m_totalBytesEvicted;
    int32_t m_totalBlocksUnevicted;
    int64_t m_totalBytesUnevicted;

    // current number of blocks in storage
    int32_t m_currentEvictedBlocks;
    int64_t m_currentEvictedBytes;
    int32_t m_currentFreeBlocks;
    int64_t m_currentFreeBytes;
};

}

#endif /* ANTICACHESTATS_H_ */
