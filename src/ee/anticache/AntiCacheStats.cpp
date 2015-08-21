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
#include "anticache/AntiCacheStats.h"
#include "stats/StatsSource.h"
#include "common/TupleSchema.h"
#include "common/ids.h"
#include "common/ValueFactory.hpp"
#include "common/tabletuple.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include <vector>
#include <string>

using namespace voltdb;
using namespace std;

vector<string> AntiCacheStats::generateAntiCacheStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();

    columnNames.push_back("ANTICACHE_ID");
    columnNames.push_back("TABLE_ID");

    columnNames.push_back("ANTICACHE_LAST_BLOCKS_EVICTED");
    columnNames.push_back("ANTICACHE_LAST_BYTES_EVICTED");
    columnNames.push_back("ANTICACHE_LAST_BLOCKS_UNEVICTED");
    columnNames.push_back("ANTICACHE_LAST_BYTES_UNEVICTED");

    columnNames.push_back("ANTICACHE_TOTAL_BLOCKS_EVICTED");
    columnNames.push_back("ANTICACHE_TOTAL_BYTES_EVICTED");
    columnNames.push_back("ANTICACHE_TOTAL_BLOCKS_UNEVICTED");
    columnNames.push_back("ANTICACHE_TOTAL_BYTES_UNEVICTED");

    columnNames.push_back("ANTICACHE_BLOCKS_STORED");
    columnNames.push_back("ANTICACHE_BYTES_STORED");
    columnNames.push_back("ANTICACHE_BLOCKS_FREE");
    columnNames.push_back("ANTICACHE_BYTES_FREE");
    
    return columnNames;
}

void AntiCacheStats::populateAntiCacheStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull);

    // ANTICACHE_ID
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
   
    // TABLE_ID
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);

    //ANTICACHE_LAST_BLOCKS_EVICTED
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_LAST_BYTES_EVICTED
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);
    
    //ANTICACHE_LAST_BLOCKS_UNEVICTED
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_LAST_BYTES_UNEVICTED
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);
    
    //ANTICACHE_TOTAL_BLOCKS_EVICTED
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_TOTAL_BYTES_EVICTED
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);
    
    //ANTICACHE_TOTAL_BLOCKS_UNEVICTED
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_TOTAL_BYTES_UNEVICTED
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);
    
    //ANTICACHE_BLOCKS_STORED
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_BYTES_STORED
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);

    //ANTICACHE_BLOCKS_FREE
    types.push_back(VALUE_TYPE_INTEGER); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER)); 
    allowNull.push_back(false);
    
    //ANTICACHE_BYTES_FREE
    types.push_back(VALUE_TYPE_BIGINT); 
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); 
    allowNull.push_back(false);
}

Table*
AntiCacheStats::generateEmptyAntiCacheStatsTable()
{
    string name = "AntiCacheStats on a per-tier basis";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    CatalogId databaseId = 1;
    vector<string> columnNames = AntiCacheStats::generateAntiCacheStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    AntiCacheStats::populateAntiCacheStatsSchema(columnTypes, columnLengths,
                                         columnAllowNull);
    TupleSchema *schema =
        TupleSchema::createTupleSchema(columnTypes, columnLengths,
                                       columnAllowNull, true);

    return
        reinterpret_cast<Table*>(TableFactory::getTempTable(databaseId,
                                                            name,
                                                            schema,
                                                            &columnNames[0],
                                                            NULL));
}

/*
 * Constructor caches reference to the AntiCacheDB from which stats are generated
 */
AntiCacheStats::AntiCacheStats(Table* table, AntiCacheDB* acdb)
    : StatsSource() 
{
    m_acdb = acdb;
    m_table = table;

    m_acid = 0;

    m_lastBlocksEvicted = 0;
    m_lastBytesEvicted = 0;
    m_lastBlocksUnevicted = 0;
    m_lastBytesUnevicted = 0;

    m_currentBlocksEvicted = 0;
    m_currentBytesEvicted = 0;
    m_currentBlocksUnevicted = 0;
    m_currentBytesUnevicted = 0;


    m_currentEvictedBlocks = 0;
    m_currentEvictedBytes = 0;
    m_currentFreeBlocks = 0;
    m_currentFreeBytes = 0;
}

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
void AntiCacheStats::configure(
        string name,
        CatalogId hostId,
        std::string hostname,
        CatalogId siteId,
        CatalogId partitionId,
        CatalogId databaseId) {
    StatsSource::configure(name, hostId, hostname, siteId, partitionId, databaseId);
    m_acid = acdb->getACID();
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> AntiCacheStats::generateStatsColumnNames() {
    return AntiCacheStats::generateAntiCacheStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void AntiCacheStats::updateStatsTuple(TableTuple *tuple) {

    AntiCacheDB acdb = m_acdb;

    int32_t totalBlocksEvicted = acdb->getBlocksEvicted();
    int64_t totalBytesEvicted = acdb->getBytesEvicted();
    int32_t totalBlocksUnevicted = acdb->getBlocksUnevicted();
    int64_t totalBytesUnevicted = acdb->getBytesUnevicted();

    m_lastBlocksEvicted = m_totalBlocksEvicted - totalBlocksEvicted;
    m_lastBytesEvicted = m_totalBytesEvicted - totalBytesEvicted;
    m_lastBlocksUnevicted = m_totalBlocksUnevicted - totalBlocksUnevicted;
    m_lastBytesUnevicted = m_totalBytesUnevicted - totalBytesUnevicted;

    m_totalBlocksEvicted = totalBlocksEvicted;
    m_totalBytesEvicted = totalBytesEvicted;
    m_totalBlocksUnevicted = totalBlocksUnevicted;
    m_totalBytesUnevicted = totalBytesUnevicted;

    m_currentEvictedBlocks = acdb->getNumBlocks();
    m_currentEvictedBytes = (int64_t)m_currentEvictedBlocks * acdb->getBlockSize();
    m_currentFreeBlocks = acdb->getFreeBlocks();
    m_currentFreeBytes = (int64_t)m_currentFreeBlocks * acdb->getBlockSize();

    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_ID"],
            ValueFactory::getIntegerValue(m_acid));
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_LAST_BLOCKS_EVICTED"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_LAST_BYTES_EVICTED"],
            ValueFactory::getBigIntValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_LAST_BLOCKS_UNEVICTED"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_LAST_BYTES_UNEVICTED"],
            ValueFactory::getBigIntValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_TOTAL_BLOCKS_EVICTED"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_TOTAL_BYTES_EVICTED"],
            ValueFactory::getBigIntValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_TOTAL_BLOCKS_UNEVICTED"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_TOTAL_BYTES_UNEVICTED"],
            ValueFactory::getBigIntValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_BLOCKS_STORED"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_BYTES_STORED"],
            ValueFactory::getBigIntValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_BLOCKS_FREE"],
            ValueFactory::getIntegerValue());
    tuple->setNValue(
            StatsSource::m_columnName2Index["ANTICACHE_BYTES_FREE"],
            ValueFactory::getBigIntValue());

}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void AntiCacheStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    AntiCacheStats::populateAntiCacheStatsSchema(types, columnLengths, allowNull);
}

AntiCacheStats::~AntiCacheStats() {
    m_tableName.free();
    m_tableType.free();
}
