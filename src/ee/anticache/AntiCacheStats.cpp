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

/*
 * Constructor caches reference to the table that will be generating the statistics
 */
AntiCacheStats::AntiCacheStats(Table* table)
    : StatsSource(),
        m_table(table),
        m_lastTuplesEvicted(0),
        m_lastBlocksEvicted(0),
        m_lastBytesEvicted(0) {
        
    // Nothing to do...
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
    m_tableName = ValueFactory::getStringValue(m_table->name());
}

vector<string> AntiCacheStats::generateStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    columnNames.push_back("TABLE_NAME");
    columnNames.push_back("TUPLES_EVICTED");
    columnNames.push_back("BLOCKS_EVICTED");
    columnNames.push_back("BYTES_EVICTED");
    return columnNames;
}

void AntiCacheStats::populateSchema(
    vector<ValueType> &types,
    vector<int32_t> &columnLengths,
    vector<bool> &allowNull) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull);
    
    // TABLE_NAME
    types.push_back(VALUE_TYPE_VARCHAR);
    columnLengths.push_back(4096);
    allowNull.push_back(false);
    
    // TUPLES_EVICTED
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);
    
    // BLOCKS_EVICTED
    types.push_back(VALUE_TYPE_INTEGER);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    allowNull.push_back(false);
    
    // BYTES_EVICTED
    types.push_back(VALUE_TYPE_BIGINT);
    columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    allowNull.push_back(false);
}


/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void AntiCacheStats::updateStatsTuple(TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["TABLE_NAME"], m_tableName);

    // TODO: Figure out where we are going to get the anti-cache information from
}



AntiCacheStats::~AntiCacheStats() {
    m_tableName.free();
}
