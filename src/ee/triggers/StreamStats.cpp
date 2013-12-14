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
#include "triggers/StreamStats.h"
#include "storage/persistenttable.h"
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

namespace voltdb {

vector<string> StreamStats::generateStreamStatsColumnNames() {
    VOLT_DEBUG("hawk : StreamStats::generateStreamStatsColumnNames ... ");
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    columnNames.push_back("STREAM_NAME");
    columnNames.push_back("EXECUTION_LATENCY");
    columnNames.push_back("DELETE_LATENCY");
    
    return columnNames;
}

void StreamStats::populateStreamStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    VOLT_DEBUG("hawk : StreamStats::populateStreamStatsSchema ... ");
    StatsSource::populateBaseSchema(types, columnLengths, allowNull);
    types.push_back(VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);
    types.push_back(VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); allowNull.push_back(false);
    types.push_back(VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); allowNull.push_back(false);
}

Table*
StreamStats::generateEmptyStreamStatsTable()
{
    VOLT_DEBUG("hawk : StreamStats::generateEmptyStreamStatsTable ... ");
    string name = "Persistent Table aggregated table stats temp table";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    CatalogId databaseId = 1;
    vector<string> columnNames = StreamStats::generateStreamStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    StreamStats::populateStreamStatsSchema(columnTypes, columnLengths,
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
 * Constructor caches reference to the table that will be generating the statistics
 */
StreamStats::StreamStats(voltdb::PersistentTable* table)
    : StatsSource(), m_stream(table)
{
    VOLT_DEBUG("hawk : StreamStats::StreamStats ... ");
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
void StreamStats::configure(
        string name,
        CatalogId hostId,
        std::string hostname,
        CatalogId siteId,
        CatalogId partitionId,
        CatalogId databaseId) {
    VOLT_DEBUG("hawk : StreamStats::configure ... ");
    StatsSource::configure(name, hostId, hostname, siteId, partitionId, databaseId);
    m_streamName = ValueFactory::getStringValue(m_stream->name());
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> StreamStats::generateStatsColumnNames() {
    VOLT_DEBUG("hawk : StreamStats::generateStatsColumnNames ... ");
    return StreamStats::generateStreamStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void StreamStats::updateStatsTuple(TableTuple *tuple) {
     
    VOLT_DEBUG("hawk : Stream - starting updateStatsTuple ");
    tuple->setNValue( StatsSource::m_columnName2Index["STREAM_NAME"], m_streamName);

    int64_t latency = m_stream->latency();
    VOLT_DEBUG("hawk : 1 - latency : %ld ... ", latency);
    tuple->setNValue( StatsSource::m_columnName2Index["EXECUTION_LATENCY"],
                      ValueFactory::
                      getBigIntValue(latency));
    
    int64_t delete_latency = m_stream->delete_latency();
    VOLT_DEBUG("hawk : 2 delete_latency : %ld... ", delete_latency);
    tuple->setNValue( StatsSource::m_columnName2Index["DELETE_LATENCY"],
                      ValueFactory::
                      getBigIntValue(delete_latency));

    VOLT_DEBUG("hawk : Stream - updateStatsTuple - latency - %ld, delete latency - %ld", latency, delete_latency);
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void StreamStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    VOLT_DEBUG("hawk : StreamStats::populateSchema ... ");
    StreamStats::populateStreamStatsSchema(types, columnLengths, allowNull);
}

StreamStats::~StreamStats() {
    m_streamName.free();
}

}
