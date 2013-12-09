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
#include "triggers/TriggerStats.h"
#include "triggers/trigger.h"
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

vector<string> TriggerStats::generateTriggerStatsColumnNames() {
    vector<string> columnNames = StatsSource::generateBaseStatsColumnNames();
    columnNames.push_back("TRIGGER_NAME");
    columnNames.push_back("EXECUTION_LATENCY");
    
    return columnNames;
}

void TriggerStats::populateTriggerStatsSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    StatsSource::populateBaseSchema(types, columnLengths, allowNull);
    types.push_back(VALUE_TYPE_VARCHAR); columnLengths.push_back(4096); allowNull.push_back(false);
    types.push_back(VALUE_TYPE_BIGINT); columnLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT)); allowNull.push_back(false);
}

Table*
TriggerStats::generateEmptyTriggerStatsTable()
{
    string name = "Persistent Table aggregated table stats temp table";
    // An empty stats table isn't clearly associated with any specific
    // database ID.  Just pick something that works for now (Yes,
    // abstractplannode::databaseId(), I'm looking in your direction)
    CatalogId databaseId = 1;
    vector<string> columnNames = TriggerStats::generateTriggerStatsColumnNames();
    vector<ValueType> columnTypes;
    vector<int32_t> columnLengths;
    vector<bool> columnAllowNull;
    TriggerStats::populateTriggerStatsSchema(columnTypes, columnLengths,
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
TriggerStats::TriggerStats(Trigger* trigger)
    : StatsSource(), m_trigger(trigger)
{
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
void TriggerStats::configure(
        string name,
        CatalogId hostId,
        std::string hostname,
        CatalogId siteId,
        CatalogId partitionId,
        CatalogId databaseId) {
    StatsSource::configure(name, hostId, hostname, siteId, partitionId, databaseId);
    m_triggerName = ValueFactory::getStringValue(m_trigger->name());
}

/**
 * Generates the list of column names that will be in the statTable_. Derived classes must override this method and call
 * the parent class's version to obtain the list of columns contributed by ancestors and then append the columns they will be
 * contributing to the end of the list.
 */
vector<string> TriggerStats::generateStatsColumnNames() {
    return TriggerStats::generateTriggerStatsColumnNames();
}

/**
 * Update the stats tuple with the latest statistics available to this StatsSource.
 */
void TriggerStats::updateStatsTuple(TableTuple *tuple) {
    tuple->setNValue( StatsSource::m_columnName2Index["TRIGGER_NAME"], m_triggerName);

	int64_t latency = m_trigger->latency();
	tuple->setNValue( StatsSource::m_columnName2Index["EXECUTION_LATENCY"],
                      ValueFactory::
                      getBigIntValue(latency));
}

/**
 * Same pattern as generateStatsColumnNames except the return value is used as an offset into the tuple schema instead of appending to
 * end of a list.
 */
void TriggerStats::populateSchema(
        vector<ValueType> &types,
        vector<int32_t> &columnLengths,
        vector<bool> &allowNull) {
    TriggerStats::populateTriggerStatsSchema(types, columnLengths, allowNull);
}

TriggerStats::~TriggerStats() {
    m_triggerName.free();
}

}
