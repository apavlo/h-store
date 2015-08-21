/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008 by H-Store Project
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

#include <iostream>
#include <stdio.h>
#include <inttypes.h>
#include <fstream>
#include <errno.h>
#include <sstream>
#include <unistd.h>
#include <locale>
#include "boost/shared_array.hpp"
#include "boost/scoped_array.hpp"
#include "boost/foreach.hpp"
#include "boost/scoped_ptr.hpp"
#include "VoltDBEngine.h"
#include "common/common.h"
#include "common/debuglog.h"
#include "common/serializeio.h"
#include "common/valuevector.h"
#include "common/TheHashinator.h"
#include "common/DummyUndoQuantum.hpp"
#include "common/tabletuple.h"
#include "common/types.h"
#include "common/executorcontext.hpp"
#include "common/FatalException.hpp"
#include "common/RecoveryProtoMessage.h"
#include "catalog/catalogmap.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/site.h"
#include "catalog/partition.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "catalog/index.h"
#include "catalog/column.h"
#include "catalog/columnref.h"
#include "catalog/procedure.h"
#include "catalog/statement.h"
#include "catalog/planfragment.h"
#include "catalog/constraint.h"
#include "catalog/materializedviewinfo.h"
#include "catalog/connector.h"
#include "plannodes/abstractplannode.h"
#include "plannodes/abstractscannode.h"
#include "plannodes/nodes.h"
#include "plannodes/plannodeutil.h"
#include "plannodes/plannodefragment.h"
#include "executors/executors.h"
#include "executors/executorutil.h"
#include "storage/table.h"
#include "storage/tablefactory.h"
#include "indexes/tableindex.h"
#include "storage/constraintutil.h"
#include "storage/persistenttable.h"
#include "storage/MaterializedViewMetadata.h"
#include "storage/StreamBlock.h"
#include "storage/TableCatalogDelegate.hpp"
#include "org_voltdb_jni_ExecutionEngine.h" // to use static values
#include "stats/StatsAgent.h"
#include "voltdbipc.h"
#include "common/FailureInjection.h"

// ARIES
#include "logging/Logrecord.h"
#include "logging/AriesLogProxy.h"
#include <string>

#define BUFFER_SIZE         1024*1024*300    // 100 MB buffer for reading in log file

using namespace std;
namespace voltdb {

const int64_t AD_HOC_FRAG_ID = -1;

VoltDBEngine::VoltDBEngine(Topend *topend, LogProxy *logProxy) :
        m_currentUndoQuantum(NULL),
        m_catalogVersion(0), m_staticParams(
        MAX_PARAM_COUNT),
        m_currentOutputDepId(-1),
        m_currentInputDepId(-1),
        m_isELEnabled(false),
        m_stringPool(16777216, 2),
        m_numResultDependencies(0),
        m_templateSingleLongTable(NULL),
        m_topend(topend),
        m_logProxy(logProxy),
        m_logManager(new LogManager(logProxy)),
        m_ARIESEnabled(false) {
    m_currentUndoQuantum = new DummyUndoQuantum();

    // init the number of planfragments executed
    m_pfCount = 0;

    // require a site id, at least, to inititalize.
    m_executorContext = NULL;

    m_ariesWriteOffset = 0;
    m_isRecovering = false;
    // m_logManager.setAriesProxyEngine(this);
}

bool VoltDBEngine::initialize(int32_t clusterIndex, int32_t siteId,
        int32_t partitionId, int32_t hostId, string hostname) {
    // Be explicit about running in the standard C locale for now.
    locale::global(locale("C"));
    m_clusterIndex = clusterIndex;
    m_siteId = siteId;
    m_partitionId = partitionId;

    // Instantiate our catalog - it will be populated later on by load()
    m_catalog = boost::shared_ptr<catalog::Catalog>(new catalog::Catalog());

    // create the template single long (int) table
    assert(m_templateSingleLongTable == NULL);
    m_templateSingleLongTable = new char[m_templateSingleLongTableSize];
    memset(m_templateSingleLongTable, 0, m_templateSingleLongTableSize);
    m_templateSingleLongTable[7] = 28; // table size
    m_templateSingleLongTable[11] = 8; // size of header
    m_templateSingleLongTable[13] = 0; // status code
    m_templateSingleLongTable[14] = 1; // number of columns
    m_templateSingleLongTable[15] = VALUE_TYPE_BIGINT; // column type
    m_templateSingleLongTable[16] = 0; // column name length
    m_templateSingleLongTable[23] = 1; // row count
    m_templateSingleLongTable[27] = 8; // row size

    // required for catalog loading.
    m_executorContext = new ExecutorContext(siteId, m_partitionId,
            m_currentUndoQuantum, getTopend(), m_isELEnabled, 0, /* epoch not yet known */
            hostname, hostId);

    return true;
}

VoltDBEngine::~VoltDBEngine() {
    // WARNING WARNING WARNING
    // The sequence below in which objects are cleaned up/deleted is
    // fragile.  Reordering or adding additional destruction below
    // greatly increases the risk of accidentally freeing the same
    // object multiple times.  Change at your own risk.
    // --izzy 8/19/2009

    // Get rid of any dummy undo quantum first so m_undoLog.clear()
    // doesn't wipe this out before we do it.
    if (m_currentUndoQuantum != NULL && m_currentUndoQuantum->isDummy()) {
        delete m_currentUndoQuantum;
    }

    // Clear the undo log before deleting the persistent tables so
    // that the persistent table schema are still around so we can
    // actually find the memory that has been allocated to non-inlined
    // strings and deallocated it.
    m_undoLog.clear();

    for (int ii = 0; ii < m_planFragments.size(); ii++) {
        delete m_planFragments[ii];
    }

    // clean up memory for the template memory for the single long (int) table
    if (m_templateSingleLongTable) {
        delete[] m_templateSingleLongTable;
    }

    // Delete table delegates and release any table reference counts.
    typedef pair<int64_t, Table*> TIDPair;
    typedef pair<string, CatalogDelegate*> CDPair;

    BOOST_FOREACH (CDPair cdPair, m_catalogDelegates){
        delete cdPair.second;
    }
    m_catalogDelegates.clear();

    BOOST_FOREACH (TIDPair tidPair, m_snapshottingTables){
        tidPair.second->decrementRefcount();
    }
    m_snapshottingTables.clear();

    BOOST_FOREACH (TIDPair tidPair, m_exportingTables){
        tidPair.second->decrementRefcount();
    }
    m_exportingTables.clear();

    delete m_topend;
    delete m_executorContext;

    delete m_logManager;
}

// ------------------------------------------------------------------
// OBJECT ACCESS FUNCTIONS
// ------------------------------------------------------------------
catalog::Catalog *VoltDBEngine::getCatalog() const {
    return (m_catalog.get());
}

Table* VoltDBEngine::getTable(int32_t tableId) const {
    // Caller responsible for checking null return value.
    map<int32_t, Table*>::const_iterator lookup = m_tables.find(tableId);
    if (lookup != m_tables.end()) {
        return lookup->second;
    }
    return NULL;
}

Table* VoltDBEngine::getTable(string name) const {
    // Caller responsible for checking null return value.
    map<string, Table*>::const_iterator lookup = m_tablesByName.find(name);
    if (lookup != m_tablesByName.end()) {
        return lookup->second;
    }
    return NULL;
}

bool VoltDBEngine::serializeTable(int32_t tableId, SerializeOutput* out) const {
    // Just look in our list of tables
    map<int32_t, Table*>::const_iterator lookup = m_tables.find(tableId);
    if (lookup != m_tables.end()) {
        Table* table = lookup->second;
        table->serializeTo(*out);
        return true;
    } else {
        throwFatalException("Unable to find table for TableId '%d'",
                (int ) tableId);
    }
}

// ------------------------------------------------------------------
// EXECUTION FUNCTIONS
// ------------------------------------------------------------------
int VoltDBEngine::executeQuery(int64_t planfragmentId,
        int32_t outputDependencyId, int32_t inputDependencyId,
        const NValueArray &params, int64_t txnId, int64_t lastCommittedTxnId,
        bool first, bool last) {
    Table *cleanUpTable = NULL;
    m_currentOutputDepId = outputDependencyId;
    m_currentInputDepId = inputDependencyId;

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies and for the dirty byte. Necessary for a
     * plan fragment because the number of produced depenencies may
     * not be known in advance.
     */
    if (first) {
        m_startOfResultBuffer = m_resultOutput.reserveBytes(
                sizeof(int32_t) + sizeof(int8_t));
        m_dirtyFragmentBatch = false;
    }

    // set this to zero for dml operations
    m_tuplesModified = 0;

    /*
     * Reserve space in the result output buffer for the number of
     * result dependencies generated by this particular plan fragment.
     * Necessary for a plan fragment because the
     * number of produced depenencies may not be known in advance.
     */
    m_numResultDependencies = 0;
    size_t numResultDependenciesCountOffset = m_resultOutput.reserveBytes(4);

    // configure the execution context.
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId,
            lastCommittedTxnId);

    // count the number of plan fragments executed
    ++m_pfCount;

    // execution lists for planfragments are cached by planfragment id
    assert(planfragmentId >= -1);
//     fprintf(stderr, "Looking to execute fragid %jd\n", (intmax_t)planfragmentId);
//     
//     std::map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator pavlo_it;
//     fprintf(stderr, "-----------------------------\n");
//     for (pavlo_it = m_executorMap.begin();
//          pavlo_it != m_executorMap.end(); pavlo_it++) {
//         fprintf(stderr, "PlanFragment: %jd\n", (intmax_t)pavlo_it->first);
//     } // FOR
//     fprintf(stderr, "-----------------------------\n");

    std::map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator iter =
            m_executorMap.find(planfragmentId);
    assert(iter != m_executorMap.end());
    boost::shared_ptr<ExecutorVector> execsForFrag = iter->second;

    // Read/Write Set Tracking
    ReadWriteTracker *tracker = NULL;
    if (m_executorContext->isTrackingEnabled()) {
        ReadWriteTrackerManager *trackerMgr =
                m_executorContext->getTrackerManager();
        tracker = trackerMgr->getTracker(txnId);
    }
    
    // PAVLO: If we see a SendPlanNode with the "fake" flag set to true,
    // then we won't really execute it and instead will send back the
    // number of tuples that we modified
    bool send_tuple_count = false;

    // Walk through the queue and execute each plannode.  The query
    // planner guarantees that for a given plannode, all of its
    // children are positioned before it in this list, therefore
    // dependency tracking is not needed here.
    size_t ttl = execsForFrag->list.size();
    for (int ctr = 0; ctr < ttl; ++ctr) {
        AbstractExecutor *executor = execsForFrag->list[ctr];
        assert(executor);

        if (executor->needsPostExecuteClear())
            cleanUpTable =
                    dynamic_cast<Table*>(executor->getPlanNode()->getOutputTable());

        // PAVLO: Check whether we don't need to execute anything and should just
        // send back the number of tuples modified
        if (executor->forceTupleCount()) {
            send_tuple_count = true;
            VOLT_TRACE(
                    "[PlanFragment %jd] Forcing tuple count at PlanNode #%02d for txn #%jd [OutputDep=%d]",
                    (intmax_t)planfragmentId,
                    executor->getPlanNode()->getPlanNodeId(), (intmax_t)txnId,
                    m_currentOutputDepId);
        } else {
            VOLT_TRACE(
                    "[PlanFragment %jd] Executing PlanNode #%02d for txn #%jd [OutputDep=%d]",
                    (intmax_t)planfragmentId,
                    executor->getPlanNode()->getPlanNodeId(), (intmax_t)txnId,
                    m_currentOutputDepId);
            try {
                // Now call the execute method to actually perform whatever action
                // it is that the node is supposed to do...
                if (!executor->execute(params, tracker)) {
                    VOLT_DEBUG(
                            "The Executor's execution at position '%d' failed for PlanFragment '%jd'",
                            ctr, (intmax_t)planfragmentId);
                    if (cleanUpTable != NULL)
                        cleanUpTable->deleteAllTuples(false);
                    // set these back to -1 for error handling
                    m_currentOutputDepId = -1;
                    m_currentInputDepId = -1;
                    return ENGINE_ERRORCODE_ERROR;
                }
            } catch (SerializableEEException &e) {
                VOLT_DEBUG(
                        "The Executor's execution at position '%d' failed for PlanFragment '%jd'",
                        ctr, (intmax_t)planfragmentId);
                VOLT_INFO("SerializableEEException: %s", e.message().c_str());
                if (cleanUpTable != NULL)
                    cleanUpTable->deleteAllTuples(false);
                resetReusedResultOutputBuffer();
                e.serialize(getExceptionOutputSerializer());

                // set these back to -1 for error handling
                m_currentOutputDepId = -1;
                m_currentInputDepId = -1;
                return ENGINE_ERRORCODE_ERROR;
            }
        }
    }
    if (cleanUpTable != NULL)
        cleanUpTable->deleteAllTuples(false);

    // assume this is sendless dml
    if (send_tuple_count || m_numResultDependencies == 0) {
        // put the number of tuples modified into our simple table
        uint64_t changedCount = htonll(m_tuplesModified);
        memcpy(m_templateSingleLongTable + m_templateSingleLongTableSize - 8,
                &changedCount, sizeof(changedCount));
        m_resultOutput.writeBytes(m_templateSingleLongTable,
                m_templateSingleLongTableSize);
        m_numResultDependencies++;
    }

    //Write the number of result dependencies if necessary.
    m_resultOutput.writeIntAt(numResultDependenciesCountOffset,
            m_numResultDependencies);

    // if a fragment modifies any tuples, the whole batch is dirty
    if (m_tuplesModified > 0)
        m_dirtyFragmentBatch = true;

    // write dirty-ness of the batch and number of dependencies output to the FRONT of
    // the result buffer
    if (last) {
        m_resultOutput.writeIntAt(m_startOfResultBuffer,
                static_cast<int32_t>((m_resultOutput.position()
                        - m_startOfResultBuffer) - sizeof(int32_t)));
        m_resultOutput.writeBoolAt(m_startOfResultBuffer + sizeof(int32_t),
                m_dirtyFragmentBatch);
    }

    // set these back to -1 for error handling
    m_currentOutputDepId = -1;
    m_currentInputDepId = -1;

    VOLT_TRACE("Finished executing.");
    return ENGINE_ERRORCODE_SUCCESS;
}

/*
 * Execute the supplied fragment in the context of the specified
 * cluster and database with the supplied parameters as arguments. A
 * catalog with all the necessary tables needs to already have been
 * loaded.
 */
int VoltDBEngine::executePlanFragment(string fragmentString,
        int32_t outputDependencyId, int32_t inputDependencyId, int64_t txnId,
        int64_t lastCommittedTxnId) {
    int retval = ENGINE_ERRORCODE_ERROR;

    m_currentOutputDepId = outputDependencyId;
    m_currentInputDepId = inputDependencyId;

    // how many current plans (too see if we added any)
    size_t frags = m_planFragments.size();

    boost::scoped_array<char> buffer(new char[fragmentString.size() * 2 + 1]);
    catalog::Catalog::hexEncodeString(fragmentString.c_str(), buffer.get());
    string hexEncodedFragment(buffer.get());

    try {
        if (initPlanFragment(AD_HOC_FRAG_ID, hexEncodedFragment)) {
            NValueArray parameterValueArray(0);
            retval = executeQuery(AD_HOC_FRAG_ID, outputDependencyId,
                    inputDependencyId, parameterValueArray, txnId,
                    lastCommittedTxnId, true, true);
        } else {
            char message[128];
            snprintf(message, 128, "Unable to load ad-hoc plan fragment for"
                    " transaction %jd.", (intmax_t) txnId);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    message);
        }
    } catch (SerializableEEException &e) {
        VOLT_TRACE(
                "executePlanFragment: failed to initialize " "ad-hoc plan fragment");
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        retval = ENGINE_ERRORCODE_ERROR;
    }

    // clean up stuff
    m_executorMap.erase(AD_HOC_FRAG_ID);

    // delete any generated plan
    size_t nowFrags = m_planFragments.size();
    if (nowFrags > frags) {
        assert((nowFrags - frags) == 1);
        delete m_planFragments.back();
        m_planFragments.pop_back();
    }

    // set these back to -1 for error handling
    m_currentOutputDepId = -1;
    m_currentInputDepId = -1;

    return retval;
}

// -------------------------------------------------
// RESULT FUNCTIONS
// -------------------------------------------------
bool VoltDBEngine::send(Table* dependency) {
    VOLT_TRACE("Sending Dependency '%d' from C++", m_currentOutputDepId);
    m_resultOutput.writeInt(m_currentOutputDepId);
    if (!dependency->serializeTo(m_resultOutput))
        return false;
    m_numResultDependencies++;
    return true;
}

int VoltDBEngine::loadNextDependency(Table* destination) {
    return m_topend->loadNextDependency(m_currentInputDepId, &m_stringPool,
            destination);
}

// -------------------------------------------------
// Catalog Functions
// -------------------------------------------------
bool VoltDBEngine::updateCatalogDatabaseReference() {
    catalog::Cluster *cluster = m_catalog->clusters().get("cluster");
    if (!cluster) {
        VOLT_ERROR("Unable to find cluster catalog information");
        return false;
    }

    m_database = cluster->databases().get("database");
    if (!m_database) {
        VOLT_ERROR("Unable to find database catalog information");
        return false;
    }

    return true;
}

bool VoltDBEngine::loadCatalog(const string &catalogPayload) {
    assert(m_catalog != NULL);
    VOLT_DEBUG("Loading catalog...");
    m_catalog->execute(catalogPayload);

    if (updateCatalogDatabaseReference() == false) {
        return false;
    }

    // initialize the list of partition ids
    bool success = initCluster();
    if (success == false) {
        VOLT_ERROR("Unable to load partition list for cluster");
        return false;
    }

    // Tables care about EL state.
    if (m_database->connectors().size() > 0
            && m_database->connectors().get("0")->enabled()) {
        VOLT_DEBUG("EL enabled.");
        m_executorContext->m_exportEnabled = true;
        m_isELEnabled = true;
    }

    // load up all the tables, adding all tables
    if (processCatalogAdditions(true) == false) {
        return false;
    }

    if (rebuildTableCollections() == false) {
        VOLT_ERROR("Error updating catalog id mappings for tables.");
        return false;
    }

    // load up all the materialized views
    initMaterializedViews(true);

    // load the plan fragments from the catalog
    if (!rebuildPlanFragmentCollections())
        return false;

    VOLT_DEBUG("Loaded catalog...");
    return true;
}

/*
 * Obtain the recent deletion list from the catalog.  For any item in
 * that list with a corresponding table delegate, process a deletion.
 *
 * TODO: This should be extended to find the parent delegate if the
 * deletion isn't a top-level object .. and delegates should have a
 * deleteChildCommand() interface.
 */
bool VoltDBEngine::processCatalogDeletes() {
    vector<string> deletions;
    m_catalog->getDeletedPaths(deletions);
    vector<string>::iterator pathIter = deletions.begin();
    while (pathIter != deletions.end()) {
        map<string, CatalogDelegate*>::iterator pos;
        if ((pos = m_catalogDelegates.find(*pathIter))
                != m_catalogDelegates.end()) {
            pos->second->deleteCommand();
            delete pos->second;
            m_catalogDelegates.erase(pos++);
        }
        ++pathIter;
    }
    return true;
}

/*
 * Create catalog delegates for new catalog items.
 */
bool VoltDBEngine::processCatalogAdditions(bool addAll) {
    // process new tables.
    map<string, catalog::Table*>::const_iterator it =
            m_database->tables().begin();
    while (it != m_database->tables().end()) {
        catalog::Table *t = it->second;
        if (addAll || t->wasAdded()) {
            TableCatalogDelegate *tcd = new TableCatalogDelegate(
                    m_catalogVersion, t->relativeIndex(), t->path());
            if (tcd->init(m_executorContext, *m_database, *t) != 0) {
                VOLT_ERROR("Failed to initialize table '%s' from catalog",
                        it->second->name().c_str());
                return false;
            }
            m_catalogDelegates[tcd->path()] = tcd;
            if (tcd->exportEnabled()) {
                tcd->getTable()->incrementRefcount();
                m_exportingTables[tcd->delegateId()] = tcd->getTable();
            }
        }
        ++it;
    }

    // new plan fragments are handled differently.
    return true;
}

/*
 * Accept a list of catalog commands expressing a diff between the
 * current and the desired catalog. Execute those commands and create,
 * delete or modify the corresponding exectution engine objects.
 */
bool VoltDBEngine::updateCatalog(const string &catalogPayload,
        int catalogVersion) {
    assert(m_catalog != NULL); // the engine must be initialized
    assert((m_catalogVersion + 1) == catalogVersion);

    VOLT_DEBUG("Updating catalog...");

    // apply the diff commands to the existing catalog
    // throws SerializeEEExceptions on error.
    m_catalog->execute(catalogPayload);
    m_catalogVersion = catalogVersion;

    if (updateCatalogDatabaseReference() == false) {
        VOLT_ERROR("Error re-caching catalog references.");
        return false;
    }

    if (processCatalogDeletes() == false) {
        VOLT_ERROR("Error processing catalog deletions.");
        return false;
    }

    if (processCatalogAdditions(false) == false) {
        VOLT_ERROR("Error processing catalog additions.");
        return false;
    }

    if (rebuildTableCollections() == false) {
        VOLT_ERROR("Error updating catalog id mappings for tables.");
        return false;
    }

    if (initMaterializedViews(false) == false) {
        VOLT_ERROR("Error update materialized view definitions.");
        return false;
    }

    // stored procedure catalog changes aren't written using delegates
    if (!rebuildPlanFragmentCollections()) {
        VOLT_ERROR("Error updating catalog planfragments");
        return false;
    }

    m_catalog->purgeDeletions();
    VOLT_DEBUG("Updated catalog...");
    return true;
}

bool VoltDBEngine::loadTable(bool allowExport, int32_t tableId,
        ReferenceSerializeInput &serializeIn, int64_t txnId,
        int64_t lastCommittedTxnId) {
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum(), txnId,
            lastCommittedTxnId);

    Table* ret = getTable(tableId);
    if (ret == NULL) {
        VOLT_ERROR("Table ID %d doesn't exist. Could not load data",
                (int ) tableId);
        return false;
    }

    PersistentTable* table = dynamic_cast<PersistentTable*>(ret);
    if (table == NULL) {
        VOLT_ERROR("Table ID %d(name '%s') is not a persistent table."
                " Could not load data", (int ) tableId, ret->name().c_str());
        return false;
    }

    return loadTable(table, serializeIn, txnId, lastCommittedTxnId, true);
}

bool VoltDBEngine::loadTable(Table *table,
        ReferenceSerializeInput &serializeIn, int64_t txnId,
        int64_t lastCommittedTxnId, bool isExecutionNormal) {

#ifdef ARIES
    // Don't do this if we are recovering
    if (isARIESEnabled() && isExecutionNormal) {
        LogRecord *logrecord = new LogRecord(computeTimeStamp(),
                LogRecord::T_BULKLOAD,    // we are bulk loading bytes directly
                LogRecord::T_FORWARD,// the system is running normally
                -1,// XXX: prevLSN
                txnId,// xid
                getSiteId(),// which execution site
                table->name(),// the table affected
                NULL,// bulk-load, no primary key
                -1,// inserting, all columns affected
                NULL,// insert, don't care about modified cols
                NULL,// no before image
                NULL// no TableTuple for after image, will store bytes directly
        );

        size_t logrecordEstLength = logrecord->getEstimatedLength();

        // We could also include the length of the
        // entire buffer of raw tuples while allocating
        // the log record buffer but that might just be too slow
        // and the allocated array just way too big.
        char *logrecordBuffer = new char[logrecordEstLength];

        FallbackSerializeOutput output;
        output.initializeWithPosition(logrecordBuffer, logrecordEstLength, 0);

        logrecord->serializeTo(output);

        LogManager* m_logManager = getLogManager();
        Logger m_ariesLogger = m_logManager->getAriesLogger();

        const Logger *logger = m_logManager->getThreadLogger(LOGGERID_MM_ARIES);

        assert(logger != NULL);

        // we could ALSO directly write via writeToAriesLogBuffer(buffer, size)
        // but not doing that for consistency while logging to Aries.
        logger->log(LOGLEVEL_INFO, output.data(), output.position());

        // CAREFUL -- the number of bytes might just be too many
        // Its possible they could cause a buffer overflow
        // in the shared Aries buffer.
        // XXX: either increase size of buffer in ExecutionEngineJNI
        // OR check buffer array bounds and flush periodically
        // as the buffer fills up. The latter could be slow at runtime.
        size_t numBytes = serializeIn.numBytesNotYetRead();

        int64_t value = htonll(numBytes);

        // first log the size of the bulkload array
        logger->log(LOGLEVEL_INFO, reinterpret_cast<char*>(&value), sizeof(value));

        // next log the raw bytes of the bulkload array
        logger->log(LOGLEVEL_INFO, reinterpret_cast<const char *>(serializeIn.getRawPointer(0)), numBytes);

        delete[] logrecordBuffer;
        logrecordBuffer = NULL;

        delete logrecord;
        logrecord = NULL;
    }
#endif

    try {
        bool allowExport = false;
        table->loadTuplesFrom(allowExport, serializeIn);
    } catch (SerializableEEException e) {
        throwFatalException("%s", e.message().c_str());
    }
    return true;
}

/*
 * Delete and rebuild id based table collections. Does not affect
 * any currently stored tuples.
 */
bool VoltDBEngine::rebuildTableCollections() {
    // 1. See header comments explaining m_snapshottingTables.
    // 2. Don't clear m_exportTables. They are still exporting, even if deleted.
    // 3. Clear everything else.
    m_tables.clear();
    m_tablesByName.clear();

    // need to re-map all the table ids.
    getStatsManager().unregisterStatsSource(STATISTICS_SELECTOR_TYPE_TABLE);

    //map<string, catalog::Table*>::const_iterator it = m_database->tables().begin();
    map<string, CatalogDelegate*>::iterator cdIt = m_catalogDelegates.begin();

    // walk the table delegates and update local table collections
    while (cdIt != m_catalogDelegates.end()) {
        TableCatalogDelegate *tcd =
                dynamic_cast<TableCatalogDelegate*>(cdIt->second);
        if (tcd) {
            catalog::Table *catTable = m_database->tables().get(
                    tcd->getTable()->name());
            m_tables[catTable->relativeIndex()] = tcd->getTable();
            m_tablesByName[tcd->getTable()->name()] = tcd->getTable();

            getStatsManager().registerStatsSource(
                    STATISTICS_SELECTOR_TYPE_TABLE, catTable->relativeIndex(),
                    tcd->getTable()->getTableStats());

            // add all of the indexes to the stats source
            std::vector<TableIndex*> tindexes = tcd->getTable()->allIndexes();
            CatalogId tableId = static_cast<CatalogId>(catTable->relativeIndex());
            for (int i = 0; i < tindexes.size(); i++) {
                TableIndex *index = tindexes[i];
                // Pay attention here because this is important!
                // Because the relative indexes for the catalog objects are based on
                // their parent object, that means that we can't use the indexes' relativeIndex
                // field to uniquely identify them because they are overwritten for 
                // each table. So this means that we have to generate a composite
                // key of the table's relativeIndex + index's relativeIndex so that can
                // uniquely identify them. The Java layer doesn't need to know
                // about this hot mess!
                CatalogId indexId = computeIndexStatsId(tableId, static_cast<CatalogId>(i+1));
                VOLT_DEBUG("CREATE IndexStats: %s.%s -> %d\n",
                           tcd->getTable()->name().c_str(), index->getName().c_str(), indexId);
                getStatsManager().registerStatsSource(
                        STATISTICS_SELECTOR_TYPE_INDEX,
                        indexId, index->getIndexStats());
            }
        }
        cdIt++;
    }

    return true;
}

/*
 * Delete and rebuild all plan fragments.
 */
bool VoltDBEngine::rebuildPlanFragmentCollections() {
    for (int ii = 0; ii < m_planFragments.size(); ii++)
        delete m_planFragments[ii];
    m_planFragments.clear();
    m_executorMap.clear();

    // initialize all the planfragments.
    map<string, catalog::Procedure*>::const_iterator proc_iterator;
    for (proc_iterator = m_database->procedures().begin();
            proc_iterator != m_database->procedures().end(); proc_iterator++) {
        // Procedure
        const catalog::Procedure *catalog_proc = proc_iterator->second;
        VOLT_TRACE("Building Procedure PlanFragment Collections for %s",
                catalog_proc->name().c_str());
        map<string, catalog::Statement*>::const_iterator stmt_iterator;
        for (stmt_iterator = catalog_proc->statements().begin();
                stmt_iterator != catalog_proc->statements().end();
                stmt_iterator++) {
            // PlanFragment
            const catalog::Statement *catalogStmt = stmt_iterator->second;
            VOLT_DEBUG("Initialize Statement: %s : %s",
                    catalogStmt->name().c_str(),
                    catalogStmt->sqltext().c_str());

            map<string, catalog::PlanFragment*>::const_iterator pf_iterator;
            for (pf_iterator = catalogStmt->fragments().begin();
                    pf_iterator != catalogStmt->fragments().end();
                    pf_iterator++) {
                int64_t fragId = uniqueIdForFragment(pf_iterator->second);
                string planNodeTree = pf_iterator->second->plannodetree();
                if (!initPlanFragment(fragId, planNodeTree)) {
                    VOLT_ERROR("Failed to initialize plan fragment '%s' from"
                            " catalogs\nFailed SQL Statement: %s",
                            pf_iterator->second->name().c_str(),
                            catalogStmt->sqltext().c_str());
                    return false;
                }
            }

            // PAVLO: Multi-partition Plan Fragments
            std::map<std::string, catalog::PlanFragment*>::const_iterator pf_iterator2;
            for (pf_iterator2 = catalogStmt->ms_fragments().begin();
                    pf_iterator2 != catalogStmt->ms_fragments().end();
                    pf_iterator2++) {
                int64_t fragId = uniqueIdForFragment(pf_iterator2->second);
//                 fprintf(stderr, "Initializing Multi-Partition: %jd\n", (intmax_t)fragId);
                std::string planNodeTree = pf_iterator2->second->plannodetree();
                if (!initPlanFragment(fragId, planNodeTree)) {
                    VOLT_ERROR(
                            "Failed to initialize multi-partition plan fragment '%s' from"
                                    " catalogs\nFailed SQL Statement: %s",
                            pf_iterator2->second->name().c_str(),
                            catalogStmt->sqltext().c_str());
                    return false;
                }
            }
            // PAVLO

        }
    }

    return true;
}

// -------------------------------------------------
// Initialization Functions
// -------------------------------------------------
bool VoltDBEngine::initPlanFragment(const int64_t fragId,
        const string planNodeTree) {

    // Deserialize the PlanFragment and stick in our local map

    map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator iter =
            m_executorMap.find(fragId);
    if (iter != m_executorMap.end()) {
        VOLT_ERROR("Duplicate PlanNodeList entry for PlanFragment '%jd' during"
                " initialization", (intmax_t )fragId);
        return false;
    }

    // catalog method plannodetree returns PlanNodeList.java
    PlanNodeFragment *pnf = PlanNodeFragment::createFromCatalog(planNodeTree,
            m_database);
    m_planFragments.push_back(pnf);
    VOLT_TRACE("\n%s\n", pnf->debug().c_str());
    assert(pnf->getRootNode());

    if (!pnf->getRootNode()) {
        VOLT_ERROR("Deserialized PlanNodeFragment for PlanFragment '%jd' "
                "does not have a root PlanNode", (intmax_t )fragId);
        return false;
    }

    boost::shared_ptr<ExecutorVector> ev = boost::shared_ptr<ExecutorVector>(
            new ExecutorVector());
    ev->tempTableMemoryInBytes = 0;

    // Initialize each node!
    for (int ctr = 0, cnt = (int) pnf->getExecuteList().size(); ctr < cnt;
            ctr++) {
        if (!initPlanNode(fragId, pnf->getExecuteList()[ctr],
                &(ev->tempTableMemoryInBytes))) {
            VOLT_ERROR("Failed to initialize PlanNode '%s' at position '%d'"
                    " for PlanFragment '%jd'",
                    pnf->getExecuteList()[ctr]->debug().c_str(), ctr,
                    (intmax_t )fragId);
            return false;
        }
    }

    // Initialize the vector of executors for this planfragment, used at runtime.
    for (int ctr = 0, cnt = (int) pnf->getExecuteList().size(); ctr < cnt;
            ctr++) {
        ev->list.push_back(pnf->getExecuteList()[ctr]->getExecutor());
    }
    m_executorMap[fragId] = ev;

    return true;
}

bool VoltDBEngine::initPlanNode(const int64_t fragId, AbstractPlanNode* node,
        int* tempTableMemoryInBytes) {
    assert(node);
    assert(node->getExecutor() == NULL);

    // Executor is created here. An executor is *devoted* to this plannode
    // so that it can cache anything for the plannode
    AbstractExecutor* executor = getNewExecutor(this, node);
    if (executor == NULL)
        return false;
    node->setExecutor(executor);

    // If this PlanNode has an internal PlanNode (e.g., AbstractScanPlanNode can
    // have internal Projections), then we need to make sure that we set that
    // internal node's executor as well
    if (node->getInlinePlanNodes().size() > 0) {
        map<PlanNodeType, AbstractPlanNode*>::iterator internal_it;
        for (internal_it = node->getInlinePlanNodes().begin();
                internal_it != node->getInlinePlanNodes().end();
                internal_it++) {
            AbstractPlanNode* inline_node = internal_it->second;
            if (!initPlanNode(fragId, inline_node, tempTableMemoryInBytes)) {
                VOLT_ERROR("Failed to initialize the internal PlanNode '%s' of"
                        " PlanNode '%s'", inline_node->debug().c_str(),
                        node->debug().c_str());
                return false;
            }
        }
    }

    // Now use the executor to initialize the plannode for execution later on
    if (!executor->init(this, m_database, tempTableMemoryInBytes)) {
        VOLT_ERROR("The Executor failed to initialize PlanNode '%s' for"
                " PlanFragment '%jd'", node->debug().c_str(), (intmax_t )fragId);
        return false;
    }

    return true;
}

/*
 * Iterate catalog tables looking for tables that are materialized
 * view sources.  When found, construct a materialized view metadata
 * object that connects the source and destination tables, and assign
 * that object to the source table.
 *
 * Assumes all tables (sources and destinations) have been constructed.
 * @param addAll Pass true to add all views. Pass false to only add new views.
 */
bool VoltDBEngine::initMaterializedViews(bool addAll) {
    map<string, catalog::Table*>::const_iterator tableIterator;
    // walk tables
    for (tableIterator = m_database->tables().begin();
            tableIterator != m_database->tables().end(); tableIterator++) {
        catalog::Table *srcCatalogTable = tableIterator->second;
        PersistentTable *srcTable =
                dynamic_cast<PersistentTable*>(m_tables[srcCatalogTable->relativeIndex()]);
        // walk views
        map<string, catalog::MaterializedViewInfo*>::const_iterator matviewIterator;
        for (matviewIterator = srcCatalogTable->views().begin();
                matviewIterator != srcCatalogTable->views().end();
                matviewIterator++) {
            catalog::MaterializedViewInfo *catalogView = matviewIterator->second;

            // Skip Vertical Partitions
            if (catalogView->verticalpartition()) {
                VOLT_DEBUG(
                        "Skipping MaterializedViewInfo %s because it is a vertical partition",
                        catalogView->name().c_str());
                continue;
            }

            // connect source and destination tables
            if (addAll || catalogView->wasAdded()) {
                const catalog::Table *destCatalogTable = catalogView->dest();
                PersistentTable *destTable =
                        dynamic_cast<PersistentTable*>(m_tables[destCatalogTable->relativeIndex()]);
                MaterializedViewMetadata *mvmd = new MaterializedViewMetadata(
                        srcTable, destTable, catalogView);
                srcTable->addMaterializedView(mvmd);
                VOLT_DEBUG("Added MaterializedViewMetadata %s [%s->%s]",
                        mvmd->name().c_str(), srcTable->name().c_str(),
                        destTable->name().c_str());
            }
        }
    }

    return true;
}

bool VoltDBEngine::initCluster() {

    catalog::Cluster* catalogCluster = m_catalog->clusters().get("cluster");

    // Find the partition id for this execution site.
//     std::map<std::string, catalog::Site*>::const_iterator site_it;
//     for (site_it = catalogCluster->sites().begin();
//          site_it != catalogCluster->sites().end();
//          site_it++)
//     {
//         catalog::Site *site = site_it->second;
//         assert (site);
//         std::string sname = site->name();
//         if (atoi(sname.c_str()) == m_siteId) {
//             assert(site->partition());
//             std::string pname = site->partition()->name();
//             m_partitionId = atoi(pname.c_str());
//             break;
//         }
//     }
    // need to update executor context as partitionId wasn't
    // available when the structure was initially created.
    m_executorContext->m_partitionId = m_partitionId;
    m_totalPartitions = catalogCluster->num_partitions();
    return true;
}

int VoltDBEngine::getResultsSize() const {
    return static_cast<int>(m_resultOutput.size());
}

void VoltDBEngine::setBuffers(char *parameterBuffer,
        int parameterBuffercapacity, char *resultBuffer,
        int resultBufferCapacity, char *exceptionBuffer,
        int exceptionBufferCapacity) {
    m_parameterBuffer = parameterBuffer;
    m_parameterBufferCapacity = parameterBuffercapacity;

    m_reusedResultBuffer = resultBuffer;
    m_reusedResultCapacity = resultBufferCapacity;

    m_exceptionBuffer = exceptionBuffer;
    m_exceptionBufferCapacity = exceptionBufferCapacity;
}

void VoltDBEngine::setBuffers(char *parameterBuffer,
        int parameterBuffercapacity, char *resultBuffer,
        int resultBufferCapacity, char *exceptionBuffer,
        int exceptionBufferCapacity, char *arieslogBuffer,
        int arieslogBufferCapacity) {
    m_parameterBuffer = parameterBuffer;
    m_parameterBufferCapacity = parameterBuffercapacity;

    m_reusedResultBuffer = resultBuffer;
    m_reusedResultCapacity = resultBufferCapacity;

    m_exceptionBuffer = exceptionBuffer;
    m_exceptionBufferCapacity = exceptionBufferCapacity;

    m_arieslogBuffer = arieslogBuffer;
    m_arieslogBufferCapacity = arieslogBufferCapacity;
}

// -------------------------------------------------
// MISC FUNCTIONS
// -------------------------------------------------

void VoltDBEngine::printReport() {
    std::cout << "==========" << std::endl;
    std::cout << "Report for Planfragment # " << m_pfCount << std::endl;
    typedef std::pair<int32_t, voltdb::Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_tables){
    std::vector<TableIndex*> indexes = table.second->allIndexes();
    if (!indexes.empty()) continue;
    BOOST_FOREACH (TableIndex *index, indexes) {
        index->printReport();
    }
}
    std::cout << "==========" << std::endl << std::endl;
}

bool VoltDBEngine::isLocalSite(const NValue& value) {
    int index = TheHashinator::hashinate(value, m_totalPartitions);
    return index == m_partitionId;
}

/** Perform once per second, non-transactional work. */
void VoltDBEngine::tick(int64_t timeInMillis, int64_t lastCommittedTxnId) {
    m_executorContext->setupForTick(lastCommittedTxnId, timeInMillis);
    typedef pair<int64_t, Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_exportingTables){
    table.second->flushOldTuples(timeInMillis);
}
}

/** For now, bring the Export system to a steady state with no buffers with content */
void VoltDBEngine::quiesce(int64_t lastCommittedTxnId) {
    m_executorContext->setupForQuiesce(lastCommittedTxnId);
    typedef pair<int64_t, Table*> TablePair;
    BOOST_FOREACH (TablePair table, m_exportingTables){
    table.second->flushOldTuples(-1L);
}
}

string VoltDBEngine::debug(void) const {
    stringstream output(stringstream::in | stringstream::out);
    map<int64_t, boost::shared_ptr<ExecutorVector> >::const_iterator iter;
    vector<AbstractExecutor*>::const_iterator executorIter;

    for (iter = m_executorMap.begin(); iter != m_executorMap.end(); iter++) {
        output << "Fragment ID: " << iter->first << ", "
                << "Executor list size: " << iter->second->list.size() << ", "
                << "Temp table memory in bytes: "
                << iter->second->tempTableMemoryInBytes << endl;

        for (executorIter = iter->second->list.begin();
                executorIter != iter->second->list.end(); executorIter++) {
            output << (*executorIter)->getPlanNode()->debug(" ") << endl;
        }
    }

    return output.str();
}

StatsAgent& VoltDBEngine::getStatsManager() {
    return m_statsManager;
}

/**
 * Retrieve a set of statistics and place them into the result buffer as a set
 * of VoltTables.
 *
 * @param selector StatisticsSelectorType indicating what set of statistics
 *                 should be retrieved
 * @param locators Integer identifiers specifying what subset of possible
 *                 statistical sources should be polled. Probably a CatalogId
 *                 Can be NULL in which case all possible sources for the
 *                 selector should be included.
 * @param numLocators Size of locators array.
 * @param interval Whether to return counters since the beginning or since the
 *                 last time this was called
 * @param Timestamp to embed in each row
 * @return Number of result tables, 0 on no results, -1 on failure.
 */
int VoltDBEngine::getStats(int selector, int locators[], int numLocators,
        bool interval, int64_t now) {
    Table *resultTable = NULL;
    vector<CatalogId> locatorIds;
    size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));

    try {
        switch (selector) {
        // -------------------------------------------------
        // TABLE STATS
        // -------------------------------------------------
        case STATISTICS_SELECTOR_TYPE_TABLE: {
            for (int ii = 0; ii < numLocators; ii++) {
                CatalogId locator = static_cast<CatalogId>(locators[ii]);
                locatorIds.push_back(locator);
            }
            for (int ii = 0; ii < numLocators; ii++) {
                CatalogId locator = static_cast<CatalogId>(locators[ii]);
                if (m_tables.find(locator) == m_tables.end()) {
                    char message[256];
                    snprintf(message, 256,
                            "getStats() called with selector %d, and"
                                    " an invalid locator %d that does not correspond to"
                                    " a table", selector, locator);
                    throw SerializableEEException(
                            VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
                }
            }

            resultTable = m_statsManager.getStats(
                    (StatisticsSelectorType) selector, locatorIds, interval,
                    now);
            break;
        }
        // -------------------------------------------------
        // INDEX STATS
        // -------------------------------------------------
        case STATISTICS_SELECTOR_TYPE_INDEX: {
            // HACK: Pavlo 2014-11-20
            // Ok here's what's going to happen in this mofo.
            // We normally don't have globally unique index ids, since we're using the
            // the relative indexes. So we'll create a composite key of the table's relativeIndex +
            // the index's relativeIndex
            for (int ii = 0; ii < numLocators; ii++) {
                CatalogId tableId = static_cast<CatalogId>(locators[ii]);
                if (m_tables.find(tableId) == m_tables.end()) {
                    char message[256];
                    snprintf(message, 256,
                            "getStats() called with selector %d, and"
                                    " an invalid locator %d that does not correspond to"
                                    " a table", selector, tableId);
                    throw SerializableEEException(
                            VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION, message);
                }
                
                // Create the composite keys for this table
                catalog::Table *catTable = m_database->tables().get(m_tables[tableId]->name());
                
                map<string, catalog::Index*>::const_iterator idx_iterator;
                for (idx_iterator = catTable->indexes().begin();
                        idx_iterator != catTable->indexes().end(); idx_iterator++) {
                    catalog::Index *catIndex = idx_iterator->second;
                    CatalogId indexId = computeIndexStatsId(catTable->relativeIndex(), catIndex->relativeIndex());
                    locatorIds.push_back(indexId);
                    VOLT_DEBUG("FETCH IndexStats: %s.%s -> %d\n",
                               catTable->name().c_str(), catIndex->name().c_str(), indexId);
                } // FOR
            } // FOR

            resultTable = m_statsManager.getStats(
                    (StatisticsSelectorType) selector, locatorIds, interval,
                    now);
            break;
        }
        // -------------------------------------------------
        // MULTITIER STATS
        // -------------------------------------------------
        case STATISTICS_SELECTOR_MULTITIER_ANTICACHE: {
            

        default:
            char message[256];
            snprintf(message, 256,
                    "getStats() called with an unrecognized selector"
                            " %d", selector);
            throw SerializableEEException(VOLT_EE_EXCEPTION_TYPE_EEEXCEPTION,
                    message);
        }
    } catch (SerializableEEException &e) {
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        return -1;
    }

    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}

/*
 * Exists to transition pre-existing unit test cases.
 */
ExecutorContext * VoltDBEngine::getExecutorContext() {
    m_executorContext->setupForPlanFragments(getCurrentUndoQuantum());
    return m_executorContext;
}

int64_t VoltDBEngine::uniqueIdForFragment(catalog::PlanFragment *frag) {
    int64_t retval = 0;

    retval = atol(frag->name().c_str());
    // PAVLO: This won't work if we have multi sets of fragments we
    // want to execute because they will end up getting the same ids
    //catalog::CatalogType *parent = frag->parent();
    //retval = static_cast<int64_t>(parent->parent()->relativeIndex()) << 32;
    //retval += static_cast<int64_t>(parent->relativeIndex()) << 16;
    //retval += static_cast<int64_t>(frag->relativeIndex());

    return retval;
}

/**
 * Activate a table stream for the specified table
 */
bool VoltDBEngine::activateTableStream(const CatalogId tableId, TableStreamType streamType) {
    map<int32_t, Table*>::iterator it = m_tables.find(tableId);
    if (it == m_tables.end()) {
        return false;
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(it->second);
    if (table == NULL) {
        assert(table != NULL);
        return false;
    }

    switch (streamType) {
    case TABLE_STREAM_SNAPSHOT:
        VOLT_WARN("TableStreamType : TABLE_STREAM_SNAPSHOT for table %s ",
                table->name().c_str())
        ;

        if (table->activateCopyOnWrite(&m_tupleSerializer, m_partitionId)) {
            return false;
        }

        // keep track of snapshotting tables. a table already in cow mode
        // can not be re-activated for cow mode.
        if (m_snapshottingTables.find(tableId) != m_snapshottingTables.end()) {
            assert(false);
            return true;
        }

        table->incrementRefcount();
        m_snapshottingTables[tableId] = table;
        break;

    case TABLE_STREAM_RECOVERY:
        if (table->activateRecoveryStream(it->first)) {
            return false;
        }
        break;
    default:
        return false;
    }

    return true;
}

/**
 * Serialize more tuples from the specified table that is in COW mode.
 * Returns the number of bytes worth of tuple data serialized or 0 if
 * there are no more.  Returns -1 if the table is no in COW mode. The
 * table continues to be in COW (although no copies are made) after
 * all tuples have been serialize until the last call to
 * cowSerializeMore which returns 0 (and deletes the COW
 * context). Further calls will return -1
 */
int VoltDBEngine::tableStreamSerializeMore(ReferenceSerializeOutput *out,
        const CatalogId tableId, const TableStreamType streamType) {

    switch (streamType) {
    case TABLE_STREAM_SNAPSHOT: {
        // If a completed table is polled, return 0 bytes serialized. The
        // Java engine will always poll a fully serialized table one more
        // time (it doesn't see the hasMore return code).  Note that the
        // dynamic cast was already verified in activateCopyOnWrite.
        map<int32_t, Table*>::iterator pos = m_snapshottingTables.find(tableId);
        if (pos == m_snapshottingTables.end()) {
            return 0;
        }

        PersistentTable *table = dynamic_cast<PersistentTable*>(pos->second);
        bool hasMore = table->serializeMore(out);
        if (!hasMore) {
            m_snapshottingTables.erase(tableId);
            table->decrementRefcount();
        }

        break;
    }

    case TABLE_STREAM_RECOVERY: {
        /*
         * Table ids don't change during recovery because
         * catalog changes are not allowed.
         */
        map<int32_t, Table*>::iterator pos = m_tables.find(tableId);
        if (pos == m_tables.end()) {
            return 0;
        }
        PersistentTable *table = dynamic_cast<PersistentTable*>(pos->second);
        table->nextRecoveryMessage(out);
        break;
    }
    default:
        return -1;
    }

    return static_cast<int>(out->position());
}

/*
 * Apply the updates in a recovery message.
 */
void VoltDBEngine::processRecoveryMessage(RecoveryProtoMsg *message) {
    CatalogId tableId = message->tableId();
    map<int32_t, Table*>::iterator pos = m_tables.find(tableId);
    if (pos == m_tables.end()) {
        throwFatalException(
                "Attempted to process recovery message for tableId %d but the table could not be found",
                tableId);
    }
    PersistentTable *table = dynamic_cast<PersistentTable*>(pos->second);
    table->processRecoveryMessage(message, NULL, false);
}

#ifdef ARIES
void VoltDBEngine::ARIESInitialize(std::string dbDir, std::string logFile) {
    VOLT_WARN("Enabling ARIES Feature at Partition %d ", m_partitionId);
    setARIESDir(dbDir);
    setARIESFile(logFile);
    setARIESEnabled(true);

    // Do this only after ARIES dir is set
    m_logManager = new LogManager(m_logProxy, this);
    m_executorContext->enableARIES(dbDir);
}
#else
void VoltDBEngine::ARIESInitialize(std::string dbDir, std::string logFile) {
    VOLT_ERROR("ARIES feature was not enabled when compiling the EE");
}
#endif

#ifdef ARIES
char* VoltDBEngine::readAriesLogForReplay(int64_t* sizes) {
    if(!isARIESEnabled()) {
        return NULL;
    }

    ifstream logfilestream;

    // read custom file names later
    ostringstream ss;
    ss << getARIESDir();
    ss << "/" << AriesLogProxy::defaultLogfileName;
    string logFileName = ss.str();
    VOLT_WARN("readAriesLogForReplay at : --%s--",logFileName.c_str());

    logfilestream.open(logFileName.c_str(), ios::binary | ios::in);

    if (!logfilestream.is_open()) {
        sizes[0] = 0;
        VOLT_WARN("Did not find aries log file at : %s", logFileName.c_str());
        return NULL;    // log file does not exist
    }

    logfilestream.seekg(0, ios::end);
    size_t length = logfilestream.tellg();

    sizes[0] = length;

    logfilestream.seekg(0, ios::beg);

    if (length == 0) {
        logfilestream.close();
        VOLT_WARN("Log file is empty : %s", logFileName.c_str());
        return NULL; //log is empty
    }

    // XXX: change the code NOT to read all of the file at once
    char *logData = new char[length];
    logfilestream.read(logData, length);

    logfilestream.close();

    /*
     while (!noMoreLogRecords) {
     // read small chunks of the file at a time
     char *logData = new char[BUFFER_SIZE];
     logfilestream.read(logData, BUFFER_SIZE);

     int64_t actualBufLen = static_cast<int64_t> (logfilestream.gcount());

     if(logfilestream.eof()) {
     noMoreLogRecords = true;
     }
     */
    return logData;
}

void VoltDBEngine::freePointerToReplayLog(char *logData) {
    if (logData != NULL) {
        delete[] logData;
        logData = NULL;
    }
}

/*
 * Do Aries recovery
 */
void VoltDBEngine::doAriesRecovery(char *logData, size_t length, int64_t replay_txnid) {
    VOLT_WARN("ARIES : doAriesRecovery check at partition : %d ",this->m_partitionId);

    if(!isARIESEnabled()) {
        return;
    }

    // every thread sets its own copy of m_isRecovering
    // XXX: could make this static but not sure if that's a good idea
    if (logData == NULL || length == 0) {
        VOLT_WARN("ARIES : logData NULL or length %lu",length);
        return;
    }

    VOLT_WARN("ARIES : doAriesRecovery at partition : %d starting from replay_txnid : %ld",this->m_partitionId, replay_txnid);

    // TIMING
    struct timeval tv1, tv2;
    gettimeofday(&tv1, NULL);

    m_isRecovering = true;

    bool noMoreLogRecords = false;

    Logger m_ariesLogger = m_logManager->getAriesLogger();
    VOLT_DEBUG("m_logManager : %p AriesLogger : %p",&m_logManager, &m_ariesLogger);
    const Logger *logger = m_logManager->getThreadLogger(LOGGERID_MM_ARIES);
    logger->log(LOGLEVEL_INFO, "Running ARIES recovery, repeating history ...");

    int64_t actualBufLen = length;
    ReferenceSerializeInput input(logData, actualBufLen);

    char *endOfBuffer = logData + actualBufLen;

    int32_t counter = 0;

    VOLT_DEBUG("actualBufLen : %ld", actualBufLen);

    while (input.getRawPointer(0) < endOfBuffer) {
        const char* logInitPosition = reinterpret_cast<const char*>(input.getRawPointer(0));

        int32_t recordSize = 0;

        // log header is 4 bytes
        int64_t breathingSpace = endOfBuffer - (logInitPosition + sizeof(int32_t));

        if (breathingSpace < 0) {
            // read the full header along with the next chunk
            // seek in reverse direction
            break;//XXX:hack when we have a single buffer for the entire file
            // logfilestream.seekg((logInitPosition - endOfBuffer), ios::cur);
        } else {
            // read log record header to determine its size.
            memcpy(&recordSize, logInitPosition, sizeof(recordSize));
            recordSize = ntohl(recordSize);

            VOLT_DEBUG("breathingSpace : %ld recordSize : %d", breathingSpace, recordSize);
            if (recordSize <= 0) {
                // hit junk, no more log records.
                noMoreLogRecords = true;
                break;
            } else {
                /* if (logInitPosition + sizeof(int32_t) + recordSize > endOfBuffer) {
                 //part of this log record is in the
                 // next buffer chunk
                 // rewind back to beginning of log record's header
                 logfilestream.seekg((logInitPosition - endOfBuffer), ios::cur);
                 break;
                 }
                 // else entire log record is in this chunk
                 */
            }
        }

        bool skipLogRecord = false;

        // find the transaction type, need to know if its a bulk load
        int8_t txnType;
        memcpy(&txnType, logInitPosition + sizeof(int32_t) + OFFSET_TO_TXNTYPE, sizeof(txnType));

        int64_t numBulkLoadBytes = 0;

        if (txnType == static_cast<int8_t>(LogRecord::T_BULKLOAD)) {
            memcpy(&numBulkLoadBytes, logInitPosition + sizeof(int32_t) + recordSize, sizeof(numBulkLoadBytes));
            numBulkLoadBytes = ntohll(numBulkLoadBytes);
        }

        // Run only if txnId is greater than the id to replay from
        int64_t txnId;
        memcpy(&txnId, logInitPosition + sizeof(int32_t) + OFFSET_TO_TXNID, sizeof(txnId));
        txnId = ntohll(txnId);

        VOLT_DEBUG("LogRecord :: TXN Type : %d TXN Id : %ld", txnType, txnId);

        // Check the site-id, re-run only if original site-id matches
        // Correctness follows because all updates from a site are to
        // a particular partition only.
        // (much like the page wise recovery in the original Aries recovery)
        int32_t origSiteId;
        memcpy(&origSiteId, logInitPosition + sizeof(int32_t) + OFFSET_TO_SITEID, sizeof(origSiteId));
        origSiteId = ntohl(origSiteId);

        if ((txnId < replay_txnid) || (origSiteId != m_siteId)) {
            // don't forget to advance the pointer, o/w there's an infinite loop
            input.readInt();
            input.getRawPointer(recordSize);

            if (txnType == static_cast<int8_t>(LogRecord::T_BULKLOAD)) {
                // skip over the load bytes as well
                input.getRawPointer(sizeof(numBulkLoadBytes) + numBulkLoadBytes);
            }

            skipLogRecord = true;
            continue;
        }

        LogRecord logrecord(input);

        PersistentTable* table = dynamic_cast<PersistentTable*>(getTable(logrecord.getTableName()));

        if (table == NULL) {
            // Invalid log record hit
            // This does not take into account log corruption,
            // for otherwise log replay semantics are ill-defined.
            break;
        }

        logrecord.populateFields(table->schema(), table->primaryKeyIndex());

        if (!logrecord.isValidRecord()) {
            // XXX: can actually NEVER happen because
            // this call always returns true.
            break;
        }

        counter++;

        TableTuple *beforeImage = NULL;
        TableTuple *afterImage = NULL;

        if (logrecord.getType() == LogRecord::T_INSERT) {
            VOLT_DEBUG("Log record recovery : INSERT start");

            // at this point, don't worry about
            // logging during recovery
            // XXX: note that duplicate inserts won't happen silently:
            // constraint failure exceptions will get thrown
            afterImage = logrecord.getTupleAfterImage();

            if (afterImage != NULL) {
                table->insertTuple(*afterImage);

                // Job is done, delete the tuple now
                logrecord.dellocateAfterImageData();

                //afterImage->freeObjectColumns();
                delete afterImage;
                afterImage = NULL;
            }

            VOLT_DEBUG("Log record recovery : INSERT end");
        } else if (logrecord.getType() == LogRecord::T_UPDATE) {
            VOLT_DEBUG("Log record recovery : UPDATE start");

            beforeImage = logrecord.getTupleBeforeImage();
            afterImage = logrecord.getTupleAfterImage();

            // XXX: setting updateIndexes to true
            // for simplicity, originally it comes from the plan
            // node during forward execution.
            // Might need to change this if problems arise.
            // XXX: should I modify the log record to track this
            // attribute too? That doesn't seem too hard.
            table->updateTuple(*beforeImage, *afterImage, true);

            logrecord.dellocateBeforeImageData();
            //beforeImage->freeObjectColumns();
            delete beforeImage;
            beforeImage = NULL;

            logrecord.dellocateAfterImageData();
            //afterImage->freeObjectColumns();
            delete afterImage;
            afterImage = NULL;

            VOLT_DEBUG("Log record recovery : UPDATE end");
        } else if (logrecord.getType() == LogRecord::T_BULKLOAD) {
            VOLT_DEBUG("Log record recovery : BULKLOAD start");

            numBulkLoadBytes = input.readLong();

            // make sure we create a separate input reader
            // for the load, otherwise we'll get the number of
            // bytes wrong in there.
            ReferenceSerializeInput bulkIn(input.getRawPointer(0), numBulkLoadBytes);

            // figure if the last committed txnId,
            // should be the replay_txnId?
            // The thing to note here is that if we have a
            // a non-trivial value for the replay_txnId,
            // NO bulk loads will be needed --
            // the snapshot reload itself will take care of the database
            // bulk reload and the reload record will be SKIPPED.

            // make a call to load table, effectively mimicking
            // the table load the client makes on an actual load.
            // let the txnId be set to 1 + last committed txnId for now
            loadTable(table, bulkIn, replay_txnid + 1, replay_txnid, false);

            // advance read position to the correct place.
            input.getRawPointer(numBulkLoadBytes);

            VOLT_DEBUG("Log record recovery : BULKLOAD end");
        } else if (logrecord.getType() == LogRecord::T_DELETE) {
            VOLT_DEBUG("Log record recovery : DELETE start");

            beforeImage = logrecord.getTupleBeforeImage();

            table->deleteTuple(*beforeImage, true);

            logrecord.dellocateBeforeImageData();
            //beforeImage->freeObjectColumns();
            delete beforeImage;
            beforeImage = NULL;

            VOLT_DEBUG("Log record recovery : DELETE end");
        } else if (logrecord.getType() == LogRecord::T_TRUNCATE) {
            table->deleteAllTuples(true);

            VOLT_DEBUG("Log record recovery : TRUNCATE");
        } else {
            // do nothing for invalid records
            VOLT_WARN("Log record recovery : Invalid Record");
        }
    }


    gettimeofday(&tv2, NULL);

    //long microseconds = (tv2.tv_sec - tv1.tv_sec) * 1000000 + ((int)tv2.tv_usec - (int)tv1.tv_usec);
    //long milliseconds = microseconds/1000;
    //VOLT_ERROR("ARIES : ######################### Recovery Latency : completed in %ld ms", milliseconds);
    //VOLT_ERROR("ARIES : ######################### Recovery Completed : %d log records replayed", counter);

    std::ostringstream sstm;
    sstm << counter;

    std::string outputString = "ARIES : recovery completed, " + sstm.str() + " log records found, all replayed.";
    logger->log(LOGLEVEL_INFO, &outputString);
}

void VoltDBEngine::writeToAriesLogBuffer(const char *data, size_t size) {
    memcpy(m_arieslogBuffer + m_ariesWriteOffset, data, size);
    m_ariesWriteOffset += size;
}

size_t VoltDBEngine::getArieslogBufferLength() {
    return m_ariesWriteOffset;
}

void VoltDBEngine::rewindArieslogBuffer() {
    m_ariesWriteOffset = 0;
}
#endif

long VoltDBEngine::exportAction(bool ackAction, bool pollAction,
        bool resetAction, bool syncAction, int64_t ackOffset, int64_t seqNo,
        int64_t tableId) {
    map<int64_t, Table*>::iterator pos = m_exportingTables.find(tableId);

    // return no data and polled offset for unavailable tables.
    if (pos == m_exportingTables.end()) {
        // ignore trying to sync a non-exported table
        if (syncAction) {
            assert(ackOffset == 0);
            return 0;
        }

        m_resultOutput.writeInt(0);
        if (ackOffset < 0) {
            return 0;
        } else {
            return ackOffset;
        }
    }

    Table *table_for_el = pos->second;

    if (syncAction) {
        table_for_el->setExportStreamPositions(seqNo, (size_t) ackOffset);
        // done after the sync
        return 0;
    }

    // perform any releases before polls.
    if (ackOffset > 0) {
        if (!table_for_el->releaseExportBytes(ackOffset)) {
            return -1;
        }
    }

    // perform resets after acks
    if (resetAction) {
        table_for_el->resetPollMarker();
    }

    // ack was successful.  Get the next buffer of committed Export bytes
    StreamBlock* block = table_for_el->getCommittedExportBytes();
    if (block == NULL) {
        return -1;
    }

    // prepend the length of the block to the results buffer
    m_resultOutput.writeInt((int) (block->unreleasedSize()));

    // if the block isn't empty, copy it into the query results buffer
    // if the block is empty, check if it is a dropped table finishing
    // export. These tables appear in the export list but not in the
    // current tables list.
    if (block->unreleasedSize() != 0) {
        m_resultOutput.writeBytes(block->dataPtr(), block->unreleasedSize());
    } else {
        map<string, CatalogDelegate*>::iterator dels =
                m_catalogDelegates.begin();
        while (dels != m_catalogDelegates.end()) {
            CatalogDelegate *tcd = dels->second;
            if (tcd->delegateId() == tableId) {
                break;
            }
            ++dels;
        }
        if (dels == m_catalogDelegates.end()) {
            table_for_el->decrementRefcount();
            m_exportingTables.erase(pos);
        }
    }

    // return the stream offset for the end of the returned block
    return (block->uso() + block->offset());
}

size_t VoltDBEngine::tableHashCode(int32_t tableId) {
    map<int32_t, Table*>::iterator it = m_tables.find(tableId);
    if (it == m_tables.end()) {
        throwFatalException(
                "Tried to calculate a hash code for a table that doesn't exist with id %d\n",
                tableId);
    }

    PersistentTable *table = dynamic_cast<PersistentTable*>(it->second);
    if (table == NULL) {
        throwFatalException(
                "Tried to calculate a hash code for a table that is not a persistent table id %d\n",
                tableId);
    }
    return table->hashCode();
}

// -------------------------------------------------
// READ/WRITE SET TRACKING FUNCTIONS
// -------------------------------------------------

void VoltDBEngine::trackingEnable(int64_t txnId) {
    // If this our first txn that wants tracking, then
    // we need to setup the tracking manager
    if (m_executorContext->isTrackingEnabled() == false) {
        VOLT_INFO("Setting up Tracking Manager at Partition %d", m_partitionId);
        m_executorContext->enableTracking();
    }
    VOLT_INFO("Creating ReadWriteTracker for txn #%ld at Partition %d", txnId,
            m_partitionId);
    ReadWriteTrackerManager *trackerMgr =
            m_executorContext->getTrackerManager();
    trackerMgr->enableTracking(txnId);
}

void VoltDBEngine::trackingFinish(int64_t txnId) {
    if (m_executorContext->isTrackingEnabled() == false) {
        VOLT_WARN("Tracking is not enable for txn #%ld at Partition %d", txnId,
                m_partitionId);
        return;
    }
    ReadWriteTrackerManager *trackerMgr =
            m_executorContext->getTrackerManager();
    VOLT_INFO("Deleting ReadWriteTracker for txn #%ld at Partition %d", txnId,
            m_partitionId);
    trackerMgr->removeTracker(txnId);
    return;
}

int VoltDBEngine::trackingTupleSet(int64_t txnId, bool writes) {
    if (m_executorContext->isTrackingEnabled() == false) {
        return (ENGINE_ERRORCODE_NO_DATA);
    }

    ReadWriteTrackerManager *trackerMgr =
            m_executorContext->getTrackerManager();
    ReadWriteTracker *tracker = trackerMgr->getTracker(txnId);

    Table *resultTable = NULL;
    if (writes) {
        VOLT_INFO("Getting WRITE tracking set for txn #%ld at Partition %d",
                txnId, m_partitionId);
        resultTable = trackerMgr->getTuplesWritten(tracker);
    } else {
        VOLT_INFO("Getting READ tracking set for txn #%ld at Partition %d",
                txnId, m_partitionId);
        resultTable = trackerMgr->getTuplesRead(tracker);
    }

    // Serialize the output table so that we can read it up in Java
    if (resultTable != NULL) {
        VOLT_DEBUG("TRACKING TABLE TXN #%ld\n%s\n", txnId,
                resultTable->debug().c_str());

        size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        VOLT_INFO("Returning tracking set for txn #%ld at Partition %d", txnId,
                m_partitionId);
        return (ENGINE_ERRORCODE_SUCCESS);
    }
    return (ENGINE_ERRORCODE_ERROR);
}

// std::vector<std::string> VoltDBEngine::trackingTablesRead(int64_t txnId) {
//     if (m_executorContext->isTrackingEnabled()) {
//         ReadWriteTracker *tracker = m_executorContext->getTrackerManager(txnId);
//         if (tracker != NULL) {
//             return tracker->getTablesRead();
//         }
//     }
//     return (NULL);
// }
// std::vector<std::string> VoltDBEngine::trackingTablesWritten(int64_t txnId) {
//     if (m_executorContext->isTrackingEnabled()) {
//         ReadWriteTracker *tracker = m_executorContext->getTrackerManager(txnId);
//         if (tracker != NULL) {
//             return tracker->getTablesWritten();
//         }
//     }
//     return (NULL);
// }

// -------------------------------------------------
// ANTI-CACHE FUNCTIONS
// -------------------------------------------------

#ifdef ANTICACHE
void VoltDBEngine::antiCacheInitialize(std::string dbDir, AntiCacheDBType dbType, bool blocking, long blockSize, long maxSize, bool blockMerge) const {
    VOLT_INFO("Enabling type %d (blocking: %d/blockMerge: %d) Anti-Cache at Partition %d: dir=%s / blockSize=%ld max=%ld", 
            (int)dbType, (int)blocking, (int)blockMerge, m_partitionId, dbDir.c_str(), blockSize, maxSize);
    m_executorContext->enableAntiCache(this, dbDir, blockSize, dbType, blocking, maxSize, blockMerge);
}

void VoltDBEngine::antiCacheAddDB(std::string dbDir, AntiCacheDBType dbType, bool blocking, long blockSize, long maxSize, bool blockMerge) const {
    VOLT_INFO("Adding type %d (blocking: %d/blockMerge: %d) Anti-Cache at Partition %d: dir=%s / blockSize=%ld max=%ld", 
            (int)dbType, (int)blocking, (int)blockMerge, m_partitionId, dbDir.c_str(), blockSize, maxSize);
    m_executorContext->addAntiCacheDB(dbDir, blockSize, dbType, blocking, maxSize, blockMerge);
}

int VoltDBEngine::antiCacheReadBlocks(int32_t tableId, int numBlocks, int32_t blockIds[], int32_t tupleOffsets[]) {
    int retval = ENGINE_ERRORCODE_SUCCESS;

    // Grab the PersistentTable referenced by the given tableId
    // This is simply the relativeIndex of the table in the catalog
    // We can assume that the ordering hasn't changed.
    PersistentTable *table = dynamic_cast<PersistentTable*>(this->getTable(tableId));
    if (table == NULL) {
        throwFatalException("Invalid table id %d", tableId);
    }

    #ifdef VOLT_INFO_ENABLED
    std::ostringstream buffer;
    for(int i = 0; i < numBlocks; i++) {
        if (i > 0) buffer << ", ";
        buffer << blockIds[i];
    }
    VOLT_INFO("Preparing to read %d evicted blocks: [%s]", numBlocks, buffer.str().c_str());
    #endif
    
    // We can now ask it directly to read in the evicted blocks that they want
    bool finalResult = true;
    AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
    try {
        for (int i = 0; i < numBlocks; i++) {
            finalResult = eviction_manager->readEvictedBlock(table, blockIds[i], tupleOffsets[i]) && finalResult;
        } // FOR

    } catch (SerializableEEException &e) {
        VOLT_ERROR("antiCacheReadBlocks: Failed to read %d evicted blocks for table '%s'\n%s",
                   numBlocks, table->name().c_str(), e.message().c_str());
        // FIXME: This won't work if we execute are executing this operation the
        //        same time that txns are running
        
        // MJG DANGER!!!!
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        retval = ENGINE_ERRORCODE_ERROR;
    }

    return (retval);
}

/**
 * Somebody wants us to forcibly evict a certain number of bytes from the given table.
 * This is likely only used for testing...
 * @param tableId
 * @param blockSize The number of bytes to evict from this table
 */
int VoltDBEngine::antiCacheEvictBlock(int32_t tableId, long blockSize, int numBlocks) {
    PersistentTable *table = dynamic_cast<PersistentTable*>(this->getTable(tableId));
    if (table == NULL) {
        throwFatalException("Invalid table id %d", tableId);
    }

    VOLT_DEBUG("Attempting to evict a block of %ld bytes from table '%s'",
            blockSize, table->name().c_str());
    size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));
    Table *resultTable = m_executorContext->getAntiCacheEvictionManager()->evictBlock(table, blockSize, numBlocks);
    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}

/**
 * Somebody wants us to forcibly evict a certain number of bytes from the given table and its children tables in batch.
 * @param tableId
 * @param childTableId
 * @param blockSize The number of bytes to evict from this table
 */
int VoltDBEngine::antiCacheEvictBlockInBatch(int32_t tableId, int32_t childTableId, long blockSize, int numBlocks) {
    PersistentTable *table = dynamic_cast<PersistentTable*>(this->getTable(tableId));
    PersistentTable *childTable = dynamic_cast<PersistentTable*>(this->getTable(childTableId));
    if (table == NULL) {
        throwFatalException("Invalid table id %d", tableId);
    }
    if (childTable == NULL) {
        throwFatalException("Invalid table id %d", childTableId);
    }

    VOLT_DEBUG("Attempting to evict a block of %ld bytes from table '%s'",
               blockSize, table->name().c_str());
    size_t lengthPosition = m_resultOutput.reserveBytes(sizeof(int32_t));
    Table *resultTable = m_executorContext->getAntiCacheEvictionManager()->evictBlockInBatch(table, childTable, blockSize, numBlocks);
    if (resultTable != NULL) {
        resultTable->serializeTo(m_resultOutput);
        m_resultOutput.writeIntAt(lengthPosition,
                                  static_cast<int32_t>(m_resultOutput.size() - sizeof(int32_t)));
        return 1;
    } else {
        return 0;
    }
}

/**
 * Merge the recently all of the unevicted data for the given tableId
 * Note: This should only be called when no other txn is running
 * @param tableId
 */
int VoltDBEngine::antiCacheMergeBlocks(int32_t tableId) {
    int retval = ENGINE_ERRORCODE_SUCCESS;
    PersistentTable *table = dynamic_cast<PersistentTable*>(this->getTable(tableId));
    if (table == NULL) {
        throwFatalException("Invalid table id %d", tableId);
    }

    VOLT_DEBUG("Merging unevicted blocks for table %d", tableId);
    // Merge all the newly unevicted blocks back into our regular table data
    try {
        m_executorContext->getAntiCacheEvictionManager()->mergeUnevictedTuples(table);
    } catch (SerializableEEException &e) {
        VOLT_INFO("Failed to merge blocks for table %d", tableId);

        VOLT_TRACE("antiCacheMerge: Failed to merge unevicted tuples for table '%s'",
                table->name().c_str());
        resetReusedResultOutputBuffer();
        e.serialize(getExceptionOutputSerializer());
        retval = ENGINE_ERRORCODE_ERROR;
    }

    return (retval);
}

void VoltDBEngine::antiCacheResetEvictedTupleTracker() {
    // Anti-Cache Evicted Tuple Tracking
    if (m_executorContext->isAntiCacheEnabled()) {
        AntiCacheEvictionManager* eviction_manager = m_executorContext->getAntiCacheEvictionManager();
        eviction_manager->initEvictedAccessTracker();
    }
}

#else
void VoltDBEngine::antiCacheInitialize(std::string dbDir, AntiCacheDBType dbType,
        long blockSize, long maxSize) const {
    // FIX :: Dummy call if ANTICACHE is not defined
    //VOLT_ERROR("Anti-Cache feature was not enable when compiling the EE");
}
#endif

// -------------------------------------------------
// STORAGE MMAP FUNCTIONS
// -------------------------------------------------

#ifdef STORAGE_MMAP
void VoltDBEngine::MMAPInitialize(std::string dbDir, long mapSize, long syncFrequency) const {
    VOLT_INFO("Enabling Storage MMAP Feature at Partition %d: dir=%s / blockSize=%ld / syncFrequency=%ld ",
            m_partitionId, dbDir.c_str(), mapSize, syncFrequency);
    m_executorContext->enableMMAP(dbDir, mapSize, syncFrequency);
}
#else
void VoltDBEngine::MMAPInitialize(std::string dbDir, long blockSize,
        long syncFrequency) const {
    VOLT_ERROR("Storage MMAP feature was not enabled when compiling the EE");
}
#endif

}
