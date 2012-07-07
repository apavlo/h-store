/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

#ifndef _EXECUTORCONTEXT_HPP_
#define _EXECUTORCONTEXT_HPP_

#include "Topend.h"
#include "common/UndoQuantum.h"

#ifdef ANTICACHE
#include <db_cxx.h>
#endif

namespace voltdb {

/*
 * EE site global data required by executors at runtime.
 *
 * This data is factored into common to avoid creating dependencies on
 * execution/VoltDBEngine throughout the storage and executor code.
 * This facilitates easier test case writing and breaks circular
 * dependencies between ee component directories.
 *
 * A better implementation that meets these goals is always welcome if
 * you see a preferable refactoring.
 */
class ExecutorContext {
  public:
    ~ExecutorContext() {

#ifdef ANTICACHE
        if (m_antiCacheEnabled) {
            anticache_dbEnv->close(0);
            delete anticache_dbEnv;

            anticache_db->close(0);
            delete anticache_db;
        }
#endif
    }

    ExecutorContext(CatalogId siteId,
                    CatalogId partitionId,
                    UndoQuantum *undoQuantum,
                    Topend* topend,
                    bool exportEnabled,
                    int64_t epoch,
                    std::string hostname,
                    CatalogId hostId) :
        m_topEnd(topend), m_undoQuantum(undoQuantum),
        m_txnId(0),
        m_siteId(siteId), m_partitionId(partitionId),
        m_hostname(hostname), m_hostId(hostId),
        m_exportEnabled(exportEnabled),
        m_epoch(epoch)
    {
        m_lastCommittedTxnId = 0;
        m_lastTickTime = 0;
        
#ifdef ANTICACHE
        m_antiCacheEnabled = false;
        anticache_nextBlockId = 0; 
#endif
    }
    
    // not always known at initial construction
    void setPartitionId(CatalogId partitionId) {
        m_partitionId = partitionId;
    }

    // not always known at initial construction
    void setEpoch(int64_t epoch) {
        m_epoch = epoch;
    }

    // helper to configure the context for a new jni call
    void setupForPlanFragments(UndoQuantum *undoQuantum,
                               int64_t txnId,
                               int64_t lastCommittedTxnId)
    {
        m_undoQuantum = undoQuantum;
        m_txnId = txnId;
        m_lastCommittedTxnId = lastCommittedTxnId;
    }

    // data available via tick()
    void setupForTick(int64_t lastCommittedTxnId,
                      int64_t lastTickTime)
    {
        m_lastCommittedTxnId = lastCommittedTxnId;
        m_lastTickTime = lastTickTime;
    }

    // data available via quiesce()
    void setupForQuiesce(int64_t lastCommittedTxnId) {
        m_lastCommittedTxnId = lastCommittedTxnId;
    }

    // for test (VoltDBEngine::getExecutorContext())
    void setupForPlanFragments(UndoQuantum *undoQuantum) {
        m_undoQuantum = undoQuantum;
    }

    UndoQuantum *getCurrentUndoQuantum() {
        return m_undoQuantum;
    }

    Topend* getTopend() {
        return m_topEnd;
    }

    /** Current or most recently executed transaction id. */
    int64_t currentTxnId() {
        return m_txnId;
    }

    /** Current or most recently executed transaction id. */
    int64_t currentTxnTimestamp() {
        return (m_txnId >> 23) + m_epoch;
    }

    /** Last committed transaction known to this EE */
    int64_t lastCommittedTxnId() {
        return m_lastCommittedTxnId;
    }

    /** Time of the last tick() invocation. */
    int64_t lastTickTime() {
        return m_lastTickTime;
    }
        
    // ------------------------------------------------------------------
    // ANTI-CACHE
    // ------------------------------------------------------------------ 
    
#ifdef ANTICACHE
    
    /**
     * Return the handle to disk-based storage object that we
     * can use to read and write tuples to
     */
    Db* getAntiCacheDB() {
        return anticache_db; 
    }
    
    uint16_t generateNextBlockID() {
        
        // TODO: merge table_id and block_id
        return ++anticache_nextBlockId; 
    }

    /**
     * Enable the anti-caching feature in the EE.
     * The input parameter is the directory where our disk-based storage
     * will write out evicted blocks of tuples for this partition
     */
    void enableAntiCache(std::string &dbDir) {
        m_antiCacheEnabled = true;
        m_antiCacheDir = dbDir;
        
        try {
            // allocate and initialize Berkeley DB database env
            anticache_dbEnv = new DbEnv(0); 
            anticache_dbEnv->open(m_antiCacheDir, DB_CREATE | DB_INIT_MPOOL, 0); 
            
            // allocate and initialize new Berkeley DB instance
            anticache_db = new Db(anticache_dbEnv, 0); 
            anticache_db->open(NULL, "anticache.db", NULL, DB_HASH, DB_CREATE, 0); 
            
        } catch(DbException &e) {
            // TODO: exit program
        }
    }
#endif
    

  private:
    Topend *m_topEnd;
    UndoQuantum *m_undoQuantum;
    int64_t m_txnId;
    
    // ANTI-CACHE VARIABLES
#ifdef ANTICACHE
    DbEnv* anticache_dbEnv;
    Db* anticache_db; 
    uint16_t anticache_nextBlockId; 
#endif

  public:
    int64_t m_lastCommittedTxnId;
    int64_t m_lastTickTime;
    CatalogId m_siteId;
    CatalogId m_partitionId;
    std::string m_hostname;
    CatalogId m_hostId;
    bool m_exportEnabled;
    
#ifdef ANTICACHE
    bool m_antiCacheEnabled;
    std::string m_antiCacheDir;
#endif

    /** local epoch for voltdb, somtime around 2008, pulled from catalog */
    int64_t m_epoch;
};

}

#endif
