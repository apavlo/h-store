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
#include "storage/ReadWriteTracker.h"

#ifdef ANTICACHE
#include "anticache/AntiCacheDB.h"
#include "anticache/AntiCacheEvictionManager.h"
#endif

namespace voltdb {
    
    class ReadWriteTrackerManager;
    
    #ifdef ANTICACHE
    class AntiCacheDB;
    class AntiCacheEvictionManager; 
    #endif
    
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
            
            if (m_trackingEnabled) {
                delete m_trackingManager;
            }
            
            #ifdef ANTICACHE
            if (m_antiCacheEnabled) {
                delete m_antiCacheDB;
                delete m_antiCacheEvictionManager; 
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
            m_antiCacheEnabled = false;
            m_trackingEnabled = false;
        }
        
        // not always known at initial construction
        void setPartitionId(CatalogId partitionId) {
            m_partitionId = partitionId;
        }
        
        CatalogId getPartitionId() const {
            return (m_partitionId);
        }
        
        CatalogId getSiteId() const {
            return (m_siteId);
        }
        
        CatalogId getHostId() const {
            return (m_hostId);
        }
        
        #ifdef STORAGE_MMAP
        std::string getDBDir() const {
        	return (m_MMAPDir);
        }

        long getFileSize() const {
        	return (m_MMAPSize);
        }

		#endif

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
        AntiCacheDB* getAntiCacheDB() const {
            return m_antiCacheDB; 
        }
        
        /**
         * Return the handle to the anti-cache manager that will update tuple timestamps
         * and can select tuples for eviction. 
         */
        AntiCacheEvictionManager* getAntiCacheEvictionManager() const {
            return m_antiCacheEvictionManager; 
        }
        
        /**
         * Enable the anti-caching feature in the EE.
         * The input parameter is the directory where our disk-based storage
         * will write out evicted blocks of tuples for this partition
         */
        void enableAntiCache(std::string &dbDir, long blockSize) {
            assert(m_antiCacheEnabled == false);
            m_antiCacheEnabled = true;
            m_antiCacheDB = new AntiCacheDB(this, dbDir, blockSize);
            m_antiCacheEvictionManager = new AntiCacheEvictionManager(); 
        }
        #endif
        
        // ------------------------------------------------------------------
        // STORAGE MMAP
        // ------------------------------------------------------------------
        #ifdef STORAGE_MMAP

        /**
         * Enable the mmap storage feature in the EE.
         * The input parameter is the directory where our disk-based storage
         * will write out mmap'ed files for this partition
         */
        void enableMMAP(std::string &dbDir, long mapSize) {
            m_MMAPDir = dbDir;
            m_MMAPSize = mapSize;
        }
        #endif


        // ------------------------------------------------------------------
        // READ-WRITE TRACKERS
        // ------------------------------------------------------------------ 
        
        inline bool isTrackingEnabled() const {
            return (m_trackingEnabled);
        }
        
        inline ReadWriteTrackerManager* getTrackerManager() const {
            return (m_trackingManager);
        }
        
        /**
         * Enable the read/write set tracking feature in the EE.
         */
        void enableTracking() {
            assert(m_trackingEnabled == false);
            m_trackingEnabled = true;
            m_trackingManager = new ReadWriteTrackerManager(this);
        }
        
    private:
        Topend *m_topEnd;
        UndoQuantum *m_undoQuantum;
        int64_t m_txnId;
        
        #ifdef ANTICACHE
        AntiCacheDB *m_antiCacheDB;
        AntiCacheEvictionManager *m_antiCacheEvictionManager; 
        #endif
        
		#ifdef STORAGE_MMAP
        long m_MMAPSize;
        std::string m_MMAPDir;
		#endif


        /** ReadWrite Trackers */
        bool m_trackingEnabled;
        ReadWriteTrackerManager *m_trackingManager;
        
    public:
        int64_t m_lastCommittedTxnId;
        int64_t m_lastTickTime;
        CatalogId m_siteId;
        CatalogId m_partitionId;
        std::string m_hostname;
        CatalogId m_hostId;
        bool m_exportEnabled;
        bool m_antiCacheEnabled;
        
        /** local epoch for voltdb, somtime around 2008, pulled from catalog */
        int64_t m_epoch;
    };
    
}

#endif
