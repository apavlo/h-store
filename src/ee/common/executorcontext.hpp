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

#include "pthread.h"
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>


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
            m_MMAPEnabled = false;
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
        
        // ------------------------------------------------------------------
        // STORAGE MMAP MANAGEMENT
        // ------------------------------------------------------------------ 
        
        std::string getDBDir() const {
            if(m_MMAPDir.empty())
                return "/tmp";          // Default : "/tmp"
        	return (m_MMAPDir);
        }

        long getFileSize() const {
        	return (m_MMAPSize);
        }
 
        bool isMMAPEnabled() const {
        	return (m_MMAPEnabled);
        }

        void setStoragePointer(void* ptr){
            m_storagePointer = ptr;
        }

        void* getStoragePointer(){
            return (m_storagePointer);
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
        // STORAGE MMAP MANAGEMENT
        // ------------------------------------------------------------------ 
        
        /**
         * Enable the mmap storage feature in the EE.
         * The input parameter is the directory where our disk-based storage
         * will write out mmap'ed files for this partition
         */                               
        void enableMMAP(std::string &dbDir, long mapSize) {
            assert(m_MMAPEnabled == false);
            m_MMAPDir = dbDir;
            m_MMAPSize = mapSize;

            // Initialize MMAP Storage File
            initStorage();

            m_MMAPEnabled = true;
            m_tbCount = 0 ;
        }
         
        /*
         * Invoked only once by ExecutorContext to set up MMAP Storage
         */
        void initStorage(){

            int MMAP_fd, ret;
            std::string MMAP_Dir, MMAP_file_name; 
            //long file_size;
            char* memory;

            const std::string NVM_fileName("storage");
            const std::string NVM_fileType(".nvm");
            const std::string pathSeparator("/");

            off_t file_size = 128 * 1024 * 1024 ; // 128 MB
    
            /** Get location for mmap'ed files **/
            MMAP_Dir = getDBDir();
            //file_size = m_executorContext->getFileSize();

            VOLT_WARN("MMAP : DBdir:: --%s--\n", MMAP_Dir.c_str());
            //VOLT_WARN("MMAP : File Size :: %ld\n", file_size);
 
            if(MMAP_Dir.empty()){
                VOLT_ERROR("MMAP : initialization error.");
                VOLT_ERROR("MMAP_Dir is empty \n");
                throwFatalException("Failed to get DB Dir.");
            }

            /** Get an unique file object for all requests **/
            MMAP_file_name  = MMAP_Dir + pathSeparator ;
            //MMAP_file_name += this->name() + "_" + m_tableRequestCountStringStream.str();
            MMAP_file_name += NVM_fileName;
            MMAP_file_name += NVM_fileType ;

            VOLT_WARN("MMAP : MMAP_file_name :: %s\n", MMAP_file_name.c_str());

            MMAP_fd = open(MMAP_file_name.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP );
            if (MMAP_fd < 0) {
                VOLT_ERROR("MMAP : initialization error.");
                VOLT_ERROR("Failed to open file %s : %s", MMAP_file_name.c_str(), strerror(errno));
                throwFatalException("Failed to open file in directory %s.", MMAP_Dir.c_str());
            }
        
            VOLT_WARN("MMAP : Set fd :: %d\n", MMAP_fd);
            
            ret = ftruncate(MMAP_fd, file_size) ;
            if(ret < 0){
                VOLT_ERROR("MMAP : initialization error.");
                VOLT_ERROR("Failed to truncate file %d : %s", MMAP_fd, strerror(errno));
                throwFatalException("Failed to truncate file in directory %s.", MMAP_Dir.c_str());
            }
         
                                              
            /*
             * Create a single large mapping and chunk it later during block allocation
             */
            memory = (char*) mmap(0, file_size , PROT_READ | PROT_WRITE , MAP_PRIVATE, MMAP_fd, 0);

            if (memory == MAP_FAILED) {
                VOLT_ERROR("MMAP : initialization error.");
                VOLT_ERROR("Failed to map file into memory.");
                throwFatalException("Failed to map file.");
            }
     

            // Setting mmap pointer in executor context 
            setStoragePointer(memory);
            
            /**
             * Closing the file descriptor does not unmap the region as mmap() automatically adds a reference
             * but can't use this fd to communicate with MMAP_PersistentTable
             */
            //close(MMAP_fd);

        }
             
        
        long getBlockCount(){
            return (m_tbCount++) ;
        }
 
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
        
        // ------------------------------------------------------------------
        // STORAGE MMAP MANAGEMENT
        // ------------------------------------------------------------------ 
        long m_MMAPSize;
        std::string m_MMAPDir;
        bool m_MMAPEnabled;
        void* m_storagePointer;

        long m_tbCount; 
        pthread_mutex_t m_tbLock;

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
