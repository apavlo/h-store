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

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/time.h>

#include "MMAPMemoryManager.hpp"

using namespace voltdb;

pthread_mutex_t MMAPMemoryManager::m_mutex = PTHREAD_MUTEX_INITIALIZER;

const size_t DEFAULT_SIZE = (size_t)16 * (size_t)1024 * (size_t)1024;
const std::string fileType = ".nvm";
const std::string pathSeparator = "/";


MMAPMemoryManager::MMAPMemoryManager(size_t size, const std::string fileName, bool persistent)
    : m_size(size), m_allocated(0),
    m_fileName(fileName), m_persistent(persistent), m_index(0)
{
    init();
}
 
void MMAPMemoryManager::init() {

    // Allocate a big chunk which will be split into smaller chunks later
    if(m_persistent == false){
        // Not backed by a file
        m_base = mmap(NULL, m_size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);

        if (m_base == MAP_FAILED) {
            VOLT_ERROR("MMAP : initialization error : mmap failed");
            throwFatalException("MMAP : initialization error : mmap failed");	  
	}
    }
    else{
        // Backed by a file

        int MMAP_fd, ret;
        std::string MMAP_Dir, MMAP_file_name;

        if(m_fileName.empty()){
            VOLT_ERROR("MMAP : initialization error : empty fileName.");
            throwFatalException("MMAP : initialization error : empty fileName");
        }

        /** Get an unique file object for the table **/
        MMAP_file_name  = m_fileName ;
	MMAP_file_name += fileType ;

        VOLT_WARN("MMAP : MMAP_file_name :: %s ", MMAP_file_name.c_str());

        MMAP_fd = open(MMAP_file_name.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP );
        if (MMAP_fd < 0) {
            VOLT_ERROR("MMAP : initialization error : open failed.");
            throwFatalException("MMAP : initialization error : open failed");
        }

        ret = ftruncate(MMAP_fd, m_size) ;
        if(ret < 0){
            VOLT_ERROR("MMAP : initialization error : ftruncate failed");
            throwFatalException("MMAP : initialization error : ftruncate failed");
        }

        m_base = mmap(NULL, m_size, PROT_READ | PROT_WRITE, MAP_SHARED , MMAP_fd, 0);

        if (m_base == MAP_FAILED) {
            VOLT_ERROR("MMAP : initialization error : mmap failed");
            throwFatalException("MMAP : initialization error : mmap failed");
        }

        // Can close file since mmap adds a reference to file implicitly
        close(MMAP_fd);
    }

    assert(m_base != NULL);
    assert(m_allocated == 0);
}

MMAPMemoryManager::~MMAPMemoryManager() {

    if (pthread_mutex_lock(&m_mutex)) {
        VOLT_ERROR("Failed to lock mutex in MMAPMemoryManager::~MemoryManager()\n");
        throwFatalException("Failed to lock mutex.");
    }

    int ret;

    if (m_base != NULL) {
        ret = munmap(m_base, m_size);

        if(ret != 0){
            VOLT_ERROR("MUNMAP : initialization error.");
            throwFatalException("MUNMAP : initialization error.");
        }
    }

    m_base = NULL;
    m_allocated = 0;

    if (pthread_mutex_unlock(&m_mutex)) {
        VOLT_ERROR("Failed to unlock mutex in MMAPMemoryManager::~MemoryManager()\n");
        throwFatalException("Failed to unlock mutex.");      
    }
}


void* MMAPMemoryManager::alloc(size_t chunkSize) {
    if (pthread_mutex_lock(&m_mutex)) {
        VOLT_ERROR("Failed to lock mutex in MMAPMemoryManager::alloc()\n");
        throwFatalException("Failed to lock mutex.");
    }

    void *memory = NULL;
    
    if (m_base == NULL) {
        init();
    }
 
    /** Update METADATA map and do the allocation **/
    m_metadata[m_index] = std::make_pair(m_allocated, chunkSize);
    m_index += 1;

    /** Not an issue since we are mmap'ing **/
    if(m_allocated + chunkSize > m_size){
        VOLT_ERROR("No more memory available in MMAP'ed file");
        throwFatalException("No more memory available in MMAP'ed file.");
    }

    memory = reinterpret_cast<char*> ((char*) m_base + m_allocated);

    VOLT_WARN("Allocated chunk at : %p ",memory);
    
    m_allocated += chunkSize;

    if (pthread_mutex_unlock(&m_mutex)) {
        VOLT_ERROR("Failed to unlock mutex in MMAPMemoryManager::alloc()");
        throwFatalException("Failed to unlock mutex.");
    }

    assert(memory != NULL);
    return memory;
}


void MMAPMemoryManager::showMetadata(){
    if(m_persistent){
        VOLT_WARN("Data Storage Map :: %s ",this->m_fileName.c_str());
        VOLT_WARN("Index :: Offset  Size  ");
        for(map<int, pair<int,int> >::const_iterator itr = m_metadata.begin(); itr != m_metadata.end(); ++itr){
            VOLT_WARN("%d :: %d %d \n",itr->first, itr->second.first,  itr->second.second);
        }
    }
}


/** ASYNC m_sync **/
bool MMAPMemoryManager::async(){
    if(m_persistent){
        int ret;

        /** Only sync till m_allocated **/
        ret = msync(m_base, m_allocated, MS_ASYNC);
        if(ret<0){
            VOLT_ERROR("msync failed with error.");
            return false;
        }
    }
    return true;
}

/** SYNC m_sync **/
bool MMAPMemoryManager::sync(){
    if(m_persistent){
        int ret;

        /** Only sync till m_allocated **/
        ret = msync(m_base, m_allocated, MS_SYNC);
        if(ret<0){
            VOLT_ERROR("msync failed with error.");
            return false;
        }
    }
    return true;
}
