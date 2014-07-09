 /* Copyright (C) 2013 by H-Store Project
  * Brown University
  * Carnegie Mellon University
  * Massachusetts Institute of Technology
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

 #include "common/MMAPMemoryManager.h"
 #include "common/FatalException.hpp"

 namespace voltdb {

   pthread_mutex_t MMAPMemoryManager::m_mutex = PTHREAD_MUTEX_INITIALIZER;

   const unsigned int DEFAULT_MMAP_SIZE = 256 * 1024 * 1024;

   MMAPMemoryManager::MMAPMemoryManager()
   : m_size(DEFAULT_MMAP_SIZE), m_allocated(0),
   m_persistent(false), m_index(0)
   {
     init();
   }


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
       MMAP_file_name += ".nvm" ;

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

     //VOLT_TRACE("Init m_base      : %p ",m_base);
     //VOLT_TRACE("Init m_allocated : %ld ",m_allocated);

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


   void* MMAPMemoryManager::allocate(size_t chunkSize) {
     if (pthread_mutex_lock(&m_mutex)) {
       VOLT_ERROR("Failed to lock mutex in MMAPMemoryManager::allocate()\n");
       throwFatalException("Failed to lock mutex.");
     }

     void *memory = NULL;

     if (m_base == NULL) {
       init();
     }

     /** Update METADATA map and do the allocation **/
     m_metadata.push_back(std::make_pair(m_allocated, chunkSize));
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
       VOLT_ERROR("Failed to unlock mutex in MMAPMemoryManager::allocate()");
       throwFatalException("Failed to unlock mutex.");
     }

     assert(memory != NULL);
     return memory;
   }

   void MMAPMemoryManager::deallocate(void* offset, size_t chunkSize) {
     if (pthread_mutex_lock(&m_mutex)) {
       printf("Failed to lock mutex in MMAPMemoryManager::deallocate()\n");
       exit(1);
     }

     /** Update METADATA map and mark the deallocation **/
     //m_metadata.push_back(std::make_pair(m_allocated, chunkSize));
     //m_index += 1;

     if (pthread_mutex_unlock(&m_mutex)) {
       printf("Failed to unlock mutex in MMAPMemoryManager::deallocate()\n");
       exit(1);
     }
   }

   void MMAPMemoryManager::showMetadata(){
     if(m_persistent){
       VOLT_WARN("Data Storage Map :: %s ",this->m_fileName.c_str());
       VOLT_WARN("Index :: Offset  Size  ");
       for(vector<pair<size_t,size_t> >::const_iterator itr = m_metadata.begin(); itr != m_metadata.end(); ++itr){
	 printf("%lu :: %lu \n",(*itr).first, (*itr).second);
       }
     }
   }


   /** ASYNC m_sync **/
   void MMAPMemoryManager::async(){
     /** Only sync till m_allocated **/
     int ret = msync(m_base, m_allocated, MS_ASYNC);
     if(ret<0){
       VOLT_ERROR("msync failed with error.");
       throwFatalException("Failed to msync.");
     }
   }

   /** SYNC m_sync **/
   void MMAPMemoryManager::sync(){
     /** Only sync till m_allocated **/
     int ret = msync(m_base, m_allocated, MS_SYNC);
     if(ret<0){
       VOLT_ERROR("msync failed with error.");
       throwFatalException("Failed to msync.");
     }
   }


 }
 
