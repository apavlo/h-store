#include "MMAPMemoryManager.h"

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

using namespace voltdb;

pthread_mutex_t MMAPMemoryManager::m_mutex = PTHREAD_MUTEX_INITIALIZER;

const size_t DEFAULT_SIZE = (size_t)16 * (size_t)1024 * (size_t)1024;

MMAPMemoryManager::MMAPMemoryManager() 
    : m_size(DEFAULT_SIZE), m_allocated(0), 
    m_fileName(std::string()), m_persistent(false), m_index(0)
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
            printf("MMAP : initialization error.");
            exit(1);
        }
    }
    else{
        // Backed by a file

        int MMAP_fd, ret;
        std::string MMAP_Dir, MMAP_file_name;

        const std::string NVM_fileType(".nvm");
        const std::string pathSeparator("/");

        if(m_fileName.empty()){
            printf("MMAP : initialization error.");
            exit(1);
        }

        // TODO
        MMAP_Dir = "./tmp";

        /** Get an unique file object for the table **/
        MMAP_file_name  = MMAP_Dir + pathSeparator ;
        MMAP_file_name += m_fileName ;
        MMAP_file_name += NVM_fileType ;

        printf("MMAP : MMAP_file_name :: %s ", MMAP_file_name.c_str());

        MMAP_fd = open(MMAP_file_name.c_str(), O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP );
        if (MMAP_fd < 0) {
            printf("MMAP : initialization error.");
            exit(1);
        }

        ret = ftruncate(MMAP_fd, m_size) ;
        if(ret < 0){
            printf("MMAP : initialization error.");
            exit(1);
        }

        m_base = mmap(NULL, m_size, PROT_READ | PROT_WRITE, MAP_SHARED , MMAP_fd, 0);

        if (m_base == MAP_FAILED) {
            printf("MMAP : initialization error.");
            exit(1);
        }

        // Can close file since mmap adds a reference to file implicitly
        close(MMAP_fd);
    }

    assert(m_base != NULL);
    assert(m_allocated == 0);
}

MMAPMemoryManager::~MMAPMemoryManager() {

    if (pthread_mutex_lock(&m_mutex)) {
        printf("Failed to lock mutex in MMAPMemoryManager::~MemoryManager()\n");
        exit(1);
    }

    int ret;

    if (m_base != NULL) {
        ret = munmap(m_base, m_size);

        if(ret != 0){
            printf("MUNMAP : initialization error.");
            exit(1);
        }
    }

    m_base = NULL;
    m_allocated = 0;

    if (pthread_mutex_unlock(&m_mutex)) {
        printf("Failed to unlock mutex in MMAPMemoryManager::~MemoryManager()\n");
        exit(1);
    }
}


void* MMAPMemoryManager::alloc(size_t chunkSize) {
    if (pthread_mutex_lock(&m_mutex)) {
        printf("Failed to lock mutex in MMAPMemoryManager::alloc()\n");
        exit(1);
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
        printf("No more memory available in MMAP'ed file\n");
        exit(1);
    }

    memory = reinterpret_cast<char*> ((char*) m_base + m_allocated);

    printf("Allocated chunk at : %p \n",memory);
    
    m_allocated += chunkSize;

    if (pthread_mutex_unlock(&m_mutex)) {
        printf("Failed to unlock mutex in MMAPMemoryManager::alloc()\n");
        exit(1);
    }

    assert(memory != NULL);
    return memory;
}


void MMAPMemoryManager::showMetadata(){
    if(m_persistent){
        printf("Data Storage Map :: %s \n",this->m_fileName.c_str());
        printf("Index :: Offset  Size  \n");
        for(map<int, pair<int,int> >::const_iterator itr = m_metadata.begin(); itr != m_metadata.end(); ++itr){
            printf("%d :: %d %d \n",itr->first, itr->second.first,  itr->second.second);
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
            perror("msync failed with error:");
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
            perror("msync failed with error:");
            return false;
        }
    }
    return true;
}
