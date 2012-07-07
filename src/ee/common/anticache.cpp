/* Copyright (C) 2012 by H-Store Project
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

#include "storage/anticache.h"
#include "common/FatalException.hpp"

namespace voltdb {

AntiCacheDB::AntiCacheDB(ExecutorContext *ctx, std::string db_dir) :
    m_executorContext(ctx),
    m_dbDir(db_dir),
    m_nextBlockId(0) {
        
    try {
        // allocate and initialize Berkeley DB database env
        m_dbEnv = new DbEnv(0); 
        m_dbEnv->open(m_dbDir, DB_CREATE | DB_INIT_MPOOL, 0); 
        
        // allocate and initialize new Berkeley DB instance
        m_db = new Db(m_dbEnv, 0); 
        m_db->open(NULL, "anticache.db", NULL, DB_HASH, DB_CREATE, 0); 
        
    } catch(DbException &e) {
        // TODO: exit program
    }
}

AntiCacheDB::~AntiCacheDB() {
    m_dbEnv->close(0);
    delete m_dbEnv;

    m_db->close(0);
    delete m_db;
}

void AntiCacheDB::writeBlock(uint16_t block_id, char* serialized_data, int serialized_data_length) {
    Dbt key; 
    key.set_data(&block_id);
    key.set_size(sizeof(uint16_t));
    
    Dbt value;
    value.set_data(serialized_data);
    value.set_size(serialized_data_length); 
    
    anti_cache_db->put(NULL, &key, &value, 0);
}

AntiCacheBlock AntiCacheDB::readBlock(uint16_t block_id) {
    Dbt key;
    key.set_data(&block_id);
    key.set_size(sizeof(uint16_t));

    Dbt value;
    value.set_flags(DB_DBT_MALLOC);
    
    int ret_value = anti_cache_db->get(NULL, &key, &value, 0);
    if (ret_value != 0) {
        throwFatalException("Invalid anti-cache blockId '%d'", block_id);
    }
    assert(value.get_data() != NULL);
    
    AntiCacheBlock block(block_id, value);
    return (block);
    
}
    
}

