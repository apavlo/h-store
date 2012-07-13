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

#include "anticache/AntiCacheEvictionManager.h"
#include "common/types.h"
#include "common/FatalException.hpp"
#include "common/ValueFactory.hpp"
#include "storage/table.h"
#include "storage/persistenttable.h"
#include "storage/temptable.h"
#include "storage/tablefactory.h"
#include "anticache/EvictionIterator.h"

#include <string>
#include <vector>

namespace voltdb
{
            
// -----------------------------------------
// AntiCacheEvictionManager Implementation 
// -----------------------------------------
    
AntiCacheEvictionManager::AntiCacheEvictionManager() {
    
    // Initialize readBlocks table
    this->initEvictResultTable();
}

AntiCacheEvictionManager::~AntiCacheEvictionManager() {
    delete m_evictResultTable;
}

void AntiCacheEvictionManager::initEvictResultTable() {
    std::string tableName = "EVICT_RESULT";
    CatalogId databaseId = 1;
    std::vector<std::string> colNames;
    std::vector<ValueType> colTypes;
    std::vector<int32_t> colLengths;
    std::vector<bool> colAllowNull;
    
    // TABLE_NAME
    colNames.push_back("TABLE_NAME");
    colTypes.push_back(VALUE_TYPE_VARCHAR);
    colLengths.push_back(4096);
    colAllowNull.push_back(false);
    
    // TUPLES_EVICTED
    colNames.push_back("TUPLES_EVICTED");
    colTypes.push_back(VALUE_TYPE_INTEGER);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    colAllowNull.push_back(false);
    
    // BLOCKS_EVICTED
    colNames.push_back("BLOCKS_EVICTED");
    colTypes.push_back(VALUE_TYPE_INTEGER);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_INTEGER));
    colAllowNull.push_back(false);
    
    // BYTES_EVICTED
    colNames.push_back("BYTES_EVICTED");
    colTypes.push_back(VALUE_TYPE_BIGINT);
    colLengths.push_back(NValue::getTupleStorageSize(VALUE_TYPE_BIGINT));
    colAllowNull.push_back(false);
    
    TupleSchema *schema = TupleSchema::createTupleSchema(colTypes,
                                                         colLengths,
                                                         colAllowNull, true);
    
    m_evictResultTable = reinterpret_cast<Table*>(TableFactory::getTempTable(
                                                        databaseId,
                                                        tableName,
                                                        schema,
                                                        &colNames[0],
                                                        NULL));
}

bool AntiCacheEvictionManager::updateTuple(TableTuple& tuple) {
    // TODO: Implement mechanism to determine least recently used tuples
    
    return true; 
}

Table* AntiCacheEvictionManager::evictBlock(PersistentTable *table, long blockSize) {
    int32_t lastTuplesEvicted = table->getTuplesEvicted();
    int32_t lastBlocksEvicted = table->getBlocksEvicted();
    int64_t lastBytesEvicted  = table->getBytesEvicted();
    
    if (table->evictBlockToDisk(blockSize) == false) {
        throwFatalException("Failed to evict tuples from table '%s'", table->name().c_str());
    }
    
    int32_t tuplesEvicted = table->getTuplesEvicted() - lastTuplesEvicted;
    int32_t blocksEvicted = table->getBlocksEvicted() - lastBlocksEvicted; 
    int64_t bytesEvicted = table->getBytesEvicted() - lastBytesEvicted;
    
    m_evictResultTable->deleteAllTuples(false);
    TableTuple tuple = m_evictResultTable->tempTuple();
    
    int idx = 0;
    tuple.setNValue(idx++, ValueFactory::getStringValue(table->name()));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(tuplesEvicted)));
    tuple.setNValue(idx++, ValueFactory::getIntegerValue(static_cast<int32_t>(blocksEvicted)));
    tuple.setNValue(idx++, ValueFactory::getBigIntValue(static_cast<int32_t>(bytesEvicted)));
    
    return (m_evictResultTable);
}

Table* AntiCacheEvictionManager::readBlocks(PersistentTable *table, int numBlocks, uint16_t blockIds[]) {
    // TODO
    
    return (m_readResultTable);
}




}

