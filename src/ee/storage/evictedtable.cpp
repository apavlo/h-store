/*
 *  EvictedTable.cpp
 *  
 *
 *  Created by Justin on 6/26/12.
 *  Copyright 2012 __MyCompanyName__. All rights reserved.
 *
 */


#include "storage/persistenttable.h"


namespace voltdb {

EvictedTable::EvictedTable(ExecutorContext *ctx) : PersistentTable(ctx, false)
{
    
}

    
/*
 Insert a tuple into the evicted table but don't create any UNDO action. 
 */
bool EvictedTable::insertTuple(TableTuple &source)
{
    // not null checks at first
    if (!checkNulls(source)) {
        throwFatalException("Failed to insert tuple into table %s for undo:"
                            " null constraint violation\n%s\n", m_name.c_str(),
                            source.debugNoHeader().c_str());
    }
    
    // First get the next free tuple This will either give us one from
    // the free slot list, or grab a tuple at the end of our chunk of
    // memory
    nextFreeTuple(&m_tmpTarget1);
    m_tupleCount++;
    
    // Then copy the source into the target
    m_tmpTarget1.copy(source);
    m_tmpTarget1.setDeletedFalse();
}
    
}

