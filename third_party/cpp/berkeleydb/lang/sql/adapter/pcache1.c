/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2010, 2015 Oracle and/or its affiliates.  All rights reserved.
 */
#include "sqliteInt.h"

#ifdef SQLITE_ENABLE_MEMORY_MANAGEMENT
int sqlite3PcacheReleaseMemory(int nReq){ return nReq; }
#endif
