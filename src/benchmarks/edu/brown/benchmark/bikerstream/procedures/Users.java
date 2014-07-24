/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//
// Rides a bike - basically inserts events theoretically
// from a gps on a bike into the bikerreadings_stream
//

package edu.brown.benchmark.bikerstream.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;

@ProcInfo (
    singlePartition = true
)
public class Users extends VoltProcedure {

    // Logging Information
    private static final Logger Log = Logger.getLogger(Users.class);

    public final SQLStmt getUsers = new SQLStmt(
                "SELECT * FROM users"
            );

    public VoltTable [] run() {
        voltQueueSQL(getUsers);
        return voltExecuteSQL(true);
    }

} // End Class

