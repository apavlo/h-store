/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tm1.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tm1.TM1Constants;

@ProcInfo(
    partitionParam = 1,
    singlePartition = false
)
public class UpdateLocation extends VoltProcedure {

    public final SQLStmt getSubscriber = new SQLStmt(
        "SELECT s_id FROM " + TM1Constants.TABLENAME_SUBSCRIBER + 
        " WHERE sub_nbr = ?"
    );

    public final SQLStmt updateSubscriber = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER + 
        " SET vlr_location = ? WHERE s_id = ?"
    );

    public final SQLStmt update = new SQLStmt(
        "UPDATE " + TM1Constants.TABLENAME_SUBSCRIBER + 
        " SET vlr_location = ? WHERE sub_nbr = ?"
    );

    public long run(long location, String sub_nbr) {
        voltQueueSQL(getSubscriber, sub_nbr);
        VoltTable results[] = voltExecuteSQL();
        if (results[0].getRowCount() > 0) {
            long s_id = results[0].asScalarLong();
            voltQueueSQL(updateSubscriber, location, s_id);
            results = voltExecuteSQL(true);
            assert results.length == 1;
            return results[0].asScalarLong();
        }
        return 0;
    }
}