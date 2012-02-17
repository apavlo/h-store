/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package edu.brown.benchmark.auctionmark.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.auctionmark.AuctionMarkProfile;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants;

/**
 * NewCommentResponse
 * @author pavlo
 * @author visawee
 */
@ProcInfo (
    partitionInfo = "USER.U_ID: 2",
    singlePartition = true
)
public class NewCommentResponse extends VoltProcedure{
    
    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------
    
    public final SQLStmt updateComment = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_ITEM_COMMENT + " " +
            "SET ic_response = ?, " +
            "    ic_updated = ? " +
        "WHERE ic_id = ? AND ic_i_id = ? AND ic_u_id = ? "
    );
    
    public final SQLStmt updateUser = new SQLStmt(
        "UPDATE " + AuctionMarkConstants.TABLENAME_USER + " " +
           "SET u_comments = u_comments - 1, " +
           "    u_updated = ? " +
        " WHERE u_id = ?"
    );

    // -----------------------------------------------------------------
    // RUN METHOD
    // -----------------------------------------------------------------
    
    public VoltTable[] run(TimestampType benchmarkTimes[], long item_id, long seller_id, long comment_id, String response) {
        final TimestampType currentTime = AuctionMarkProfile.getScaledTimestamp(benchmarkTimes[0], benchmarkTimes[1], new TimestampType());
        voltQueueSQL(updateComment, response, currentTime, comment_id, item_id, seller_id);
        voltQueueSQL(updateUser, currentTime, seller_id);
        return (voltExecuteSQL());
    }   
}