/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
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
package edu.brown.benchmark.tpce.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

/**
 * TradeCleanup Transaction <br/>
 * TPC-E Section 3.3.12
 */
public class TradeCleanup extends VoltProcedure {

    public final SQLStmt selectTradeRequest = new SQLStmt("SELECT TR_T_ID FROM TRADE_REQUEST ORDER BY TR_T_ID");

    public final SQLStmt insertTradeHistory = new SQLStmt("INSERT INTO TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) VALUES (?, ?, ?)");

    public final SQLStmt updateTrade = new SQLStmt("UPDATE TRADE SET T_ST_ID = ?, T_DTS = ? WHERE T_ID = ?");

    public final SQLStmt deleteTradeRequest = new SQLStmt("DELETE FROM TRADE_REQUEST");

    public final SQLStmt selectTrade = new SQLStmt("SELECT T_ID FROM TRADE WHERE T_ID >= ? AND T_ST_ID = ?");

    /**
     * @param canceled_id
     * @param pending_id
     * @param submitted_id
     * @param start_trade_id
     * @return
     */
    public long run(String canceled_id, String pending_id, String submitted_id, long start_trade_id) {

        // Find pending trades from TRADE_REQUEST
        voltQueueSQL(selectTradeRequest);
        VoltTable[] results = voltExecuteSQL();

        // Insert a submitted followed by canceled record into TRADE_HISTORY,
        // mark
        // the trade canceled and delete the pending trade
        while (results[0].advanceRow()) {
            long tr_trade_id = results[0].getLong(0);
            TimestampType now = new TimestampType();

            voltQueueSQL(insertTradeHistory, tr_trade_id, now, submitted_id);
            voltQueueSQL(updateTrade, canceled_id, now, tr_trade_id);
            voltQueueSQL(insertTradeHistory, tr_trade_id, now, canceled_id);
        } // WHILE
        voltQueueSQL(deleteTradeRequest);
        voltExecuteSQL();

        // Find submitted trades, change the status to canceled and insert a
        // canceled record into TRADE_HISTORY
        voltQueueSQL(selectTrade, start_trade_id, submitted_id);
        results = voltExecuteSQL();

        while (results[0].advanceRow()) {
            long trade_id = results[0].getLong(0);
            TimestampType now = new TimestampType();

            // Mark the trade as canceled, and record the time
            voltQueueSQL(updateTrade, canceled_id, now, trade_id);
            voltQueueSQL(insertTradeHistory, trade_id, now, canceled_id);
        } // WHILE
        voltExecuteSQL();

        return (0l);
    }
}
