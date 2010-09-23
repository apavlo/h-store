/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
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
package edu.brown.benchmark.tpce.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

/**
 * Trade-Status transaction <br/>
 * TPC-E Section 3.3.5
 */
@ProcInfo(
    partitionInfo = "TRADE.T_CA_ID: 0",
    singlePartition = true
)
public class TradeStatus extends VoltProcedure {

    // This query takes a long time to plan in VoltCompiler, but it should work
    public final SQLStmt getTradeStatus = new SQLStmt(
            "select T_ID, T_DTS, ST_NAME, TT_NAME, T_S_SYMB, T_QTY, T_EXEC_NAME, T_CHRG, S_NAME, EX_NAME " +
            "  FROM TRADE, STATUS_TYPE, TRADE_TYPE, SECURITY, EXCHANGE " +
            " WHERE T_CA_ID = ? " +
            "   AND ST_ID = T_ST_ID " +
            "   AND TT_ID = T_TT_ID " +
            "   AND S_SYMB = T_S_SYMB " + 
            "   AND EX_ID = S_EX_ID " +
            " ORDER BY T_DTS DESC LIMIT 50"
    );

    public final SQLStmt getName = new SQLStmt(
            "select C_L_NAME, C_F_NAME, B_NAME "
                    + "from CUSTOMER_ACCOUNT, CUSTOMER, BROKER "
                    + "where CA_ID = ? and C_ID = CA_C_ID and B_ID = CA_B_ID");

    public VoltTable[] run(long acct_id) throws VoltAbortException {
        voltQueueSQL(getTradeStatus, acct_id);
        voltQueueSQL(getName, acct_id);
        return voltExecuteSQL();
    }

}
