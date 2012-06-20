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
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

/** 
 * Trade-Lookup transaction <br/>
 * TPC-E section 3.3.3
 * 
 * H-Store exceptions:
 *   1) There are a lot of output parameters for some frames. Although, since they are not required for the transaction's
 *      output, we do not retrieve them from tables, but the statements are executed anyway
 *   2) getTrade_frame2, getTrade_frame3 SQL: LIMIT is set to 20 (the default value for the EGen), however it must be 'max_trade' parameter.
 *      H-Store does not support variable limits
 *   3) getHoldingHistory is modified to use a self-join instead of an "IN". H-Store does not support "IN"
 *   4) getTrade_frame2 and _frame3 are given "CMPT" string as parameter, since we cannot specify it in the SQL query string -- parser error
 */
public class TradeLookup extends VoltProcedure {
    private final VoltTable trade_lookup_ret_template_frame1 = new VoltTable(
            new VoltTable.ColumnInfo("is_cash", VoltType.INTEGER),
            new VoltTable.ColumnInfo("is_market", VoltType.INTEGER)
    );
    private final VoltTable trade_lookup_ret_template_frame23 = new VoltTable(
            new VoltTable.ColumnInfo("is_cash", VoltType.INTEGER),
            new VoltTable.ColumnInfo("trade_id", VoltType.BIGINT)
    );
    private final VoltTable trade_lookup_ret_template_frame4 = new VoltTable(
            new VoltTable.ColumnInfo("trade_id", VoltType.BIGINT)
    );

    public final SQLStmt getTrade_frame1 = new SQLStmt("select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, TT_IS_MRKT, T_TRADE_PRICE from TRADE, TRADE_TYPE where T_ID = ? and T_TT_ID = TT_ID");

    public final SQLStmt getSettlement = new SQLStmt("select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from SETTLEMENT where SE_T_ID = ?");

    public final SQLStmt getCash = new SQLStmt("select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt getTradeHistory = new SQLStmt("select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS");
    

    public final SQLStmt getTrade_frame2 = new SQLStmt("select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE from TRADE " +
            "where T_CA_ID = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? order by T_DTS asc limit 20");

    
    public final SQLStmt getTrade_frame3 = new SQLStmt("select T_CA_ID, T_EXEC_NAME, T_IS_CASH, T_TRADE_PRICE, T_QTY, T_DTS, T_ID, T_TT_ID " +
            "from TRADE where T_S_SYMB = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? order by T_DTS asc limit 20");

    public final SQLStmt getTrade_frame4 = new SQLStmt("select T_ID from TRADE where T_CA_ID = ? and T_DTS >= ? order by T_DTS asc limit 1");

    // modified to use join
    public final SQLStmt getHoldingHistory = new SQLStmt("select HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY from HOLDING_HISTORY where HH_H_T_ID = HH_T_ID and HH_T_ID = ?");

    public VoltTable[] run(long[] trade_ids, long acct_id, long max_acct_id, long frame_to_execute, long max_trades, TimestampType end_trade_dts, TimestampType start_trade_dts, String symbol)
            throws VoltAbortException {
            
        VoltTable result = null;
        
        // all frames are mutually exclusive here -- the frame number is a parameter
        if (frame_to_execute == 1) {
            for (int i = 0; i < max_trades; i++) {
                voltQueueSQL(getTrade_frame1, trade_ids[i]);
                voltQueueSQL(getSettlement, trade_ids[i]);
                voltQueueSQL(getTradeHistory, trade_ids[i]);
            }
            
            VoltTable[] res = voltExecuteSQL();
            int[] is_cash = new int[(int)max_trades];
            int[] is_market = new int[(int)max_trades];
            
            for (int i = 0; i < max_trades; i++) {
                VoltTable trade = res[3 * i];
                VoltTable settle = res[3 * i + 1];
                VoltTable hist = res[3 * i + 2];
                assert trade.getRowCount() == 1;
                assert settle.getRowCount() == 1;
                assert hist.getRowCount() == 2 || hist.getRowCount() == 3;
                
                /*
                 * we should "retrieve" some values here, however only is_cash and is_mrkt are required
                 * other values just serve as frame OUT params
                 * the values are in memory so it would not be cheating not to retrieve them -- it's cheap anyway
                 */
                VoltTableRow trade_row = trade.fetchRow(0);
                is_cash[i] = (int)trade_row.getLong("T_IS_CASH");
                is_market[i] = (int)trade_row.getLong("TT_IS_MRKT");
                
                if (is_cash[i] == 1) {
                    voltQueueSQL(getCash, trade_ids[i]);
                }
            }
            
            voltExecuteSQL(); // for possible cash transactions; the result is not needed for the client
            
            // results
            result = trade_lookup_ret_template_frame1.clone(256);
            for (int i = 0; i < max_trades; i++) {
                result.addRow(is_cash[i], is_market[i]);
            }
        }
        else if (frame_to_execute == 2 || frame_to_execute == 3) {
            if (frame_to_execute == 2) {
                voltQueueSQL(getTrade_frame2, acct_id, "CMPT", start_trade_dts, end_trade_dts);
            }
            else {
                voltQueueSQL(getTrade_frame3, symbol, "CMPT", start_trade_dts, end_trade_dts);
            }
            
            VoltTable trades = voltExecuteSQL()[0];
            
            result = trade_lookup_ret_template_frame23.clone(256);
            for (int i = 0; i < trades.getRowCount(); i++) {
                VoltTableRow trade_row = trades.fetchRow(i);
                
                long trade_id = trade_row.getLong("T_ID");
                int is_cash = (int)trade_row.getLong("T_IS_CASH");
                
                voltQueueSQL(getSettlement, trade_id);
                voltQueueSQL(getTradeHistory, trade_id);
                if (is_cash == 1) {
                    voltQueueSQL(getCash, trade_id);
                }
                
                voltExecuteSQL(); // the client does not need the results
                
                result.addRow(is_cash, trade_id);               
            }
        }
        else if (frame_to_execute == 4) {
            voltQueueSQL(getTrade_frame4, acct_id, start_trade_dts);
            VoltTable trades = voltExecuteSQL()[0];
            
            result = trade_lookup_ret_template_frame4.clone(128);
            
            // there might be a case of no trades
            if (trades.getRowCount() > 0) {
                long trade_id = trades.fetchRow(0).getLong("T_ID");
                result.addRow(trade_id);
                
                voltQueueSQL(getHoldingHistory, trade_id);
                voltExecuteSQL(); // the client does not need the results
            }
        }
        else {
            assert false;
        }
        
        return new VoltTable[] {result};
    }
}
