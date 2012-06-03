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

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

/**
 * Trade-Update transaction <br/>
 * TPC-E Section 3.3.4
 * 
 *   1) There are a lot of output parameters for some frames. Although, since they are not required for the transaction's
 *      output, we do not retrieve them from tables, but the statements are executed anyway
 *   2) getTradeTradeTypeSecurity and getTrade2 SQL: LIMIT is set to 20 (the default value for the EGen), however it must be 'max_trade' parameter.
 *      H-Store does not support variable limits
 *   3) getTradeTradeTypeSecurity and getTrade2 are given "CMPT" string as parameter,
 *      since we cannot specify it in the SQL query string -- parser error
 *   4) The number of rows updated, 'num_updated', is not returned since the client does not need it. However, it is required by the specification
 */
public class TradeUpdate extends VoltProcedure {
    private final VoltTable trade_update_ret_template_frame1 = new VoltTable(
            new VoltTable.ColumnInfo("is_cash", VoltType.INTEGER),
            new VoltTable.ColumnInfo("is_market", VoltType.INTEGER)
    );
    private final VoltTable trade_update_ret_template_frame2 = new VoltTable(
            new VoltTable.ColumnInfo("is_cash", VoltType.INTEGER),
            new VoltTable.ColumnInfo("trade_id", VoltType.BIGINT)
    );
    private final VoltTable trade_update_ret_template_frame3 = new VoltTable(
            new VoltTable.ColumnInfo("trade_id", VoltType.BIGINT)
    );
    
    public final SQLStmt getTrade1 = new SQLStmt("select T_EXEC_NAME from TRADE where T_ID = ?");

    public final SQLStmt updateTrade = new SQLStmt("update TRADE set T_EXEC_NAME = ? where T_ID = ?");

    public final SQLStmt getTradeTradeType = new SQLStmt("select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, TT_IS_MRKT, T_TRADE_PRICE from TRADE, TRADE_TYPE where T_ID = ? and T_TT_ID = TT_ID");

    public final SQLStmt getSettlement = new SQLStmt("select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from SETTLEMENT where SE_T_ID = ?");

    public final SQLStmt getCashTransaction = new SQLStmt("select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt getTradeHistory = new SQLStmt("select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS");

    public final SQLStmt getTrade2 = new SQLStmt("select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE from TRADE "
            + "where T_CA_ID = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? order by T_DTS asc limit 20");

    public final SQLStmt getTradeNoSTID = new SQLStmt("select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE from TRADE "
            + " where T_CA_ID = ? and T_DTS >= ? and T_DTS <= ? order by T_DTS asc limit 20");

    public final SQLStmt getCashType = new SQLStmt("select SE_CASH_TYPE from SETTLEMENT where SE_T_ID = ?");

    public final SQLStmt updateSettlement = new SQLStmt("update SETTLEMENT SET SE_CASH_TYPE = ? where SE_T_ID = ?");

    public final SQLStmt getTradeTradeTypeSecurity = new SQLStmt("select T_CA_ID, T_EXEC_NAME, T_IS_CASH, T_TRADE_PRICE, T_QTY, S_NAME, T_DTS, T_ID, "
            + "T_TT_ID, TT_NAME from TRADE, TRADE_TYPE, SECURITY where T_S_SYMB = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? and "
            + "TT_ID = T_TT_ID and S_SYMB = T_S_SYMB order by T_DTS asc limit 20");

    public final SQLStmt getCTName = new SQLStmt("select CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt updateCTName = new SQLStmt("update CASH_TRANSACTION set CT_NAME = ? where CT_T_ID = ?");

    public final SQLStmt getTradeHistoryAsc = new SQLStmt("select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS asc");

    public VoltTable[] run(long trade_ids[], long acct_id, long max_acct_id, long frame_to_execute, int max_trades, int max_updates, TimestampType end_trade_dts, TimestampType start_trade_dts,
            String symbol) throws VoltAbortException {
        
        VoltTable result = null;
        
        // all frames are mutually exclusive here -- the frame number is a parameter
        if (frame_to_execute == 1) {
            int num_updated = 0;
            Pattern p = Pattern.compile(".* X .*");
            int[] is_cash = new int[max_trades];
            int[] is_market = new int[max_trades];
            
            for (int i = 0; i < max_trades; i++) {
                if (num_updated < max_updates) {
                    voltQueueSQL(getTrade1, trade_ids[i]);
                    VoltTable trade = voltExecuteSQL()[0];
                    
                    assert trade.getRowCount() == 1;
                    String ex_name = trade.fetchRow(0).getString("T_EXEC_NAME");
                    
                    Matcher m = p.matcher(ex_name);
                    
                    // ex_name LIKE "% X %"
                    if (m.matches()) {
                        ex_name.replace(" X ", " ");
                    }
                    else {
                        ex_name.replace(" ", " X ");
                    }
                    
                    voltQueueSQL(updateTrade, ex_name, trade_ids[i]);
                    voltExecuteSQL();
                    num_updated++;
                }
                
                voltQueueSQL(getTradeTradeType, trade_ids[i]);
                voltQueueSQL(getSettlement, trade_ids[i]);
                VoltTable[] res = voltExecuteSQL();
                
                // we do not need to retrieve settlement here
                VoltTable trade = res[0];
                assert trade.getRowCount() == 1;
                VoltTableRow trade_row = trade.fetchRow(0);
                
                is_cash[i] = (int)trade_row.getLong("T_IS_CASH");
                is_market[i] = (int)trade_row.getLong("TT_IS_MRKT");
                
                if (is_cash[i] == 1) {
                    voltQueueSQL(getCashTransaction, trade_ids[i]);
                }
                
                voltQueueSQL(getTradeHistory, trade_ids[i]);
                voltExecuteSQL();
            }
            
            // results
            result = trade_update_ret_template_frame1.clone(256);
            for (int i = 0; i < max_trades; i++) {
                result.addRow(is_cash[i], is_market[i]);
            }
        }
        else if (frame_to_execute == 2) {
            voltQueueSQL(getTrade2, acct_id, "CMPT", start_trade_dts, end_trade_dts);
            VoltTable trades = voltExecuteSQL()[0];
            
            result = trade_update_ret_template_frame2.clone(256);
            int num_updated = 0;
            
            for (int i = 0; i < trades.getRowCount(); i++) {
                VoltTableRow trade_row = trades.fetchRow(i);
                
                long trade_id = trade_row.getLong("T_ID");
                int is_cash = (int)trade_row.getLong("T_IS_CASH");
                
                if (num_updated < max_updates) {
                    voltQueueSQL(getCashType, trade_id);
                    VoltTable cash = voltExecuteSQL()[0];
                    
                    assert cash.getRowCount() == 1;
                    String cash_type = cash.fetchRow(0).getString("SE_CASH_TYPE");
                    
                    if (is_cash == 1) {
                        if (cash_type.equals("Cash Account")) {
                            cash_type = "Cash";
                        }
                        else {
                            cash_type = "Cash Account";
                        }
                    }
                    else {
                        if (cash_type.equals("Margin Account")) {
                            cash_type = "Margin";
                        }
                        else {
                            cash_type = "Margin Account";
                        }
                        
                    }
                    
                    voltQueueSQL(updateSettlement, cash_type, trade_id);
                    voltExecuteSQL();
                    num_updated++;
                }
                
                voltQueueSQL(getSettlement, trade_id);
                if (is_cash == 1) {
                    voltQueueSQL(getCashTransaction, trade_id);
                }
                voltQueueSQL(getTradeHistory, trade_id);
                voltExecuteSQL();                
                
                result.addRow(is_cash, trade_id);               
            }
        }
        else if (frame_to_execute == 3) {
            voltQueueSQL(getTradeTradeTypeSecurity, symbol, "CMPT", start_trade_dts, end_trade_dts);
            VoltTable trades = voltExecuteSQL()[0];
            
            result = trade_update_ret_template_frame3.clone(128);
            int num_updated = 0;
            Pattern p = Pattern.compile(".* shares of .*");
            
            for (int i = 0; i < trades.getRowCount(); i++) {
                VoltTableRow trade_row = trades.fetchRow(i);
                
                long trade_id = trade_row.getLong("T_ID");
                int is_cash = (int)trade_row.getLong("T_IS_CASH");
                String type_name = trade_row.getString("TT_NAME");
                String s_name = trade_row.getString("S_NAME");
                int quantity = (int)trade_row.getLong("T_QTY");
                
                voltQueueSQL(getSettlement, trade_id);
                voltExecuteSQL(); // don't need the result
                
                if (is_cash == 1) {
                    if (num_updated < max_updates) {
                        voltQueueSQL(getCTName, trade_id);
                        VoltTable cash = voltExecuteSQL()[0];
                        
                        assert cash.getRowCount() == 1;
                        String ct_name = cash.fetchRow(0).getString("CT_NAME");
                        Matcher m = p.matcher(ct_name);
                        
                        // ct_name LIKE "% shares of %"
                        if (m.matches()) {
                            ct_name = type_name + " " + quantity + " Shares of " + s_name;
                        }
                        else {
                            ct_name = type_name + " " + quantity + " shares of " + s_name;
                        }
                        
                        voltQueueSQL(updateCTName, ct_name, trade_id);
                        voltExecuteSQL();
                        num_updated++;
                    }
                    
                    voltQueueSQL(getCashTransaction, trade_id);
                    voltExecuteSQL();
                }
                
                voltQueueSQL(getTradeHistory, trade_id);
                voltExecuteSQL();
                
                result.addRow(trade_id);               
            }
        }
        else {
            assert false;
        }
        
        return new VoltTable[] {result};
    }        
}
