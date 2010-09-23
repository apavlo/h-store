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

import java.util.HashMap;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * Trade-Update transaction <br/>
 * TPC-E Section 3.3.4
 */
public class TradeUpdate extends VoltProcedure {

    public final SQLStmt getTrade1 = new SQLStmt(
            "select T_EXEC_NAME from TRADE where T_ID = ?");

    public final SQLStmt updateTrade = new SQLStmt(
            "update TRADE set T_EXEC_NAME = ? where T_ID = ?");

    public final SQLStmt getTradeTradeType = new SQLStmt(
            "select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, TT_IS_MRKT, T_TRADE_PRICE "
                    + "from TRADE, TRADE_TYPE where T_ID = ? and T_TT_ID = TT_ID");

    public final SQLStmt getSettlement = new SQLStmt(
            "select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE from SETTLEMENT where SE_T_ID = ?");

    public final SQLStmt getCashTransaction = new SQLStmt(
            "select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt getTradeHistory = new SQLStmt(
            "select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS");

    // Note: The parameter max_trade should be used for the LIMIT value but
    //       we don't support parameters in the LIMIT clause. Hardcoding to 20 for now.
    // Note: We can't use hardcoded strings in lookup against T_ST_ID. Must pass as parameter
    public final SQLStmt getTrade2 = new SQLStmt(
            "select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE "
                    + "from TRADE "
                    + "where T_CA_ID = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? " // T_ST_ID=\"CMPT\"
                    + "order by T_DTS asc limit 20"); // limit ?
    
    public final SQLStmt getTradeNoSTID = new SQLStmt(
            "select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE " +
            "  from TRADE " +
            " where T_CA_ID = ? and T_DTS >= ? and T_DTS <= ? order by T_DTS asc limit 20");

    public final SQLStmt getCashType = new SQLStmt(
            "select SE_CASH_TYPE from SETTLEMENT where SE_T_ID = ?");

    public final SQLStmt updateSettlement = new SQLStmt(
            "update SETTLEMENT SET SE_CASH_TYPE = ? where SE_T_ID = ?");

    // Note: The parameter max_trade should be used for the LIMIT value but
    //       we don't support parameters in the LIMIT clause. Hardcoding to 20 for now.
    // Note: We can't use hardcoded strings in lookup against T_ST_ID. Must pass as parameter
    public final SQLStmt getTradeTradeTypeSecurity = new SQLStmt(
            "select T_CA_ID, T_EXEC_NAME, T_IS_CASH, T_TRADE_PRICE, T_QTY, TT_NAME, T_DTS, T_ID, "
                    + "T_TT_ID, TT_NAME from TRADE, TRADE_TYPE, SECURITY "
                    + "where T_S_SYMB = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? and " // T_ST_ID=\"CMPT\"
                    + "TT_ID = T_TT_ID and S_SYMB = T_S_SYMB order by T_DTS asc limit 20"); // limit ?

    public final SQLStmt getCTName = new SQLStmt(
            "select CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt updateCTName = new SQLStmt(
            "update CASH_TRANSACTION set CT_NAME = ? where CT_T_ID = ?");

    public final SQLStmt getTradeHistoryAsc = new SQLStmt(
            "select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS asc");

    public VoltTable[] run(
            long trade_ids[],
            long acct_id,
            long max_acct_id, 
            long frame_to_execute,
            long max_trades,
            long max_updates,
            TimestampType end_trade_dts,
            TimestampType start_trade_dts,
            String symbol) throws VoltAbortException {
        Map<String, Object[]> ret = new HashMap<String, Object[]>();

        String ex_name;

        /** FRAME 1 **/
        if (frame_to_execute == 1) {
            // frame 1
            long num_found = max_trades;

            ret.put("num_found", new Object[] { num_found });

            int num_updated = 0;
            for (int i = 0; i < max_trades; i++) {
                if (num_updated < max_updates) {
                    ProcedureUtil.execute(ret, this, getTrade1,
                            new Object[] { trade_ids[i] },
                            new String[] { "ex_name" },
                            new Object[] { "T_EXEC_NAME" });
                    ex_name = (String) ret.get("ex_name")[0];

                    // TODO
                    // if (ex_name like "% x %")
                    // select ex_name = REPLACE(ex_name, " X ", " ");
                    // else
                    // select ex_name = REPLACE(ex_name, " ", " X ");

                    ProcedureUtil.execute(this, updateTrade,
                            new Object[] { ex_name, trade_ids[i] });
                }
                ProcedureUtil.execute(ret, this, getTradeTradeType,
                        new Object[] { trade_ids[i] },
                        new String[] { "bid_price." + i, "exec_name." + i, "is_cash." + i, "is_market." + i, "trade_price." + i },
                        new Object[] { "T_BID_PRICE", "T_EXEC_NAME", "T_IS_CASH", "TT_IS_MRKT", "T_TRADE_PRICE" });
                
                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { trade_ids[i] },
                        new String[] { "settlement_amount." + i, "settlement_cash_due_date." + i, "settlement_cash_type." + i },
                        new Object[] { "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (ret.get("is_cash" + i)[i].equals(TPCEConstants.TRUE)) {
                    ProcedureUtil.execute(ret, this, getCashTransaction,
                            new Object[] { trade_ids[i] },
                            new String[] { "cash_transaction_amount." + i, "cash_transaction_dts." + i, "cash_transaction_name." + i },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });
                }

                ProcedureUtil.execute(ret, this, getTradeHistory,
                        new Object[] { trade_ids[i] },
                        new String[] { "trade_history_dts." + i, "trade_history_status_id." + i },
                        new Object[] { "TH_DTS", "TH_ST_ID" });
            } // FOR

            return ProcedureUtil.mapToTable(ret);
            
        /** FRAME 2 **/
        } else if (2 == frame_to_execute) {
            int row_count = ProcedureUtil.execute(ret, this, getTrade2,
                    new Object[] { acct_id, "CMPT", start_trade_dts, end_trade_dts, max_trades },
                    new String[] { "bid_price", "exec_name", "is_cash",
                            "trade_list", "trade_price" }, new Object[] {
                            "T_BID_PRICE", "T_EXEC_NAME", "T_IS_CASH", "T_ID",
                            "T_TRADE_PRICE" });
            int num_found = row_count;
            int num_updated = 0;

            Object[] trade_list = ret.get("trade_list");
            Object[] is_cash = ret.get("is_cash");

            for (int i = 0; i < num_found; i++) {
                if (num_updated < max_updates) {
                    ProcedureUtil.execute(ret, this, getCashType,
                            new Object[] { trade_list[i] },
                            new String[] { "cash_type" },
                            new Object[] { "SE_CASH_TYPE" });

                    Object cash_type = ret.get("cash_type")[0];

                    if (is_cash[i].equals(TPCEConstants.TRUE)) {
                        if (cash_type.equals("Cash Account"))
                            cash_type = "Cash";
                        else
                            cash_type = "Cash Account";
                    } else {
                        if (cash_type.equals("Margin Account"))
                            cash_type = "Margin";
                        else
                            cash_type = "Margin Account";
                    }

                    ProcedureUtil.execute(this, updateSettlement, new Object[] {
                            cash_type, trade_list[i] });

                    num_updated += row_count;
                }

                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { trade_list[i] }, new String[] {
                                "settlement_amount." + i,
                                "settlement_cash_due_date." + i,
                                "settlement_cash_type." + i }, new Object[] {
                                "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (is_cash[i].equals(TPCEConstants.TRUE))
                    ProcedureUtil.execute(ret, this, getCashTransaction,
                            new Object[] { trade_list[i] }, new String[] {
                                    "cash_transaction_amount." + i,
                                    "cash_transaction_dts." + i,
                                    "cash_transaction_name." + i },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });

                ProcedureUtil.execute(ret, this, getTradeHistory,
                        new Object[] { trade_list[i] }, new String[] {
                                "trade_history_dts." + i,
                                "trade_history_status_id." + i }, new Object[] {
                                "TH_DTS", "TH_ST_ID" });
            }
            return ProcedureUtil.mapToTable(ret);
            
        /** FRAME 3 **/
        } else if (3 == frame_to_execute) {
            int row_count = ProcedureUtil.execute(ret, this, getTradeTradeTypeSecurity,
                    new Object[] { symbol, "CMPT", start_trade_dts, end_trade_dts, max_trades },
                    new String[] { "acct_id", "exec_name", "is_cash", "price", "quantity", "s_name", "trade_dts", "trade_list", "trade_type", "type_name" },
                    new Object[] { "T_CA_ID", "T_EXEC_NAME", "T_IS_CASH", "T_TRADE_PRICE", "T_QTY", "S_NAME", "T_DTS", "T_ID", "T_TT_ID", "TT_NAME" });

            Object[] trade_list = ret.get("trade_list");
            Object[] is_cash = ret.get("is_cash");

            int num_found = row_count;
            int num_updated = 0;
            for (int i = 0; i < num_found; i++) {
                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { trade_list[i] },
                        new String[] { "settlement_amount." + i, "settlement_cash_due_date." + i, "settlement_cash_type." + i },
                        new Object[] { "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (is_cash[i].equals(TPCEConstants.TRUE)) {
                    if (num_updated < max_updates) {
                        ProcedureUtil.execute(ret, this, getCTName,
                                new Object[] { trade_list[i] },
                                new String[] { "ct_name" },
                                new Object[] { "CT_NAME" });

                        // TODO
                        // if (ct_name like "% shares of %")
                        // ct_name = type_name[i] + " " + quantity[i] +
                        // " Shares of " + s_name[i];
                        // else
                        // ct_name = type_name[i] + " " + quantity[i] +
                        // " shares of " + s_name[i];

                        ProcedureUtil.execute(this, updateCTName,
                                new Object[] { " share of ", trade_list[i] });
                    }

                    ProcedureUtil.execute(ret, this, getCashTransaction,
                            new Object[] { trade_list[i] },
                            new String[] { "cash_transaction_amoubt." + i, "cash_transaction_dts." + i, "cash_transaction_name." + i },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });
                }

                ProcedureUtil.execute(ret, this, getTradeHistoryAsc,
                        new Object[] { trade_list[i] }, new String[] {
                                "trade_history_dts." + i,
                                "trade_history_status_id." + i }, new Object[] {
                                "TH_DTS", "TH_ST_ID" });
            }

            return ProcedureUtil.mapToTable(ret);
        }

        throw new RuntimeException("Invalid frame_to_execute: "
                + frame_to_execute);
    }
}
