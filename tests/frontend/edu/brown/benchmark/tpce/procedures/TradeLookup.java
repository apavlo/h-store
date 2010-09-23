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
 * Trade-Lookup transaction <br/>
 * TPC-E section 3.3.3
 */
public class TradeLookup extends VoltProcedure {

    public final SQLStmt getTrade_frame1 = new SQLStmt(
            "select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, TT_IS_MRKT, T_TRADE_PRICE "
                    + "from TRADE, TRADE_TYPE "
                    + "where T_ID = ? and T_TT_ID = TT_ID");

    public final SQLStmt getSettlement = new SQLStmt(
            "select SE_AMT, SE_CASH_DUE_DATE, SE_CASH_TYPE "
                    + "from SETTLEMENT " + "where SE_T_ID = ?");

    public final SQLStmt getCash = new SQLStmt(
            "select CT_AMT, CT_DTS, CT_NAME from CASH_TRANSACTION where CT_T_ID = ?");

    public final SQLStmt getTradeHistory = new SQLStmt(
            "select TH_DTS, TH_ST_ID from TRADE_HISTORY where TH_T_ID = ? order by TH_DTS");

    // Note: The parameter max_trade should be used for the LIMIT value but
    //       we don't support parameters in the LIMIT clause. Hardcoding to 20 for now.
    public final SQLStmt getTrade_frame2 = new SQLStmt(
            "select T_BID_PRICE, T_EXEC_NAME, T_IS_CASH, T_ID, T_TRADE_PRICE "
                    + "from TRADE "
                    + "where T_CA_ID = ? and T_DTS >= ? and T_DTS <= ? "
                    + "order by T_DTS asc limit 20"); // limit ?

    // Note: The parameter max_trade should be used for the LIMIT value but
    //       we don't support parameters in the LIMIT clause. Hardcoding to 20 for now.
    // Note: We can't use hardcoded strings in lookup against T_ST_ID. Must pass as parameter
    public final SQLStmt getTrade_frame3 = new SQLStmt(
            "select T_CA_ID, T_EXEC_NAME, T_IS_CASH, T_TRADE_PRICE, T_QTY, T_DTS, T_ID, T_TT_ID "
                    + "from TRADE "
                    + "where T_S_SYMB = ? and T_ST_ID = ? and T_DTS >= ? and T_DTS <= ? " // T_ST_ID='CMPT' 
                    + "order by T_DTS asc limit 20"); // limit ?

    public final SQLStmt getTrade_frame4 = new SQLStmt(
            "select T_ID from TRADE " + "where T_CA_ID = ? and T_DTS >= ? "
                    + "order by T_DTS asc limit 1");

    // modified to use join
    public final SQLStmt getHoldingHistory = new SQLStmt(
            "select HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY from "
                    + "HOLDING_HISTORY where HH_H_T_ID = HH_T_ID and HH_T_ID = ?");

    public VoltTable[] run(
            long[] trade_ids,
            long acct_id,
            long max_acct_id,
            long frame_to_execute,
            long max_trades,
            TimestampType end_trade_dts,
            TimestampType start_trade_dts,
            String symbol) throws VoltAbortException {
        Map < String, Object[]> ret = new HashMap < String, Object[]>();

        /** FRAME 1 **/
        if (1 == frame_to_execute) {
            ret.put("t_num_found", new Object[] { max_trades });

            for (int i = 0; i < max_trades; i++) {
                ProcedureUtil.execute(ret, this, getTrade_frame1,
                        new Object[] { trade_ids[i] },
                        new String[] { "bid_price", "exec_name", "is_cash", "is_market", "trade_price" },
                        new Object[] { "T_BID_PRICE", "T_EXEC_NAME", "T_IS_CASH", "TT_IS_MRKT", "T_TRADE_PRICE" });

                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { trade_ids[i] },
                        new String[] { "settlement_amount", "settlement_cash_due_date", "settlement_cash_type" },
                        new Object[] { "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (ret.get("is_cash")[i].equals(TPCEConstants.TRUE)) {
                    ProcedureUtil.execute(ret, this, getCash,
                            new Object[] { trade_ids[i] },
                            new String[] { "cash_transaction_amount", "cash_transaction_dts", "cash_transaction_name" },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });
                }

                ProcedureUtil.execute(ret, this, getTradeHistory,
                        new Object[] { trade_ids[i] },
                        new String[] { "trade_history", "trade_history_status_id" },
                        new Object[] { "TH_DTS", "TH_ST_ID" });
            }

            return ProcedureUtil.mapToTable(ret);
            
        /** FRAME 2 **/
        } else if (2 == frame_to_execute) {

            int row_count = ProcedureUtil.execute(ret, this, getTrade_frame2,
                    new Object[] { acct_id, "CMPT", start_trade_dts, end_trade_dts, max_trades },
                    new String[] { "bid_price", "exec_name", "is_cash", "trade_list", "trade_price" },
                    new Object[] { "T_BID_PRICE", "T_EXEC_NAME", "T_IS_CASH", "T_ID", "T_TRADE_PRICE" });

            ret.put("num_found", new Object[] { row_count });

            for (int i = 0; i < row_count; i++) {

                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { ret.get("trade_list")[i] },
                        new String[] { "settlement_amount", "settlement_cash_due_date", "settlement_cash_type" },
                        new Object[] { "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (!ret.get("is_cash")[i].equals(TPCEConstants.TRUE)) {
                    ProcedureUtil.execute(ret, this, getCash,
                            new Object[] { ret.get("trade_list")[i] },
                            new String[] { "cash_transaction_amount", "cash_transaction_dts", "cash_transaction_name" },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });
                }

                ProcedureUtil.execute(ret, this, getTradeHistory,
                        new Object[] { ret.get("trade_list")[i] },
                        new String[] { "trade_history_dts", "trade_history_status_id" },
                        new Object[] { "TH_DTS", "TH_ST_ID" });
            }

            return ProcedureUtil.mapToTable(ret);
        } else if (3 == frame_to_execute) {

            int row_count = ProcedureUtil.execute(ret, this, getTrade_frame3,
                            new Object[] { symbol, "CMPT", max_trades },
                            new String[] { "acct_id", "exec_name", "is_cash", "price", "quantity", "trade_dts", "trade_list", "trade_type" },
                            new Object[] { "T_CA_ID", "T_EXEC_NAME", "T_IS_CASH", "T_TRADE_PRIDE", "T_QTY", "T_DTS", "T_ID", "T_TT_ID" });

            ret.put("num_found", new Object[] { row_count });

            for (int i = 0; i < row_count; i++) {
                ProcedureUtil.execute(ret, this, getSettlement,
                        new Object[] { ret.get("trade_list")[i] },
                        new String[] { "settlement_amount", "settlement_cash_due_date", "settlement_cash_type" },
                        new Object[] { "SE_AMT", "SE_CASH_DUE_DATE", "SE_CASH_TYPE" });

                if (ret.get("is_cash")[i].equals(TPCEConstants.TRUE)) {
                    ProcedureUtil.execute(ret, this, getCash,
                            new Object[] { ret.get("trade_list")[i] },
                            new String[] { "cash_transaction_amount", "cash_transaction_dts", "cash_transaction_name" },
                            new Object[] { "CT_AMT", "CT_DTS", "CT_NAME" });
                }

                ProcedureUtil.execute(ret, this, getTradeHistory,
                        new Object[] { ret.get("trade_list")[i] },
                        new String[] { "trade_history_dts", "trade_history_status_id" },
                        new Object[] { "TH_DTS", "TH_ST_ID" });
            }

            return ProcedureUtil.mapToTable(ret);
        } else {
            assert (4 == frame_to_execute);

            ProcedureUtil.execute(ret, this, getTrade_frame4,
                    new Object[] { acct_id, start_trade_dts },
                    new String[] { "trade_id" },
                    new Object[] { "T_ID" });

            int row_count = ProcedureUtil.execute(ret, this, getHoldingHistory,
                    new Object[] { ret.get("trade_id")[0] },
                    new String[] { "holding_history_id", "holding_history_trade_id", "quantity_before", "quantity_after" },
                    new Object[] { "HH_H_T_ID", "HH_T_ID", "HH_BEFORE_QTY", "HH_AFTER_QTY" });

            ret.put("num_found", new Object[] { row_count });

            return ProcedureUtil.mapToTable(ret);
        }
    }

}
