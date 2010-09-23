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

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * TradeResult Transaction <br/>
 * TPC-E Section 3.3.2b
 */
public class TradeResult extends VoltProcedure {

    public final SQLStmt getTrade = new SQLStmt(
            "select T_CA_ID, T_TT_ID, T_S_SYMB, T_QTY, T_CHRG, T_LIFO, T_IS_CASH from TRADE where T_ID = ?");

    public final SQLStmt getTradeType = new SQLStmt(
            "select TT_NAME, TT_IS_SELL, TT_IS_MRKT from TRADE_TYPE where TT_ID = ?");

    public final SQLStmt getHoldingSummary = new SQLStmt(
            "select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = ? and HS_S_SYMB = ?");

    public final SQLStmt getCustomerAccount = new SQLStmt(
            "select CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?");

    public final SQLStmt getCustomerAccountBalance = new SQLStmt(
            "select ca_bal from CUSTOMER_ACCOUNT where CA_ID = ?");
    
    public final SQLStmt insertHoldingSummary = new SQLStmt(
            "insert into HOLDING_SUMMARY(HS_CA_ID, HS_S_SYMB, HS_QTY) values (?, ?, ?)");

    public final SQLStmt updateHoldingSummary = new SQLStmt(
            "update HOLDING_SUMMARY set HS_QTY = ? where HS_CA_ID = ? and HS_S_SYMB = ?");
    
    public final SQLStmt updateHolding = new SQLStmt(
            "UPDATE holding SET h_qty = ? WHERE h_t_id = ?");

    public final SQLStmt getHoldingDesc = new SQLStmt(
            "select H_T_ID, H_QTY, H_PRICE " + "from HOLDING "
                    + "where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc");

    public final SQLStmt getHoldingAsc = new SQLStmt(
            "select H_T_ID, H_QTY, H_PRICE " + "from HOLDING "
                    + "where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc");

    public final SQLStmt insertHoldingHistory = new SQLStmt(
            "insert into HOLDING_HISTORY (HH_H_T_ID, HH_T_ID, HH_BEFORE_QTY, HH_AFTER_QTY) "
                    + "values (?, ?, ?, ?)");

    public final SQLStmt insertHolding = new SQLStmt(
            "insert into HOLDING (H_T_ID, H_CA_ID, H_S_SYMB, H_DTS, H_PRICE, H_QTY) "
                    + "values (?, ?, ?, ?, ?, ?)");

    public final SQLStmt deleteHoldingSummary = new SQLStmt(
            "delete from HOLDING_SUMMARY where HS_CA_ID = ? and HS_S_SYMB = ?");

    public final SQLStmt deleteHolding = new SQLStmt(
            "DELETE FROM holding WHERE h_t_id = ?");
    
    public final SQLStmt getTaxrate = new SQLStmt(
            "select sum(TX_RATE) from TAXRATE, CUSTOMER_TAXRATE "
                    + "where TX_ID = CX_TX_ID and CX_TX_ID = ?");

    public final SQLStmt updateTrade1 = new SQLStmt(
            "update TRADE set T_TAX = ? where T_ID = ?");

    public final SQLStmt getSecurity = new SQLStmt(
            "select S_EX_ID, S_NAME from SECURITY where S_SYMB = ?");

    public final SQLStmt getCustomer = new SQLStmt(
            "select C_TIER from CUSTOMER where C_ID = ?");

    public final SQLStmt getCommissionRate = new SQLStmt(
            "select CR_RATE from COMMISSION_RATE " + "where CR_C_TIER = ? and "
                    + "CR_TT_ID = ? and " + "CR_EX_ID = ? and "
                    + "CR_FROM_QTY <= ? and " + "CR_TO_QTY >= ? " + "limit 1");

    public final SQLStmt updateTrade2 = new SQLStmt(
            "update TRADE set T_COMM = ?, T_DTS = ?, T_ST_ID = ?, T_TRADE_PRICE = ? where T_ID = ?");

    public final SQLStmt insertTradeHistory = new SQLStmt(
            "insert into TRADE_HISTORY (TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)");

    public final SQLStmt updateBroker = new SQLStmt(
            "update BROKER set B_COMM_TOTAL = B_COMM_TOTAL + ?, B_NUM_TRADES = B_NUM_TRADES + 1 where B_ID = ?");

    public final SQLStmt insertSettlement = new SQLStmt(
            "insert into SETTLEMENT (SE_T_ID, SE_CASH_TYPE, SE_CASH_DUE_DATE, SE_AMT) values (?, ?, ?, ?)");

    public final SQLStmt updateCustomerAccount = new SQLStmt(
            "update CUSTOMER_ACCOUNT set CA_BAL=CA_BAL + ? where CA_ID = ?");

    public final SQLStmt insertCashTransaction = new SQLStmt(
            "insert into CASH_TRANSACTION (CT_DTS, CT_T_ID, CT_AMT, CT_NAME) values (?, ?, ?, ?)");

    public VoltTable[] run(
            long trade_id,
            float trade_price,
            String st_completed_id) throws VoltAbortException {
        // frame 1
        Map<String, Object[]> ret = new HashMap<String, Object[]>();

        ProcedureUtil.execute(ret, this, getTrade,
                new Object[] { trade_id },
                new String[] { "acct_id", "type_id", "symbol", "trade_qty",
                        "charge", "is_lifo", "trade_is_cash" }, new Object[] {
                        "T_CA_ID", "T_TT_ID", "T_S_SYMB", "T_QTY", "T_CHRG",
                        "T_LIFO", "T_IS_CASH" });

        Object acct_id = ret.get("acct_id")[0];

        ProcedureUtil.execute(ret, this, getTradeType, new Object[] { ret
                .get("type_id")[0] }, new String[] { "type_name",
                "type_is_sell", "type_is_market" }, new Object[] { "TT_NAME",
                "TT_IS_SELL", "TT_IS_MRKT" });

        Object symbol = ret.get("symbol")[0];
        Object trade_qty = ret.get("trade_qty")[0];

        ProcedureUtil.execute(ret, this, getHoldingSummary, new Object[] {
                acct_id, symbol }, new String[] { "hs_qty" },
                new Object[] { "HS_QTY" });

        Integer hs_qty = (Integer) ret.get("hs_qty")[0];

        if (hs_qty == null)
            ret.put("hs_qty", new Object[] { 0 });

        // frame 2
        long hold_id, hold_qty, needed_qty;
        double hold_price;
        Date trade_dts = Calendar.getInstance().getTime();

        double buy_value = 0, sell_value = 0;
        needed_qty = (Long) trade_qty;

        ProcedureUtil.execute(ret, this, getCustomerAccount,
                new Object[] { acct_id }, new String[] { "broker_id",
                        "cust_id", "tax_status" }, new Object[] { "CA_B_ID",
                        "CA_C_ID", "CA_TAX_ST" });

        long cust_id = (Long) ret.get("cust_id")[0];

        Integer is_lifo = (Integer) ret.get("is_lifo")[0];

        if (ret.get("type_is_sell")[0].equals(TPCEConstants.TRUE)) {
            if (hs_qty.equals(0)) {
                ProcedureUtil.execute(this, insertHoldingSummary, new Object[] {
                        acct_id, symbol, -(Integer) trade_qty });
            } else {
                if (!hs_qty.equals(trade_qty)) {
                    ProcedureUtil.execute(this, updateHoldingSummary,
                            new Object[] {
                                    (Integer) hs_qty - (Integer) trade_qty,
                                    acct_id, symbol });
                }

                if (hs_qty > 0) {

                    VoltTable hold_list = null;

                    if (is_lifo.equals(TPCEConstants.TRUE)) {
                        voltQueueSQL(getHoldingDesc, acct_id, symbol);
                        hold_list = voltExecuteSQL()[0];
                    } else {
                        voltQueueSQL(getHoldingAsc, acct_id, symbol);
                        hold_list = voltExecuteSQL()[0];
                    }

                    for (int i = 0; i < hold_list.getRowCount()
                            && needed_qty != 0; i++) {
                        hold_id = hold_list.fetchRow(i).getLong("H_T_ID");
                        hold_qty = hold_list.fetchRow(i).getLong("H_QTY");
                        hold_price = hold_list.fetchRow(i).getDouble("H_PRICE");

                        if (hold_qty > needed_qty) {
                            ProcedureUtil.execute(this, insertHoldingHistory,
                                    new Object[] { hold_id, trade_id, hold_qty,
                                            hold_qty - needed_qty });
                            buy_value += needed_qty * hold_price;
                            sell_value += needed_qty * trade_price;
                            needed_qty = 0;
                        } else {
                            ProcedureUtil.execute(this, insertHoldingHistory,
                                    new Object[] { hold_id, trade_id, hold_qty,
                                            0 });
                            buy_value += hold_qty * hold_price;
                            sell_value += hold_qty * trade_price;
                            needed_qty = needed_qty - hold_qty;
                        }
                    }
                }

                if (needed_qty > 0) {
                    ProcedureUtil
                            .execute(this, insertHoldingHistory, new Object[] {
                                    trade_id, trade_id, 0, -needed_qty });

                    ProcedureUtil.execute(this, insertHolding,
                            new Object[] {trade_id, acct_id, symbol, trade_dts, trade_price, -needed_qty });
                } else {
                    if (hs_qty.equals(trade_qty)) {
                        ProcedureUtil.execute(this, deleteHoldingSummary,
                                new Object[] { acct_id, symbol });
                    }
                }
            }
        } else {
            if (hs_qty.equals(0)) {
                ProcedureUtil.execute(this, insertHoldingSummary, new Object[] {
                        acct_id, symbol, trade_qty });
            } else if (!trade_qty.equals(-hs_qty)) {
                ProcedureUtil.execute(this, updateHoldingSummary, new Object[] {
                        hs_qty + (Integer) trade_qty, acct_id, symbol });
            }

            if (hs_qty < 0) {
                VoltTable hold_list;
                if (is_lifo.equals(TPCEConstants.TRUE)) {
                    voltQueueSQL(getHoldingDesc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                } else {
                    voltQueueSQL(getHoldingAsc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                }

                for (int i = 0; i < hold_list.getRowCount() && needed_qty != 0; i++) {
                    hold_id = hold_list.fetchRow(i).getLong("H_T_ID");
                    hold_qty = hold_list.fetchRow(i).getLong("H_QTY");
                    hold_price = hold_list.fetchRow(i).getDouble("H_PRICE");

                    if (hold_qty + needed_qty < 0) {
                        ProcedureUtil.execute(this, insertHoldingHistory,
                                new Object[] { hold_id, trade_id, hold_qty,
                                        hold_qty + needed_qty });
                        sell_value += needed_qty * hold_price;
                        buy_value += needed_qty * trade_price;
                        needed_qty = 0;
                    } else {
                        ProcedureUtil
                                .execute(this, insertHoldingHistory,
                                        new Object[] { hold_id, trade_id,
                                                hold_qty, 0 });
                        hold_qty = -hold_qty;
                        sell_value += hold_qty * hold_price;
                        buy_value += hold_qty * trade_price;
                        needed_qty = needed_qty - hold_qty;
                    }
                }

                if (needed_qty > 0) {
                    ProcedureUtil.execute(this, insertHoldingHistory,
                            new Object[] { trade_id, trade_id, 0, needed_qty });
                    ProcedureUtil.execute(this, insertHolding, new Object[] {
                            trade_id, acct_id, symbol, trade_dts, trade_price,
                            needed_qty });
                } else if (trade_qty.equals(-hs_qty)) {
                    ProcedureUtil.execute(this, deleteHoldingSummary,
                            new Object[] { acct_id, symbol });
                }
            }
        }

        // frame 3
        double tax_amount = 0;
        Integer tax_status = (Integer) ret.get("tax_status")[0];
        if ((tax_status.equals(1) || tax_status.equals(2))
                && sell_value > buy_value) {
            ProcedureUtil.execute(ret, this, getTaxrate, new Object[] { cust_id },
                    new String[] { "tax_rates" }, new Object[] { 0 });
            tax_amount = (sell_value - buy_value)
                    * (Double) ret.get("tax_rates")[0];
            ProcedureUtil.execute(this, updateTrade1, new Object[] { trade_id });
        }

        // frame 4
        ProcedureUtil.execute(ret, this, getSecurity, new Object[] { symbol },
                new String[] { "s_ex_id", "s_name" }, new Object[] { "S_EX_ID",
                        "S_NAME" });
        ProcedureUtil.execute(ret, this, getCustomer, new Object[] { cust_id },
                new String[] { "c_tier" }, new Object[] { "C_TIER" });
        ProcedureUtil.execute(ret, this, getCommissionRate, new Object[] {
                ret.get("c_tier")[0], ret.get("type_id")[0],
                ret.get("s_ex_id")[0], trade_qty, trade_qty },
                new String[] { "comm_rate" }, new Object[] { "CR_RATE" });

        // frame 5
        double comm_amount = (Double) (ret.get("comm_rate")[0]) / 100
                * (Integer) trade_qty * (double) trade_price;
        ProcedureUtil.execute(this, updateTrade2, new Object[] { comm_amount, trade_dts, st_completed_id, trade_price });
        ProcedureUtil.execute(this, insertTradeHistory, new Object[] { trade_id,
                trade_dts, st_completed_id });
        ProcedureUtil.execute(this, updateBroker, new Object[] { comm_amount,
                ret.get("broker_id")[0] });

        // frame 6
        Calendar cal = Calendar.getInstance();
        cal.setTime(trade_dts);
        cal.add(Calendar.DAY_OF_YEAR, 2);
        Date due_date = cal.getTime();

        double se_amount;
        double charge = (Double) ret.get("charge")[0];
        if (ret.get("type_is_sell")[0].equals(TPCEConstants.TRUE)) {
            se_amount = ((Integer) trade_qty * trade_price) - charge
                    - comm_amount;
        } else {
            se_amount = -(((Integer) trade_qty * trade_price) + charge + comm_amount);
        }

        if (tax_status.equals(1))
            se_amount = se_amount - tax_amount;

        String cash_type;
        Integer trade_is_cash = (Integer) ret.get("trade_is_cash")[0];
        if (trade_is_cash.equals(TPCEConstants.TRUE)) {
            cash_type = "Cash Account";
        } else {
            cash_type = "Margin";
        }

        ProcedureUtil.execute(this, insertSettlement, new Object[] { trade_id,
                cash_type, due_date, se_amount });

        if (trade_is_cash.equals(TPCEConstants.TRUE)) {
            ProcedureUtil.execute(this, updateCustomerAccount, new Object[] {
                    se_amount, acct_id });
            ProcedureUtil.execute(this, insertCashTransaction, new Object[] {
                    trade_dts,
                    trade_id,
                    se_amount,
                    ret.get("type_name")[0] + " " + trade_qty + " shares of "
                            + ret.get("s_name")[0] });
        }

        ProcedureUtil.execute(ret, this, getCustomerAccount,
                new Object[] { acct_id }, new String[] { "acct_bal" },
                new Object[] { "CA_BAL" });

        return ProcedureUtil.mapToTable(ret);
    }
}
