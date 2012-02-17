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
 * Trade-Order Transaction <br/>
 * TPC-E Section 3.3.1
 */
public class TradeOrder extends VoltProcedure {

    public final SQLStmt getAccount1 = new SQLStmt("select CA_NAME, CA_B_ID, CA_C_ID, CA_TAX_ST from CUSTOMER_ACCOUNT where CA_ID = ?");

    public final SQLStmt getCustomer = new SQLStmt("select C_F_NAME, C_L_NAME, C_TIER, C_TAX_ID from CUSTOMER where C_ID = ?");

    public final SQLStmt getBroker = new SQLStmt("select B_NAME from BROKER where B_ID = ?");

    public final SQLStmt getACL = new SQLStmt("select AP_ACL " + "from ACCOUNT_PERMISSION " + "where AP_CA_ID = ? and AP_F_NAME = ? and AP_L_NAME = ? and AP_TAX_ID = ?");

    public final SQLStmt getCompany = new SQLStmt("select CO_ID from COMPANY where CO_NAME = ?");

    public final SQLStmt getSecurity1 = new SQLStmt("select S_EX_ID, S_NAME, S_SYMB from SECURITY where S_CO_ID = ? and S_ISSUE = ?");

    public final SQLStmt getSecurity2 = new SQLStmt("select S_CO_ID, S_EX_ID, S_NAME from SECURITY where S_SYMB = ?");

    public final SQLStmt getCompany2 = new SQLStmt("select CO_NAME from COMPANY where CO_ID = ?");

    public final SQLStmt getLastTrade = new SQLStmt("select LT_PRICE from LAST_TRADE where LT_S_SYMB = ?");

    public final SQLStmt getTradeType = new SQLStmt("select TT_IS_MRKT, TT_IS_SELL from TRADE_TYPE where TT_ID = ?");

    public final SQLStmt getHoldingSummmary = new SQLStmt("select HS_QTY from HOLDING_SUMMARY where HS_CA_ID = ? and HS_S_SYMB = ?");

    public final SQLStmt getHoldingDesc = new SQLStmt("select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS desc");

    public final SQLStmt getHoldingAsc = new SQLStmt("select H_QTY, H_PRICE from HOLDING where H_CA_ID = ? and H_S_SYMB = ? order by H_DTS asc");

    // modified using join
    public final SQLStmt getTaxrate = new SQLStmt("select sum(TX_RATE) from TAXRATE, CUSTOMER_TAXRATE " + "where TX_ID = CX_TX_ID and CX_C_ID = ?");

    public final SQLStmt getCommissionRate = new SQLStmt("select CR_RATE " + "from COMMISSION_RATE " + "where CR_C_TIER = ? and CR_TT_ID = ? and CR_EX_ID = ? and CR_FROM_QTY <= ? and CR_TO_QTY >= ?");

    public final SQLStmt getCharge = new SQLStmt("select CH_CHRG from CHARGE where CH_C_TIER = ? and CH_TT_ID = ?");

    public final SQLStmt getAccount2 = new SQLStmt("select CA_BAL from CUSTOMER_ACCOUNT where CA_ID = ?");

    // Currently not supported by H-Store
    public final SQLStmt getHoldingAssets = new SQLStmt(
    // Benchmark Spec: SUM(HS_QTY * LT_PRICE)
            "select HS_QTY, LT_PRICE from HOLDING_SUMMARY, LAST_TRADE where HS_CA_ID = ? and LT_S_SYMB = HS_S_SYMB");

    public final SQLStmt insertTrade = new SQLStmt("insert into TRADE(T_ID, T_DTS, T_ST_ID, T_TT_ID, T_IS_CASH, T_S_SYMB, T_QTY, "
            + "T_BID_PRICE, T_CA_ID, T_EXEC_NAME, T_TRADE_PRICE, T_CHRG, T_COMM, T_TAX, T_LIFO) " + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt insertTradeRequest = new SQLStmt("insert into TRADE_REQUEST (TR_T_ID, TR_TT_ID, TR_S_SYMB, TR_QTY, TR_BID_PRICE, TR_CA_ID) " + "values (?, ?, ?, ?, ?, ?)");

    public final SQLStmt insertTradeHistory = new SQLStmt("insert into TRADE_HISTORY(TH_T_ID, TH_DTS, TH_ST_ID) values (?, ?, ?)");

    public VoltTable[] run(double requested_price, long acct_id, long is_lifo, long roll_it_back, long trade_qty, long type_is_margin, String co_name, String exec_f_name, String exec_l_name,
            String exec_tax_id, String issue, String st_pending_id, String st_submitted_id, String symbol, String trade_type_id) throws VoltAbortException {
        Map<String, Object[]> ret = new HashMap<String, Object[]>();

        // frame 1
        ProcedureUtil.execute(ret, this, getAccount1, new Object[] { acct_id }, new String[] { "acct_name", "broker_id", "cust_id", "tax_status" }, new Object[] { "CA_NAME", "CA_B_ID", "CA_C_ID",
                "CA_TAX_ST" });

        assert (ret.containsKey("cust_id"));
        assert (ret.get("cust_id").length > 0);
        System.out.println("cust_id: " + ret.get("cust_id").length);
        ProcedureUtil.execute(ret, this, getCustomer, new Object[] { ret.get("cust_id")[0] }, new String[] { "cust_f_name", "cust_l_name", "cust_tier", "tax_id" }, new Object[] { "C_F_NAME",
                "C_L_NAME", "C_TIER", "C_TAX_ID" });

        assert (ret.containsKey("broker_id"));
        ProcedureUtil.execute(ret, this, getBroker, new Object[] { ret.get("broker_id")[0] }, new String[] { "broker_name" }, new Object[] { "B_NAME" });

        // frame 2
        if (!exec_l_name.equals(ret.get("cust_l_name")[0]) || !exec_f_name.equals(ret.get("cust_f_name")[0]) || !exec_tax_id.equals(ret.get("tax_id")[0])) {

            ProcedureUtil.execute(ret, this, getACL, new Object[] { acct_id, exec_f_name, exec_l_name, exec_tax_id }, new String[] { "ap_acl" }, new Object[] { "AP_ACL" });

            if (ret.get("ap_acl")[0] == null)
                throw new VoltAbortException("ap_acl is null");
        }

        // frame 3
        if (symbol.equals("")) {
            ProcedureUtil.execute(ret, this, getCompany, new Object[] { co_name }, new String[] { "co_id" }, new Object[] { "CO_ID" });

            ProcedureUtil.execute(ret, this, getSecurity1, new Object[] { ret.get("co_id")[0], issue }, new String[] { "exch_id", "s_name", "symbol" }, new Object[] { "S_EX_ID", "S_NAME", "S_SYMB" });
        } else {
            ProcedureUtil.execute(ret, this, getSecurity2, new Object[] { symbol }, new String[] { "co_id", "exch_id", "s_name" }, new Object[] { "S_CO_ID", "S_EX_ID", "S_NAME" });
            ProcedureUtil.execute(ret, this, getCompany2, new Object[] { ret.get("co_id")[0] }, new String[] { "co_name" }, new Object[] { "CO_NAME" });
        }

        ProcedureUtil.execute(ret, this, getLastTrade, new Object[] { symbol }, new String[] { "market_price" }, new Object[] { "LT_PRICE" });

        ProcedureUtil.execute(ret, this, getTradeType, new Object[] { trade_type_id }, new String[] { "type_is_market", "type_is_sell" }, new Object[] { "TT_IS_MRKT", "TT_IS_SELL" });

        if (ret.get("type_is_market")[0].equals(TPCEConstants.TRUE)) {
            requested_price = (Double) ret.get("market_price")[0];
        }

        double buy_value = 0;
        double sell_value = 0;
        long needed_qty = trade_qty;

        ProcedureUtil.execute(ret, this, getHoldingSummmary, new Object[] { acct_id, symbol }, new String[] { "hs_qty" }, new Object[] { "HS_QTY" });

        if (ret.get("type_is_sell")[0].equals(TPCEConstants.TRUE)) {
            if ((Integer) ret.get("hs_qty")[0] > 0) {
                VoltTable hold_list = null;
                if (is_lifo == TPCEConstants.TRUE) {
                    voltQueueSQL(getHoldingDesc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                } else {
                    voltQueueSQL(getHoldingAsc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                }

                for (int i = 0; i < hold_list.getRowCount() && needed_qty != 0; i++) {
                    int hold_qty = (int) hold_list.fetchRow(i).getLong("H_QTY");
                    double hold_price = hold_list.fetchRow(i).getDouble("H_PRICE");

                    if (hold_qty > needed_qty) {
                        buy_value += needed_qty * hold_price;
                        sell_value += needed_qty * requested_price;
                        needed_qty = 0;
                    } else {
                        buy_value += hold_qty * hold_price;
                        sell_value += hold_qty * requested_price;
                        needed_qty = needed_qty - hold_qty;
                    }
                }
            }
        } else {
            if ((Integer) ret.get("hs_qty")[0] < 0) {
                VoltTable hold_list = null;
                if (is_lifo == TPCEConstants.TRUE) {
                    voltQueueSQL(getHoldingDesc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                } else {
                    voltQueueSQL(getHoldingAsc, acct_id, symbol);
                    hold_list = voltExecuteSQL()[0];
                }

                for (int i = 0; i < hold_list.getRowCount() && needed_qty != 0; i++) {
                    int hold_qty = (int) hold_list.fetchRow(i).getLong("H_QTY");
                    double hold_price = hold_list.fetchRow(i).getDouble("H_PRICE");
                    if (hold_qty + needed_qty < 0) {
                        sell_value += needed_qty * hold_price;
                        buy_value += needed_qty * requested_price;
                        needed_qty = 0;
                    } else {
                        hold_qty = 0 - hold_qty;
                        sell_value += hold_qty * hold_price;
                        buy_value += hold_qty * requested_price;
                        needed_qty = needed_qty - hold_qty;
                    }
                }
            }
        }

        ret.put("buy_value", new Object[] { buy_value });
        ret.put("sell_value", new Object[] { sell_value });

        double tax_amount = 0;
        long tax_status = 1; // FIXME

        if (sell_value > buy_value && (tax_status == 1 || tax_status == 2)) {
            ProcedureUtil.execute(ret, this, getTaxrate, new Object[] { ret.get("cust_id")[0] }, new String[] { "tax_rates" }, new Object[] { 0 });
            tax_amount = (sell_value - buy_value) * (Double) ret.get("tax_rates")[0];
            ret.put("tax_amount", new Object[] { tax_amount });
        }

        ProcedureUtil.execute(ret, this, getCommissionRate, new Object[] { ret.get("cust_tier")[0], trade_type_id, ret.get("exch_id")[0], trade_qty, trade_qty }, new String[] { "comm_rate" },
                new Object[] { "CR_RATE" });

        ProcedureUtil.execute(ret, this, getCharge, new Object[] { ret.get("cust_tier")[0], trade_type_id }, new String[] { "charge_amount" }, new Object[] { "CH_CHRG" });

        double cust_assets = 0;
        if (type_is_margin == TPCEConstants.TRUE) {
            ProcedureUtil.execute(ret, this, getAccount2, new Object[] { acct_id }, new String[] { "acct_bal" }, new Object[] { "CA_BAL" });

            // TPCEClient.execute(ret, this, getHoldingAssets,
            // new Object[] { acct_id }, new String[] { "hold_assets" },
            // new Object[] { 0 });

            if (ret.get("hold_assets")[0] == null)
                cust_assets = (Double) ret.get("acct_bal")[0];
            else
                cust_assets = (Double) ret.get("hold_assets")[0] + (Double) ret.get("acct_bal")[0];

            ret.put("cust_assets", new Object[] { cust_assets });
        }

        ret.put("status_id", new Object[] { ret.get("type_is_market")[0].equals(TPCEConstants.TRUE) ? st_submitted_id : st_pending_id });

        // frame 4
        double comm_amount = (Double) ret.get("comm_rate")[0] / (double) 100 * trade_qty * requested_price;
        String exec_name = exec_f_name + " " + exec_l_name;
        int is_cash = (type_is_margin == TPCEConstants.TRUE ? TPCEConstants.FALSE : TPCEConstants.TRUE);
        double charge_amount = 0.0d; // FIXME

        Date now_dts = Calendar.getInstance().getTime();
        long trade_id = 0;
        ProcedureUtil.execute(this, insertTrade, new Object[] { trade_id, now_dts, ret.get("status_id")[0], trade_type_id, is_cash, symbol, trade_qty, requested_price, acct_id, exec_name, null,
                charge_amount, comm_amount, is_lifo });

        if (ret.get("type_is_market")[0].equals(TPCEConstants.FALSE)) {
            ProcedureUtil.execute(this, insertTradeRequest, new Object[] { trade_id, trade_type_id, symbol, trade_qty, requested_price, acct_id });
        }

        ProcedureUtil.execute(this, insertTradeHistory, new Object[] { trade_id, now_dts, ret.get("status_id")[0] });

        // frame 5
        if (roll_it_back == TPCEConstants.TRUE) {
            throw new VoltAbortException("roll_it_back is true");
        }

        // frame 6

        // TODO
        // if (ret.get("type_is_market")[0] == Constants.TRUE){
        // eAction = eMEEProcessOrder;
        // } else {
        // eAction = eMEESetLimitOrderTrigger;
        // }
        // send_to_market(requested_price, symbol, trade_id, trade_qty,
        // trade_type_id, eAction);

        return ProcedureUtil.mapToTable(ret);
    }
}
