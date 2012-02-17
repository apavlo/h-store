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

import edu.brown.benchmark.tpce.TPCEClient;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * MarketWatch Transaction <br/>
 * TPC-E Section 3.3.10
 */
public class MarketWatch extends VoltProcedure {

    private static final int BAD_INPUT_DATA = -1;

    // Replacement getStockList1 that uses joins
    public final SQLStmt getStockListCustomer = new SQLStmt("select WI_S_SYMB from WATCH_ITEM, WATCH_LIST " + " where wl_c_id = ? " + "   AND wi_wl_id = wl_id " + " ORDER BY wi_s_symb ASC LIMIT 1");

    // note: between (?, ?) not supported
    public final SQLStmt getStockListIndustry = new SQLStmt("select S_SYMB from INDUSTRY, COMPANY, SECURITY " + "where IN_NAME = ? and CO_IN_ID = IN_ID and S_CO_ID = CO_ID "
            + "and CO_ID >= ? AND CO_ID <= ?");
    // + "and CO_ID between (?, ?)");

    public final SQLStmt getStockListCustomerAccount = new SQLStmt("select HS_S_SYMB from HOLDING_SUMMARY where HS_CA_ID = ?");

    public final SQLStmt getNewPrice = new SQLStmt("select LT_PRICE from LAST_TRADE where LT_S_SYMB = ?");

    public final SQLStmt getNumOut = new SQLStmt("select S_NUM_OUT from SECURITY where S_SYMB = ?");

    public final SQLStmt getOldPrice = new SQLStmt("select DM_CLOSE from DAILY_MARKET where DM_S_SYMB = ? order by DM_DATE desc limit 1");

    public VoltTable[] run(long acct_id, long cust_id, long ending_co_id, long starting_co_id, String industry_name) throws VoltAbortException {
        VoltTable stock_list = null;
        int status = TPCEClient.STATUS_SUCCESS;

        // CUSTOMER ID
        if (cust_id != 0) {
            voltQueueSQL(getStockListCustomer, cust_id);
            stock_list = voltExecuteSQL()[0];

            // INDUSTRY NAME
        } else if (!industry_name.equals("")) {
            // note: between (?, ?) not supported now
            voltQueueSQL(getStockListIndustry, industry_name, starting_co_id, ending_co_id);
            stock_list = voltExecuteSQL()[0];

            // CUSTOMER ACCOUNT ID
        } else if (acct_id != 0) {
            voltQueueSQL(getStockListCustomerAccount, acct_id);
            stock_list = voltExecuteSQL()[0];

            // INVALID
        } else {
            status = BAD_INPUT_DATA;
        }

        double old_mkt_cap = 0.0, new_mkt_cap = 0.0;
        if (status != BAD_INPUT_DATA) {
            boolean check = false;
            for (int i = 0, cnt = stock_list.getRowCount(); i < cnt; i++) {
                final String symbol = stock_list.fetchRow(i).getString(0);
                voltQueueSQL(getNewPrice, symbol);
                VoltTable vt = voltExecuteSQL()[0];
                check = vt.advanceRow();
                assert (check);
                double new_price = vt.getDouble(0);

                voltQueueSQL(getNumOut, symbol);
                vt = voltExecuteSQL()[0];
                check = vt.advanceRow();
                assert (check);
                long s_num_out = vt.getLong(0);

                voltQueueSQL(getOldPrice, symbol);
                vt = voltExecuteSQL()[0];
                check = vt.advanceRow();
                assert (check);
                double old_price = vt.getDouble(0);

                old_mkt_cap += s_num_out * old_price;
                new_mkt_cap += s_num_out * new_price;
            }
            double pct_change = 100 * (new_mkt_cap / old_mkt_cap - 1);

            Map<String, Object[]> ret = new HashMap<String, Object[]>();
            ret.put("pct_change", new Object[] { pct_change });
            ret.put("status", new Object[] { status });

            return ProcedureUtil.mapToTable(ret);
        } else {
            throw new VoltAbortException("rollback MarketWatch");
        }
    }
}
