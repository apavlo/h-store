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

import java.util.HashSet;
import java.util.Set;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/**
 * MarketWatch Transaction <br/>
 * TPC-E Section 3.3.10
 * 
 * H-Store quirks:
 *   1) getStockListCustomer is modified from "IN (sub-query)" to a join between WATCH_LIST and WATCH_ITEM.
 *   2) getStockListCustomer should contain "distinct" in the SELECT clause. Not supported. We perform it via a Java set.
 *   3) The original getStockListIndustry uses between for CO_ID. We use '>=' and '<=' instead. Between is not supported.
 */
public class MarketWatch extends VoltProcedure {

    public final SQLStmt getStockListCustomer = new SQLStmt("select WI_S_SYMB from WATCH_ITEM, WATCH_LIST " +
            "where WL_C_ID = ? and WI_WL_ID = WL_ID");

    public final SQLStmt getStockListIndustry = new SQLStmt("select S_SYMB from INDUSTRY, COMPANY, SECURITY " +
            "where IN_NAME = ? and CO_IN_ID = IN_ID and S_CO_ID = CO_ID and CO_ID >= ? AND CO_ID <= ?" );

    public final SQLStmt getStockListCustomerAccount = new SQLStmt("select HS_S_SYMB from HOLDING_SUMMARY where HS_CA_ID = ?");

    public final SQLStmt getNewPrice = new SQLStmt("select LT_PRICE from LAST_TRADE where LT_S_SYMB = ?");

    public final SQLStmt getNumOut = new SQLStmt("select S_NUM_OUT from SECURITY where S_SYMB = ?");

    public final SQLStmt getOldPrice = new SQLStmt("select DM_CLOSE from DAILY_MARKET where DM_S_SYMB = ? order by DM_DATE desc limit 1");

    public VoltTable[] run(long acct_id, long cust_id, long ending_co_id, long starting_co_id, String industry_name) throws VoltAbortException {
        
        // first, fetch security symbols
        VoltTable stock_list = null;
        if (cust_id != 0) {
            voltQueueSQL(getStockListCustomer, cust_id);
            stock_list = voltExecuteSQL()[0];
        }
        else if (!industry_name.equals("")) {
            voltQueueSQL(getStockListIndustry, industry_name, starting_co_id, ending_co_id);
            stock_list = voltExecuteSQL()[0];
        }
        else if (acct_id != 0) {
            voltQueueSQL(getStockListCustomerAccount, acct_id);
            stock_list = voltExecuteSQL()[0];
        }
        else {
            throw new VoltAbortException("Bad input data (intentional) in the Market-Watch transaction");
        }

        
        double old_mkt_cap = 0.0, new_mkt_cap = 0.0;
        Set<String> watch_symbols = new HashSet<String>(); // for 'distinct' if symbols are from watch lists
        
        for (int i = 0; i < stock_list.getRowCount(); i++) {
            String symbol = stock_list.fetchRow(i).getString(0);
            
            // if we had gotten symbols through watch lists, we have to do manual 'distinct'
            if (cust_id != 0) {
                if (watch_symbols.contains(symbol)) {
                    continue;
                }
                else {
                    watch_symbols.add(symbol);
                }
            }
            
            voltQueueSQL(getNewPrice, symbol);
            VoltTable vt = voltExecuteSQL()[0];
            
            assert vt.getRowCount() == 1; 
            double new_price = vt.fetchRow(0).getDouble("LT_PRICE");

            voltQueueSQL(getNumOut, symbol);
            vt = voltExecuteSQL()[0];
            
            assert vt.getRowCount() == 1; 
            long s_num_out = vt.fetchRow(0).getLong("S_NUM_OUT");
            

            voltQueueSQL(getOldPrice, symbol);
            vt = voltExecuteSQL()[0];
            
            assert vt.getRowCount() == 1; 
            double old_price = vt.fetchRow(0).getDouble("DM_CLOSE");

            old_mkt_cap += s_num_out * old_price;
            new_mkt_cap += s_num_out * new_price;
        }
        
        double pct_change = 100 * (new_mkt_cap / old_mkt_cap - 1);

        VoltTable res = new VoltTable(new VoltTable.ColumnInfo("pct_change", VoltType.FLOAT));
        res.addRow(pct_change);
        
        return new VoltTable[] {res};
    }
}
