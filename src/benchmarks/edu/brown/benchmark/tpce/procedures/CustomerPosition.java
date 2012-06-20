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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

/**
 * Customer Position Transaction <br/>
 * TPC-E Section 3.3.6
 * 
 * H-Store specific quirks:
 *   1) getAssets is severely reduced and most of the code is moved to Java. Basically, we cannot do SUM(X * Y) and that
 *      ruins GROUP BY, ORDER BY and LIMIT parts too (see the specification for the original statement).
 *   2) Getting trade history in the second frame is split into two SQL statements. That is because H-Store does not
 *      support sub-queries in from clause. LIMIT 30 is impossible in this case also, since we do not know how many rows is
 *      returned for each trade.
 */
public class CustomerPosition extends VoltProcedure {
    private final static VoltTable ret_cust_template = new VoltTable(
            new VoltTable.ColumnInfo("c_st_id", VoltType.STRING),
            new VoltTable.ColumnInfo("c_l_name", VoltType.STRING),
            new VoltTable.ColumnInfo("c_f_name", VoltType.STRING),
            new VoltTable.ColumnInfo("c_m_name", VoltType.STRING),
            new VoltTable.ColumnInfo("c_gndr", VoltType.STRING),
            new VoltTable.ColumnInfo("c_tier", VoltType.INTEGER),
            new VoltTable.ColumnInfo("c_dob", VoltType.TIMESTAMP),
            new VoltTable.ColumnInfo("c_ad_id", VoltType.BIGINT),
            new VoltTable.ColumnInfo("c_ctry_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_area_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_local_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_ext_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_ctry_2", VoltType.STRING),
            new VoltTable.ColumnInfo("c_area_2", VoltType.STRING),
            new VoltTable.ColumnInfo("c_local_2", VoltType.STRING),
            new VoltTable.ColumnInfo("c_ext_2", VoltType.STRING),
            new VoltTable.ColumnInfo("c_ctry_3", VoltType.STRING),
            new VoltTable.ColumnInfo("c_area_3", VoltType.STRING),
            new VoltTable.ColumnInfo("c_local_3", VoltType.STRING),
            new VoltTable.ColumnInfo("c_ext_3", VoltType.STRING),
            new VoltTable.ColumnInfo("c_email_1", VoltType.STRING),
            new VoltTable.ColumnInfo("c_email_2", VoltType.STRING)
    );
    private final static VoltTable ret_acct_template = new VoltTable(
            new VoltTable.ColumnInfo("acct_id", VoltType.BIGINT),
            new VoltTable.ColumnInfo("asset_total", VoltType.FLOAT),
            new VoltTable.ColumnInfo("cash_bal", VoltType.FLOAT)
    );

    private static final int MAX_ACCT_LEN = 10;

    public final SQLStmt getCID = new SQLStmt("select C_ID from CUSTOMER where C_TAX_ID = ?");

    public final SQLStmt getCustomer = new SQLStmt("select C_ST_ID, C_L_NAME, C_F_NAME, C_M_NAME, C_GNDR, C_TIER, C_DOB, C_AD_ID, "
            + "C_CTRY_1, C_AREA_1, C_LOCAL_1, C_EXT_1, C_CTRY_2, C_AREA_2, C_LOCAL_2, C_EXT_2, "
            + "C_CTRY_3, C_AREA_3, C_LOCAL_3, C_EXT_3, C_EMAIL_1, C_EMAIL_2  from CUSTOMER where C_ID = ?");

    public final SQLStmt getAssets = new SQLStmt("select CA_ID, CA_BAL, HS_QTY * LT_PRICE " +
            "from CUSTOMER_ACCOUNT left outer join HOLDING_SUMMARY on HS_CA_ID = CA_ID, LAST_TRADE " +
            "where CA_C_ID = ? and LT_S_SYMB = HS_S_SYMB");

    public final SQLStmt getTrades = new SQLStmt("select T_ID from TRADE where T_CA_ID = ? order by T_DTS desc limit 10");

    public final SQLStmt getTradeHistory = new SQLStmt("select T_ID, T_S_SYMB, T_QTY, ST_NAME, TH_DTS " +
            "from TRADE, TRADE_HISTORY, STATUS_TYPE where T_ID = ? and TH_T_ID = T_ID and ST_ID = TH_ST_ID " +
            "order by TH_DTS desc");

    public VoltTable[] run(long acct_id_idx, long cust_id, long get_history, String tax_id) throws VoltAbortException {
        /** FRAME 1 **/
        // Use the tax_id to get the cust_id
        if (cust_id == 0) {
            voltQueueSQL(getCID, tax_id);
            VoltTable cust = voltExecuteSQL()[0];
            
            assert cust.getRowCount() == 1;
            cust_id = cust.fetchRow(0).getLong("C_ID");
        }
        
        voltQueueSQL(getCustomer, cust_id);
        VoltTable cust = voltExecuteSQL()[0];
        
        assert cust.getRowCount() == 1;
        VoltTableRow customer = cust.fetchRow(0);
        
        voltQueueSQL(getAssets, cust_id);
        VoltTable assets = voltExecuteSQL()[0];
        
        /*
         * Here goes the code that should have gone to the SQL part, but could not because of H-Store limitations.
         * Probably not the most efficient way to do this. Especially sorting. Oh, well...
         */
        Map<Long, Double> cust_bal = new HashMap<Long, Double>();
        Map<Long, Double> cust_holds = new HashMap<Long, Double>();
        
        for (int i = 0; i < assets.getRowCount(); i++) {
            VoltTableRow asset  = assets.fetchRow(i);
            
            long acct_id = asset.getLong("CA_ID");
            double cash_bal = asset.getDouble("CA_BAL");
            double hold_asset = asset.getDouble(2);
            
            // might be null, if no holdings for the account
            if (assets.wasNull()) {
                hold_asset = 0;
            }
            
            if (!cust_bal.containsKey(acct_id)) {
                cust_bal.put(acct_id, cash_bal);
                cust_holds.put(acct_id, hold_asset);
            }
            else {
                double prev_asset = cust_holds.get(acct_id);
                cust_holds.put(acct_id, prev_asset + hold_asset);
            }
        }
        
        // have to sort cust_holds according to prices
        List<Entry<Long, Double>> cust_holds_list = new ArrayList<Entry<Long, Double>>(cust_holds.entrySet());
        Collections.sort(cust_holds_list, new Comparator<Entry<Long, Double>>() {
            public int compare(Entry<Long, Double> e1, Entry<Long, Double> e2) {
                return e1.getValue().compareTo(e2.getValue());
            }
        });
        
        assert cust_holds_list.size() <= MAX_ACCT_LEN;
        
        /** FRAME 2 **/
        if (get_history == 1) {
            long acct_id = cust_holds_list.get((int)acct_id_idx).getKey();
            voltQueueSQL(getTrades, acct_id);
            VoltTable trades = voltExecuteSQL()[0];
            
            // since we split the original SQL statement we have to retrieve every trade separately 
            for (int i = 0; i < trades.getRowCount(); i++) {
                long trade_id = trades.fetchRow(i).getLong("T_ID");
                voltQueueSQL(getTradeHistory, trade_id);
            }
            
            // we cannot limit them by 30 as required, so we will retrieve the whole history
            voltExecuteSQL();
        }
        
        VoltTable cust_res = ret_cust_template.clone(256);
        cust_res.add(customer);
        
        VoltTable assets_res = ret_acct_template.clone(256);
        for (Entry<Long, Double> e: cust_holds_list) {
            long acct_id = e.getKey();
            double total_assets = e.getValue();
            double cash_bal = cust_bal.get(acct_id);
            
            assets_res.addRow(acct_id, total_assets, cash_bal);
        }
        
        return new VoltTable[] {cust_res, assets_res};
    }
}
