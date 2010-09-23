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

import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * Customer Position Transaction <br/>
 * TPC-E Section 3.3.6
 */
public class CustomerPosition extends VoltProcedure {

    private static final int max_acct_len = 10;

    public final SQLStmt getCID = new SQLStmt(
            "select C_ID from CUSTOMER where C_TAX_ID=?");

    public final SQLStmt getCustomer = new SQLStmt(
            "select C_ST_ID, C_L_NAME, C_F_NAME, C_M_NAME, C_GNDR, C_TIER, C_DOB, C_AD_ID, "
                    + "C_CTRY_1, C_AREA_1, C_LOCAL_1, C_EXT_1, C_CTRY_2, C_AREA_2, C_LOCAL_2, C_EXT_2, "
                    + "C_CTRY_3, C_AREA_3, C_LOCAL_3, C_EXT_3, C_EMAIL_1, C_EMAIL_2 "
                    + "from CUSTOMER " + "where C_ID=?");

    // Note: The parameter max_trade should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 10 for now.
    public final SQLStmt getAssets = new SQLStmt(
            "select CA_ID, CA_BAL " // FIXME ifnull((sum(HS_QTY*LT_PRICE)),0) "
                    + "from CUSTOMER_ACCOUNT left outer join HOLDING_SUMMARY on HS_CA_ID=CA_ID, LAST_TRADE "
                    + "where CA_C_ID=? and LT_S_SYMB=HS_S_SYMB "
                    + "group by CA_ID,CA_BAL "
                    // FIXME "order by 3 asc"
                    + " limit 10"); // limit ?
    
    public final SQLStmt getTrades = new SQLStmt(
        "SELECT t_id FROM trade WHERE t_ca_id = ? ORDER BY t_dts DESC LIMIT 10"
    );
    
    public final SQLStmt getTradeHistory = new SQLStmt(
        "SELECT t_id, t_s_symb, t_qty, st_name, th_dts " +
        "  FROM trade, trade_history, status_type " +
        " WHERE t_id = ? " + // 10 trades from getTrades
        "   AND th_t_id = t_id " +
        "   AND st_id = th_st_id " +
        " ORDER BY th_dts DESC " +
        " LIMIT 10" // max_hist_len?
    );
    

    public VoltTable[] run(
            long acct_id_idx,
            long cust_id,
            long get_history,
            String tax_id) throws VoltAbortException {
        Map<String, Object[]> ret = new HashMap<String, Object[]>();
        
        /** FRAME 1 **/
        
        // Use the tax_id to get the cust_id
        if (cust_id == 0) {
            ProcedureUtil.execute(ret, this, getCID,
                    new Object[] { tax_id },
                    new String[] { "cust_id" },
                    new Object[] { "C_ID" });
        }

        ProcedureUtil.execute(ret, this, getCustomer,
                new Object[] { cust_id },
                new String[] { "c_st_id", "c_l_name", "c_f_name", "c_m_name", "c_gndr", "c_tier", "c_dob", "c_ad_id", "c_ctry_1", "c_area_1", "c_local_1", "c_ext_1", "c_ctry_2", "c_area_2", "c_local_2", "c_ext_2", "c_ctry_3", "c_area_3", "c_local_3", "c_ext_3", "c_email_1", "c_email_2" },
                new Object[] { "C_ST_ID", "C_L_NAME", "C_F_NAME", "C_M_NAME", "C_GNDR", "C_TIER", "C_DOB", "C_AD_ID", "C_CTRY_1", "C_AREA_1", "C_LOCAL_1", "C_EXT_1", "C_CTRY_2", "C_AREA_2", "C_LOCAL_2", "C_EXT_2", "C_CTRY_3", "C_AREA_3", "C_LOCAL_3", "C_EXT_3", "C_EMAIL_1", "C_EMAIL_2" });
        int row_count = ProcedureUtil.execute(ret, this, getAssets,
                new Object[] { cust_id },
                new String[] { "acct_id", "cash_bal" }, // Add "assets_total" once getAssets is fixed
                new Object[] { "CA_ID", "CA_BAL"});     // Add 2 to this list once getAssets is fixed

        ret.put("acct_len", new Integer[] { row_count });

        /** FRAME 2 **/
        voltQueueSQL(getTrades, cust_id);
        VoltTable results[] = voltExecuteSQL();
        assert results.length == 1;
        while (results[0].advanceRow()) {
            long t_id = results[0].getLong(0);
            voltQueueSQL(getTradeHistory, t_id); 
        } // WHILE
        results = voltExecuteSQL();
        
        return ProcedureUtil.mapToTable(ret);
    }
}
