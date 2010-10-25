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

import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import java.util.Map.Entry;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.utils.Pair;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * Customer Position Transaction <br/>
 * TPC-E Section 3.3.6
 */
public class CustomerPosition extends VoltProcedure {

    private static final int max_acct_len = 10;

    public final SQLStmt getCID = new SQLStmt(
        "SELECT C_ID FROM CUSTOMER WHERE C_TAX_ID = ?"
    );

    public final SQLStmt getCustomer = new SQLStmt(
        "SELECT * FROM CUSTOMER WHERE C_ID = ?"
    );

    // Note: The parameter max_trade should be used for the LIMIT value but
    // we don't support parameters in the LIMIT clause. Hardcoding to 10 for now.
    public final SQLStmt getAssets = new SQLStmt(
        "SELECT CA_ID, CA_BAL, HS_QTY * LT_PRICE " + // FIXME ifnull((sum(HS_QTY*LT_PRICE)),0) "
        "  FROM CUSTOMER_ACCOUNT LEFT OUTER JOIN HOLDING_SUMMARY ON HS_CA_ID = CA_ID, " +
        "       LAST_TRADE " +
        " WHERE CA_C_ID = ? " +
        "   AND LT_S_SYMB = HS_S_SYMB "
        // " GROUP BY CA_ID, CA_BAL "
        // "order by 3 asc"
        // " LIMIT " + max_acct_len
    );
    private final static ColumnInfo getAssetsColumns[] = new ColumnInfo[] {
        new ColumnInfo("CA_ID", VoltType.BIGINT),
        new ColumnInfo("CA_BAL", VoltType.DECIMAL),
        new ColumnInfo("SUM", VoltType.DECIMAL),
    };
    
    public final SQLStmt getTrades = new SQLStmt(
        "SELECT T_ID FROM TRADE WHERE T_CA_ID = ? ORDER BY T_DTS DESC LIMIT 10"
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
    

    /**
     * 
     * @param acct_id_idx
     * @param cust_id
     * @param get_history
     * @param tax_id
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(
            long acct_id_idx,
            long cust_id,
            long get_history,
            String tax_id) throws VoltAbortException {
        VoltTable[] results = null;
        final Vector<VoltTable> final_results = new Vector<VoltTable>(); 
        
        /** FRAME 1 **/
        
        // Use the tax_id to get the cust_id
        if (cust_id == 0) {
            voltQueueSQL(getCID, tax_id);
            results = voltExecuteSQL();
            assert(results.length == 1);
            boolean advrow = results[0].advanceRow();
            assert(advrow);
            cust_id = results[0].getLong(0);
            assert(cust_id > 0);
        }

        // Get customer information
        voltQueueSQL(getCustomer, cust_id);
        voltQueueSQL(getAssets, cust_id);
        results = voltExecuteSQL();
        assert(results.length == 2);
        
        // We can stick the results from the first query directly into our final result
        final_results.add(results[0]);
        
        // We have to perform the aggregation ourselves here...
        Map<Pair<Long, Double>, Double> aggregate = new TreeMap<Pair<Long, Double>, Double>();
        while (results[1].advanceRow()) {
            long ca_id = results[1].getLong(0); // CA_ID
            double ca_bal = results[1].getDouble(1); // CA_BAL
            double agg = results[1].getDouble(2);   // HS_QTY * LT_PRICE
            if (results[1].wasNull()) agg = 0.0d;
            
            Pair<Long, Double> key = Pair.of(ca_id, ca_bal);
            Double sum = aggregate.get(key);
            if (sum == null) sum = 0.0d;
            aggregate.put(key, sum + agg);
        }
        
        final VoltTable getResultsTable = new VoltTable(getAssetsColumns);
        for (Entry<Pair<Long, Double>, Double> e : aggregate.entrySet()) {
            getResultsTable.addRow(e.getKey().getFirst(), e.getKey().getSecond(), e.getValue());
        } // FOR
        final_results.add(getResultsTable);

        /** FRAME 2 **/
        if (get_history == TPCEConstants.TRUE) {
            voltQueueSQL(getTrades, cust_id);
            results = voltExecuteSQL();
            assert results.length == 1;
            while (results[0].advanceRow()) {
                long t_id = results[0].getLong(0);
                voltQueueSQL(getTradeHistory, t_id); 
            } // WHILE
            final_results.add(ProcedureUtil.combineTables(voltExecuteSQL()));
        }
        
        return final_results.toArray(new VoltTable[]{});
    }
}