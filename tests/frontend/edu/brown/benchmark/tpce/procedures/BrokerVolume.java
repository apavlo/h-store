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
import java.util.Map.Entry;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;

import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * BrokerVolume Transaction <br/>
 * TPC-E Section 3.3.7
 */
@ProcInfo (
    partitionInfo = "TRADE.T_CA_ID: 0",
    singlePartition = false
)
public class BrokerVolume extends VoltProcedure {

    public final SQLStmt getSectorIndustries = new SQLStmt(
        "SELECT IN_ID " +
        "  FROM SECTOR, INDUSTRY " +
        " WHERE SC_NAME = ? " +
        "   AND IN_SC_ID = SC_ID"
    );
    
    // Note: sum(TR_QTY * TR_BID_PRICE) not supported
    public final SQLStmt get = new SQLStmt(
        "SELECT B_NAME, TR_QTY * TR_BID_PRICE" +  // sum(TR_QTY * TR_BID_PRICE)
        "  FROM TRADE_REQUEST, COMPANY, BROKER, SECURITY, CUSTOMER_ACCOUNT " +
        " WHERE TR_CA_ID = CA_ID " +
        "   AND CA_B_ID = B_ID " +
        "   AND TR_S_SYMB = S_SYMB " +
        "   AND S_CO_ID = CO_ID " +
        "   AND CO_IN_ID = ? " +
        "   AND B_NAME = ? "
        
        // ORIGINAL QUERY
//        "SELECT B_NAME, TR_QTY * TR_BID_PRICE" +  // sum(TR_QTY * TR_BID_PRICE)
//        "  FROM TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY, CUSTOMER_ACCOUNT " +
//        " WHERE TR_CA_ID = CA_ID " +
//        "   AND CA_B_ID = B_ID " +
//        "   AND TR_S_SYMB = S_SYMB " +
//        "   AND S_CO_ID = CO_ID " +
//        "   AND CO_IN_ID = IN_ID " +
//        "   AND SC_ID = IN_SC_ID " +
//        "   AND B_NAME = ? " + // B_NAME IN (?)
//        "   AND SC_NAME = ? "
//         " GROUP BY B_NAME "
//         " ORDER BY 2 DESC " 

    );

    
    private final ColumnInfo[] returnColumns = {
        new ColumnInfo("broker_name", VoltType.STRING), 
        new ColumnInfo("volume", VoltType.DECIMAL),  
    };
    
    /**
     * 
     * @param broker_list
     * @param sector_name
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(
            String[] broker_list,
            String sector_name) throws VoltAbortException {
        Map<String, Double> totals = new HashMap<String, Double>();
        
        voltQueueSQL(getSectorIndustries, sector_name);
        final VoltTable sec_results[] = voltExecuteSQL();
        assert(sec_results.length == 1);
        while (sec_results[0].advanceRow()) {
            long in_id = sec_results[0].getLong(0);
            for (String bname : broker_list) {
                Double total = totals.get(bname);
                if (total == null) total = 0.0d;
                
                voltQueueSQL(get, in_id, bname);
                final VoltTable results[] = voltExecuteSQL();
                assert(results.length > 0);
                for (VoltTable vt : results) {
                    boolean advrow = vt.advanceRow();
                    assert(advrow);
                    assert(vt.getColumnCount() == 2);
                    total += vt.getDouble(1);
                } // FOR
                totals.put(bname, total);
            } // FOR
        } // WHILE
        
        VoltTable ret = new VoltTable(returnColumns);
        for (Entry<String, Double> e : totals.entrySet()) {
            ret.addRow(e.getKey(), e.getValue());
        } // FOR
        return (new VoltTable[] { ProcedureUtil.getStatusTable(true), ret });
    }
}