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
import java.util.List;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

/**
 * BrokerVolume Transaction <br/>
 * TPC-E Section 3.3.7
 * 
 * H-Store quirks:
 *    1) SUM(TR_QTY * TR_BID_PRICE) is not supported, which ruins group by and order by clauses as well -- moved to Java.
 *    2) It seems there is no way to do "B_NAME in (broker_list)". So, we submit one SQL query for each broker separately. 
 */
@ProcInfo(partitionInfo = "TRADE.T_CA_ID: 1", singlePartition = false)
public class BrokerVolume extends VoltProcedure {

    public final SQLStmt getBrokerInfo = new SQLStmt("select B_NAME, TR_QTY * TR_BID_PRICE " +
            "from TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY, CUSTOMER_ACCOUNT " +
            "where TR_CA_ID = CA_ID and CA_B_ID = B_ID and TR_S_SYMB = S_SYMB and " +
            "S_CO_ID = CO_ID and CO_IN_ID = IN_ID and SC_ID = IN_SC_ID and " +
            "B_NAME = ? and SC_NAME = ?"
    );

    public VoltTable[] run(String[] broker_list, String sector_name) throws VoltAbortException {
        // it seems we should return only the volumes
        VoltTable result = new VoltTable(new VoltTable.ColumnInfo("volume", VoltType.FLOAT));
        
        List<Double> volumes = new ArrayList<Double>();
        for (int i = 0; i < broker_list.length; i++) {
            voltQueueSQL(getBrokerInfo, broker_list[i], sector_name);
            VoltTable brok_res = voltExecuteSQL()[0];
            
            if (brok_res.getRowCount() > 0) {
                double vol = 0;
                for (int j = 0; j < brok_res.getRowCount(); j++) {
                    vol += brok_res.fetchRow(j).getDouble(1);
                }
                volumes.add(vol);
            }
        }
        
        // sort the values
        Collections.sort(volumes);
        
        // populating the result table in reverse order (since we need order by desc)
        for (int i = volumes.size() - 1; i >= 0; i--) {
            result.addRow(volumes.get(i));
        }
        
        return new VoltTable[] {result};
    }
}
