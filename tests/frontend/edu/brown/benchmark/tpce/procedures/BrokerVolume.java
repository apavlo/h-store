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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.tpce.util.ProcedureUtil;

/**
 * BrokerVolume Transaction <br/>
 * TPC-E Section 3.3.7
 */
@ProcInfo (
    partitionInfo = "TRADE.T_CA_ID: 1",
    singlePartition = false
)
public class BrokerVolume extends VoltProcedure {

    // Note: sum(TR_QTY * TR_BID_PRICE) not supported
    // Compiling this statement results in "java.lang.OutOfMemoryError: GC overhead limit exceed"
    public final SQLStmt get = new SQLStmt(
            "select B_NAME "
                    // +" , sum(TR_QTY * TR_BID_PRICE) "
                    + "from TRADE_REQUEST, SECTOR, INDUSTRY, COMPANY, BROKER, SECURITY "
                    + "where TR_S_SYMB = S_SYMB and " + "S_CO_ID = CO_ID and "
                    + "CO_IN_ID = IN_ID and " + "SC_ID = IN_SC_ID and "
                    // FIXME + "B_NAME in (?) and "
                    + "B_NAME = ? and "
                    + " SC_NAME = ? " + "group by B_NAME "
    // + "order by 2 DESC"
    );

    public VoltTable[] run(
            String[] broker_list,
            String sector_name) throws VoltAbortException {
        
        Map<String, Object[]> ret = new HashMap<String, Object[]>();
        int row_count = ProcedureUtil.execute(ret, this, get,
                new Object[] { broker_list[0], sector_name },
                new String[] { "broker_name", "volume" },
                new Object[] { "B_NAME", 1 });

        ret.put("list_len", new Integer[] { row_count });

        return ProcedureUtil.mapToTable(ret);
    }

}
