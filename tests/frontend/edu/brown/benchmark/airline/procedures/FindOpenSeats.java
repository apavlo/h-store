/* This file is part of VoltDB. 
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be 
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR 
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.                       
 */

package edu.brown.benchmark.airline.procedures;

import org.voltdb.*;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0",
    singlePartition = true
)
public class FindOpenSeats extends VoltProcedure {
 
    private final VoltTable.ColumnInfo outputColumns[] = {
        new VoltTable.ColumnInfo("F_ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("SEAT", VoltType.INTEGER)
    };
    
    public final SQLStmt GetSeats = new SQLStmt(
            "SELECT R_ID, R_F_ID, R_SEAT " + 
            "  FROM " + AirlineConstants.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ?");
     
    public VoltTable[] run(long fid) {
        
        // 150 seats
        final long seatmap[] = new long[]
          {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,     
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
           -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};
        
        
        voltQueueSQL(GetSeats, fid);
        final VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        
        while (results[0].advanceRow()) {
            int seatnum = (int)results[0].getLong(2);
            // System.out.printf("ROW fid %d rid %d seat %d\n", results[0].getLong(0), results[0].getLong(1), seatnum);
            seatmap[seatnum] = results[0].getLong(1);
        }

        final VoltTable retarray[] = new VoltTable[]{ new VoltTable(outputColumns) };
        for (int i=0; i < seatmap.length; ++i) {
            if (seatmap[i] != -1) {
                Object[] row = new Object[] {fid, new Long(i+1)};
                retarray[0].addRow(row);
            }
        }        
        return retarray;
    }
            
}
