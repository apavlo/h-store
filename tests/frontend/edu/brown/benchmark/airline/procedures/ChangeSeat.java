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

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo(
    partitionInfo = "RESERVATION.R_F_ID: 0",
    singlePartition = true
)
public class ChangeSeat extends VoltProcedure {

    public final SQLStmt CheckSeat = new SQLStmt(
            "SELECT R_ID FROM RESERVATION WHERE R_F_ID = ? and R_SEAT = ?");
    
    public final SQLStmt CheckCID = new SQLStmt(
            "SELECT R_ID FROM RESERVATION WHERE R_ID = ? and R_C_ID = ?");
    
    public final SQLStmt ReserveSeat = new SQLStmt(
            "UPDATE RESERVATION SET R_SEAT = ? WHERE R_F_ID = ? and R_C_ID = ?");
    
    public VoltTable[] run(long f_id, long c_id, long seatnum) throws VoltAbortException {
        
        // check if the seat is occupied
        // check if the customer has multiple seats on this flight
        voltQueueSQL(CheckSeat, f_id, seatnum);
        voltQueueSQL(CheckCID, f_id, c_id);
        final VoltTable[] results = voltExecuteSQL();
        
        assert(results.length == 2);
        if (results[0].getRowCount() > 0) {
            throw new VoltAbortException("Seat reservation conflict");
        }
        if (results[1].getRowCount() > 1) {
            throw new VoltAbortException("Customer owns multiple reservations");
        }
       
        // update the seat reservation for the customer
        voltQueueSQL(ReserveSeat, seatnum, f_id, c_id);
        VoltTable[] updates = voltExecuteSQL();
        assert(updates.length == 1);
        
        return updates;
    }  
}
