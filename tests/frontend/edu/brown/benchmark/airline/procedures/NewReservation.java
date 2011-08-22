package edu.brown.benchmark.airline.procedures;

import org.voltdb.*;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class NewReservation extends VoltProcedure {
    
    public final SQLStmt GetFlight = new SQLStmt(
            "SELECT F_AL_ID, F_SEATS_LEFT, " +
                    AirlineConstants.TABLENAME_AIRLINE + ".* " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT + ", " +
                        AirlineConstants.TABLENAME_AIRLINE +
            " WHERE F_ID = ? AND F_AL_ID = AL_ID");
    
    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT - 1 " + 
            " WHERE F_ID = ? ");
    
    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_CUSTOMER +
            "   SET C_IATTR10 = C_IATTR10 + 1, " + 
            "       C_IATTR11 = C_IATTR11 + 1, " +
            "       C_IATTR12 = ?, " +
            "       C_IATTR13 = ?, " +
            "       C_IATTR14 = ?, " +
            "       C_IATTR15 = ? " +
            " WHERE C_ID = ? ");
    
    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + AirlineConstants.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 + 1, " + 
            "       FF_IATTR11 = ?, " +
            "       FF_IATTR12 = ?, " +
            "       FF_IATTR13 = ?, " +
            "       FF_IATTR14 = ? " +
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ?");
    
    public final SQLStmt InsertReservation = new SQLStmt(
            "INSERT INTO " + AirlineConstants.TABLENAME_RESERVATION + " (" +
            "   R_ID, " +
            "   R_C_ID, " +
            "   R_F_ID, " +
            "   R_SEAT, " +
            "   R_IATTR00, " +
            "   R_IATTR01, " +
            "   R_IATTR02, " +
            "   R_IATTR03, " +
            "   R_IATTR04, " +
            "   R_IATTR05, " +
            "   R_IATTR06, " +
            "   R_IATTR07, " +
            "   R_IATTR08 " +
            ") VALUES (" +
            "   ?, " +  // R_ID
            "   ?, " +  // R_C_ID
            "   ?, " +  // R_F_ID
            "   ?, " +  // R_SEAT
            "   ?, " +  // R_ATTR00
            "   ?, " +  // R_ATTR01
            "   ?, " +  // R_ATTR02
            "   ?, " +  // R_ATTR03
            "   ?, " +  // R_ATTR04
            "   ?, " +  // R_ATTR05
            "   ?, " +  // R_ATTR06
            "   ?, " +  // R_ATTR07
            "   ? " +   // R_ATTR08
            ")");
    
    public VoltTable[] run(long r_id, long c_id, long f_id, long seatnum, long attrs[]) throws VoltAbortException {
        voltQueueSQL(GetFlight, f_id);
        final VoltTable[] flight_results = voltExecuteSQL();
        assert(flight_results.length == 1);

        if (flight_results[0].getRowCount() != 1) {
            throw new VoltAbortException("Invalid flight id: " + f_id);
        }
        flight_results[0].advanceRow();
        if (flight_results[0].getLong(1) <= 0) {
            throw new VoltAbortException("No more seats available for flight id: " + f_id);
        }
        long airline_id = flight_results[0].getLong(0);
        
        voltQueueSQL(InsertReservation, r_id, c_id, f_id, seatnum,
                            attrs[0], attrs[1], attrs[2], attrs[3],
                            attrs[4], attrs[5], attrs[6], attrs[7],
                            attrs[8]);
        voltQueueSQL(UpdateFlight, f_id);
        voltQueueSQL(UpdateCustomer, attrs[0], attrs[1], attrs[2], attrs[3], c_id);
        voltQueueSQL(UpdateFrequentFlyer, attrs[4], attrs[5], attrs[6], attrs[7], c_id, airline_id);
        
        final VoltTable[] results = voltExecuteSQL();
        return (results);
    }
}
