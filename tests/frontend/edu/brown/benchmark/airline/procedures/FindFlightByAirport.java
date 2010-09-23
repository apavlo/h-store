package edu.brown.benchmark.airline.procedures;

import java.util.*;

import org.voltdb.*;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class FindFlightByAirport extends VoltProcedure {
    
    public final SQLStmt GET_FLIGHTS = new SQLStmt(
            "SELECT F_ID, F_DEPART_TIME, F_ARRIVE_TIME " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT +
            " WHERE F_DEPART_A_ID = ? " +
            "   AND F_ARRIVE_A_ID = ? " +
            "   AND F_DEPART_TIME >= ? " +
            "   AND F_DEPART_TIME <= ?");
    
    public VoltTable[] run(long depart_aid, long arrive_aid, Date start_date, Date end_date) {
        voltQueueSQL(GET_FLIGHTS, depart_aid, arrive_aid, start_date, end_date);
        final VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        
        while (results[0].advanceRow()) {
            System.out.println("F_ID:   " + results[0].getLong(0));
            System.out.println("DEPART: " + results[0].getTimestampAsTimestamp(1));
            System.out.println("ARRIVE: " + results[0].getTimestampAsTimestamp(2));
        }
        return (results);
    }
}
