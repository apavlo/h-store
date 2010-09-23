package edu.brown.benchmark.airline.procedures;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.voltdb.*;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class FindFlightByNearbyAirport extends FindFlightByAirport {
    
    public final SQLStmt GET_NEARBY = new SQLStmt(
            "SELECT * " +
            "  FROM " + AirlineConstants.TABLENAME_AIRPORT_DISTANCE +
            " WHERE D_AP_ID0 = ? " +
            "   AND D_DISTANCE <= ?"
    );
 
    public final String NEARBY_FLIGHTS =
            "SELECT F_ID, F_DEPART_A_ID, F_DEPART_TIME, F_ARRIVE_A_ID, F_ARRIVE_TIME " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT + 
            " WHERE F_DEPART_A_ID = ? " +
            "   AND F_DEPART_TIME >= ? " +
            "   AND F_DEPART_TIME <= ?";
    
    public final SQLStmt GET_NEARBY_FLIGHTS1 = new SQLStmt(NEARBY_FLIGHTS + " AND F_ARRIVE_A_ID IN (?)");
    public final SQLStmt GET_NEARBY_FLIGHTS2 = new SQLStmt(NEARBY_FLIGHTS + " AND F_ARRIVE_A_ID IN (?, ?)");
    public final SQLStmt GET_NEARBY_FLIGHTS3 = new SQLStmt(NEARBY_FLIGHTS + " AND F_ARRIVE_A_ID IN (?, ?, ?)");
     
    public VoltTable[] run(long depart_aid, long arrive_aid, Date start_date, Date end_date, long distance) {
        
        //
        // First get the nearby airports for the departure and arrival cities
        //
        voltQueueSQL(GET_NEARBY, depart_aid, distance);
        final VoltTable[] nearby_results = voltExecuteSQL();
        assert(nearby_results.length == 1);
        
        List<Long> depart_nearby = new ArrayList<Long>();
        while (nearby_results[0].advanceRow()) {
            System.out.println("DEPART NEARBY: " + nearby_results[0].getLong(0) + " distance=" + nearby_results[0].getLong(1) + " miles");
            depart_nearby.add(nearby_results[0].getLong(0));
        } // WHILE
        
        //
        // Volt doesn't support IN clauses, so we'll only get nearby flights to nearby arrival cities
        //
        int num_nearby = depart_nearby.size(); 
        if (num_nearby > 0) {
            if (num_nearby == 1) {
                voltQueueSQL(GET_NEARBY_FLIGHTS1, depart_aid, start_date, end_date, depart_nearby.get(0));
            } else if (num_nearby == 2) {
                voltQueueSQL(GET_NEARBY_FLIGHTS2, depart_aid, start_date, end_date, depart_nearby.get(0), depart_nearby.get(1));
            } else {
                voltQueueSQL(GET_NEARBY_FLIGHTS3, depart_aid, start_date, end_date, depart_nearby.get(0), depart_nearby.get(1), depart_nearby.get(2));
            }
            final VoltTable[] results = voltExecuteSQL();
        
            while (results[0].advanceRow()) {
                System.out.println("F_ID:   " + results[0].getLong(0));
                System.out.println("DEPART: " + results[0].getString(1) + " - " + results[0].getTimestampAsTimestamp(1));
                System.out.println("ARRIVE: " + results[0].getString(2) + " - " + results[0].getTimestampAsTimestamp(3));
            } // WHILE
            return (results);
        }
        return (new VoltTable[0]);
     }
}
