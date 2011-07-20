package edu.brown.benchmark.airline.procedures;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class FindFlightByNearbyAirport extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(FindFlightByNearbyAirport.class);
    
    public final SQLStmt GetNearbyAirports = new SQLStmt(
            "SELECT * " +
            "  FROM " + AirlineConstants.TABLENAME_AIRPORT_DISTANCE +
            " WHERE D_AP_ID0 = ? " +
            "   AND D_DISTANCE <= ?"
    );
 
    public final String BaseGetNearbyFlights =
            "SELECT F_ID, F_DEPART_AP_ID, F_DEPART_TIME, F_ARRIVE_AP_ID, F_ARRIVE_TIME " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT + 
            " WHERE F_DEPART_AP_ID = ? " +
            "   AND F_DEPART_TIME >= ? " +
            "   AND F_DEPART_TIME <= ?";
    
    public final SQLStmt GetNearbyFlights1 = new SQLStmt(BaseGetNearbyFlights + " AND F_ARRIVE_AP_ID = ?");
    public final SQLStmt GetNearbyFlights2 = new SQLStmt(BaseGetNearbyFlights + " AND (F_ARRIVE_AP_ID = ? OR F_ARRIVE_AP_ID = ?)");
    public final SQLStmt GetNearbyFlights3 = new SQLStmt(BaseGetNearbyFlights + " AND (F_ARRIVE_AP_ID = ? OR F_ARRIVE_AP_ID = ? OR F_ARRIVE_AP_ID = ?)");
     
    public VoltTable[] run(long depart_aid, long arrive_aid, TimestampType start_date, TimestampType end_date, long distance) {
        
        // First get the nearby airports for the departure and arrival cities
        voltQueueSQL(GetNearbyAirports, depart_aid, distance);
        final VoltTable[] nearby_results = voltExecuteSQL();
        assert(nearby_results.length == 1);
        
        List<Long> depart_nearby = new ArrayList<Long>();
        while (nearby_results[0].advanceRow()) {
            LOG.debug("DEPART NEARBY: " + nearby_results[0].getLong(0) + " distance=" + nearby_results[0].getLong(1) + " miles");
            depart_nearby.add(nearby_results[0].getLong(0));
        } // WHILE
        
        // H-Store doesn't support IN clauses, so we'll only get nearby flights to nearby arrival cities
        int num_nearby = depart_nearby.size(); 
        if (num_nearby > 0) {
            if (num_nearby == 1) {
                voltQueueSQL(GetNearbyFlights1, depart_aid, start_date, end_date, depart_nearby.get(0));
            } else if (num_nearby == 2) {
                voltQueueSQL(GetNearbyFlights2, depart_aid, start_date, end_date, depart_nearby.get(0), depart_nearby.get(1));
            } else {
                voltQueueSQL(GetNearbyFlights3, depart_aid, start_date, end_date, depart_nearby.get(0), depart_nearby.get(1), depart_nearby.get(2));
            }
            final VoltTable[] results = voltExecuteSQL();
        
            if (LOG.isDebugEnabled()) {
                while (results[0].advanceRow()) {
                    LOG.debug("F_ID:   " + results[0].getLong(0));
                    LOG.debug("DEPART: " + results[0].getString(1) + " - " + results[0].getTimestampAsTimestamp(1));
                    LOG.debug("ARRIVE: " + results[0].getString(2) + " - " + results[0].getTimestampAsTimestamp(3));
                } // WHILE
                results[0].resetRowPosition();
            }
            return (results);
        }
        return (new VoltTable[0]);
     }
}
