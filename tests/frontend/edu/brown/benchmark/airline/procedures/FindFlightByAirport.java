package edu.brown.benchmark.airline.procedures;

import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.airline.AirlineConstants;

@ProcInfo(
    singlePartition = false
)
public class FindFlightByAirport extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(FindFlightByAirport.class);
    
    public final SQLStmt GetFlights = new SQLStmt(
            "SELECT F_ID, F_DEPART_TIME, F_ARRIVE_TIME " +
            "  FROM " + AirlineConstants.TABLENAME_FLIGHT +
            " WHERE F_DEPART_AP_ID = ? " +
            "   AND F_ARRIVE_AP_ID = ? " +
            "   AND F_DEPART_TIME >= ? " +
            "   AND F_DEPART_TIME <= ?");
    
    public VoltTable[] run(long depart_aid, long arrive_aid, TimestampType start_date, TimestampType end_date) {
        voltQueueSQL(GetFlights, depart_aid, arrive_aid, start_date, end_date);
        final VoltTable[] results = voltExecuteSQL();
        assert (results.length == 1);
        
        if (LOG.isDebugEnabled()) {
            while (results[0].advanceRow()) {
                LOG.debug("F_ID:   " + results[0].getLong(0));
                LOG.debug("DEPART: " + results[0].getTimestampAsTimestamp(1));
                LOG.debug("ARRIVE: " + results[0].getTimestampAsTimestamp(2));
            } // WHILE
            results[0].resetRowPosition();
        }
        return (results);
    }
}
