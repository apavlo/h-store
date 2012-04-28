package edu.brown.benchmark.seats.procedures;

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.seats.SEATSConstants;

@ProcInfo(
    singlePartition = false
)
public class GetTableCounts extends VoltProcedure {
    
    public final SQLStmt CountryCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_COUNTRY);
    public final SQLStmt AirlineCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_AIRLINE);
    public final SQLStmt CustomerCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_CUSTOMER);
    public final SQLStmt FrequentFlyerCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_FREQUENT_FLYER);
    public final SQLStmt AirportCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_AIRPORT);
    public final SQLStmt AirportDistanceCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_AIRPORT_DISTANCE);
    public final SQLStmt FlightCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_FLIGHT);
    public final SQLStmt ReservationCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_RESERVATION);
    public final SQLStmt ConfigProfileCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_CONFIG_PROFILE);
    public final SQLStmt ConfigHistogramsCount = new SQLStmt("SELECT COUNT(*) FROM " + SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS);
    
    private final HashMap<String, SQLStmt> table_map = new HashMap<String, SQLStmt>();
    {
        this.table_map.put(SEATSConstants.TABLENAME_COUNTRY, CountryCount);
        this.table_map.put(SEATSConstants.TABLENAME_AIRLINE, AirlineCount);
        this.table_map.put(SEATSConstants.TABLENAME_CUSTOMER, CustomerCount);
        this.table_map.put(SEATSConstants.TABLENAME_FREQUENT_FLYER, FrequentFlyerCount);
        this.table_map.put(SEATSConstants.TABLENAME_AIRPORT, AirportCount);
        this.table_map.put(SEATSConstants.TABLENAME_AIRPORT_DISTANCE, AirportDistanceCount);
        this.table_map.put(SEATSConstants.TABLENAME_FLIGHT, FlightCount);
        this.table_map.put(SEATSConstants.TABLENAME_RESERVATION, ReservationCount);
        this.table_map.put(SEATSConstants.TABLENAME_CONFIG_PROFILE, ConfigProfileCount);
        this.table_map.put(SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS, ConfigHistogramsCount);
    }
    
    private final VoltTable.ColumnInfo columns[] = {
        new VoltTable.ColumnInfo("name", VoltType.STRING),
        new VoltTable.ColumnInfo("size", VoltType.BIGINT),
    };
    
    public VoltTable[] run() {
        VoltTable ret = new VoltTable(this.columns);
        for (Entry<String, SQLStmt> e : this.table_map.entrySet()) {
            // System.err.println("Retrieving number of tuples for " + e.getKey());
            voltQueueSQL(e.getValue());
            VoltTable results[] = voltExecuteSQL();
            assert(results.length == 1) : "Got " + results.length + " results for table " + e.getKey();
            assert(results[0].getRowCount() > 0);
            boolean adv = results[0].advanceRow();
            assert(adv) : "Unable to advance results row for table " + e.getKey();
            ret.addRow(e.getKey(), results[0].getLong(0));
        } // FOR
        return (new VoltTable[]{ ret });
    }
}
