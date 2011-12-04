package edu.brown.benchmark.airline.procedures;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.airline.AirlineConstants;

public class LoadConfig extends VoltProcedure {

    public final SQLStmt getConfigProfile = new SQLStmt(
        "SELECT * FROM " + AirlineConstants.TABLENAME_CONFIG_PROFILE
    );
    
    public final SQLStmt getConfigHistogram = new SQLStmt(
        "SELECT * FROM " + AirlineConstants.TABLENAME_CONFIG_HISTOGRAMS
    );
    
    public final SQLStmt getCountryCodes = new SQLStmt(
        "SELECT CO_ID, CO_CODE_3 FROM " + AirlineConstants.TABLENAME_COUNTRY
    );
    
    public final SQLStmt getAirportCodes = new SQLStmt(
        "SELECT AP_ID, AP_CODE FROM " + AirlineConstants.TABLENAME_AIRPORT
    );
    
    public final SQLStmt getAirlineCodes = new SQLStmt(
        "SELECT AL_ID, AL_IATA_CODE FROM " + AirlineConstants.TABLENAME_AIRLINE +
        " WHERE AL_IATA_CODE != ''"
    );
    
    public final SQLStmt getFlights = new SQLStmt(
        "SELECT f_id FROM " + AirlineConstants.TABLENAME_FLIGHT +
        " ORDER BY F_DEPART_TIME DESC " + 
        " LIMIT " + AirlineConstants.CACHED_FLIGHT_ID_SIZE
    );
    
    public VoltTable[] run() {
        voltQueueSQL(getConfigProfile);
        voltQueueSQL(getConfigHistogram);
        voltQueueSQL(getCountryCodes);
        voltQueueSQL(getAirportCodes);
        voltQueueSQL(getAirlineCodes);
        voltQueueSQL(getFlights);
        return voltExecuteSQL(true);
    }
}
