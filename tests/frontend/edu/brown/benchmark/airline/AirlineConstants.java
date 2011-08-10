package edu.brown.benchmark.airline;

public abstract class AirlineConstants {
    
    public static final int DISTANCES[] = { 5 }; // , 10, 25, 50, 100 };
    
    public static final int DAYS_PAST = 7;

    public static final int DAYS_FUTURE = 14;

    
    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------
    public static final int FREQUENCY_DELETE_RESERVATION            = 10;
    public static final int FREQUENCY_CHANGE_SEAT                   = 10;
    public static final int FREQUENCY_FIND_FLIGHT_BY_AIRPORT        = 10;
    public static final int FREQUENCY_FIND_FLIGHT_BY_NEARBY_AIRPORT = 10;
    public static final int FREQUENCY_FIND_OPEN_SEATS               = 10;
    public static final int FREQUENCY_NEW_RESERVATION               = 10;
    public static final int FREQUENCY_UPDATE_FREQUENT_FLYER         = 10;
    public static final int FREQUENCY_UPDATE_RESERVATION            = 10;

    // Initial Data Sizes
    public static final int NUM_CUSTOMERS = 1000000;
    
    /**
     * Average # of flights per day
     * Source: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
     */
    // public static final int AVG_FLIGHTS_PER_DAY = 17000;
    public static final int NUM_FLIGHTS_PER_DAY = 15000;
    
    // Max Number of FREQUENT_FLYER records per CUSTOMER 
    public static final int MAX_FREQUENTFLYER_PER_CUSTOMER = 5;
    
    // Number of seats available per flight
    public static final int NUM_SEATS_PER_FLIGHT = 150;
    
    // Probability that a customer books a non-roundtrip flight (0% - 100%)
    public static final int PROB_SINGLE_FLIGHT_RESERVATION = 10;
    
    // Probability that a customer will change their seat (0% - 100%)
    public static final int PROB_CHANGE_SEAT = 5;
    
    // The maximum number of days that we allow a customer to wait before needing
    // a reservation on a return to their original departure airport
    public static final int MAX_RETURN_FLIGHT_DAYS = 14;
    
    /**
     * Number of milliseconds in a day
     */
    public static final long MILISECONDS_PER_DAY = 86400000;
    
    /**
     * The rate in which a flight can travel between two airports (miles per hour)
     */
    public static final double FLIGHT_TRAVEL_RATE = 570.0; // Boeing 747

    /**
     * Table Names
     */
    public static final String TABLENAME_COUNTRY = "COUNTRY";
    public static final String TABLENAME_AIRLINE = "AIRLINE";
    public static final String TABLENAME_CUSTOMER = "CUSTOMER";
    public static final String TABLENAME_FREQUENT_FLYER = "FREQUENT_FLYER";
    public static final String TABLENAME_AIRPORT = "AIRPORT";
    public static final String TABLENAME_AIRPORT_DISTANCE = "AIRPORT_DISTANCE";
    public static final String TABLENAME_FLIGHT = "FLIGHT";
    public static final String TABLENAME_RESERVATION = "RESERVATION";

    /**
     * Histogram Data Set Names
     */
    public static final String HISTOGRAM_FLIGHTS_PER_AIRPORT = "flights_per_airport";
    public static final String HISTOGRAM_FLIGHTS_PER_DEPART_TIMES = "flights_per_time";
    
    // Tables that are loaded from data files
    public static final String TABLE_DATA_FILES[] = {
        AirlineConstants.TABLENAME_COUNTRY,
        AirlineConstants.TABLENAME_AIRPORT,
        AirlineConstants.TABLENAME_AIRLINE,
    };
    
    // Tables generated from random data
    public static final String TABLE_SCALING[] = {
        AirlineConstants.TABLENAME_CUSTOMER,
        AirlineConstants.TABLENAME_FREQUENT_FLYER,
        AirlineConstants.TABLENAME_AIRPORT_DISTANCE,
        AirlineConstants.TABLENAME_FLIGHT,
        AirlineConstants.TABLENAME_RESERVATION,
    };
    
    // Histograms generated from data files
    public static final String HISTOGRAM_DATA_FILES[] = {
        AirlineConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT,
        AirlineConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES,
    };
}
