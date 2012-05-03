/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.seats;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class SEATSConstants {
    
    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0% - 100%)
    // ----------------------------------------------------------------
    
    public static final int FREQUENCY_DELETE_RESERVATION        = 10;
    public static final int FREQUENCY_FIND_FLIGHTS              = 10;
    public static final int FREQUENCY_FIND_OPEN_SEATS           = 35;
    public static final int FREQUENCY_NEW_RESERVATION           = 20;
    public static final int FREQUENCY_UPDATE_CUSTOMER           = 10;
    public static final int FREQUENCY_UPDATE_RESERVATION        = 15;

//    public static final int FREQUENCY_DELETE_RESERVATION        = 0;
//    public static final int FREQUENCY_FIND_FLIGHTS              = 0;
//    public static final int FREQUENCY_FIND_OPEN_SEATS           = 100;
//    public static final int FREQUENCY_NEW_RESERVATION           = 0;
//    public static final int FREQUENCY_UPDATE_CUSTOMER           = 0;
//    public static final int FREQUENCY_UPDATE_RESERVATION        = 0;

    // ----------------------------------------------------------------
    // FLIGHT CONSTANTS
    // ----------------------------------------------------------------
    
    /** 
     * The different distances that we can look-up for nearby airports
     * This is similar to the customer selecting a dropdown when looking for flights 
     */
    public static final int DISTANCES[] = { 5 }; // , 10, 25, 50, 100 };
    
    /**
     * The number of days in the past and future that we will generate flight information for
     */
    public static final int FLIGHTS_DAYS_PAST = 1;
    public static final int FLIGHTS_DAYS_FUTURE = 50;

    /**
     * Average # of flights per day
     * NUM_FLIGHTS_PER_DAY = 15000
     * Source: http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time
     */
    public static final int FLIGHTS_PER_DAY_MIN = 1125;
    public static final int FLIGHTS_PER_DAY_MAX = 1875;
    
    /**
     * Number of seats available per flight
     * If you change this then you must also change FindOpenSeats
     */
    public static final int FLIGHTS_NUM_SEATS = 150;
    
    /**
     * How many First Class seats are on a given flight
     * These reservations are more expensive
     */
    public static final int FLIGHTS_FIRST_CLASS_OFFSET = 10;
    
    /**
     * The rate in which a flight can travel between two airports (miles per hour)
     */
    public static final double FLIGHT_TRAVEL_RATE = 570.0; // Boeing 747
    
    // ----------------------------------------------------------------
    // CUSTOMER CONSTANTS
    // ----------------------------------------------------------------
    
    /**
     * Default number of customers in the database
     */
    public static final int CUSTOMERS_COUNT = 100000;
    
    /**
     * Max Number of FREQUENT_FLYER records per CUSTOMER
     */ 
    public static final int CUSTOMER_NUM_FREQUENTFLYERS_MIN = 0;
    public static final int CUSTOMER_NUM_FREQUENTFLYERS_MAX = 10;
    public static final double CUSTOMER_NUM_FREQUENTFLYERS_SIGMA = 2.0;
    
    /**
     * The maximum number of days that we allow a customer to wait before needing
     * a reservation on a return to their original departure airport
     */
    public static final int CUSTOMER_RETURN_FLIGHT_DAYS_MIN = 1;
    public static final int CUSTOMER_RETURN_FLIGHT_DAYS_MAX = 14;

    // ----------------------------------------------------------------
    // RESERVATION CONSTANTS
    // ----------------------------------------------------------------
    
    public static final int RESERVATION_PRICE_MIN = 100;
    public static final int RESERVATION_PRICE_MAX = 1000;
    
    public static final int MAX_OPEN_SEATS_PER_TXN = 100;
    
    // ----------------------------------------------------------------
    // PROBABILITIES
    // ----------------------------------------------------------------
    
    /**
     * Probability that a customer books a non-roundtrip flight (0% - 100%)
     */
    public static final int PROB_SINGLE_FLIGHT_RESERVATION = 10;
    
    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_CUSTOMER_ID_STR = 20;
    
    /**
     * Probability that a customer will invoke UpdateCustomer using the string
     * version of their Customer Id (0% - 100%)
     */
    public static final int PROB_UPDATE_WITH_CUSTOMER_ID_STR = 20;
    
    /**
     * Probability that a customer will invoke DeleteReservation using the string
     * version of their FrequentFlyer Id (0% - 100%)
     */
    public static final int PROB_DELETE_WITH_FREQUENTFLYER_ID_STR = 20;
    
    /**
     * Probability that is a seat is initially occupied (0% - 100%)
     */
    public static final int PROB_SEAT_OCCUPIED = 1; // 25;
    
    /**
     * Probability that UpdateCustomer should update FrequentFlyer records
     */
    public static final int PROB_UPDATE_FREQUENT_FLYER = 25;
    
    /**
     * Probability that a new Reservation will be added to the DeleteReservation queue
     */
    public static final int PROB_DELETE_RESERVATION = 50;
    
    /**
     * Probability that a new Reservation will be added to the UpdateReservation queue
     */
    public static final int PROB_UPDATE_RESERVATION = 50;

    /**
     * Probability that a deleted Reservation will be requeued for another NewReservation call
     */
    public static final int PROB_REQUEUE_DELETED_RESERVATION = 90;
    
    /**
     * Probability that FindFlights will use the distance search
     */
    public static final int PROB_FIND_FLIGHTS_NEARBY_AIRPORT = 25;
    
    /**
     * Probability that FindFlights will use two random airports as its input
     */
    public static final int PROB_FIND_FLIGHTS_RANDOM_AIRPORTS = 10;
    
    // ----------------------------------------------------------------
    // TIME CONSTANTS
    // ----------------------------------------------------------------
    
    /** Number of microseconds in a day */
    public static final long MICROSECONDS_PER_MINUTE = 60000000l;
    
    /** Number of microseconds in a day */
    public static final long MICROSECONDS_PER_DAY = 86400000000l; // 60sec * 60min * 24hr * 1,000 
    
    /**
     * The format of the time codes used in HISTOGRAM_FLIGHTS_PER_DEPART_TIMES
     */
    public static final Pattern TIMECODE_PATTERN = Pattern.compile("([\\d]{2,2}):([\\d]{2,2})");
    
    // ----------------------------------------------------------------
    // CACHE SIZES
    // ----------------------------------------------------------------
    
    /**
     * The number of FlightIds we want to keep cached locally at a client
     */
    public static final int CACHE_LIMIT_FLIGHT_IDS = 10000;
    
    public static final int CACHE_LIMIT_PENDING_INSERTS = 10000;
    public static final int CACHE_LIMIT_PENDING_UPDATES = 5000;
    public static final int CACHE_LIMIT_PENDING_DELETES = 5000;
    
    // ----------------------------------------------------------------
    // DATA SET INFORMATION
    // ----------------------------------------------------------------
    
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
    
    public static final String TABLENAME_CONFIG_PROFILE = "CONFIG_PROFILE";
    public static final String TABLENAME_CONFIG_HISTOGRAMS = "CONFIG_HISTOGRAMS";

    /**
     * Histogram Data Set Names
     */
    public static final String HISTOGRAM_FLIGHTS_PER_AIRPORT = "flights_per_airport";
    public static final String HISTOGRAM_FLIGHTS_PER_DEPART_TIMES = "flights_per_time";
    
    /** Tables that are loaded from data files */
    public static final String TABLES_DATAFILES[] = {
        SEATSConstants.TABLENAME_COUNTRY,
        SEATSConstants.TABLENAME_AIRPORT,
        SEATSConstants.TABLENAME_AIRLINE,
    };
    
    /**
     * Tables generated from random data
     * IMPORTANT: FLIGHT must come before FREQUENT_FLYER
     */
    public static final String TABLES_SCALING[] = {
        SEATSConstants.TABLENAME_CUSTOMER,
        SEATSConstants.TABLENAME_AIRPORT_DISTANCE,
        SEATSConstants.TABLENAME_FLIGHT,
        SEATSConstants.TABLENAME_FREQUENT_FLYER,
        SEATSConstants.TABLENAME_RESERVATION,
    };
    
    /** Configuration Tables */
    public static final String TABLES_CONFIG[] = {
        SEATSConstants.TABLENAME_CONFIG_PROFILE,
        SEATSConstants.TABLENAME_CONFIG_HISTOGRAMS,
    };
    
    /** Histograms generated from data files */
    public static final String HISTOGRAM_DATA_FILES[] = {
        SEATSConstants.HISTOGRAM_FLIGHTS_PER_AIRPORT,
        SEATSConstants.HISTOGRAM_FLIGHTS_PER_DEPART_TIMES,
    };

    /**
     * Tuple Code to Tuple Id Mapping
     * For some tables, we want to store a unique code that can be used to map
     * to the id of a tuple. Any table that has a foreign key reference to this table
     * will use the unique code in the input data tables instead of the id. Thus, we need
     * to keep a table of how to map these codes to the ids when loading.
     */
    public static final String CODE_TO_ID_COLUMNS[][] = {
        {TABLENAME_COUNTRY, "CO_CODE_3",    "CO_ID"},
        {TABLENAME_AIRPORT, "AP_CODE",      "AP_ID"},
        {TABLENAME_AIRLINE, "AL_IATA_CODE", "AL_ID"},
    };
}
