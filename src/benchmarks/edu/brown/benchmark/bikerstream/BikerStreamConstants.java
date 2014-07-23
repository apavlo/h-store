/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *                                                                      *
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

package edu.brown.benchmark.bikerstream;

/* a few constants, mainly a placeholder class */

public abstract class BikerStreamConstants {

    public static final String TABLENAME_BIKEREADINGS = "bikereadings_table";
    public static final String BIKEREADINGS_STREAM = "bikereadings_stream";
    public static final String BIKEREADINGS_WINDOW_ROWS = "bikereadings_window_rows";
    public static final String TABLENAME_BIKEREADINGS_COUNT= "count_bikereadings_table";

    // potential return codes
    public static final long BIKEREADING_SUCCESSFUL  = 0;
    public static final long ERR_INVALID_BIKEREADING = 1;

    // ===========================================================================
    // INITIALIZATION PARAMETERS
    //
    // These guys are for generating a random map and filling it in with random sets
    // of zones, stations and docks.
    //

    public static final int NUM_STATIONS = 10;
    public static final int NUM_BIKES_PER_STATION = 20;

    public static final String ROUTES_DIR =
            "src/benchmarks/edu/brown/benchmark/bikerstream/routes";

    public static final String[] STATION_LOCATIONS = new String[] {
        "OHSU_South_Waterfront",
        "Waterfront_Park",
        "Eastbank_Esplanade",
        "Moda_Center",
        "Portland_State_University",
        "Overlook_Park",
        "Civic_Stadium",
    };


    // ===========================================================================
    // SIGNUP
    //

    // Return Values
    public static final long INSERT_RIDER_SUCCESS = 0;
    public static final long INSERT_RIDER_FAILED  = 1;
    public static final long INSERT_CARD_FAILED   = 2;

    // Firstnames for the signup process.
    public static final String[] FIRSTNAMES = new String[] {
        "Adam", "Albert", "Derrick", "Erik", "John", "Jerry",
        "Marry", "Tim", "Jane", "Jenny", "Harry", "Loyd",
        "Gary", "Mark", "Sherry", "Kristin", "Beth", "Tom",
        "Paul", "Jay", "Andrea", "Jack", "Eryn", "Nesime",
        "Ben", "Bebe", "Ann", "Alex", "Carolyn", "Krieger",
        "Ellie", "Collen", "Clifford", "Christopher","Johnathan",
        "Kanye", "David", "Craig", "Morgan", "Sara" };

    // Lastnames for the sign up process
    public static final String[] LASTNAMES = new String[] {
        "Smith", "Phelps", "Sutherland", "Sampson", "Tufte",
        "Maes", "Kiss", "Mulvaney", "Logan", "Sarreal", "Cruise",
        "Archer", "Clark", "Casey", "Mack", "Garmin", "Sabath",
        "Giossi", "Murphy", "Hong", "West", "Ramage", "Meinschein",
        "Harvey", "Kane" };

    // ===========================================================================
    // BIKE RESERVATIONS
    //

    // Maximum number of times to try and reserve a bike
    public static final int MAX_RESERVE_ATTEMPTS = 5;

    // Return Values
    public static final long BIKE_RESERVED     = 0;
    public static final long BIKE_NOT_RESERVED = 1;

    // ===========================================================================
    // BIKE CHECKOUT
    //

    public static final int MAX_CHECKOUT_ATTEMPTS = 5;

    public static final long BIKE_CHECKEDOUT      = 0;
    public static final long BIKE_NOT_CHECKEDOUT  = 1;
    public static final long NO_AVAILIBLE_BIKES   = 2;

    // ===========================================================================
    // BIKE RIDE
    //

    public static final long MILI_BETWEEN_GPS_EVENTS = 100;

    // ===========================================================================
    // DOCK RESERVATIONS
    //

    // Maximum number of times to try and reserve a bike
    public static final int MAX_DOCK_RESERVE_ATTEMPTS = 5;

    // Return Values
    public static final long DOCK_RESERVED     = 0;
    public static final long DOCK_NOT_RESERVED = 1;

    // ===========================================================================
    // BIKE CHECKIN
    //

    public static final int MAX_CHECKIN_ATTEMPTS  = 5;

    public static final long BIKE_CHECKEDIN       = 0;
    public static final long BIKE_NOT_CHECKEDIN   = 1;
    public static final long NO_AVAILIBLE_DOCKS   = 2;

}
