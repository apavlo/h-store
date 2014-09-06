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

import org.apache.commons.lang.ArrayUtils;

public abstract class BikerStreamConstants {

    // potential return codes
    public static final long BIKEREADING_SUCCESSFUL  = 0;

    public static final int NUM_WAITERS = 9;
    public static final long GAME_TIMER = 20000;
    public static final int GAME_STATION = 3;

    public static final long FAILED_CHECKOUT = -1;
    public static final long FAILED_CHECKIN = -2;
    public static final long FAILED_SIGNUP = -3;
    public static final long FAILED_POINT_ADD = -4;
    public static final long FAILED_ACCEPT_DISCOUNT = -5;
    public static final long NO_BIKE_CHECKED_OUT    = -6;
    public static final long USER_ALREADY_HAS_BIKE  = -7;
    public static final long USER_DOESNT_EXIST      = -8;
    public static final long NULL_RIDER_ID = -9;
    public static final long BIKE_DOESNT_EXIST = -10;

    public static final int  ANOMOLY_CHANCE = 10;
    public static final int  ANOMOLY_SKIP   = 7;

    // ===========================================================================
    // Callback Constants


    // ===========================================================================
    // INITIALIZATION PARAMETERS
    //
    // These guys are for generating a random map and filling it in with random sets
    // of zones, stations and docks.
    //

    public static final int NUM_BIKES_PER_STATION = 10;
    public static final int NUM_DOCKS_PER_STATION = 20;

    public static final String ROUTES_DIR =
            "src/benchmarks/edu/brown/benchmark/bikerstream/routes";

    public static final String[] STATION_NAMES = new String[]{
            "OHSU_South_Waterfront",
            "Waterfront_Park",
            "Eastbank_Esplanade",
            "Moda_Center",
            "Portland_State_University",
            "Overlook_Park",
            "Civic_Stadium", // KT - incorrect,but leave as this is the filename
    };

    public static final String[] STATION_LOGICAL_NAMES = new String[]{
            "OHSU_South_Waterfront",
            "Waterfront_Park",
            "Eastbank_Esplanade",
            "Moda_Center",
            "Portland_State_University",
            "Overlook_Park",
            "Providence_Park",
    };

    public static final String[] STATION_ADDRESSES = new String[]{
            "3303 SW Bond Ave, Portland",
            "SW Naito Parkway, Portland",
            "Eastbank Esplanade, Portland",
            "1 N Center St, Portland",
            "1825 SW Broadway, Portland",
            "1599 N Fremont St, Portland",
            "1844 SW Morrison St, Portland",
    };

    public static final String[] DP_NAMES = new String[]{
            "Decision_Point_1",
            "Decision_Point_2",
            "Decision_Point_3",
            "Decision_Point_4",
    };

    public static final String[] DP_LOGICAL_NAMES = new String[]{
            "Schrunk Plaza (DP)",
            "Pioneer Cemetary (DP)",
            "Pizza Schmizza (DP)",
            "Portland Music Co (DP)",
    };

    public static final String[] LOGICAL_NAMES = (String[]) ArrayUtils.addAll(STATION_LOGICAL_NAMES, DP_LOGICAL_NAMES);
    public static final double[] STATION_LONS = new double[]{
            -122.670743465424,
            -122.673382759094,
            -122.66716003418,
            -122.667524814606,
            -122.681311368942,
            -122.681010961533,
            -122.690554261208
    };

    public static final double[] STATION_LATS = new double[]{
            45.4992785100733,
            45.5153465357174,
            45.5182333316815,
            45.5309439966742,
            45.5093168644112,
            45.5491969282445,
            45.5220708871078
    };


    public static final String[] ALL_STOPS = (String[]) ArrayUtils.addAll(STATION_NAMES, DP_NAMES);

    public static final String[] STATION_LOCATIONS = new String[]{
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

    // The Highest id that can be given to a rider
    public static final int MAX_ID = 10000;

    // Return Values
    public static final long INSERT_RIDER_SUCCESS = 0;

    // Firstnames for the signup process.
    public static final String[] FIRSTNAMES = new String[]{
            "Adam", "Albert", "Derrick", "Erik", "John", "Jerry",
            "Marry", "Tim", "Jane", "Jenny", "Harry", "Loyd",
            "Gary", "Mark", "Sherry", "Kristin", "Beth", "Tom",
            "Paul", "Jay", "Andrea", "Jack", "Eryn", "Nesime",
            "Ben", "Bebe", "Ann", "Alex", "Carolyn", "Krieger",
            "Ellie", "Collen", "Clifford", "Christopher", "Johnathan",
            "Kanye", "David", "Craig", "Morgan", "Sara", "Hong"};

    // Lastnames for the sign up process
    public static final String[] LASTNAMES = new String[]{
            "Smith", "Phelps", "Sutherland", "Sampson", "Tufte",
            "Maes", "Mulvaney", "Logan", "Sarreal", "Cruise",
            "Archer", "Clark", "Casey", "Mack", "Garmin", "Sabath",
            "Giossi", "Murphy", "Hong", "West", "Ramage", "Meinschein",
            "Harvey", "Kane", "Tatbul", "Quach"};

    // ===========================================================================
    // BIKE CHECKOUT
    //

    // The discount threshold is the number of bikes necessary at a station before discounts
    // begin being added.
    public static final long DISCOUNT_THRESHOLD = 5;

    public static final long CHECKOUT_SUCCESS = 0;

    // ===========================================================================
    // BIKE RIDE
    //

    public static final long MILI_BETWEEN_GPS_EVENTS = 100;

    // ===========================================================================
    // BIKE CHECKIN
    //

    public static final long CHECKIN_SUCCESS = 0;

    // ===========================================================================
    // NEAR BY STATIONS
    //

    public static final long N_NEAR_BY_STATIONS = 3;

    // ===========================================================================
    // ANOMALIES
    //

    public static final double SPEED_SCALING = 1.0;
    public static final double STOLEN_SPEED = 1000.0;
    public static final long STOLEN_STATUS = 1;
}
