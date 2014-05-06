
package edu.brown.benchmark.biker;

public abstract class BikerConstants {


    public static final String[] LOCATIONS = {
        "Portland",
        "Milwaukie",
        "New New York",
        "Springfield",
        "Hillsboro",
        "Lake Oswego"};

    public static final long NO_BIKES_AVAILIBLE = 1;
    public static final long NO_DOCKS_AVAILIBLE = 2;
    public static final long RIDE_SUCCESS = 3;

    public static final long DOCK_FULL  = 4;
    public static final long NOT_A_DOCK = 5;
    public static final long DOCK_EMPTY = 6;
    public static final long DOCK_UNAVAILIBLE = 7;

    public static final long BIKE_RESERVED  = 8;
    public static final long DOCK_RESERVED  = 9;

    public static final long CHECKIN_ERROR   = 10;
    public static final long CHECKOUT_ERROR  = 11;

    public static final long CHECKIN_SUCCESS   = 12;
    public static final long CHECKOUT_SUCCESS  = 13;

    public static final long RESERVATION_DURATION = 900000000;

}
