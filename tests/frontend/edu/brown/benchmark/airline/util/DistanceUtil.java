package edu.brown.benchmark.airline.util;

import org.voltdb.utils.Pair;

/**
 * Based on code found here:
 * http://www.zipcodeworld.com/samples/distance.java.html
 */
public abstract class DistanceUtil {

    /**
     * Calculate the distance between two points
     * @param lat0
     * @param lon0
     * @param lat1
     * @param lon1
     * @return
     */
    public static double distance(double lat0, double lon0, double lat1, double lon1) {
        double theta = lon0 - lon1;
        double dist = Math.sin(deg2rad(lat0)) * Math.sin(deg2rad(lat1)) + Math.cos(deg2rad(lat0)) * Math.cos(deg2rad(lat1)) * Math.cos(deg2rad(theta));
        dist = Math.acos(dist);
        dist = rad2deg(dist);
        return (dist * 60 * 1.1515);
    }
    
    public static double distance(Pair<Double, Double> loc0, Pair<Double, Double> loc1) {
        return (DistanceUtil.distance(loc0.getFirst(), loc0.getSecond(), loc1.getFirst(), loc1.getSecond()));
    }

    private static double deg2rad(double deg) {
        return (deg * Math.PI / 180.0);
    }

    private static double rad2deg(double rad) {
        return (rad * 180.0 / Math.PI);
    }
}
