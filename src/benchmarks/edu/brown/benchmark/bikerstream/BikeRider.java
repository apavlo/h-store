/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.                                               *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *
 *                                                                         *
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

import org.apache.commons.lang.ArrayUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.LinkedList;
import java.util.Random;

/**
 * Class to generate readings from bikes. Idea is that
 * this class generates readings like might be generated
 * from a bike GPS unit.
 */
public class BikeRider {

    /**
     * TODO: I think we could really benefit by reading in one file of points at a time.
     * This will save memory and make it easier to deviate from our current path.
     *
     * TODO: Keep track of current location state
     * In order to deviate from the path, we need to know where we are starting from
     * therefore we should have some type keep track of which waypoint we have just left from.
     * When all points have been exhasted, increment the waypoint counter and pull in more points.
     */

    // Store the ID for this rider, more likely than not to be
    // a random number.
    private long rider_id;

    // list of points that a rider will travel through.
    // The first entry will be the initial dock, and the final entry
    // should be the final dock. Entries in between should be decision points,
    // or docks that the rider will visit along the trip. During these points, the
    // rider should check for discounts.
    private int[] waypoints;

    private static int numStations = BikerStreamConstants.STATION_NAMES.length;
    private static int numChoices  = BikerStreamConstants.DP_NAMES.length;

    // Our list of points gathered from the file.
    private LinkedList<Reading> route = new LinkedList();

    private LinkedList<LinkedList<Reading>> legs = new LinkedList<LinkedList<Reading>>();

    // ---------------------------------------------------------------------------------------
    // Constructors

    // Construct an empty rider. use the default file for point/station
    // information.
    public BikeRider(long rider_id) throws IOException {
        this.rider_id = rider_id;


        System.out.println("Generating Route");
        genRandStations();
        routeGenerator();
        System.out.println("Route Generated");
    }

    public BikeRider(long rider_id, int start_station, int end_station, int[] choices) throws IOException {
        this.rider_id = rider_id;

        int[] temp = ArrayUtils.addAll(new int[]{start_station}, choices);
        waypoints  = ArrayUtils.addAll(temp, new int[]{end_station});

        System.out.println("Generating Route");
        try {
            routeGenerator();
        } catch (IOException e) {
            throw new IOException("Could not Generate the given Route" + e);

        }
        System.out.println("Route Generated");
    }

    // Rider ID functions
    // returns the rider's id number
    public long getRiderId() {
        return rider_id;
    }

    // Get the starting/final stations
    public long getStartingStation() {
        return this.waypoints[0];
    }

    public long getFinalStation() {
        return this.waypoints[waypoints.length -1];
    }

    public int[] getWaypoints(){
        return this.waypoints;
    }


    // The reading class contatins the struct that denotes a single gps coordinate.
    public class Reading {
        public double lat;
        public double lon;
        public double alt;

        public Reading(double lat, double lon) {
            this.lat = lat;
            this.lon = lon;
            this.alt = 0;
        }

        public Reading(String lat, String lon) {
            this.lat = Double.parseDouble(lat);
            this.lon = Double.parseDouble(lon);
            this.alt = 0;
        }

        public Reading(double lat, double lon, double alt) {
            this.lat = lat;
            this.lon = lon;
            this.alt = alt;
        }

        @Override
        public String toString(){
            return "Point(" + this.lat + "," + this.lon + ")";
        }


    }

    // ---------------------------------------------------------------------------------------
    // Route generation

    private void genRandStations(){
        Random gen = new Random();
        int numDecisions = gen.nextInt(2);

        // TODO: VERIFY (numStations || numStations -1)
        int start = gen.nextInt(numStations);
        int[] decisions = new int[numDecisions];

        for (int i = 0; i < numDecisions; ++i){
            // TODO: VERIFY (numChoices || numChoices -1)
            decisions[i] = gen.nextInt(numChoices) + numStations;
        }

        int end = start;

        while (end == start) {
            end = gen.nextInt(numStations - 1);
        }

        int[] temp = ArrayUtils.addAll(new int[]{start}, decisions);
        waypoints  = ArrayUtils.addAll(temp, new int[]{end});

        try {
            this.routeGenerator();
        } catch (IOException e) {
            System.out.println("Failed to generate routes");
            System.out.println("Trying again");
            genRandStations();
        }
    }

    private void routeGenerator() throws IOException {
        int numWaypoints = waypoints.length -1;
        System.out.println("Number of waypoints are: " + (numWaypoints+1));
        assert (numWaypoints > 0);

        for (int i = 0; i < numWaypoints; ++i){
            addRoute(waypoints[i], waypoints[i+1]);
        }
    }

    private void addRoute(int begin, int end) throws IOException {
        System.out.println("Generating rout from " + begin + "->" + end);
        String routeN = routeName(begin, end);
        LinkedList<Reading> points = new LinkedList<Reading>();

        try {
            points = readInPoints(routeN);
        } catch (IOException e) {
            throw new IOException("Failed to generate the route between : " + begin + " and " + end + "\n Error: " + e);
        }
        assert(points != null);

        legs.add(points);
    }

    private String routeName(int begin, int end){
        String bp = Integer.toString(begin+1);
        String ep = Integer.toString(end+1);
        String s = BikerStreamConstants.ALL_STOPS[begin];
        String e = BikerStreamConstants.ALL_STOPS[end];
        String ret = ( bp + "_" + ep + "_" + s + "_to_" + e);
        System.out.println("RouteName: " + ret);
        return ret;
    }


    private LinkedList<Reading> readInPoints(String filename) throws IOException {
        System.out.println("Reading in file: " + filename);
        LinkedList<Reading> points = new LinkedList<Reading>();
        Path path = Paths.get(BikerStreamConstants.ROUTES_DIR + "/" + filename);
        BufferedReader reader;

        try {
            reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
            String line = reader.readLine(); //reads in the first line which is the header

            line = reader.readLine(); // First line of data
            while (line != null) {
                String[] fields = line.split(",");
                Reading point = new Reading(fields[3], fields[4]);
                //System.out.println("read in point:" + point);
                points.add(point);
                line = reader.readLine(); // First line of data
            }
        } catch (IOException e) {
            throw new IOException("Error reading in points: " + e);
        }


        return points;
    }

    public LinkedList<Reading> getNextRoute() {
        return legs.remove();
    }

    public boolean hasMorePoints(){
        return legs.size() > 0;
    }

}
