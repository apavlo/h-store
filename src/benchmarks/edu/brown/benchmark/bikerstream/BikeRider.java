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
import org.voltdb.types.TimestampType;

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

    // Store the ID for this rider, more likely than not to be
    // a random number.
    private long rider_id;

    /**
     * list of points that a rider will travel through.
     * The first entry will be the initial dock, and the final entry
     * should be the final dock. Entries in between should be decision points,
     * or docks that the rider will visit along the trip. During these points, the
     * rider should check for discounts.
      */
    private int[] waypoints;

    /**
     * CurrentPoint, is an index into the waypoints denoting where the rider is currently
     * In their journey. It should always point to the point of origin, meaning that it should point
     * To the initial dock, until the rider has exhasted all GPS points for the current leg. at which
     * point it will immediately switch to the next point once the next set of points is requested.
      */
    private int currentIndex = 0;

    /**
     * isAnomolicRider.
     * Is this rider going to be an anomoly?
     */
    private boolean anomolyRider = false;

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
        setAnomoly();

        //comment("Generating Route");
        genRandStations();
        //comment("Route Generated");
    }

    public BikeRider(long rider_id, int start) throws IOException {
        this.rider_id = rider_id;
        setAnomoly();

        //comment("Generating Route from station: " + start);
        genRandStations(start);
        //comment("Route Generated");
    }

    public BikeRider(long rider_id, int start_station, int end_station, int[] choices) throws IOException {
        this.rider_id = rider_id;
        this.currentIndex = 0;
        setAnomoly();

        //comment("Generating Route");
        int[] temp = ArrayUtils.addAll(new int[]{start_station}, choices);
        waypoints  = ArrayUtils.addAll(temp, new int[]{end_station});
        //comment("Route Generated");
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

    public void setAnomoly() {
        int chance = (new Random()).nextInt(BikerStreamConstants.ANOMOLY_CHANCE);
        if (chance == 0){
            //System.out.println("Rider: " + this.rider_id + ": is an Anomaly");
            this.anomolyRider = true;
        }
    }

    public boolean getAnomoly() {
        return this.anomolyRider;
    }

    public void comment(String str){
        System.out.println("Rider: " + rider_id + " : " + str);
    }

    public void deviateRandomly() throws IOException {
        Random gen = new Random();
        int currentLocation = waypoints[currentIndex];

        int stationIndex;
        while ((stationIndex = gen.nextInt(numStations)) == currentLocation) {}

        comment("Deviating route to station: " + 
                    BikerStreamConstants.LOGICAL_NAMES[stationIndex]);
        waypoints = new int[] {currentLocation, stationIndex};
        currentIndex = 0;
    }

    public void deviateDirectly(int stationIndex) throws IOException {
        Random gen = new Random();
        int currentLocation = waypoints[currentIndex];

        if (currentLocation == stationIndex) {
            deviateRandomly();
            return;
        }

        comment("Deviating route to station: " + 
                    BikerStreamConstants.LOGICAL_NAMES[stationIndex]);
        waypoints = new int[] {currentLocation, stationIndex};
        currentIndex = 0;
    }


    // The reading class contains the struct that denotes a single gps coordinate.
    public class Reading {
        public double lat;
        public double lon;

        public Reading(double lon, double lat) {
            this.lat = lat;
            this.lon = lon;
        }

        public Reading(String lon, String lat) {
            this.lat = Double.parseDouble(lat);
            this.lon = Double.parseDouble(lon);
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
        int start = gen.nextInt(numStations);
        genRandStations(start);
    }

    private void genRandStations(int start){
        Random gen = new Random();
        int numDecisions = gen.nextInt(3);

        // TODO: VERIFY (numStations || numStations -1)
        int[] decisions = new int[numDecisions];

        for (int i = 0; i < numDecisions; ++i){
            // TODO: VERIFY (numChoices || numChoices -1)
            int nextStation = gen.nextInt(numChoices) + numStations;
            while (i != 0 && ((nextStation = gen.nextInt(numChoices) + numStations) == decisions[i-1])){}
            decisions[i] = nextStation;
        }

        int end = start;

        while (end == start) {
            end = gen.nextInt(numStations - 1);
        }

        int[] temp = ArrayUtils.addAll(new int[]{start}, decisions);
        waypoints  = ArrayUtils.addAll(temp, new int[]{end});

    }

    private String routeName(int begin, int end){
        String bp = Integer.toString(begin+1);
        String ep = Integer.toString(end+1);
        String s = BikerStreamConstants.ALL_STOPS[begin];
        String e = BikerStreamConstants.ALL_STOPS[end];
        String ret = ( bp + "_" + ep + "_" + s + "_to_" + e);
        //System.out.println("RouteName: " + ret);
        return ret;
    }

    private LinkedList<Reading> readInPoints(String filename) throws IOException {
        //System.out.println("Reading in file: " + filename);
        LinkedList<Reading> points = new LinkedList<Reading>();
        Path path = Paths.get(BikerStreamConstants.ROUTES_DIR + "/" + filename);
        BufferedReader reader;
        String line = "No line read in";

        try {
            reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
            line = reader.readLine(); // First line of data
            while (line != null) {
                String[] fields = line.split(",");
                Reading point = new Reading(fields[3], fields[4]);
                //System.out.println("read in point:" + point);
                points.add(point);
                line = reader.readLine(); // First line of data
            }
        } catch (IndexOutOfBoundsException e){
            comment("trouble with line: " + line );
        } catch (IOException e) {
            throw new IOException("Error reading in points: " + e);
        }


        return points;
    }

    private LinkedList<Reading> skipNPoints(LinkedList<Reading> points) {
        LinkedList<Reading> newRoute = new LinkedList<Reading>();
        int listLength = points.size();
        for (int i=0; i < listLength; i += BikerStreamConstants.ANOMOLY_SKIP){
            newRoute.add(points.get(i));
        }
        return newRoute;
    }

    public LinkedList<Reading> getNextRoute() throws IOException {

        if (this.hasMorePoints()){
            int thisStation = waypoints[currentIndex];
            int nextStation = waypoints[currentIndex +1];
            String file = routeName(thisStation, nextStation);
            ++currentIndex;
            comment("Heading from " + 
                    BikerStreamConstants.LOGICAL_NAMES[thisStation]
                    + " to " +
                    BikerStreamConstants.LOGICAL_NAMES[nextStation]);

            LinkedList<Reading> points = readInPoints(file);
            return anomolyRider ? skipNPoints(points) : points;

        }
        return null;
    }

    public boolean hasMorePoints() {
       return currentIndex +1 < waypoints.length;
    }
}
