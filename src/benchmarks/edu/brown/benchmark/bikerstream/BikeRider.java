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
 * <p/>
 * I used the clientId as the bikeid (so each client would
 * have its own bikeid) and the lat/lon are completely made
 * up - probably in antartica or something.
 */
public class BikeRider {


    // Store the ID for this rider, more likely than not to be
    // a random number.
    private long rider_id;

    // Whis station are we going to start from. This is determined
    // in the file from which our gpr poinst will come from.
    private int startingStation;

    // Whis station are we going to end at. This is determined
    // in the file from which our gpr poinst will come from.
    private int finalStation;

    // Our list of points gathered from the file.
    private LinkedList<Reading> route = new LinkedList();

    // Construct an empty rider. use the default file for point/station
    // information.
    public BikeRider(long rider_id) throws Exception {
        this.rider_id = rider_id;

        System.out.println("Generating Route");
        genRandStations();
        System.out.println("RouteGenerated");
    }

    public BikeRider(long rider_id, int start_station, int end_station) {
        this.rider_id = rider_id;

        this.startingStation = start_station;
        this.finalStation = end_station;

        System.out.println("Generating Route");
        try {
            this.route = readInPoints(routeName());
        } catch (IOException e) {
            System.out.println("ReGenerating Route");
            genRandStations();
        }
    }

    private void genRandStations(){
        Random gen = new Random();
        int numStations = BikerStreamConstants.STATION_LOCATIONS.length;

        this.startingStation = gen.nextInt(numStations -1);
        this.finalStation = this.startingStation;

        while (this.finalStation == this.startingStation) {
            this.finalStation = gen.nextInt(numStations - 1);
        }
        try {
            this.route = readInPoints(routeName());
        } catch (IOException e) {
            System.out.println("ReGenerating Route");
            genRandStations();
        }
    }


    // Rider ID functions
    // returns the rider's id number
    public long getRiderId() {
        return rider_id;
    }

    // Get the starting/final stations
    public long getStartingStation() {
        return this.startingStation;
    }

    public long getFinalStation() {
        return this.finalStation;
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
    }

    private String routeName(){
        String s = BikerStreamConstants.STATION_LOCATIONS[startingStation+1];
        String e = BikerStreamConstants.STATION_LOCATIONS[finalStation+1];
        return (s + "_to_" + e);
    }


    private LinkedList<Reading> readInPoints(String filename) throws IOException {
        LinkedList<Reading> points = new LinkedList<Reading>();
        Path path = Paths.get(BikerStreamConstants.ROUTES_DIR + "/" + filename);
        BufferedReader reader;

        try {
            reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
            String line = reader.readLine(); //reads in the first line which is the header
            System.out.println("The line reads: " + line);
            assert (line.equals("tripid,routenum,sequence,lon,lat"));
            line = reader.readLine(); // First line of data
            while (line != null) {
                System.out.println("The next line reads: " + line);
                String[] fields = line.split(",");
                Reading point = new Reading(fields[3], fields[4]);
                points.add(point);
                line = reader.readLine(); // First line of data
            }
        } catch (IOException e) {
            throw new IOException(e);
        }


        return points;
    }

    public Reading getPoint() {
        if (hasPoints()) {

            Reading ret = route.pop();
            return ret;
        }

        return null;
    }

    public Reading getPoint(long index) {
        if (hasPoints()) {

            Reading ret = route.pop();
            return ret;
        }

        return null;
    }

    public boolean hasPoints() {
        int lsize = route.size();
        if (lsize > 0)
            return true;
        return false;
    }

    public Reading pedal() {
        return getPoint();
    }

}
