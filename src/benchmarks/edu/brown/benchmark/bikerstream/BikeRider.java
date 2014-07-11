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

import java.awt.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.LinkedList;
import java.util.Scanner;
import java.util.InputMismatchException;

import java.io.File;


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
    private long startingStation;

    // Whis station are we going to end at. This is determined
    // in the file from which our gpr poinst will come from.
    private long finalStation;

    // Our list of points gathered from the file.
    private LinkedList<Reading> initPath = new LinkedList();
    private LinkedList<Reading> restPath = new LinkedList();

    private LinkedList<Long> altStations = new LinkedList();
    private LinkedList<LinkedList<Reading>> altPaths = new LinkedList();

    // Construct an empty rider. use the default file for point/station
    // information.
    public BikeRider(long rider_id) throws Exception {
        this.rider_id = rider_id;

        this.startingStation = -1;
        this.finalStation = -1;

        makePoints(BikerStreamConstants.DEFAULT_GPS_FILE);
    }

    public BikeRider(long rider_id, int start_station, int end_station) {
        this.rider_id = rider_id;

        this.startingStation = start_station;
        this.finalStation = end_station;
    }


    // construct a rider, and use a specific file for gps/station information.
    public BikeRider(long rider_id, String filename) throws Exception {
        this.rider_id = rider_id;

        this.startingStation = -1;
        this.finalStation = -1;

        makePoints(filename);
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

    private Reading[] readInPoints(String filename){
        LinkedList<Reading> points = new LinkedList<Reading>();
        Path path = Paths.get(BikerStreamConstants.ROUTES_DIR + filename);
        BufferedReader reader;

        try {
            reader = Files.newBufferedReader(path, StandardCharsets.US_ASCII);
        } catch (IOException e) {
            System.out.println(e);
            return null;
        }

        try{
            String line = reader.readLine(); //reads in the first line which is the header
            assert (line.equals("tripid,routenum,sequence,lon,lat"));
            line = reader.readLine(); // First line of data
            while (line != null){
                String[] fields = line.split(",");
                Reading point = new Reading(fields[2], fields[3]);
                points.add(point);
            }
        } catch (IOException e){
            System.out.println(e);
            return null;
        }
        return points.toArray(new Reading[points.size()]);
    }

    /*
     * Read a File containing the GPS events for a ride and store
     * them in an internal Linked List. Function has no return.
     * Only used for it's side-effects used to accumulate the gps events.
     * this assumes that the file is a list of \n delimited points, with
     * lat, long, and altitude delimited by whitespace.
     */
    public void makePoints(String filename) throws Exception {

        int lineCount = 0;
        String stage = "INITIAL STATIONS";
        File f = new File(filename);
        Scanner s = new Scanner(f);

        try {
            ++lineCount;
            startingStation = s.nextInt();
            ++lineCount;
            finalStation = s.nextInt();

            double lat_t;
            double lon_t;
            double alt_t;

            stage = "INIT PATH";
            while (!(s.hasNext("BREAK"))) {
                ++lineCount;
                lat_t = s.nextDouble();
                lon_t = s.nextDouble();
                alt_t = s.nextDouble();
                initPath.add(new Reading(lat_t, lon_t));
            }
            stage = "SKIPPING 1";
            ++lineCount;
            s.next("BREAK");

            stage = "REST PATH";
            while (!(s.hasNext("BREAK"))) {
                lat_t = s.nextDouble();
                lon_t = s.nextDouble();
                alt_t = s.nextDouble();
                restPath.add(new Reading(lat_t, lon_t));
                ++lineCount;
            }
            stage = "SKIPPING 2";
            ++lineCount;
            s.next("BREAK");

            while (!(s.hasNext("DONE"))) {

                ++lineCount;
                stage = "ALT STATION";

                altStations.add((long) s.nextInt());
                stage = "ALT PATH";

                LinkedList<Reading> altPathGen = new LinkedList<Reading>();

                while (!(s.hasNext("BREAK")) && !(s.hasNext("DONE"))) {
                    ++lineCount;
                    lat_t = s.nextDouble();
                    lon_t = s.nextDouble();
                    alt_t = s.nextDouble();
                    altPathGen.add(new Reading(lat_t, lon_t));

                }
                if (s.hasNext("BREAK"))
                    s.next("BREAK");
                altPaths.add(altPathGen);
            }

        } catch (InputMismatchException e) {
            String next = s.nextLine();
            System.out.println("InputMismatch on line: " + lineCount +
                    " At stage :" + stage + " : " + next + " : " + e);
        } catch (Exception e) {
            System.out.println("File Not Found" + e);
        } finally {
            s.close();
        }
    }

    public Reading getPoint() {
        if (hasPoints()) {

            Reading ret = initPath.pop();
            return ret;
        }

        return null;
    }

    public Reading getPoint(long index) {
        if (hasPoints()) {

            Reading ret = initPath.pop();
            return ret;
        }

        return null;
    }

    public boolean hasPoints() {
        int lsize = initPath.size();
        if (lsize > 0)
            return true;
        return false;
    }

    public Reading pedal() {
        return getPoint();
    }

}
