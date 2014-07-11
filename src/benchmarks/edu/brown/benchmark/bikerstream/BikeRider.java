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

import java.util.Random;
import edu.brown.utils.MathUtil;
import java.util.LinkedList;
import java.util.Scanner;
import java.io.File;


  /**
   * Class to generate readings from bikes. Idea is that
   * this class generates readings like might be generated
   * from a bike GPS unit.

   * I used the clientId as the bikeid (so each client would
   * have its own bikeid) and the lat/lon are completely made
   * up - probably in antartica or something.
   */
public class BikeRider {


    // Store the ID for this rider, more likely than not to be
    // a random number.
    private long rider_id;

    // TODO: Do I need this?
    // Maybe we will store a bike_id but this may be unnessesary.
    private long current_bike;

    // TODO: Do I need this?
    // Do we posess a reservation?
    private long has_reservation;

    // Whis station are we going to start from. This is determined
    // in the file from which our gpr poinst will come from.
    private long startingStation;

    // Whis station are we going to end at. This is determined
    // in the file from which our gpr poinst will come from.
    private long finalStation;

    // Our list of points gathered from the file.
    private LinkedList<Reading> path = new LinkedList();

    // Construct an empty rider. use the default file for point/station
    // information.
    public BikeRider(long rider_id) throws Exception {
        this.rider_id        = rider_id;
        this.current_bike    = -1;
        this.has_reservation = -1;

        this.startingStation = -1;
        this.finalStation    = -1;

        makePoints(BikerStreamConstants.DEFAULT_GPS_FILE);
    }

    // construct a rider, and use a specific file for gps/station information.
    public BikeRider(long rider_id, String filename) throws Exception {
        this.rider_id        = rider_id;
        this.current_bike    = -1;
        this.has_reservation = -1;

        this.startingStation = -1;
        this.finalStation    = -1;

        makePoints(filename);
    }

    // Rider ID functions
    // returns the rider's id number
    public long getRiderId() { return rider_id; }

    // Bike Functions
    public long getcurrentBikeID() { return current_bike == -1 ? null : current_bike; }

    public void setCurrentBikeID(long b_id) {
        current_bike = b_id;
    }

    // Get the starting/final stations
    public long getStartingStation() { return this.startingStation; }
    public long getFinalStation() { return this.finalStation; }


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

        public Reading(double lat, double lon, double alt) {
            this.lat = lat;
            this.lon = lon;
            this.alt = alt;
        }
    }

    /*
     * Read a File containing the GPS events for a ride and store
     * them in an internall Linked List. Function has no return.
     * Only used for it's side-effects used to accumulate the gps events.
     * this assumes that the file is a list of \n delimited points, with
     * lat, long, and altitude delimited by whitespace.
     */
    public void makePoints(String filename) throws Exception {

        File    f = new File(filename);
        Scanner s = new Scanner(f);

        try {
            startingStation = s.nextInt();
            finalStation = s.nextInt();

        } catch (Exception e) {
        }
        try {
            double lat_t;
            double lon_t;
            double alt_t;

            while (s.hasNext()){
                lat_t = s.nextDouble();
                lon_t = s.nextDouble();
                alt_t = s.nextDouble();
                path.add(new Reading(lat_t, lon_t));
            }
        }
        catch (Exception e) {
            System.out.println("File Not Found" + e);
        }
        finally {
           s.close();
        }
    }

    public Reading getPoint() {
        if(hasPoints()) {

            Reading ret = path.pop();
            return ret;
        }

        return null;
    }

    public boolean hasPoints() {
        int lsize = path.size();
        if (lsize > 0)
            return true;
        return false;
    }


    /**
     * Receives/generates a simulated voting call
     *
     * @return Call details (calling number and contestant to whom the vote is given)
     */
    public Reading pedal()
    {
        return getPoint();
    }

}
