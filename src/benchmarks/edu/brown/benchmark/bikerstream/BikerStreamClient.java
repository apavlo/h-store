/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *  Portland State University                                              *
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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.bikerstream.BikeRider.*;

public class BikerStreamClient extends BenchmarkComponent {

    // Bike Readings
    AtomicLong failure = new AtomicLong(0);

    public static void main(String args[]) {
        BenchmarkComponent.main(BikerStreamClient.class, args, false);
    }

    public BikerStreamClient(String args[]) {
        super(args);
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                try {
                    runOnce();
                } catch (Exception e) {
                    failure.incrementAndGet();
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {

        // generate a client class for the current thread
        Client client = this.getClientHandle();
        ClientResponse cr;

        // Bike reading generator
        try {

            // Signup a random rider and get the rider's id
            cr = client.callProcedure("SignUpRand");
            incrementTransactionCounter(cr, 0);
            long rider_id;
            if (cr.getException() == null)
                rider_id = cr.getResults()[0].asScalarLong();
            else
                return false;

            // Create the rider using the id returned from the signup procedure
            BikeRider rider = new BikeRider(rider_id);

            // Checkout a bike from the Biker's initial station
            // If the checkout fails then return, there must not be any bikes available at that station
            cr = client.callProcedure("CheckoutBike", rider.getRiderId(), rider.getStartingStation());
            incrementTransactionCounter(cr, 1);
            if (cr.getException() != null)
                return false;

            // Get ready for getting the Biker's route data
            LinkedList<Reading> route;
            Reading point;

            // The biker trip is divided into legs. after each leg the biker will stop to see if any nearby stations
            // are providing discounts. If so the biker should deviate toward that station. If there are no more legs
            // available then the rider is considered to be at the final destination and should attempt to checkin the
            // bike.
            while ((route = rider.getNextRoute()) != null) {

                // As long as there are points in the current leg, put them into the data base at a rate specified by
                // the MILI_BETWEEN_GPS_EVENTS Constant.
                while ((point = route.poll()) != null) {
                    cr = client.callProcedure("RideBike", rider.getRiderId(), point.lat, point.lon);
                    incrementTransactionCounter(cr, 1);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {
                    }
                }

                // Check to see if we have more legs. This tells us that we are somewhere in the middle of a trip, and
                // there may be a station close by offering discounts to us.
                // TODO: check for discounts and alter route accordingly
                if (rider.hasMorePoints()) {
                    cr = client.callProcedure("GetNearDiscounts", rider_id);

                    if (cr.getException() == null){

                        VoltTable table = cr.getResults()[0];
                        int numDiscounts = table.getRowCount();

                        // Need to acquire the discount, if fail, try the next one if available
                        for (int i=0; i < numDiscounts; ++i) {
                            cr = client.callProcedure("AcceptDiscount", rider_id, table.fetchRow(i).getLong("station_id"));
                            if (cr.getException() == null){
                                rider.deviateDirectly((int) cr.getResults()[0].asScalarLong());
                                break;
                            }
                        }
                    }
                }
            }

            // When all legs of the journey are finished. Attempt to park the bike. The callback will handle whether or
            //not we were successful.
            cr = client.callProcedure("CheckinBike", rider.getRiderId(), rider.getFinalStation());
            incrementTransactionCounter(cr, 3);
            while (cr.getException() != null) {

                rider.deviateRandomly();
                route = rider.getNextRoute();

                // Put those points into the DB
                while ((point = route.poll()) != null) {
                    cr = client.callProcedure("RideBike", rider.getRiderId(), point.lat, point.lon);
                    incrementTransactionCounter(cr, 2);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}
                }

                // Try again to checkin the bike. Loop to check for success
                cr = client.callProcedure("CheckinBike", rider.getRiderId(), rider.getFinalStation());
                incrementTransactionCounter(cr, 3);

            }
        } catch (NoSuchElementException e) {
            e.printStackTrace();
            return false;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
                "Riders signed up",
                "Bikes checked out",
                "Points added to the DB",
                "Bikes checked in",
        };
        return (procNames);
    }

}

