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
    AtomicLong failedCheckouts = new AtomicLong(0);

    Set usedIds        = Collections.synchronizedSet(new HashSet<Long>());
    Set hasBikeSet     = Collections.synchronizedSet(new HashSet<Long>());
    Set checkedBikeSet = Collections.synchronizedSet(new HashSet<Long>());


    public static void main(String args[]) {
        BenchmarkComponent.main(BikerStreamClient.class, args, false);
    };

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

        // Bike reading generator
        try {
            Random gen = new Random();

            // Generate a random Rider ID
            // Make sure it hasn't already been used by checking it against the hash map

            long rider_id = 1;

            while (usedIds.contains(rider_id = (long) gen.nextInt(1000))) {}
            usedIds.add(rider_id);

            // Create a new Rider Struct
            //BikeRider rider = new BikeRider(rider_id, 1, 2 , new int[]{});
            BikeRider rider = new BikeRider(rider_id);

            // Sign the rider up, by sticking rider information into the DB
            client.callProcedure(new BikerCallback(0,rider_id),
                    "SignUp",
                    rider.getRiderId());

            // Checkout a bike from the Biker's initial station
            client.callProcedure(new BikerCallback(1, rider_id),
                    "CheckoutBike",
                    rider.getRiderId(),
                    rider.getStartingStation());


            // Get ready for getting the Biker's route data
            LinkedList<Reading> route;
            Reading point;

            // The biker trip is deivided into legs. after each leg the biker will stop to see if any nearby stations
            // are providing discounts. If so the biker should deviate toward that station. If there are no more legs
            // available then the rider is considered to be at the final destination and should attempt to checkin the
            // bike.
            while ((route = rider.getNextRoute()) != null) {

                // As long as there are points in the current leg, put them into the data base at a rate specified by
                // the MILI_BETWEEN_GPS_EVENTS Constant.
                while ((point = route.poll()) != null){
                    client.callProcedure(new BikerCallback(2),
                            "RideBike",
                            rider.getRiderId(),
                            point.lat,
                            point.lon);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}
                }

                // Check to see if we have more legs. Thios tells us that we aer somewhere in the middle of a trip, and
                // there may be a station close by offering dicounts to us.
                // TODO: check for discounts and alter route accordingly
                if (rider.hasMorePoints()){
                }
            }

            // When all legs of the journey are finished. Attempt to park the bike. The callback will handle whether or
            //not we were successfull.
            client.callProcedure(new BikerCallback(3, rider_id),
                    "CheckinBike",
                    rider.getRiderId(),
                    rider.getFinalStation());

            // The handle will insert our bike id into the hashmap when we are successful.
            while (!(checkedBikeSet.contains(rider_id))){

                // Deviate course if we did not appear in the hashmap. We will receive a new route to the next station.
                route = rider.deviateRandomly();

                // Put those points into the DB
                while ((point = route.poll()) != null){
                    client.callProcedure(new BikerCallback(2),
                            "RideBike",
                            rider.getRiderId(),
                            point.lat,
                            point.lon);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}
                }

                // Try again to checkin the bike. Loop to check for success
                client.callProcedure(new BikerCallback(3, rider_id),
                        "CheckinBike",
                        rider.getRiderId(),
                        rider.getFinalStation());
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
            "SignUp",
            "CheckoutBike",
            "RideBike",
            "CheckinBike",
            "GetDiscount"
        };
        return (procNames);
    }



    private class BikerCallback implements ProcedureCallback {

        private long rider_id;
        private int callback;

        public BikerCallback(int callback_id){
            this.rider_id = 0;
            this.callback = callback_id;
        }

        public BikerCallback(int callback_id, long rider_id){
            this.rider_id = rider_id;
            this.callback = callback_id;
        }

        @Override
        public void clientCallback(ClientResponse cr){

            if (callback == BikerStreamConstants.SignupCallback){
                incrementTransactionCounter(cr, 0);
            }//if

            if (callback == BikerStreamConstants.CheckoutCallback){
                incrementTransactionCounter(cr, 1);

                VoltTable results[] = cr.getResults();

                if (cr.getException() == null)
                    hasBikeSet.add(rider_id);
                else
                    failedCheckouts.incrementAndGet();

            }//if

            if (callback == BikerStreamConstants.RideBikeCallback){
                incrementTransactionCounter(cr, 2);
            }//if

            if (callback == BikerStreamConstants.CheckinCallback){
                incrementTransactionCounter(cr, 3);

                if (cr.getException() == null)
                    checkedBikeSet.add(rider_id);
                else
                    failedCheckouts.incrementAndGet();

            } //if
        }
    }

}
