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
            client.callProcedure(new SignUpCallback(), "SignUp",  rider.getRiderId());

            client.callProcedure(new CheckoutCallback(), "CheckoutBike",  rider.getRiderId(), rider.getStartingStation());

            /*
            long startStation = rider.getStartingStation();
            long endStation   = rider.getFinalStation();

            client.callProcedure(new TestCallback(5), "LogRiderTrip", rider_id, startStation, endStation);
            */

            LinkedList<Reading> route;
            Reading point;

            while ((route = rider.getNextRoute()) != null) {

                while ((point = route.poll()) != null){
                    client.callProcedure(new RideCallback(), "RideBike",  rider.getRiderId(), point.lat, point.lon);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}
                }

                // Check to see if we have more legs
                // TODO: check for discounts and alter route accordingly
                if (rider.hasMorePoints()){
                }
            }

            client.callProcedure(new CheckinCallback(), "CheckinBike", rider.getRiderId(), rider.getFinalStation());


            while (!(checkedBikeSet.contains(rider_id))){
                route = rider.deviateDirectly(1);
                //assert (route != null);

                while ((point = route.poll()) != null){
                    client.callProcedure(new RideCallback(), "RideBike",  rider.getRiderId(), point.lat, point.lon);
                    long lastTime = (new TimestampType()).getMSTime();
                    while (((new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}
                }

                client.callProcedure(new CheckinCallback(), "CheckinBike", rider.getRiderId(), rider.getFinalStation());

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
            "TestProcedure",
            "LogRiderTrip"
        };
        return (procNames);
    }


    private class SignUpCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 0);

        }

    } // END CLASS

    private class CheckoutCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 1);

            VoltTable results[] = clientResponse.getResults();
            assert (results.length == 1);
            long rider_id = results[0].asScalarLong();

            if (rider_id > -1){
                hasBikeSet.add(rider_id);
            } else {
                failedCheckouts.incrementAndGet();
            }


        }

    } // END CLASS

    private class RideCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 2);

        }

    } // END CLASS

    private class CheckinCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 3);

            VoltTable results[] = clientResponse.getResults();
            if (clientResponse.hasDebug()) {
                return;
            }

            //assert (results.length == 1);
            long rider_id = results[0].asScalarLong();

            if (rider_id > -1){
                checkedBikeSet.add(rider_id);
            } else {
                failedCheckouts.incrementAndGet();
            }

        }

    } // END CLASS

}
