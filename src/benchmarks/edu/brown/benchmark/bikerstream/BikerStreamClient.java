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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.lang.Object;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.types.TimestampType;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

import edu.brown.benchmark.bikerstream.BikerStreamConstants;
import edu.brown.benchmark.bikerstream.BikeRider.*;

public class BikerStreamClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BikerStreamClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    // Bike Readings
    AtomicLong failedPointInsert    = new AtomicLong(0);

    AtomicLong failure              = new AtomicLong(0);
    AtomicLong success              = new AtomicLong(0);

    public static void main(String args[]) {
        BenchmarkComponent.main(BikerStreamClient.class, args, false);
    }

    public BikerStreamClient(String args[]) {
        super(args);
        //int numContestants = VoterWinSStoreUtil.getScaledNumContestants(this.getScaleFactor());
        // this.bikes= new BikeReadingGenerator(this.getClientId());
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

        // gnerate a client class struct for the current thread
        Client client = this.getClientHandle();


        // Bike reading generator
        try {

            // Generate a random Rider ID
            long rider_id = (int) (Math.random() * 100000);

            // Create a new Rider Struct
            BikeRider rider = new BikeRider(rider_id, "src/benchmarks/edu/brown/benchmark/bikerstream/BSSS");

            long startStation = (long) rider.getStartingStation();
            long endStation   = (long) rider.getFinalStation();

            // Sign the rider up, by sticking rider information into the DB
            try {
                client.callProcedure(new SignUpCallback(), "SignUp",  rider.getRiderId());
            } catch (Exception e) {
                return false;
            }

            try {
                client.callProcedure(new TestCallback(5), "LogRiderTrip", rider_id, startStation, endStation);
            } catch (Exception e) {
                return false;
            }

            try {
                client.callProcedure(new TestCallback(4), "TestProcedure");
            } catch (Exception e) {
                return false;
            }

            try {
                client.callProcedure(new CheckoutCallback(), "CheckoutBike",  rider.getRiderId(), rider.getStartingStation());
            } catch (Exception e) {
                failure.incrementAndGet();
            }

            Reading point;
            long lastTime = (new TimestampType()).getMSTime();
            long time_t;
            while (rider.hasPoints()) {

                point = rider.getPoint();
                client.callProcedure(new RideCallback(), "RideBike",  rider.getRiderId(), point.lat, point.lon);

                // Sit and spin for specified tome, to ensure spacing of gps points
                while ((time_t = (new TimestampType()).getMSTime()) - lastTime < BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS) {}

            }

            try {
                client.callProcedure(new CheckinCallback(), "CheckinBike",  rider.getRiderId(), rider.getFinalStation());
            } catch (Exception e) {
                failure.incrementAndGet();
            }

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
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);

        }

    } // END CLASS

    private class CheckoutCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 1);

        }

    } // END CLASS

    private class RideCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 2);

        }

    } // END CLASS

    private class CheckinCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 3);

        }

    } // END CLASS

    private class TestCallback implements ProcedureCallback {

        private int procNum;

        public TestCallback(int numProc) {
            procNum = numProc;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, procNum);

        }


    } // END CLASS
}
