/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
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

//import weka.classifiers.meta.Vote;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.benchmark.bikerstream.BikerStreamConstants;

/* Biker stream client. Very simple - just calls
 * getBikeReading - a few hacks and TODOS where I didn't
 * understand what needed to be done or didn't understand
 * existing code
 */

public class BikerStreamClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BikerStreamClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();


    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete    = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // Bike Readings
    AtomicLong countBikerReadings   = new AtomicLong(0);
    AtomicLong badBikerReadings     = new AtomicLong(0);
    AtomicLong failedInserts        = new AtomicLong(0);

    // Keep track of reservations
    AtomicLong failedReservations   = new AtomicLong(0);
    AtomicLong reservations         = new AtomicLong(0);

    final Callback callback         = new Callback();

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
                    failedInserts.incrementAndGet();
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
            BikeReadingGenerator gps = new BikeReadingGenerator(BikerStreamConstants.DEFAULT_GPS_FILE);
            BikeReadingGenerator.Reading read;

            // Create a random rider_id
            int id = (int) (Math.random() * 100000000);

            client.callProcedure(new SignUpCallback(),"SignUp",id);
            if ((id % 10) != 0) client.callProcedure(new ReserveBikeCallback(),"ReserveBike",id);
            client.callProcedure(new CheckoutCallback(),"CheckoutBike",id);

            while (gps.hasPoints()) {

                read = gps.getPoint();
                client.callProcedure(new RideCallback(),
                                     "RideBike",
                                     id,
                                     read.lat,
                                     read.lon);
                Thread.sleep(BikerStreamConstants.MILI_BETWEEN_GPS_EVENTS);
            }

            if ((id % 20) != 0) client.callProcedure(new ReserveDockCallback(),"ReserveDock",id);
            client.callProcedure(new CheckinCallback(),"CheckinBike",id);

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
            "ReserveBike",
            "CheckoutBike",
            "RideBike",
            "ReserveDock",
            "CheckinBike"
        };
        return (procNames);
    }

    private class SignUpCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 0);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }

    private class ReserveBikeCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 1);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }

    private class CheckoutCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 2);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }

    private class RideCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 3);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }

    private class ReserveDockCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 4);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }

    private class CheckinCallback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 5);
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();

                if (status == BikerStreamConstants.BIKE_RESERVED) {
                    reservations.incrementAndGet();
                }
                else if (status == BikerStreamConstants.BIKE_NOT_RESERVED) {
                    failedReservations.incrementAndGet();
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    }


    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);

            // Keep track of state (optional)
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();
                if (status == BikerStreamConstants.BIKEREADING_SUCCESSFUL) {
                    countBikerReadings.incrementAndGet();
                }
                else if (status == BikerStreamConstants.ERR_INVALID_BIKEREADING) {
                    badBikerReadings.incrementAndGet();
                }
                else {
                    // REALLY SHOULD TOSS AN ERROR HERE
                }
            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                if (clientResponse.getException() != null) {
                    clientResponse.getException().printStackTrace();
                }
                if (debug.val && clientResponse.getStatusString() != null) {
                    LOG.warn(clientResponse.getStatusString());
                }
            }
        }
    } // END CLASS
}
