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
import edu.brown.benchmark.bikerstream.BikeRider;

/* Biker stream client. Very simple - just calls
 * getBikeReading - a few hacks and TODOS where I didn't
 * understand what needed to be done or didn't understand
 * existing code
 */

public class BikerStreamClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(BikerStreamClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    // Bike Readings
    AtomicLong failure              = new AtomicLong(0);
    AtomicLong success              = new AtomicLong(0);

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

        Callback callback = new Callback();

        // Bike reading generator
        try {

            int rider_id = (int) (Math.random() * 100000);

            BikeRider rider = new BikeRider(rider_id);

            client.callProcedure(callback, "SignUp",  rider.getRiderId());

            try {
                client.callProcedure(callback, "CheckoutBike",  rider.getRiderId(), rider.getStartingStation());
            } catch (Exception e) {
                failure.incrementAndGet();
            }

            try {
                client.callProcedure(callback, "CheckinBike",  rider.getRiderId(), rider.getFinalStation());
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
            "CheckinBike"
        };
        return (procNames);
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

                if (status == BikerStreamConstants.INSERT_RIDER_SUCCESS) {
                    success.incrementAndGet();
                }

                else if (status == BikerStreamConstants.INSERT_RIDER_FAILED) {
                    failure.incrementAndGet();
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
