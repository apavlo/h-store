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

package edu.brown.benchmark.biker;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class BikerClient extends BenchmarkComponent {

    private static final Logger LOG = Logger.getLogger(BikerClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    AtomicLong ride    = new AtomicLong(0);
    AtomicLong noBikes = new AtomicLong(0);
    AtomicLong noDocks = new AtomicLong(0);
    AtomicLong cin     = new AtomicLong(0);
    AtomicLong cout    = new AtomicLong(0);
    AtomicLong fails   = new AtomicLong(0);

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(BikerClient.class, args, false);
    }

    public BikerClient(String args[]) {
        super(args);
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                try {
                    runOnce();
                } catch (Exception e) {
                    fails.incrementAndGet();
                }

            }
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback, "RideABike");
        return (response);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
            "RideABike"
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

                if (status == BikerConstants.NO_BIKES_AVAILIBLE) {
                    noBikes.incrementAndGet();
                }

                else if (status == BikerConstants.NO_DOCKS_AVAILIBLE) {
                    noDocks.incrementAndGet();
                }

                else if (status == BikerConstants.RIDE_SUCCESS) {
                    ride.incrementAndGet();
                }

                else if (status == BikerConstants.CHECKOUT_ERROR) {
                    cout.incrementAndGet();
                }

                else if (status == BikerConstants.CHECKIN_ERROR) {
                    cin.incrementAndGet();
                }

            }
            else if (clientResponse.getStatus() == Status.ABORT_UNEXPECTED) {
                LOG.warn(clientResponse.getStatusString());
            }
            else if (clientResponse.getStatus() == Status.ABORT_REJECT) {
                LOG.warn(clientResponse.getStatusString());
            }
        }
    } // END CLASS
}
