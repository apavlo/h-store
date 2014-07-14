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

package edu.brown.benchmark.voterdemosstorewinsp1scheduler;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import weka.classifiers.meta.Vote;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.voterdemosstorewinsp1scheduler.procedures.GenerateLeaderboard;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class VoterDemoSStoreWinSP1SchedulerClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(VoterDemoSStoreWinSP1SchedulerClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static long lastTime;
    private static int timestamp;

    // Phone number generator
    PhoneCallGenerator switchboard;

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voterdemosstorewinsp1 benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(VoterDemoSStoreWinSP1SchedulerClient.class, args, false);
    }

    public VoterDemoSStoreWinSP1SchedulerClient(String args[]) {
        super(args);
        int numContestants = VoterDemoSStoreWinSP1SchedulerUtil.getScaledNumContestants(this.getScaleFactor());
        this.switchboard = new PhoneCallGenerator(this.getClientId(), numContestants);
        lastTime = System.nanoTime();
        timestamp = 0;
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                // synchronously call the "Vote" procedure
                try {
                    runOnce();
                } catch (Exception e) {
                    failedVotes.incrementAndGet();
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        // Get the next phone call
    	if(System.nanoTime() - lastTime >= VoterDemoSStoreWinSP1SchedulerConstants.TS_DURATION)
        {
        	lastTime = System.nanoTime();
        	timestamp++;
        }
    	
        PhoneCallGenerator.PhoneCall call = switchboard.receive();

        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
                                                "Vote",
                                                call.voteId,
                                                call.phoneNumber,
                                                call.contestantNumber,
                                                VoterDemoSStoreWinSP1SchedulerConstants.MAX_VOTES,
                                                timestamp);
        //System.out.println(String.format("call vote - %d", (int)call.voteId));
//        boolean response = client.callStreamProcedure(callback,
//                "Vote",
//                (int)call.voteId,
//                call.voteId,
//                call.phoneNumber,
//                call.contestantNumber,
//                VoterDemoSStoreWinSP1SchedulerConstants.MAX_VOTES,
//                timestamp);
        
        return response;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
            Vote.class.getSimpleName(),
            GenerateLeaderboard.class.getSimpleName()
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
                if (status == VoterDemoSStoreWinSP1SchedulerConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                    //incrementTransactionCounter(clientResponse, 1);
                }
                else if (status == VoterDemoSStoreWinSP1SchedulerConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == VoterDemoSStoreWinSP1SchedulerConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
                    badVoteCountVotes.incrementAndGet();
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
