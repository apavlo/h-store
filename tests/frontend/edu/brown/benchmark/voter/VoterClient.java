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

package edu.brown.benchmark.voter;

import java.util.Timer;
import java.util.Random; 
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.io.IOException; 

import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.NoConnectionsException;

import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.benchmark.BenchmarkComponent;


public class VoterClient extends BenchmarkComponent {
	
    // handy, rather than typing this out several times
    static final String HORIZONTAL_RULE =
			"----------" + "----------" + "----------" + "----------" +
			"----------" + "----------" + "----------" + "----------" + "\n";
	
	
    // Reference to the database connection we will use
     Client client;
	
    // Phone number generator
    PhoneCallGenerator switchboard;
	
    // Timer for periodic stats printing
    Timer timer;
	
    // Benchmark start time
    long benchmarkStartTS;
	
    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);
	
	
    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);
	
	
    public static void main(String args[]) {
        BenchmarkComponent.main(VoterClient.class, args, false);
    }

    public VoterClient(String args[]) {
        super(args);
		
		switchboard = new PhoneCallGenerator(VoterConstants.NUM_CONTESTANTS); 
    }

    @Override
    public void runLoop() {
        try {
            client = this.getClientHandle();
            Random rand = new Random();
			
            while (true) {				
				
				// Get the next phone call
                PhoneCallGenerator.PhoneCall call = switchboard.receive();
				
                // synchronously call the "Vote" procedure
                try {
					runOnce(); 
                }
                catch (Exception e) {
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
		
		boolean response = true; 
		client = this.getClientHandle();
		
		// Get the next phone call
		PhoneCallGenerator.PhoneCall call = switchboard.receive();
		
		try 
		{			
			Callback callback = new Callback();
			response = client.callProcedure(callback, 
											"Vote",
											call.phoneNumber,
											call.contestantNumber,
											VoterConstants.MAX_VOTES);
			 
			if(response == false)
				throw new IOException(); 
			
		}
		catch(IOException e) {
			throw e; 
		}
		catch (Exception e) {
			e.printStackTrace(); 
		}
		
		return response; 
	}
	
	@Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[1];
		procNames[0] = "Vote"; 
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        public Callback() {
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
        }
    } // END CLASS
}
