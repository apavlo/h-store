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

package edu.brown.benchmark.voterdemosstoreanother;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import weka.classifiers.meta.Vote;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.voterdemosstoreanother.procedures.GenerateLeaderboard;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class VoterDemoSStoreAnotherClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(VoterDemoSStoreAnotherClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static long lastTime;
    private static int timestamp;

    // Phone number generator
    //PhoneCallGenerator switchboard;
    edu.brown.stream.VoteGenClient switchboard;
    
    public static Object lock = new Object();

    private String stat_filename;
    public static AtomicLong count = new AtomicLong(0);
    public static AtomicLong fixnum = new AtomicLong(10000l);

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voterdemosstore benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    final Callback callback = new Callback();
    final StatisticCallback stat_callback =  new StatisticCallback();
    
    public static void main(String args[]) {
        BenchmarkComponent.main(VoterDemoSStoreAnotherClient.class, args, false);
    }

    public VoterDemoSStoreAnotherClient(String args[]) throws UnknownHostException, IOException {
        super(args);
        int numContestants = VoterDemoSStoreAnotherUtil.getScaledNumContestants(this.getScaleFactor());
        //this.switchboard = new PhoneCallGenerator(this.getClientId(), numContestants);
        this.switchboard = new edu.brown.stream.VoteGenClient();
        
        lastTime = System.nanoTime();
        timestamp = 0;
        
        stat_filename = "voterdemosstoreanother" + ".txt";
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
    	if(System.nanoTime() - lastTime >= 1000000000)
        {
        	lastTime = System.nanoTime();
        	timestamp++;
        }
    	
    	boolean response = true;
    	
        synchronized (lock) {  
            //PhoneCallGenerator.PhoneCall call = switchboard.receive();
            edu.brown.stream.VoteGenClient.CurrentCall call = switchboard.getNextCall();
            if(call==null)
                return true;
            
            System.out.println(" clientId: " + this.getClientId() + " - " + call.getString());
            
            Client client = this.getClientHandle();
            response = client.callProcedure(callback,
                                                    "Vote",
                                                    call.voteId,
                                                    call.phoneNumber,
                                                    call.contestantNumber,
                                                    VoterDemoSStoreAnotherConstants.MAX_VOTES,
                                                    call.timestamp);
                                                    //timestamp);
            GetStatisticInfo();
        }
        return response;
    }
    
    private synchronized void GetStatisticInfo()
    {
        try
        {
            VoterDemoSStoreAnotherClient.count.incrementAndGet();
            boolean beSame = (VoterDemoSStoreAnotherClient.count.get()==VoterDemoSStoreAnotherClient.fixnum.get());
            //System.out.println(" clientId: " + this.getClientId() + " - fixnum: " + VoterDemoSStoreAnotherClient.fixnum.get() + "- count: " + VoterDemoSStoreAnotherClient.count.get() + " - " +  beSame);
            
            if( beSame==true )
            {
                //System.out.println("GetStatisticInfo() 1- " + String.valueOf(VoterDemoHStoreAnotherClient.fixnum));
                //System.out.println("call GetStatisticInfo ...");
                
                Client client = this.getClientHandle();
                client.callProcedure(stat_callback, "Results");

                VoterDemoSStoreAnotherClient.fixnum.addAndGet(10000l);
                //System.out.println("GetStatisticInfo() 2- " + String.valueOf(VoterDemoHStoreAnotherClient.fixnum));
                
            }
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
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
            System.out.println(clientResponse.getStatus());
            
            incrementTransactionCounter(clientResponse, 0);
            
            // Keep track of state (optional)
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable results[] = clientResponse.getResults();
                assert(results.length == 1);
                long status = results[0].asScalarLong();
                if (status == VoterDemoSStoreAnotherConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                    incrementTransactionCounter(clientResponse, 1);
                }
                else if (status == VoterDemoSStoreAnotherConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == VoterDemoSStoreAnotherConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
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
    
    private class StatisticCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse)
        {
            if (clientResponse.getStatus() == Status.OK) {
                VoltTable vt = clientResponse.getResults()[0];
                
                int row_len = vt.getRowCount();
                
                long fixnum = VoterDemoSStoreAnotherClient.fixnum.get();
                String line =  String.valueOf(fixnum - 10000l) + ": ";
                for(int i=0;i<row_len; i++)
                {
                    VoltTableRow row = vt.fetchRow(i);
                    String contestant_name = row.getString(0);
                    long total_votes = row.getLong(2);
                    
                    String content = contestant_name + "-" + String.valueOf(total_votes);
                    
                    line += content + " ";
                }
                
                //System.out.println(line);
                
                try {
                    WriteToFile(line);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                
            }            
        }
        
        private synchronized void WriteToFile(String content) throws IOException
        {
            //System.out.println(stat_filename + " : " + content );
            PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(stat_filename, true)));
            out.println(content);
            out.close();
        }
    }
}
