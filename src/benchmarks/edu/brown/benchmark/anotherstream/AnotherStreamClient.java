package edu.brown.benchmark.anotherstream;

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
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class AnotherStreamClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(AnotherStreamClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    // Phone number generator
    PhoneCallGenerator switchboard;

    // Flags to tell the worker threads to stop or go
    AtomicBoolean warmupComplete = new AtomicBoolean(false);
    AtomicBoolean benchmarkComplete = new AtomicBoolean(false);

    // voter benchmark state
    AtomicLong acceptedVotes = new AtomicLong(0);
    AtomicLong badContestantVotes = new AtomicLong(0);
    AtomicLong badVoteCountVotes = new AtomicLong(0);
    AtomicLong failedVotes = new AtomicLong(0);

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(AnotherStreamClient.class, args, false);
    }

    public AnotherStreamClient(String args[]) {
        super(args);
        int numContestants = VoterUtil.getScaledNumContestants(this.getScaleFactor());
        this.switchboard = new PhoneCallGenerator(this.getClientId(), numContestants);
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
        PhoneCallGenerator.PhoneCall call = switchboard.receive();

        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
                                                "Vote",
                                                call.voteId,
                                                call.phoneNumber,
                                                call.contestantNumber);
        return response;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
             Vote.class.getSimpleName()
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
                if (status == VoterConstants.VOTE_SUCCESSFUL) {
                    acceptedVotes.incrementAndGet();
                }
                else if (status == VoterConstants.ERR_INVALID_CONTESTANT) {
                    badContestantVotes.incrementAndGet();
                }
                else if (status == VoterConstants.ERR_VOTER_OVER_VOTE_LIMIT) {
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

