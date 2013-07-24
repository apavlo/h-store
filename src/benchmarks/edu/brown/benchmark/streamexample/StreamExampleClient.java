package edu.brown.benchmark.streamexample;

import java.io.IOException;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;


public class StreamExampleClient extends BenchmarkComponent {

	final Callback callback = new Callback(2);
	boolean hasRun = false;
	
    public static void main(String args[]) {
        BenchmarkComponent.main(StreamExampleClient.class, args, false);
    }

    public StreamExampleClient(String[] args) {
        super(args);
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                // synchronously call the "Vote" procedure
                try {
                    runOnce();
                } catch (Exception e) {
                    System.err.println(e.toString());
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }
        
    @Override
    protected boolean runOnce() throws IOException {
        if(!hasRun)
        {
        	Client client = this.getClientHandle();
        	boolean response = client.callProcedure(callback, "GetAllData");
        	hasRun = true;
        	return response;
        }
        else
        	return false;
    }

    private class Callback implements ProcedureCallback {
        private final int idx;

        public Callback(int idx) {
            this.idx = idx;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
        }
    } // END CLASS

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[StreamExampleProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = StreamExampleProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
