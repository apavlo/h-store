package edu.brown.benchmark.example;

import java.io.IOException;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;

public class ExampleClient extends BenchmarkComponent {

    public static void main(String args[]) {
        BenchmarkComponent.main(ExampleClient.class, args, false);
    }

    public ExampleClient(String[] args) {
        super(args);
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }

    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            Random rand = new Random();
            while (true) {
                // Select a random transaction to execute and generate its input
                // parameters. The procedure index (procIdx) needs to the same as the array
                // of procedure names returned by getTransactionDisplayNames()
                int procIdx = rand.nextInt(ExampleProjectBuilder.PROCEDURES.length);
                String procName = ExampleProjectBuilder.PROCEDURES[procIdx].getSimpleName();
                Object procParams[] = null; // TODO

                // Create a new Callback handle that will be executed when the
                // transaction completes
                Callback callback = new Callback(procIdx);

                // Invoke the stored procedure through the client handle. This
                // is non-blocking
                client.callProcedure(callback, procName, procIdx);

                // Check whether all the nodes are backed-up and this client
                // should block before sending new requests.
                client.backpressureBarrier();
            } // WHILE
        } catch (NoConnectionsException e) {
            // Client has no clean mechanism for terminating with the DB.
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            // At shutdown an IOException is thrown for every connection to
            // the DB that is lost Ignore the exception here in order to not
            // get spammed, but will miss lost connections at runtime
        }
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
        String procNames[] = new String[ExampleProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = ExampleProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
