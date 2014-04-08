package edu.brown.benchmark.simpledistribution;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

import edu.brown.benchmark.simpledistribution.procedures.SimpleCall;

public class SimpleDistributionClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SimpleDistributionClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(SimpleDistributionClient.class, args, false);
    }

    public SimpleDistributionClient(String args[]) {
        super(args);
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                try {
                    runOnce();
                } catch (Exception e) {
	            e.printStackTrace();
                }

            } // WHILE
        } catch (Exception e) {
            // Client has no clean mechanism for terminating with the DB.
            e.printStackTrace();
        }
    }

    @Override
    protected boolean runOnce() throws IOException {
        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
                                                "SimpleCall");
        return response;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
             SimpleCall.class.getSimpleName()
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 0);
        }
    } // END CLASS
}

