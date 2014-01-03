package edu.brown.benchmark.simplenoop;

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

import org.voltdb.VoltSystemProcedure;
import org.voltdb.sysprocs.NoOp;

public class SimpleNoOpClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SimpleNoOpClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    final Callback callback = new Callback();

    public static void main(String args[]) {
        BenchmarkComponent.main(SimpleNoOpClient.class, args, false);
    }

    public SimpleNoOpClient(String args[]) {
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
         String procName = VoltSystemProcedure.procCallName(NoOp.class);
        boolean response = client.callProcedure(callback,
                                                procName );
        return response;
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
             VoltSystemProcedure.procCallName(NoOp.class)
        };
        return (procNames);
    }

    private class Callback implements ProcedureCallback {

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, 0);
        }
    } // END CLASS
}

