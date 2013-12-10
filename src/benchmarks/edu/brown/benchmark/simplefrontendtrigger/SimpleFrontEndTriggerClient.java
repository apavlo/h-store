package edu.brown.benchmark.simplefrontendtrigger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.stream.*;
import edu.brown.benchmark.simplefrontendtrigger.procedures.SimpleCall;

public class SimpleFrontEndTriggerClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SimpleFrontEndTriggerClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    private Client client = null;

    public static void main(String args[]) {
        BenchmarkComponent.main(SimpleFrontEndTriggerClient.class, args, false);
    }

    public SimpleFrontEndTriggerClient(String args[]) {
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
        
        client = this.getClientHandle();
        assert(client != null) : "client is null, this is bad";
        
        try {
            ClientResponse clientResponse = client.callProcedure("SimpleCall");
            incrementTransactionCounter(clientResponse, 0);
           
        } catch (ProcCallException e) {
            e.printStackTrace();
        }

        return true;
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[]{
             SimpleCall.class.getSimpleName()
        };
        return (procNames);
    }

}

