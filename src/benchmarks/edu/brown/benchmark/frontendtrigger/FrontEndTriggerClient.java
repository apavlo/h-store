package edu.brown.benchmark.frontendtrigger;

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
import edu.brown.benchmark.frontendtrigger.procedures.SimpleCall;

public class FrontEndTriggerClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(FrontEndTriggerClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();

    private Client client = null;

    public static void main(String args[]) {
        BenchmarkComponent.main(FrontEndTriggerClient.class, args, false);
    }

    public FrontEndTriggerClient(String args[]) {
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
            
            // call following procedures
            callFollowingProcedures(client, clientResponse);

        } catch (ProcCallException e) {
            e.printStackTrace();
        }

        return true;
    }
    
    private void callFollowingProcedures(Client client, ClientResponse clientResponse) 
    {
        for(String procedure : clientResponse.getFollowingProcedures())
        {
                //System.out.println("client running frontend trigger procedure : " + procedure);
                //assert(client != null);
                try {
                    clientResponse = client.callProcedure(procedure);
                    incrementTransactionCounter(clientResponse, 0);
                    callFollowingProcedures(client, clientResponse);
                }
                catch (Exception e)
                {
                    e.printStackTrace();
                }
        }
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

