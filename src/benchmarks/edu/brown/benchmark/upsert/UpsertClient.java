package edu.brown.benchmark.upsert;

import java.io.IOException;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.upsert.procedures.Upsert;
import edu.brown.benchmark.upsert.procedures.Update;

public class UpsertClient extends BenchmarkComponent {
    
    final Callback callback = new Callback();
    
    public static void main(String args[]) {
        BenchmarkComponent.main(UpsertClient.class, args, false);
    }

    public UpsertClient(String[] args) {
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
            e.printStackTrace();
        }

    }

	@Override
    protected boolean runOnce() throws IOException {
        Client client = this.getClientHandle();
        boolean response = client.callProcedure(callback,
                                                //"Upsert",
                                                "Update",
                                                100000);
        return response;
    }


    private class Callback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, 0);
        }
    } // END CLASS

    @Override
    public String[] getTransactionDisplayNames() {
        String procNames[] = new String[]{
                //Upsert.class.getSimpleName()
                Update.class.getSimpleName()
           };
           return (procNames);
    }
}
