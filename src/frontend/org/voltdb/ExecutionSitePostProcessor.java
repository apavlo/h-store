package org.voltdb;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;

import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.interfaces.Shutdownable;

public final class ExecutionSitePostProcessor implements Runnable, Shutdownable {

    /**
     * Whether we should stop processing our queue
     */
    private boolean stop = false;
    
    /**
     * ClientResponses that can be immediately returned to the client
     */
    private final LinkedBlockingDeque<Object[]> ready_responses = new LinkedBlockingDeque<Object[]>();

    /**
     * Handle to ourselves
     */
    private Thread self = null; 
    
    /**
     * 
     * @param es
     * @param ts
     * @param cresponse
     */
    public void processClientResponse(ExecutionSite es, LocalTransactionState ts, ClientResponseImpl cresponse) {
        this.ready_responses.add(new Object[]{es, ts, cresponse});
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        Object triplet[] = null;
        while (this.stop == false) {
            try {
                triplet = this.ready_responses.takeFirst();
                assert(triplet != null);
                assert(triplet.length == 3) : "Unexpected response: " + Arrays.toString(triplet);
            } catch (InterruptedException ex) {
                this.stop = true;
                break;
            }
            ExecutionSite es = (ExecutionSite)triplet[0];
            LocalTransactionState ts = (LocalTransactionState)triplet[1];
            ClientResponseImpl cr = (ClientResponseImpl)triplet[2];
            es.processClientResponse(ts, cr);
        } // WHILE
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.stop);
    }
    
    @Override
    public void prepareShutdown() {
        this.ready_responses.clear();
    }
    
    @Override
    public void shutdown() {
        this.stop = true;
        this.self.interrupt();
    }

}
