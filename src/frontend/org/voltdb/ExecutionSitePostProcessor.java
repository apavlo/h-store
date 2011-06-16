package org.voltdb;

import java.util.Arrays;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.interfaces.Shutdownable;

public final class ExecutionSitePostProcessor implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(ExecutionSitePostProcessor.class);
    private boolean d = LOG.isDebugEnabled();

    private final HStoreSite hstore_site;
    
    /**
     * Whether we should stop processing our queue
     */
    private boolean stop = false;
    
    /**
     * ClientResponses that can be immediately returned to the client
     */
    private final LinkedBlockingDeque<Object[]> ready_responses = new LinkedBlockingDeque<Object[]>();
    private final AtomicInteger queue_size = new AtomicInteger(0);

    /**
     * Handle to ourselves
     */
    private Thread self = null; 
    
    /**
     * 
     * @param hstore_site
     */
    public ExecutionSitePostProcessor(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    public int getQueueSize() {
        return (this.queue_size.get());
    }
    
    /**
     * 
     * @param es
     * @param ts
     * @param cr
     */
    public void processClientResponse(ExecutionSite es, LocalTransactionState ts, ClientResponseImpl cr) {
        if (d) LOG.debug(String.format("Adding ClientResponse for %s from partition %d to processing queue [status=%s, size=%d]",
                                       ts, es.getPartitionId(), cr.getStatusName(), this.ready_responses.size()));
        this.queue_size.incrementAndGet();
        this.ready_responses.add(new Object[]{es, ts, cr});
    }
    
    @Override
    public void run() {
        this.self = Thread.currentThread();
        this.self.setName(this.hstore_site.getThreadName("post"));
        
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
            if (d) LOG.debug(String.format("Processing ClientResponse for %s at partition %d [status=%s]",
                                           ts, es.getPartitionId(), cr.getStatusName()));
            try {
                es.processClientResponse(ts, cr);
            } catch (Throwable ex) {
                if (this.isShuttingDown() == false) throw new RuntimeException(ex);
                break;
            }
            this.queue_size.decrementAndGet();
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
        if (this.self != null) this.self.interrupt();
    }

}
