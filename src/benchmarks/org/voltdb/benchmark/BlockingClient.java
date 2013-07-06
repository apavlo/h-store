/**
 * 
 */
package org.voltdb.benchmark;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.voltdb.StoredProcedureInvocationHints;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientStatusListener;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;

/**
 * @author pavlo
 *
 */
public class BlockingClient extends Semaphore implements Client {
    static final Logger LOG = Logger.getLogger(BlockingClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final long serialVersionUID = 1L;
    private final Client inner;
    private final ProfileMeasurement idle = new ProfileMeasurement("CLIENT_IDLE");
    
    static {
        LoggerUtil.setupLogging();
    }
    
    private class BlockingCallback implements ProcedureCallback {
        private final ProcedureCallback inner_callback;
        private final String proc_name;
        
        public BlockingCallback(String proc_name, ProcedureCallback inner_callback) {
            assert(inner_callback != null);
            this.inner_callback = inner_callback;
            this.proc_name = proc_name;
            
            if (debug.val) LOG.debug("Created a new BlockingCallback around " + inner_callback.getClass().getSimpleName() + " for '" + proc_name + "'");
            try {
                if (debug.val) 
                    LOG.debug("Trying to acquire procedure invocation lock: " + BlockingClient.this.availablePermits());
                
                idle.start();
                BlockingClient.this.acquire();
                idle.stop();
                
                if (debug.val) LOG.debug("We got it! Let's get it on! [proc_name=" + this.proc_name + "]");
            } catch (InterruptedException ex) {
                LOG.fatal("Got interrupted while waiting for lock", ex);
                System.exit(1);
            }
            
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (debug.val) LOG.debug("BlockingCallback is forwarding the client callback for to inner callback [" +
                               "txn=" + clientResponse.getTransactionId() + ", " +
                               "proc=" + this.proc_name + "]");
            BlockingClient.this.release();
            this.inner_callback.clientCallback(clientResponse);
        }
    }
    
    /**
     * 
     */
    public BlockingClient(Client inner, int max_concurrent) {
        super(max_concurrent);
        this.inner = inner;
        if (LOG.isDebugEnabled()) LOG.debug("Created new BlockingClient [max_concurrent=" + max_concurrent + "]");
    }

    public Client getClient() {
        return (this.inner);
    }
    
    /* (non-Javadoc)
     * @see org.voltdb.client.Client#addClientStatusListener(org.voltdb.client.ClientStatusListener)
     */
    @Override
    public void addClientStatusListener(ClientStatusListener listener) {
        this.inner.addClientStatusListener(listener);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#backpressureBarrier()
     */
    @Override
    public void backpressureBarrier() throws InterruptedException {
        this.inner.backpressureBarrier();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#blocking()
     */
    @Override
    public boolean blocking() {
        return this.inner.blocking();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#calculateInvocationSerializedSize(java.lang.String, java.lang.Object[])
     */
    @Override
    public int calculateInvocationSerializedSize(String procName, Object... parameters) {
        return this.inner.calculateInvocationSerializedSize(procName, parameters);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#callProcedure(java.lang.String, java.lang.Object[])
     */
    @Override
    public ClientResponse callProcedure(String procName, Object... parameters) throws IOException,
            NoConnectionsException, ProcCallException {
        return this.inner.callProcedure(procName, null, parameters);
    }
    
    @Override
    public ClientResponse callProcedure(String procName, StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException, ProcCallException {
        return this.inner.callProcedure(procName, hints, parameters);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#callProcedure(org.voltdb.client.ProcedureCallback, java.lang.String, java.lang.Object[])
     */
    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName, Object... parameters) throws IOException,
            NoConnectionsException {
        return this.inner.callProcedure(new BlockingCallback(procName, callback), procName, null, parameters);
    }
    
    @Override
    public boolean callProcedure(ProcedureCallback callback, String procName, StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException {
        return this.inner.callProcedure(new BlockingCallback(procName, callback), procName, hints, parameters);
    }


    /* (non-Javadoc)
     * @see org.voltdb.client.Client#callProcedure(org.voltdb.client.ProcedureCallback, int, java.lang.String, java.lang.Object[])
     */
    @Override
    public boolean callProcedure(ProcedureCallback callback, int expectedSerializedSize, String procName,
            StoredProcedureInvocationHints hints, Object... parameters) throws IOException, NoConnectionsException {
        return this.inner.callProcedure(callback, expectedSerializedSize, procName, hints, parameters);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#close()
     */
    @Override
    public void close() throws InterruptedException {
        this.inner.close();
        if (LOG.isDebugEnabled()) LOG.debug("Client Idle Time: " + this.idle.debug());
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#configureBlocking(boolean)
     */
    @Override
    public void configureBlocking(boolean blocking) {
        this.inner.configureBlocking(blocking);
    }
    
    @Override
    public void createConnection(String host, int port) throws UnknownHostException, IOException {
        this.inner.createConnection(host, port);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#createConnection(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void createConnection(Integer siteId, String host, int port, String username, String password) throws UnknownHostException,
            IOException {
        this.inner.createConnection(siteId, host, port, username, password);
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#drain()
     */
    @Override
    public void drain() throws NoConnectionsException, InterruptedException {
        this.inner.drain();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getBuildString()
     */
    @Override
    public String getBuildString() {
        return this.inner.getBuildString();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getIOStats()
     */
    @Override
    public VoltTable getIOStats() {
        return this.inner.getIOStats();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getIOStatsInterval()
     */
    @Override
    public VoltTable getIOStatsInterval() {
        return this.inner.getIOStatsInterval();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getInstanceId()
     */
    @Override
    public Object[] getInstanceId() {
        return this.inner.getInstanceId();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getProcedureStats()
     */
    @Override
    public VoltTable getProcedureStats() {
        return this.inner.getProcedureStats();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#getProcedureStatsInterval()
     */
    @Override
    public VoltTable getProcedureStatsInterval() {
        return this.inner.getProcedureStatsInterval();
    }

    /* (non-Javadoc)
     * @see org.voltdb.client.Client#removeClientStatusListener(org.voltdb.client.ClientStatusListener)
     */
    @Override
    public boolean removeClientStatusListener(ClientStatusListener listener) {
        return this.inner.removeClientStatusListener(listener);
    }
    @Override
    public ProfileMeasurement getQueueTime() {
        return this.inner.getQueueTime();
    }
}
