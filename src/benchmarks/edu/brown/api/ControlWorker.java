package edu.brown.api;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.profilers.ProfileMeasurement;
import edu.brown.utils.ThreadUtil;

/**
 * Thread that executes the derives classes run loop which invokes stored
 * procedures indefinitely
 */
class ControlWorker extends Thread {
    private static final Logger LOG = Logger.getLogger(ControlWorker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug);
    }
    
    /**
     * 
     */
    private final BenchmarkComponent cmp;
    
    /**
     * Time in milliseconds since requests were last sent.
     */
    private long m_lastRequestTime;

    private boolean profiling = false;
    private ProfileMeasurement execute_time = new ProfileMeasurement("EXECUTE");
    private ProfileMeasurement block_time = new ProfileMeasurement("BLOCK");
    
    
    /**
     * Constructor
     * @param benchmarkComponent
     */
    public ControlWorker(BenchmarkComponent benchmarkComponent) {
        cmp = benchmarkComponent;
    }

    @Override
    public void run() {
        Thread self = Thread.currentThread();
        self.setName(String.format("worker-%03d", cmp.getClientId()));
        
        cmp.invokeStartCallback();
        try {
            if (cmp.m_txnRate == -1) {
                if (cmp.m_sampler != null) {
                    cmp.m_sampler.start();
                }
                cmp.runLoop();
            } else {
                if (debug.val) LOG.debug(String.format("Running rate controlled [m_txnRate=%d, m_txnsPerMillisecond=%f]", cmp.m_txnRate, cmp.m_txnsPerMillisecond));
                this.rateControlledRunLoop();
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        } finally {
            if (cmp.m_exitOnCompletion) {
                if (debug.val) LOG.debug(String.format("Stopping %s thread [id=%d]",
                                           this.getClass().getSimpleName(), cmp.getClientId()));
                        
                return;
            }
        }
    }

    private void rateControlledRunLoop() throws InterruptedException {
        final Client client = cmp.getClientHandle();
        m_lastRequestTime = System.currentTimeMillis();
        
        boolean hadErrors = false;
        boolean bp = false;
        while (true) {
            // If there is back pressure don't send any requests. Update the
            // last request time so that a large number of requests won't
            // queue up to be sent when there is no longer any back
            // pressure.
            if (bp) {
                if (this.profiling) this.block_time.start();
                try {
                    client.backpressureBarrier();
                } finally {
                    if (this.profiling) this.block_time.stop();
                }
                bp = false;
            }
            
            // Check whether we are currently being paused
            // We will block until we're allowed to go again
            if (cmp.m_controlState == ControlState.PAUSED) {
                if (debug.val) LOG.debug("Pausing until control lock is released");
                cmp.m_pauseLock.acquire();
                if (debug.val) LOG.debug("Control lock is released! Resuming execution! Tiger style!");
            }
            assert(cmp.m_controlState != ControlState.PAUSED) : "Unexpected " + cmp.m_controlState;

            // Generate the correct number of transactions based on how much
            // time has passed since the last time transactions were sent.
            final long now = System.currentTimeMillis();
            final long delta = now - m_lastRequestTime;
            if (delta > 0) {
                final int transactionsToCreate = (int) (delta * cmp.m_txnsPerMillisecond);
                if (transactionsToCreate < 1) {
                    Thread.sleep(25);
                    continue;
                }

                if (debug.val) LOG.debug(String.format("Submitting %d txn requests from client #%d",
                                           transactionsToCreate, cmp.getClientId()));
                if (this.profiling) execute_time.start();
                try {
                    for (int ii = 0; ii < transactionsToCreate; ii++) {
                        bp = !cmp.runOnce();
                        if (bp || cmp.m_controlState != ControlState.RUNNING) {
                            break;
                        }
                    } // FOR
                } catch (final IOException e) {
                    if (hadErrors) return;
                    hadErrors = true;
                    
                    // HACK: Sleep for a little bit to give time for the site logs to flush
//                    if (debug.val) 
                    LOG.error("Failed to execute transaction: " + e.getMessage(), e);
                    ThreadUtil.sleep(5000);
                } finally {
                    if (this.profiling) execute_time.stop();
                }
            }
            else {
                Thread.sleep(25);
            }

            m_lastRequestTime = now;
        } // WHILE
    }
 
    public void enableProfiling(boolean val) {
        this.profiling = val;
    }
    public ProfileMeasurement getExecuteTime() {
        return (execute_time);
    }
    public ProfileMeasurement getBlockedTime() {
        return (this.block_time);
    }
    
}