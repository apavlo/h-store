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
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
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

    
    private ProfileMeasurement execute_time = new ProfileMeasurement("EXECUTE");
    
    
    /**
     * Constructor
     * @param benchmarkComponent
     */
    public ControlWorker(BenchmarkComponent benchmarkComponent) {
        cmp = benchmarkComponent;
    }

    @Override
    public void run() {
        cmp.invokeStartCallback();
        try {
            if (cmp.m_txnRate == -1) {
                if (cmp.m_sampler != null) {
                    cmp.m_sampler.start();
                }
                cmp.runLoop();
            } else {
                if (debug.get()) LOG.debug(String.format("Running rate controlled [m_txnRate=%d, m_txnsPerMillisecond=%f]", cmp.m_txnRate, cmp.m_txnsPerMillisecond));
                rateControlledRunLoop();
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
            System.exit(0);
        } finally {
            if (cmp.m_exitOnCompletion) {
                LOG.debug("Stopping BenchmarkComponent thread");
                System.exit(0);
            }
        }
    }

    private void rateControlledRunLoop() {
        final boolean profile = cmp.getHStoreConf().client.profiling;
        final Client client = cmp.getClientHandle();
        m_lastRequestTime = System.currentTimeMillis();
        
        boolean hadErrors = false;
        while (true) {
            boolean bp = false;
            try {
                // If there is back pressure don't send any requests. Update the
                // last request time so that a large number of requests won't
                // queue up to be sent when there is no longer any back
                // pressure.
                client.backpressureBarrier();
                
                // Check whether we are currently being paused
                // We will block until we're allowed to go again
                if (cmp.m_controlState == ControlState.PAUSED) {
                    if (debug.get()) LOG.debug("Pausing until control lock is released");
                    cmp.m_pauseLock.acquire();
                    if (debug.get()) LOG.debug("Control lock is released! Resuming execution! Tiger style!");
                }
                assert(cmp.m_controlState != ControlState.PAUSED) : "Unexpected " + cmp.m_controlState;
                
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }

            final long now = System.currentTimeMillis();

            /*
             * Generate the correct number of transactions based on how much
             * time has passed since the last time transactions were sent.
             */
            final long delta = now - m_lastRequestTime;
            if (delta > 0) {
                final int transactionsToCreate = (int) (delta * cmp.m_txnsPerMillisecond);
                if (transactionsToCreate < 1) {
                    // Thread.yield();
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    continue;
                }

                if (debug.get())
                    LOG.debug("Submitting " + transactionsToCreate + " transaction requests from client #" + cmp.getClientId());
                if (profile) execute_time.start();
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
                    if (debug.get()) LOG.error("Failed to execution transaction: " + e.getMessage());
                    ThreadUtil.sleep(5000);
                } finally {
                    if (profile) execute_time.stop();
                }
            }
            else {
                // Thread.yield();
                try {
                    Thread.sleep(50);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }

            m_lastRequestTime = now;
        } // WHILE
    }
    
    public ProfileMeasurement getExecuteTime() {
        return execute_time;
    }
    
}