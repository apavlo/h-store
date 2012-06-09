package edu.brown.benchmark;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.client.Client;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Thread that executes the derives classes run loop which invokes stored
 * procedures indefinitely
 */
class ControlWorker extends Thread {
    private static final Logger LOG = Logger.getLogger(ControlWorker.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * 
     */
    private final BenchmarkComponent cmp;
    
    /**
     * Time in milliseconds since requests were last sent.
     */
    private long m_lastRequestTime;

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
        Client client = cmp.getClientHandle();
        m_lastRequestTime = System.currentTimeMillis();
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
                    cmp.m_pauseLock.acquire();
                }
                assert(cmp.m_controlState != ControlState.PAUSED) : "Unexpected " + cmp.m_controlState;
                
            } catch (InterruptedException e1) {
                throw new RuntimeException();
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
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    continue;
                }

                for (int ii = 0; ii < transactionsToCreate; ii++) {
                    try {
                        bp = !cmp.runOnce();
                        if (bp) {
                            m_lastRequestTime = now;
                            break;
                        }
                    }
                    catch (final IOException e) {
                        return;
                    }
                }
            }
            else {
                // Thread.yield();
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }

            m_lastRequestTime = now;
        }
    }
}