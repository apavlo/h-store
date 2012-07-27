/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.hstore.util;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.MarkovGraph;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

/**
 * 
 * @author pavlo
 */
@Deprecated
public class PartitionExecutorHelper implements Runnable {
    public static final Logger LOG = Logger.getLogger(PartitionExecutorHelper.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * How many milliseconds will we keep around old transaction states
     */
    private final int txn_expire;
    /**
     * The maximum number of transactions to clean up per poll round
     */
    private final int txn_per_round;
    /**
     * The sites we need to invoke cleanupTransaction() + tick() for
     */
    private final Collection<PartitionExecutor> executors;
    /**
     * HStoreSite Handle
     */
    private final HStoreSite hstore_site;
    /**
     * Set to false after the first time we are invoked
     */
    private boolean first = true;
    
    private int total_cleaned = 0;
    
    /**
     * List of MarkovGraphs that need to be recomputed
     */
    private final LinkedBlockingDeque<MarkovGraph> markovs_to_recompute = new LinkedBlockingDeque<MarkovGraph>(); 
    
    /**
     * Shutdown Observer
     * This gets invoked when the HStoreSite is shutting down
     */
    private final EventObserver<Object> shutdown_observer = new EventObserver<Object>() {
        @Override
        public void update(EventObservable<Object> o, Object t) {
            PartitionExecutorHelper.this.shutdown();
            LOG.debug("Got shutdown notification from HStoreSite. Dumping profile information");
        }
    };
    
    /**
     * Constructor
     * @param executors
     * @param max_txn_per_round
     * @param txn_expire
     * @param enable_profiling
     */
    public PartitionExecutorHelper(HStoreSite hstore_site, Collection<PartitionExecutor> executors, int max_txn_per_round, int txn_expire, boolean enable_profiling) {
        assert(executors != null);
        assert(executors.isEmpty() == false);
        this.executors = executors;
        this.txn_expire = txn_expire;
        this.txn_per_round = max_txn_per_round;

        this.hstore_site = hstore_site;
        assert(this.hstore_site != null) : "Missing HStoreSite!";
        
        assert(this.executors.size() > 0) : "No ExecutionSites for helper";
        PartitionExecutor executor = CollectionUtil.first(this.executors);
        assert(executor != null);
        this.hstore_site.getShutdownObservable().addObserver(this.shutdown_observer);

        if (debug.get()) LOG.debug(String.format("Instantiated new ExecutionSiteHelper [txn_expire=%d, per_round=%d]",
                                                 this.txn_expire, this.txn_per_round));
    }
    
    /**
     * 
     * @param markov
     */
    public void queueMarkovToRecompute(MarkovGraph markov) {
        this.markovs_to_recompute.add(markov);
    }
    
    @Override
    public synchronized void run() {
        final boolean d = debug.get();
        final boolean t = trace.get();
        
        if (this.first) {
            Thread self = Thread.currentThread();
            self.setName(HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_HELPER));
            hstore_site.getThreadManager().registerProcessingThread();
            this.first = false;
        }
        if (t) LOG.trace("New invocation of the ExecutionSiteHelper. Let's clean-up some txns!");

        this.hstore_site.updateLogging();
        for (PartitionExecutor es : this.executors) {
//            if (t) LOG.trace(String.format("Partition %d has %d finished transactions", es.partitionId, es.finished_txn_states.size()));
//            long to_remove = System.currentTimeMillis() - this.txn_expire;
            
            int cleaned = 0;
//            while (es.finished_txn_states.isEmpty() == false && (this.txn_per_round < 0 || cleaned < this.txn_per_round)) {
//                AbstractTransaction ts = es.finished_txn_states.peek();
//                if (ts.getEE_FinishedTimestamp() < to_remove) {
////                    if (traceLOG.info(String.format("Want to clean txn #%d [done=%s, type=%s]", ts.getTransactionId(), ts.getHStoreSiteDone(), ts.getClass().getSimpleName()));
//                    if (ts.isHStoreSite_Finished() == false) break;
//                    
//                    if (t) LOG.trace("Cleaning txn #" + ts.getTransactionId());
//                    
//
//                    es.cleanupTransaction(ts);
//                    es.finished_txn_states.remove();
//                    cleaned++;
//                    this.total_cleaned++;
//                } else break;
//            } // WHILE
            if (d && cleaned > 0) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d [total=%d]", cleaned, es.getPartitionId(), this.total_cleaned));
            // Only call tick here!
//            es.tick();
        } // FOR
        
        // Recompute MarkovGraphs if we have them
        MarkovGraph m = null;
        while ((m = this.markovs_to_recompute.poll()) != null) {
            if (d) LOG.debug(String.format("Recomputing MarkovGraph for %s [recomputed=%d, hashCode=%d]",
                                           m.getProcedure().getName(), m.getRecomputeCount(), m.hashCode()));
            m.calculateProbabilities();
            if (d && m.isValid() == false) {
                LOG.error("Invalid MarkovGraph after recomputing! Crashing...");
                Exception error = new Exception(String.format("Invalid %s MarkovGraph for after recomputing", m.getProcedure().getName()));
                this.hstore_site.getCoordinator().shutdownCluster(error);
            }
        } // WHILE
    }
    
    /**
     * Final clean-up of the TransactionStates at our sites
     * This is only really necessary if profiling is enabled  
     */
    private synchronized void shutdown() {
        LOG.info("Shutdown event received. Cleaning all transactions");
        
//        for (ExecutionSite es : this.executors) {
//            int cleaned = es.finished_txn_states.size();
//            es.finished_txn_states.clear();
//            assert(es.finished_txn_states.isEmpty());
//            if (debug.get()) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d", cleaned, es.partitionId));
//        } // FOR
    }

}
