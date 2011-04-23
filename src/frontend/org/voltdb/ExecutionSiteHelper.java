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
package org.voltdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * 
 * @author pavlo
 */
public class ExecutionSiteHelper implements Runnable {
    public static final Logger LOG = Logger.getLogger(ExecutionSiteHelper.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Enable profiling calculations 
     */
    private final boolean enable_profiling;
    /**
     * Maintain a set of tuples for the times
     */
    private final Map<Procedure, List<long[]>> proc_profiles = new TreeMap<Procedure, List<long[]>>();
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
    private final Collection<ExecutionSite> executors;
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
     * Shutdown Observer
     * This gets invoked when the HStoreSite is shutting down
     */
    private final EventObserver shutdown_observer = new EventObserver() {
        final StringBuilder sb = new StringBuilder();
        
        @Override
        public void update(Observable o, Object arg) {
            ExecutionSiteHelper.this.shutdown();
            LOG.debug("Got shutdown notification from HStoreSite. Dumping profile information");
            sb.append("\n")
              .append(ExecutionSiteHelper.this.dumpProfileInformation())
              .append("\n")
              .append(ExecutionSiteHelper.this.hstore_site.statusSnapshot());
            LOG.info("\n" + sb.toString());
        }
    };
    
    /**
     * Constructor
     * @param executors
     * @param max_txn_per_round
     * @param txn_expire
     * @param enable_profiling
     */
    public ExecutionSiteHelper(Collection<ExecutionSite> executors, int max_txn_per_round, int txn_expire, boolean enable_profiling) {
        assert(executors != null);
        assert(executors.isEmpty() == false);
        this.executors = executors;
        this.txn_expire = txn_expire;
        this.txn_per_round = max_txn_per_round;
        this.enable_profiling = enable_profiling;

        assert(this.executors.size() > 0) : "No ExecutionSites for helper";
        ExecutionSite executor = CollectionUtil.getFirst(this.executors);
        assert(executor != null);
        this.hstore_site = executor.getHStoreSite();
        assert(this.hstore_site != null) : "Missing HStoreSite!";
        
        if (this.enable_profiling) {
            this.prepareProfileInformation(CatalogUtil.getDatabase(executor.getCatalogSite()));
            this.hstore_site.addShutdownObservable(this.shutdown_observer);
        }
        
        if (debug.get()) LOG.debug(String.format("Instantiated new ExecutionSiteHelper [txn_expire=%d, per_round=%d, profiling=%s]",
                                                 this.txn_expire, this.txn_per_round, this.enable_profiling));
    }
    
    @Override
    public synchronized void run() {
        final boolean d = debug.get();
        final boolean t = trace.get();
        
        if (this.first) {
            Thread self = Thread.currentThread();
            self.setName(this.hstore_site.getThreadName("help"));
            this.first = false;
        }
        if (t) LOG.trace("New invocation of the ExecutionSiteHelper. Let's clean-up some txns!");

        this.hstore_site.updateLogging();
        for (ExecutionSite es : this.executors) {
            if (t) LOG.trace(String.format("Partition %d has %d finished transactions", es.partitionId, es.finished_txn_states.size()));
            long to_remove = System.currentTimeMillis() - this.txn_expire;
            
            int cleaned = 0;
            while (es.finished_txn_states.isEmpty() == false && (this.txn_per_round < 0 || cleaned < this.txn_per_round)) {
                TransactionState ts = es.finished_txn_states.peek();
                if (ts.getFinishedTimestamp() < to_remove) {
//                    if (traceLOG.info(String.format("Want to clean txn #%d [done=%s, type=%s]", ts.getTransactionId(), ts.getHStoreSiteDone(), ts.getClass().getSimpleName()));
                    if (ts.getHStoreSiteDone() == false) break;
                    
                    if (t) LOG.trace("Cleaning txn #" + ts.getTransactionId());
                    
                    // We have to calculate the profile information *before* we call ExecutionSite.cleanup!
                    if (this.enable_profiling && ts instanceof LocalTransactionState) {
                        this.calculateProfileInformation((LocalTransactionState)ts);
                    }
                    ts.setHStoreSiteDone(false);
                    es.cleanupTransaction(ts);
                    es.finished_txn_states.remove();
                    cleaned++;
                    this.total_cleaned++;
                } else break;
            } // WHILE
            if (d && cleaned > 0) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d [total=%d]", cleaned, es.partitionId, this.total_cleaned));
            // Only call tick here!
            es.tick();
        } // FOR
    }
    
    /**
     * Final clean-up of the TransactionStates at our sites
     * This is only really necessary if profiling is enabled  
     */
    private synchronized void shutdown() {
        LOG.info("Shutdown event received. Cleaning all transactions");
        TransactionState ts = null;
        
        for (ExecutionSite es : this.executors) {
            int cleaned = 0;
            if (this.enable_profiling) {
                while ((ts = es.finished_txn_states.poll()) != null) {
                    if (ts instanceof LocalTransactionState) {
                        this.calculateProfileInformation((LocalTransactionState)ts);
                    }
                    cleaned++;
                } // WHILE
            } else {
                cleaned += es.finished_txn_states.size();
                es.finished_txn_states.clear();
            }
            assert(es.finished_txn_states.isEmpty());
            if (debug.get()) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d", cleaned, es.partitionId));
        } // FOR
    }

    /**
     * 
     * @param catalog_db
     */
    public void prepareProfileInformation(Database catalog_db) {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.proc_profiles.put(catalog_proc, new ArrayList<long[]>());
        } // FOR
    }

    /**
     * 
     * @param ts
     */
    public void calculateProfileInformation(LocalTransactionState ts) {
        if (ts.sysproc || ts.total_time.isStopped() == false) return;
        if (trace.get()) LOG.info("Calculating profile information for txn #" + ts.getTransactionId());
        ProfileMeasurement pms[] = {
            ts.total_time,
            ts.init_time,
            ts.queue_time,
            ts.java_time,
            ts.coord_time,
            ts.plan_time,
            ts.ee_time,
            ts.est_time,
            ts.finish_time,
            ts.blocked_time,
        };
        long tuple[] = new long[pms.length];
        for (int i = 0; i < pms.length; i++) {
            if (pms[i] != null) tuple[i] = pms[i].getTotalThinkTime();
            if (i == 0) assert(tuple[i] > 0) : "????";
        } // FOR
        
        Procedure catalog_proc = ts.getProcedure();
        assert(catalog_proc != null);
        if (trace.get()) LOG.trace(String.format("Txn #%d - %s: %s", ts.getTransactionId(), catalog_proc.getName(), Arrays.toString(tuple)));
        this.proc_profiles.get(catalog_proc).add(tuple);
    }
    
    public String dumpProfileInformation() {
        
        String header[] = {
            "",
            "# of Txns",
            "Total Time",
            "Initialization",
            "Queue Time",
            "Procedure",
            "Coordinator",
            "Planner",
            "EE",
            "Estimation",
            "Finish",
            "Blocked",
            "Miscellaneous",
        };
        int num_procs = 0;
        for (List<long[]> tuples : this.proc_profiles.values()) {
            if (tuples.size() > 0) num_procs++;
        } // FOR
        if (num_procs == 0) return ("<NONE>");
        
        Object rows[][] = new String[num_procs][header.length];
        long totals[] = new long[header.length-1];
//        String f = "%.02f";
        
        int row_idx = 0;
        for (Entry<Procedure, List<long[]>> e : this.proc_profiles.entrySet()) {
            int num_tuples = e.getValue().size();
            if (num_tuples == 0) continue;
            for (int i = 0; i < totals.length; i++) totals[i] = 0;
            totals[0] = num_tuples;
            
            // Sum up the total time for each category
            for (long tuple[] : e.getValue()) {
                long tuple_total = 0;
                for (int i = 0, cnt = totals.length-1; i < cnt; i++) {
                    // The last one should be the total time minus the time in the
                    // Java/EE/Coord/Est parts. This is will be considered the misc/bookkeeping time
                    if (i == tuple.length) {
                        totals[i+1] = tuple[0] - tuple_total;
//                        LOG.info(String.format("misc=%d, total=%d, else=%d", totals[i+1], tuple[0], tuple_total));
                    } else {
                        totals[i+1] += tuple[i];
                        if (i > 0) tuple_total += tuple[i];
                    }
                } // FOR
            } // FOR
            
            // Now calculate the average
            rows[row_idx] = new String[header.length];
            rows[row_idx][0] = e.getKey().getName();
            
            for (int i = 0; i < totals.length; i++) {
                // # of Txns
                if (i == 0) {
                    rows[row_idx][i+1] = Long.toString(totals[i]);
                // Everything Else
                } else {
                    rows[row_idx][i+1] = String.format("%.03f", totals[i] / 1000000d);
                }
            } // FOR
            row_idx++;
        }
        return (StringUtil.csv(header, rows));
    }

}
