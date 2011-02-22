package org.voltdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * 
 * @author pavlo
 */
public class ExecutionSiteHelper implements Runnable {
    public static final Logger LOG = Logger.getLogger(ExecutionSiteHelper.class);
    private final static AtomicBoolean debug = new AtomicBoolean(LOG.isDebugEnabled());
    private final static AtomicBoolean trace = new AtomicBoolean(LOG.isTraceEnabled());
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
    private final Collection<ExecutionSite> sites;
    /**
     * Set to false after the first time we are invoked
     */
    private boolean first = true;
    
    /**
     * 
     * @param sites
     */
    public ExecutionSiteHelper(Collection<ExecutionSite> sites, int max_txn_per_round, int txn_expire, boolean enable_profiling) {
        assert(sites != null);
        assert(sites.isEmpty() == false);
        this.sites = sites;
        this.txn_expire = txn_expire;
        this.txn_per_round = max_txn_per_round;
        this.enable_profiling = enable_profiling;
        
        if (this.enable_profiling) {
            ExecutionSite executor = CollectionUtil.getFirst(this.sites);
            assert(executor != null);
            this.prepareProfileInformation(CatalogUtil.getDatabase(executor.getCatalogSite()));
            executor.getHStoreSite().addShutdownObservable(new EventObserver() {
                @Override
                public void update(Observable o, Object arg) {
                    ExecutionSiteHelper.this.shutdown();
                    LOG.info("Got shutdown notification from HStoreSite. Dumping profile information");
                    System.err.println(StringUtil.box(ExecutionSiteHelper.this.dumpProfileInformation()));
                }
            });
        }
    }
    
    /**
     * 
     * @param sites
     */
    public ExecutionSiteHelper(Collection<ExecutionSite> sites, int max_txn_per_round, int txn_expire) {
        this(sites, max_txn_per_round, txn_expire, false);
    }
    
    @Override
    public synchronized void run() {
        final boolean d = debug.get();
        final boolean t = trace.get();
        
        if (this.first) {
            Thread self = Thread.currentThread();
            HStoreSite hstore_site = CollectionUtil.getFirst(this.sites).hstore_site;
            self.setName(hstore_site.getThreadName("help"));
            this.first = false;
        }
        if (t) LOG.trace("New invocation of the ExecutionSiteHelper. Let's clean-up some txns!");
        
        long to_remove = System.currentTimeMillis() - this.txn_expire;
        for (ExecutionSite es : this.sites) {
            if (t) LOG.trace(String.format("Partition %d has %d finished transactions", es.partitionId, es.finished_txn_states.size()));
            
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
                } else break;
            } // WHILE
            if (d && cleaned > 0) LOG.debug(String.format("Cleaned %d TransactionStates at partition %d", cleaned, es.partitionId));
            // Only call tick here!
            es.tick();
        } // FOR
    }
    
    /**
     * 
     */
    private synchronized void shutdown() {
        LOG.info("Shutdown event received. Cleaning all transactions");
        TransactionState ts = null;
        for (ExecutionSite es : this.sites) {
            int cleaned = 0;
            while ((ts = es.finished_txn_states.poll()) != null) {
                if (this.enable_profiling && ts instanceof LocalTransactionState) {
                    this.calculateProfileInformation((LocalTransactionState)ts);
                }
                cleaned++;
            } // WHILE
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
        if (ts.sysproc) return;
        if (trace.get()) LOG.info("Calculating profile information for txn #" + ts.getTransactionId());
        ProfileMeasurement pms[] = {
            ts.total_time,
            ts.java_time,
            ts.coord_time,
            ts.plan_time,
            ts.ee_time,
            ts.est_time,
        };
        long tuple[] = new long[pms.length];
        for (int i = 0; i < pms.length; i++) {
            if (pms[i] != null) tuple[i] = pms[i].getTotalThinkTime();
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
            "Java Procedure",
            "Coordinator",
            "Planner",
            "ExecutionEngine",
            "Estimation",
            "Miscellaneous",
        };
        int num_procs = 0;
        for (List<long[]> tuples : this.proc_profiles.values()) {
            if (tuples.size() > 0) num_procs++;
        } // FOR
        if (num_procs == 0) return ("<NONE>");
        
        Object rows[][] = new String[num_procs][header.length];
        long totals[] = new long[header.length-1];
        String f = "%.02f";
        
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
                // Total Time
                } else if (i == 1) {
                    // rows[row_idx][i+1] = String.format(f, totals[i] / (double)num_tuples) + "ms";
                    rows[row_idx][i+1] = String.format("%.02f", totals[i] / 1000000d);
                // Everything Else
                } else {
//                    rows[row_idx][i+1] = String.format(f, (totals[i] / (double)totals[0]) * 100) + "%";
                    rows[row_idx][i+1] = String.format("%.02f", totals[i] / 1000000d);
                }
            } // FOR
            row_idx++;
        }
        return (StringUtil.table(header, rows));
    }

}
