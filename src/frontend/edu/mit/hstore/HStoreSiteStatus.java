package edu.mit.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionProfile;
import edu.mit.hstore.dtxn.TransactionState;
import edu.mit.hstore.interfaces.Shutdownable;

/**
 * 
 * @author pavlo
 */
public class HStoreSiteStatus implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(HStoreSiteStatus.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final String POOL_FORMAT = "Active:%-5d / Idle:%-5d / Created:%-5d / Destroyed:%-5d / Passivated:%-7d";

    private static final Set<TxnCounter> TXNINFO_COL_DELIMITERS = new HashSet<TxnCounter>();
    private static final Set<TxnCounter> TXNINFO_ALWAYS_SHOW = new HashSet<TxnCounter>();
    private static final Set<TxnCounter> TXNINFO_EXCLUDES = new HashSet<TxnCounter>();
    static {
        CollectionUtil.addAll(TXNINFO_COL_DELIMITERS, TxnCounter.EXECUTED,
                                                      TxnCounter.MULTI_PARTITION,
                                                      TxnCounter.MISPREDICTED);
        CollectionUtil.addAll(TXNINFO_ALWAYS_SHOW,    TxnCounter.MULTI_PARTITION,
                                                      TxnCounter.SINGLE_PARTITION,
                                                      TxnCounter.MISPREDICTED);
        CollectionUtil.addAll(TXNINFO_EXCLUDES,       TxnCounter.SYSPROCS);
    }
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final int interval; // milliseconds
    private final Histogram<Integer> partition_txns = new Histogram<Integer>();
    private final TreeMap<Integer, ExecutionSite> executors;
    
    private Integer last_completed = null;
    private AtomicInteger snapshot_ctr = new AtomicInteger(0);
    
    private Integer inflight_min = null;
    private Integer inflight_max = null;
    
    private Integer processing_min = null;
    private Integer processing_max = null;
    
    private Thread self;

    /**
     * Maintain a set of tuples for the transaction profile times
     */
    private final Map<Procedure, LinkedBlockingDeque<long[]>> proc_profiles = new TreeMap<Procedure, LinkedBlockingDeque<long[]>>();
    private final Map<Procedure, long[]> profile_proc_totals = Collections.synchronizedSortedMap(new TreeMap<Procedure, long[]>());
    
    final Map<String, Object> m_pool = new ListOrderedMap<String, Object>();
    final Map<String, Object> header = new ListOrderedMap<String, Object>();
    
    final TreeSet<Thread> sortedThreads = new TreeSet<Thread>(new Comparator<Thread>() {
        @Override
        public int compare(Thread o1, Thread o2) {
            return o1.getName().compareToIgnoreCase(o2.getName());
        }
    });
    
    public HStoreSiteStatus(HStoreSite hstore_site, HStoreConf hstore_conf) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_conf;
        
        // Pull the parameters we need from HStoreConf
        this.interval = hstore_conf.site.status_interval;
        
        this.executors = new TreeMap<Integer, ExecutionSite>();
        
        this.partition_txns.setKeepZeroEntries(true);
        for (Integer partition : hstore_site.getLocalPartitionIds()) {
            this.partition_txns.put(partition, 0);
            this.executors.put(partition, hstore_site.getExecutionSite(partition));
        } // FOR
        
        this.initTxnProfileInfo(hstore_site.catalog_db);
        
        this.header.put(String.format("%s Status", HStoreSite.class.getSimpleName()), hstore_site.getSiteName());
        this.header.put("Number of Partitions", this.executors.size());
    }
    
    @Override
    public void run() {
        self = Thread.currentThread();
        self.setName(this.hstore_site.getThreadName("mon"));

        if (LOG.isDebugEnabled()) LOG.debug(String.format("Starting HStoreSite status monitor thread [interval=%d, kill=%s]", this.interval, hstore_conf.site.status_kill_if_hung));
        while (!self.isInterrupted() && this.hstore_site.isShuttingDown() == false) {
            try {
                Thread.sleep(this.interval);
            } catch (InterruptedException ex) {
                return;
            }
            if (this.hstore_site.isShuttingDown()) break;
            if (this.hstore_site.isReady() == false) continue;

            // Out we go!
            this.printSnapshot();
            
            // If we're not making progress, bring the whole thing down!
            int completed = TxnCounter.COMPLETED.get();
            if (hstore_conf.site.status_kill_if_hung && this.last_completed != null &&
                this.last_completed == completed && hstore_site.getInflightTxnCount() > 0) {
                String msg = String.format("HStoreSite #%d is hung! Killing the cluster!", hstore_site.getSiteId()); 
                LOG.fatal(msg);
                this.hstore_site.getMessenger().shutdownCluster(new RuntimeException(msg));
            }
            this.last_completed = completed;
        } // WHILE
    }
    
    private void printSnapshot() {
        LOG.info("STATUS SNAPSHOT #" + this.snapshot_ctr.incrementAndGet() + "\n" +
                 StringUtil.box(this.snapshot(hstore_conf.site.status_show_txn_info,
                                              hstore_conf.site.status_show_executor_info,
                                              hstore_conf.site.status_show_thread_info,
                                              hstore_conf.site.pool_profiling)));
    }
    
    @Override
    public void prepareShutdown() {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void shutdown() {
        this.printSnapshot();
        if (hstore_conf.site.txn_profiling) {
            String csv = this.txnProfileCSV();
            if (csv != null) System.out.println(csv);
        }
        if (this.self != null) this.self.interrupt();
    }
    
    @Override
    public boolean isShuttingDown() {
        return this.hstore_site.isShuttingDown();
    }
    
    // ----------------------------------------------------------------------------
    // EXECUTION INFO
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @return
     */
    protected Map<String, Object> executorInfo() {
        int inflight_cur = hstore_site.getInflightTxnCount();
        if (inflight_min == null || inflight_cur < inflight_min) inflight_min = inflight_cur;
        if (inflight_max == null || inflight_cur > inflight_max) inflight_max = inflight_cur;
        
        int processing_cur = hstore_site.getExecutionSitePostProcessor().getQueueSize();
        if (processing_min == null || processing_cur < processing_min) processing_min = processing_cur;
        if (processing_max == null || processing_cur > processing_max) processing_max = processing_cur;
        
        Map<String, Object> m_exec = new ListOrderedMap<String, Object>();
        m_exec.put("Completed Txns", TxnCounter.COMPLETED.get());
        m_exec.put("InFlight Txns", String.format("%d [totalMin=%d, totalMax=%d]", inflight_cur, inflight_min, inflight_max));
        m_exec.put("Processing Txns", String.format("%d [totalMin=%d, totalMax=%d]", processing_cur, processing_min, processing_max));
        m_exec.put("Incoming Throttle", String.format("%-5s [limit=%d, release=%d]",
                                                      this.hstore_site.isIncomingThrottled(),
                                                      hstore_conf.site.txn_incoming_queue_max,
                                                      hstore_conf.site.txn_incoming_queue_release));
        m_exec.put("Redirect Throttle", String.format("%-5s [limit=%d, release=%d]\n",
                                                      this.hstore_site.isRedirectedThrottled(),
                                                      hstore_conf.site.txn_redirect_queue_max,
                                                      hstore_conf.site.txn_redirect_queue_release));

        
        for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
            ExecutionSite es = e.getValue();
            int partition = e.getKey().intValue();
            TransactionState ts = es.getCurrentDtxn();
            String key = String.format("    Partition[%02d]", partition);
            
            StringBuilder sb = new StringBuilder();
            
            // Queue Information
            sb.append(String.format("%3d total / %3d queued / %3d blocked / %3d waiting\n",
                                    this.partition_txns.get(partition),
                                    es.getWorkQueueSize(),
                                    es.getBlockedQueueSize(),
                                    es.getWaitingQueueSize()));
            
            // Execution Info
            sb.append("Current DTXN:   ").append(ts == null ? "-" : ts).append("\n");
            sb.append("Execution Mode: ").append(es.getExecutionMode()).append("\n");
            
            // Queue Time
            if (hstore_conf.site.exec_profiling) {
                ProfileMeasurement pm = es.getWorkQueueProfileMeasurement();
                sb.append(String.format("Idle Time:      %.2fms total / %.2fms avg\n",
                                        pm.getTotalThinkTime()/1000000d,
                                        pm.getAverageThinkTime()/1000000d));
            }
            
            m_exec.put(key, sb.toString());
        } // FOR
        return (m_exec);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION EXECUTION INFO
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @return
     */
    protected Map<String, String> txnExecInfo() {
        Set<TxnCounter> cnts_to_include = new TreeSet<TxnCounter>();
        Set<String> procs = TxnCounter.getAllProcedures();
        if (procs.isEmpty()) return (null);
        for (TxnCounter tc : TxnCounter.values()) {
            if (TXNINFO_ALWAYS_SHOW.contains(tc) || (tc.get() > 0 && TXNINFO_EXCLUDES.contains(tc) == false)) cnts_to_include.add(tc);
        } // FOR
        
        boolean first = true;
        int num_cols = cnts_to_include.size() + 1;
        String header[] = new String[num_cols];
        Object rows[][] = new String[procs.size()+2][];
        String col_delimiters[] = new String[num_cols];
        String row_delimiters[] = new String[rows.length];
        int i = -1;
        int j = 0;
        for (String proc_name : procs) {
            j = 0;
            rows[++i] = new String[num_cols];
            rows[i][j++] = proc_name;
            if (first) header[0] = "";
            for (TxnCounter tc : cnts_to_include) {
                if (first) header[j] = tc.toString().replace("partition", "P");
                Long cnt = tc.getHistogram().get(proc_name);
                rows[i][j++] = (cnt != null ? Long.toString(cnt) : "-");
            } // FOR
            first = false;
        } // FOR
        
        j = 0;
        rows[++i] = new String[num_cols];
        rows[i+1] = new String[num_cols];
        rows[i][j++] = "TOTAL";
        row_delimiters[i] = "-"; // "\u2015";
        
        for (TxnCounter tc : cnts_to_include) {
            if (TXNINFO_COL_DELIMITERS.contains(tc)) col_delimiters[j] = " | ";
            
            if (tc == TxnCounter.COMPLETED || tc == TxnCounter.RECEIVED) {
                rows[i][j] = Integer.toString(tc.get());
                rows[i+1][j] = "";
            } else {
                Double ratio = tc.ratio();
                rows[i][j] = (ratio == null ? "-" : Integer.toString(tc.get()));
                rows[i+1][j] = (ratio == null ? "-": String.format("%.3f", ratio));
            }
            j++;
        } // FOR
        
        if (debug.get()) {
            for (i = 0; i < rows.length; i++) {
                LOG.debug("ROW[" + i + "]: " + Arrays.toString(rows[i]));
            }
        }
        TableUtil.Format f = new TableUtil.Format("   ", col_delimiters, row_delimiters, true, false, true, false, false, false, true, true, null);
        return (TableUtil.tableMap(f, header, rows));
    }
    
    // ----------------------------------------------------------------------------
    // THREAD INFO
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @return
     */
    protected Map<String, Object> threadInfo() {
        final Map<String, Object> m_thread = new ListOrderedMap<String, Object>();
        final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
        sortedThreads.clear();
        sortedThreads.addAll(threads.keySet());
        m_thread.put("Number of Threads", threads.size());
        for (Thread t : sortedThreads) {
            StackTraceElement stack[] = threads.get(t);
            String trace = null;
            if (stack.length == 0) {
                trace = "<NONE>";
//            } else if (t.getName().startsWith("Thread-")) {
//                trace = Arrays.toString(stack);
            } else {
                trace = stack[0].toString();
            }
            m_thread.put(StringUtil.abbrv(t.getName(), 24, true), trace);
        } // FOR
        return (m_thread);
    }
    
    // ----------------------------------------------------------------------------
    // TRANSACTION PROFILING
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @param catalog_db
     */
    private void initTxnProfileInfo(Database catalog_db) {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.proc_profiles.put(catalog_proc, new LinkedBlockingDeque<long[]>());
            
            long totals[] = new long[TransactionProfile.PROFILE_FIELDS.length + 1];
            for (int i = 0; i < totals.length; i++) {
                totals[i] = 0;
            } // FOR
            this.profile_proc_totals.put(catalog_proc, totals);
        } // FOR
    }
    
    /**
     * 
     * @param tp
     */
    public void addTxnProfile(Procedure catalog_proc, TransactionProfile tp) {
        if (tp.total_time.isStopped() == false) return;
        if (trace.get()) LOG.info("Calculating TransactionProfile information");

        long tuple[] = tp.getTuple();
        assert(tuple != null);
        assert(catalog_proc != null);
        if (trace.get()) LOG.trace(String.format("Appending TransactionProfile: %s", tp, Arrays.toString(tuple)));
        this.proc_profiles.get(catalog_proc).offer(tuple);
    }
    
    private void calculateTxnProfileTotals(Procedure catalog_proc) {
        long totals[] = this.profile_proc_totals.get(catalog_proc);
        
        long tuple[] = null;
        LinkedBlockingDeque<long[]> queue = this.proc_profiles.get(catalog_proc); 
        while ((tuple = queue.poll()) != null) {
            totals[0]++;
            for (int i = 0, cnt = tuple.length; i < cnt; i++) {
                totals[i+1] += tuple[i];
            } // FOR
        } // FOR
    }
    
    /**
     * 
     * TODO: This should be broken out in a separate component that stores the data
     *       down in the EE. That way we can extract it in a variety of ways
     * 
     * @param dump_csv
     * @return
     */
    private Pair<String[], Object[][]> generateTxnProfileSnapshot() {
        
        // TABLE HEADER
        int idx = 0;
        String header[] = new String[TransactionProfile.PROFILE_FIELDS.length + 2];
        header[idx++] = "";
        header[idx++] = "txns";
        for (int i = 0; i < TransactionProfile.PROFILE_FIELDS.length; i++) {
            header[idx++] = TransactionProfile.PROFILE_FIELDS[i].getName().replace("_time", "");
        } // FOR
        
        // TABLE ROWS
        List<Object[]> rows = new ArrayList<Object[]>(); 
        for (Entry<Procedure, long[]> e : this.profile_proc_totals.entrySet()) {
            this.calculateTxnProfileTotals(e.getKey());
            long totals[] = e.getValue();
            if (totals[0] == 0) continue;

            int col_idx = 0;
            Object row[] = new String[header.length];
            row[col_idx++] = e.getKey().getName();
            
            for (int i = 0; i < totals.length; i++) {
                // # of Txns
                if (i == 0) {
                    row[col_idx++] = Long.toString(totals[i]);
                // Everything Else
                } else {
                    row[col_idx++] = (totals[i] > 0 ? String.format("%.02f", totals[i] / 1000000d) : null);
                }
            } // FOR
            if (debug.get()) LOG.debug("ROW[" + rows.size() + "] " + Arrays.toString(row));
            rows.add(row);
        } // FOR
        if (rows.isEmpty()) return (null);
        Object rows_arr[][] = rows.toArray(new String[rows.size()][header.length]);
        assert(rows_arr.length == rows.size());
        
        return (Pair.of(header, rows_arr));
    }
    
    public Map<String, String> txnProfileInfo() {
        Pair<String[], Object[][]> pair = this.generateTxnProfileSnapshot();
        if (pair == null) return (null);
        String header[] = pair.getFirst();
        Object rows[][] = pair.getSecond();

        String col_delimiters[] = new String[header.length];
        col_delimiters[2] = " | ";
        
        TableUtil.Format f = new TableUtil.Format("   ", col_delimiters, null, true, false, true, false, false, false, true, true, "-");
        return (TableUtil.tableMap(f, header, rows));
    }
    
    public String txnProfileCSV() {
        Pair<String[], Object[][]> pair = this.generateTxnProfileSnapshot();
        if (pair == null) return (null);
        String header[] = pair.getFirst();
        Object rows[][] = pair.getSecond();
        
        if (debug.get()) {
            for (int i = 0; i < rows.length; i++) {
                if (i == 0) LOG.debug("HEADER: " + Arrays.toString(header));
                LOG.debug("ROW[" + i + "] " + Arrays.toString(rows[i]));
            } // FOR
        }
        TableUtil.Format f = TableUtil.defaultCSVFormat().clone();
        f.replace_null_cells = 0;
        f.prune_null_rows = true;
        return (TableUtil.table(f, header, rows));
    }
    
    // ----------------------------------------------------------------------------
    // SNAPSHOT PRETTY PRINTER
    // ----------------------------------------------------------------------------
    
    public synchronized String snapshot(boolean show_txns, boolean show_exec, boolean show_threads, boolean show_poolinfo) {
        this.partition_txns.clearValues();
        for (Entry<Long, LocalTransactionState> e : hstore_site.getAllTransactions()) {
            this.partition_txns.put(e.getValue().getBasePartition());
        } // FOR
        
        // ----------------------------------------------------------------------------
        // Transaction Information
        // ----------------------------------------------------------------------------
        Map<String, String> m_txn = (show_txns ? this.txnExecInfo() : null);
        
        // ----------------------------------------------------------------------------
        // Executor Information
        // ----------------------------------------------------------------------------
        Map<String, Object> m_exec = (show_exec ? this.executorInfo() : null);

        // ----------------------------------------------------------------------------
        // Thread Information
        // ----------------------------------------------------------------------------
        Map<String, Object> threadInfo = (show_threads ? this.threadInfo() : null);

        // ----------------------------------------------------------------------------
        // Transaction Profiling
        // ----------------------------------------------------------------------------
        Map<String, String> txnProfiles = (hstore_conf.site.txn_profiling ? this.txnProfileInfo() : null);
        
        // ----------------------------------------------------------------------------
        // Object Pool Information
        // ----------------------------------------------------------------------------
        m_pool.clear();
        if (show_poolinfo) {
            // BatchPlanners
            m_pool.put("BatchPlanners", ExecutionSite.POOL_BATCH_PLANNERS.size());

            // MarkovPathEstimators
            StackObjectPool pool = (StackObjectPool)TransactionEstimator.getEstimatorPool();
            CountingPoolableObjectFactory<?> factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m_pool.put("Estimators", this.formatPoolCounts(pool, factory));

            // TransactionEstimator.States
            pool = (StackObjectPool)TransactionEstimator.getStatePool();
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m_pool.put("EstimationStates", this.formatPoolCounts(pool, factory));
            
            // DependencyInfos
            pool = (StackObjectPool)DependencyInfo.INFO_POOL;
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m_pool.put("DependencyInfos", this.formatPoolCounts(pool, factory));
            
            // ForwardTxnRequestCallbacks
            pool = (StackObjectPool)HStoreSite.POOL_FORWARDTXN_REQUEST;
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m_pool.put("ForwardTxnRequests", this.formatPoolCounts(pool, factory));
            
            // ForwardTxnResponseCallbacks
            pool = (StackObjectPool)HStoreSite.POOL_FORWARDTXN_RESPONSE;
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m_pool.put("ForwardTxnResponses", this.formatPoolCounts(pool, factory));
            
            // BatchPlans
            int active = 0;
            int idle = 0;
            int created = 0;
            int passivated = 0;
            int destroyed = 0;
            for (BatchPlanner bp : ExecutionSite.POOL_BATCH_PLANNERS.values()) {
                pool = (StackObjectPool)bp.getBatchPlanPool();
                factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
                
                active += pool.getNumActive();
                idle += pool.getNumIdle();
                created += factory.getCreatedCount();
                passivated += factory.getPassivatedCount();
                destroyed += factory.getDestroyedCount();
            } // FOR
            m_pool.put("BatchPlans", String.format(POOL_FORMAT, active, idle, created, destroyed, passivated));
            
            // Partition Specific
            String labels[] = new String[] {
                "LocalTxnState",
                "RemoteTxnState",
            };
            int total_active[] = new int[labels.length];
            int total_idle[] = new int[labels.length];
            int total_created[] = new int[labels.length];
            int total_passivated[] = new int[labels.length];
            int total_destroyed[] = new int[labels.length];
            for (int i = 0, cnt = labels.length; i < cnt; i++) {
                total_active[i] = total_idle[i] = total_created[i] = total_passivated[i] = total_destroyed[i] = 0;
            }
            
            for (ExecutionSite e : executors.values()) {
                int i = 0;
                for (ObjectPool p : new ObjectPool[] { e.localTxnPool, e.remoteTxnPool }) {
                    pool = (StackObjectPool)p;
                    factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
                    
                    total_active[i] += p.getNumActive();
                    total_idle[i] += p.getNumIdle(); 
                    total_created[i] += factory.getCreatedCount();
                    total_passivated[i] += factory.getPassivatedCount();
                    total_destroyed[i] += factory.getDestroyedCount();
                    i += 1;
                } // FOR
            } // FOR
            
            for (int i = 0, cnt = labels.length; i < cnt; i++) {
                m_pool.put(labels[i], String.format(POOL_FORMAT, total_active[i], total_idle[i], total_created[i], total_destroyed[i], total_passivated[i]));
            } // FOR
        }
        return (StringUtil.formatMaps(header, m_exec, m_txn, threadInfo, txnProfiles, m_pool));
    }
    
    private String formatPoolCounts(StackObjectPool pool, CountingPoolableObjectFactory<?> factory) {
        return (String.format(POOL_FORMAT, pool.getNumActive(),
                                           pool.getNumIdle(),
                                           factory.getCreatedCount(),
                                           factory.getDestroyedCount(),
                                           factory.getPassivatedCount()));
    }
} // END CLASS