package edu.brown.hstore;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.ClientResponse;

import edu.brown.hstore.callbacks.TransactionInitQueueCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.TransactionProfile;
import edu.brown.hstore.dtxn.TransactionQueueManager;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.PartitionExecutorPostProcessor;
import edu.brown.hstore.util.ThrottlingQueue;
import edu.brown.hstore.util.TxnCounter;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.logging.RingBufferAppender;
import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;
import edu.brown.utils.TypedPoolableObjectFactory;

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
    
    
//    private static final Pattern THREAD_REGEX = Pattern.compile("(edu\\.brown|edu\\.mit|org\\.voltdb)");
    

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
    private final TreeMap<Integer, PartitionExecutor> executors;
    
    private final Set<AbstractTransaction> last_finishedTxns;
    private final Set<AbstractTransaction> cur_finishedTxns;
    
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
    private final Map<Procedure, LinkedBlockingDeque<long[]>> txn_profile_queues = new TreeMap<Procedure, LinkedBlockingDeque<long[]>>();
    private final Map<Procedure, long[]> txn_profile_totals = Collections.synchronizedSortedMap(new TreeMap<Procedure, long[]>());
    private TableUtil.Format txn_profile_format;
    private String txn_profiler_header[];
    
    final Map<String, Object> header = new ListOrderedMap<String, Object>();
    
    final TreeSet<Thread> sortedThreads = new TreeSet<Thread>(new Comparator<Thread>() {
        @Override
        public int compare(Thread o1, Thread o2) {
            return o1.getName().compareToIgnoreCase(o2.getName());
        }
    });
    
    /**
     * Constructor
     * @param hstore_site
     * @param hstore_conf
     */
    public HStoreSiteStatus(HStoreSite hstore_site, HStoreConf hstore_conf) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_conf;
        this.interval = hstore_conf.site.status_interval;
        
        // The list of transactions that were sitting in the queue as finished
        // the last time that we checked
        if (hstore_conf.site.status_check_for_zombies) {
            this.last_finishedTxns = new HashSet<AbstractTransaction>();
            this.cur_finishedTxns = new HashSet<AbstractTransaction>();
        } else {
            this.last_finishedTxns = null;
            this.cur_finishedTxns = null;
        }
        
        this.executors = new TreeMap<Integer, PartitionExecutor>();
        for (Integer partition : hstore_site.getLocalPartitionIds()) {
            this.executors.put(partition, hstore_site.getPartitionExecutor(partition));
        } // FOR

        // Print a debug message when the first non-sysproc shows up
        this.hstore_site.getStartWorkloadObservable().addObserver(new EventObserver<AbstractTransaction>() {
            @Override
            public void update(EventObservable<AbstractTransaction> arg0, AbstractTransaction arg1) {
//                if (debug.get())
                    LOG.info(HStoreConstants.SITE_FIRST_TXN + " - " + arg1);
            }
        });
        
        // Pre-Compute Header
        this.header.put(String.format("%s Status", HStoreSite.class.getSimpleName()), hstore_site.getSiteName());
        this.header.put("Number of Partitions", this.executors.size());
        
        // Pre-Compute TransactionProfile Information
        this.initTxnProfileInfo(hstore_site.getDatabase());
    }
    
    @Override
    public void run() {
        self = Thread.currentThread();
        self.setName(HStoreThreadManager.getThreadName(hstore_site, "mon"));
        if (hstore_conf.site.cpu_affinity)
            hstore_site.getThreadManager().registerProcessingThread();

        if (LOG.isDebugEnabled()) LOG.debug(String.format("Starting HStoreSite status monitor thread [interval=%d, kill=%s]", this.interval, hstore_conf.site.status_kill_if_hung));
        while (!self.isInterrupted() && this.hstore_site.isShuttingDown() == false) {
            try {
                Thread.sleep(this.interval);
            } catch (InterruptedException ex) {
                return;
            }
            if (this.hstore_site.isShuttingDown()) break;
            if (this.hstore_site.isRunning() == false) continue;

            // Out we go!
            this.printSnapshot();
            
            // If we're not making progress, bring the whole thing down!
            int completed = TxnCounter.COMPLETED.get();
            if (hstore_conf.site.status_kill_if_hung && this.last_completed != null &&
                this.last_completed == completed && hstore_site.getInflightTxnCount() > 0) {
                String msg = String.format("HStoreSite #%d is hung! Killing the cluster!", hstore_site.getSiteId()); 
                LOG.fatal(msg);
                this.hstore_site.getHStoreCoordinator().shutdownCluster(new RuntimeException(msg));
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
    public void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void shutdown() {
        // Dump LOG buffers...
        LOG.debug("Looking for RingBufferAppender messages...");
        for (String msg : RingBufferAppender.getLoggingMessages(Logger.getRootLogger().getLoggerRepository())) {
            System.out.print(msg);
        } // FOR
        
//        hstore_conf.site.status_show_thread_info = true;
        this.printSnapshot();
        
        // Quick Sanity Check!
//        for (int i = 0; i < 2; i++) {
//            Histogram<Long> histogram = new Histogram<Long>();
//            Collection<Integer> localPartitions = hstore_site.getLocalPartitionIds(); 
//            TransactionQueueManager manager = hstore_site.getTransactionQueueManager();
//            for (Integer p : localPartitions) {
//                Long txn_id = manager.getCurrentTransaction(p);
//                if (txn_id != null) histogram.put(txn_id);
//            } // FOR
//            if (histogram.isEmpty()) break;
//            for (Long txn_id : histogram.values()) {
//                if (histogram.get(txn_id) == localPartitions.size()) continue;
//                
//                Map<String, String> m = new ListOrderedMap<String, String>();
//                m.put("TxnId", "#" + txn_id);
//                for (Integer p : hstore_site.getLocalPartitionIds()) {
//                    Long cur_id = manager.getCurrentTransaction(p);
//                    String status = "MISSING";
//                    if (txn_id == cur_id) {
//                        status = "READY";
//                    } else if (manager.getQueue(p).contains(txn_id)) {
//                        status = "QUEUED";
//                        // status += " / " + manager.getQueue(p); 
//                    }
//                    status += " / " + cur_id;
//                    m.put(String.format("  [%02d]", p), status);
//                } // FOR
//                LOG.info(manager.getClass().getSimpleName() + " Status:\n" + StringUtil.formatMaps(m));
//            } // FOR
//            LOG.info("Checking queue again...");
//            manager.checkQueues();
//            break;
//        } // FOR
        
        if (hstore_conf.site.txn_profiling) {
            String csv = this.txnProfileCSV();
            if (csv != null) System.out.println(csv);
        }
        
//        for (ExecutionSite es : this.executors.values()) {
//            TransactionEstimator te = es.getTransactionEstimator();
//            ProfileMeasurement pm = te.CONSUME;
//            System.out.println(String.format("[%02d] CONSUME %.2fms total / %.2fms avg / %d calls",
//                                              es.getPartitionId(), pm.getTotalThinkTimeMS(), pm.getAverageThinkTimeMS(), pm.getInvocations()));
//            pm = te.CACHE;
//            System.out.println(String.format("     CACHE %.2fms total / %.2fms avg / %d calls",
//                                             pm.getTotalThinkTimeMS(), pm.getAverageThinkTimeMS(), pm.getInvocations()));
//            System.out.println(String.format("     ATTEMPTS %d / SUCCESS %d", te.batch_cache_attempts.get(), te.batch_cache_success.get())); 
//        }
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
        ListOrderedMap<String, Object> m_exec = new ListOrderedMap<String, Object>();
        m_exec.put("Completed Txns", TxnCounter.COMPLETED.get());
        
        TransactionQueueManager queueManager = hstore_site.getTransactionQueueManager();
        TransactionQueueManager.DebugContext queueManagerDebug = queueManager.getDebugContext();
        HStoreThreadManager thread_manager = hstore_site.getThreadManager();
        
        int inflight_cur = hstore_site.getInflightTxnCount();
        int inflight_local = queueManagerDebug.getInitQueueSize();
        if (inflight_min == null || inflight_cur < inflight_min) inflight_min = inflight_cur;
        if (inflight_max == null || inflight_cur > inflight_max) inflight_max = inflight_cur;
        
        // Check to see how many of them are marked as finished
        // There is no guarantee that this will be accurate because txns could be swapped out
        // by the time we get through it all
        int inflight_finished = 0;
        int inflight_zombies = 0;
        if (this.cur_finishedTxns != null) this.cur_finishedTxns.clear();
        for (AbstractTransaction ts : hstore_site.getInflightTransactions()) {
           if (ts instanceof LocalTransaction) {
//               LocalTransaction local_ts = (LocalTransaction)ts;
//               ClientResponse cr = local_ts.getClientResponse();
//               if (cr.getStatus() != null) {
//                   inflight_finished++;
//                   // Check for Zombies!
//                   if (this.cur_finishedTxns != null && local_ts.isPredictSinglePartition() == false) {
//                       if (this.last_finishedTxns.contains(ts)) {
//                           inflight_zombies++;
//                       }
//                       this.cur_finishedTxns.add(ts);
//                   }
//               }
           }
        } // FOR
        
        m_exec.put("InFlight Txns", String.format("%d total / %d dtxn / %d finished [totalMin=%d, totalMax=%d]",
                        inflight_cur,
                        inflight_local,
                        inflight_finished,
                        inflight_min,
                        inflight_max
        ));
        
        if (this.cur_finishedTxns != null) {
            m_exec.put("Zombie Txns", inflight_zombies +
                                      (inflight_zombies > 0 ? " - " + CollectionUtil.first(this.cur_finishedTxns) : ""));
//            for (AbstractTransaction ts : this.cur_finishedTxns) {
//                // HACK
//                if (ts instanceof LocalTransaction && this.last_finishedTxns.remove(ts)) {
//                    LocalTransaction local_ts = (LocalTransaction)ts;
//                    local_ts.markAsDeletable();
//                    hstore_site.deleteTransaction(ts.getTransactionId(), local_ts.getClientResponse().getStatus());
//                }
//            }
            this.last_finishedTxns.clear();
            this.last_finishedTxns.addAll(this.cur_finishedTxns);
        }
        
        ProfileMeasurement pm = this.hstore_site.getEmptyQueueTime();
        m_exec.put("Empty Queue", String.format("%d txns / %.2fms total / %.2fms avg",
                        pm.getInvocations(),
                        pm.getTotalThinkTimeMS(),
                        pm.getAverageThinkTimeMS()
        ));
        
        if (hstore_conf.site.exec_postprocessing_thread) {
            int processing_cur = hstore_site.getQueuedResponseCount();
            if (processing_min == null || processing_cur < processing_min) processing_min = processing_cur;
            if (processing_max == null || processing_cur > processing_max) processing_max = processing_cur;
            
            String val = String.format("%-5d [min=%d, max=%d]", processing_cur, processing_min, processing_max);
            int i = 0;
            for (PartitionExecutorPostProcessor espp : hstore_site.getExecutionSitePostProcessors()) {
                pm = espp.getExecTime();
                val += String.format("\n[%02d] %d total / %.2fms total / %.2fms avg",
                                     i++,
                                     pm.getInvocations(),
                                     pm.getTotalThinkTimeMS(),
                                     pm.getAverageThinkTimeMS());
            } // FOR
            
            m_exec.put("Post-Processing Txns", val);
        }
        m_exec.put(" ", null);

        // EXECUTION ENGINES
        Map<Integer, String> partitionLabels = new HashMap<Integer, String>();
        Histogram<Integer> invokedTxns = new Histogram<Integer>();
        for (Entry<Integer, PartitionExecutor> e : this.executors.entrySet()) {
            int partition = e.getKey().intValue();
            String partitionLabel = String.format("%02d", partition);
            partitionLabels.put(partition, partitionLabel);
            
            PartitionExecutor es = e.getValue();
            ThrottlingQueue<?> es_queue = es.getThrottlingQueue();
            ThrottlingQueue<?> dtxn_queue = queueManagerDebug.getInitQueue(partition);
            AbstractTransaction current_dtxn = es.getCurrentDtxn();
            
            // Queue Information
            Map<String, Object> m = new ListOrderedMap<String, Object>();
            
            m.put(String.format("%3d total / %3d queued / %3d blocked / %3d waiting\n",
                                    es_queue.size(),
                                    es.getWorkQueueSize(),
                                    es.getBlockedQueueSize(),
                                    es.getWaitingQueueSize()), null);
            
            // Execution Info
            String status = String.format("%-5s [limit=%d, release=%d]%s",
                                          es_queue.size(), es_queue.getQueueMax(), es_queue.getQueueRelease(),
                                          (es_queue.isThrottled() ? " *THROTTLED*" : ""));
            m.put("Exec Queue", status);
            
            // TransactionQueueManager Info
            status = String.format("%-5s [limit=%d, release=%d]%s / ",
                                   dtxn_queue.size(), dtxn_queue.getQueueMax(), dtxn_queue.getQueueRelease(),
                                   (dtxn_queue.isThrottled() ? " *THROTTLED*" : ""));
            Long txn_id = queueManager.getCurrentTransaction(partition);
            if (txn_id != null) {
                TransactionInitQueueCallback callback = queueManagerDebug.getInitCallback(txn_id);
                int len = status.length();
                status += "#" + txn_id;
                AbstractTransaction ts = hstore_site.getTransaction(txn_id);
                if (ts == null) {
                    // This is ok if the txn is remote
                    // status += " MISSING?";
                } else {
                    status += " [hashCode=" + ts.hashCode() + "]";
                }
                
                if (callback != null) {
                    status += "\n" + StringUtil.repeat(" ", len);
                    status += String.format("Partitions=%s / Remaining=%d", callback.getPartitions(), callback.getCounter());
                }
            }
            m.put("DTXN Queue", status);
            
            // TransactionQueueManager - Blocked
            m.put("Blocked Transactions", queueManagerDebug.getBlockedQueueSize());
            
            // TransactionQueueManager - Requeued Txns
            m.put("Waiting Requeues", queueManagerDebug.getRestartQueueSize());
            
//            if (is_throttled && queue_size < queue_release && hstore_site.isShuttingDown() == false) {
//                LOG.warn(String.format("Partition %d is throttled when it should not be! [inflight=%d, release=%d]",
//                                        partition, queue_size, queue_release));
//            }
            
            
            if (hstore_conf.site.exec_profiling) {
                txn_id = es.getCurrentTxnId();
                m.put("Current Txn", String.format("%s / %s", (txn_id != null ? "#"+txn_id : "-"), es.getExecutionMode()));
                
                m.put("Current DTXN", (current_dtxn == null ? "-" : current_dtxn));
                
                txn_id = es.getLastExecutedTxnId();
                m.put("Last Executed Txn", (txn_id != null ? "#"+txn_id : "-"));
                
                txn_id = es.getLastCommittedTxnId();
                m.put("Last Committed Txn", (txn_id != null ? "#"+txn_id : "-"));
                
                pm = es.getWorkExecTime();
                m.put("Txn Execution", String.format("%d total / %.2fms total / %.2fms avg",
                                                pm.getInvocations(),
                                                pm.getTotalThinkTimeMS(),
                                                pm.getAverageThinkTimeMS()));
                invokedTxns.put(partition, (int)es.getTransactionCounter());
                
                pm = es.getWorkIdleTime();
                m.put("Idle Time", String.format("%.2fms total / %.2fms avg",
                                                pm.getTotalThinkTimeMS(),
                                                pm.getAverageThinkTimeMS()));
            }
            
            
            String label = "    Partition[" + partitionLabel + "]";
            
            // Get additional partition info
            Thread t = es.getExecutionThread();
            if (t != null && thread_manager.isRegistered(t)) {
                for (Integer cpu : thread_manager.getCPUIds(t)) {
                    label += "\n       \u2192 CPU *" + cpu + "*";
                } // FOR
            }
            
            m_exec.put(label, StringUtil.formatMaps(m) + "\n");
        } // FOR
        
        // Incoming Partition Distribution
        if (hstore_site.getIncomingPartitionHistogram().isEmpty() == false) {
            Histogram<Integer> incoming = hstore_site.getIncomingPartitionHistogram();
            incoming.setDebugLabels(partitionLabels);
            m_exec.put("Incoming Txns\nBase Partitions", incoming.toString(50, 10) + "\n");
        }
        if (invokedTxns.isEmpty() == false) {
            invokedTxns.setDebugLabels(partitionLabels);
            m_exec.put("Invoked Txns", invokedTxns.toString(50, 10) + "\n");
        }
        
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
                rows[i][j++] = (cnt != null ? cnt.toString() : "-");
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
    // BATCH PLANNER INFO
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @return
     */
    protected Map<String, String> batchPlannerInfo() {
        // First get all the BatchPlanners that we have
        Collection<BatchPlanner> bps = new HashSet<BatchPlanner>();
        for (PartitionExecutor es : this.executors.values()) {
            bps.addAll(es.batchPlanners.values());
        } // FOR
        Map<Procedure, ProfileMeasurement[]> proc_totals = new HashMap<Procedure, ProfileMeasurement[]>();
        ProfileMeasurement final_totals[] = null;
        int num_cols = 0;
        for (BatchPlanner bp : bps) {
            ProfileMeasurement times[] = bp.getProfileTimes();
            
            Procedure catalog_proc = bp.getProcedure();
            ProfileMeasurement totals[] = proc_totals.get(catalog_proc);
            if (totals == null) {
                num_cols = times.length+2;
                totals = new ProfileMeasurement[num_cols-1];
                final_totals = new ProfileMeasurement[num_cols-1];
                proc_totals.put(catalog_proc, totals);
            }
            for (int i = 0; i < totals.length; i++) {
                if (i == 0) {
                    if (totals[i] == null) totals[i] = new ProfileMeasurement("total");
                } else {
                    if (totals[i] == null)
                        totals[i] = new ProfileMeasurement(times[i-1]);
                    else
                        totals[i].appendTime(times[i-1]);
                    totals[0].appendTime(times[i-1], false);
                }
                if (final_totals[i] == null) final_totals[i] = new ProfileMeasurement(totals[i].getType());
            } // FOR
        } // FOR
        if (proc_totals.isEmpty()) return (null);
        
        boolean first = true;
        String header[] = new String[num_cols];
        Object rows[][] = new String[proc_totals.size()+2][];
        String col_delimiters[] = new String[num_cols];
        String row_delimiters[] = new String[rows.length];
        int i = -1;
        int j = 0;
        for (Procedure proc : proc_totals.keySet()) {
            j = 0;
            rows[++i] = new String[num_cols];
            rows[i][j++] = proc.getName();
            if (first) header[0] = "";
            for (ProfileMeasurement pm : proc_totals.get(proc)) {
                if (first) header[j] = pm.getType();
                final_totals[j-1].appendTime(pm, false);
                rows[i][j] = Long.toString(Math.round(pm.getTotalThinkTimeMS()));
                j++;
            } // FOR
            first = false;
        } // FOR
        
        j = 0;
        rows[++i] = new String[num_cols];
        rows[i+1] = new String[num_cols];
        rows[i][j++] = "TOTAL";
        row_delimiters[i] = "-"; // "\u2015";

        for (int final_idx = 0; final_idx < final_totals.length; final_idx++) {
            if (final_idx == 0) col_delimiters[j] = " | ";
            
            ProfileMeasurement pm = final_totals[final_idx];
            rows[i][j] = Long.toString(Math.round(pm.getTotalThinkTimeMS()));
            rows[i+1][j] = (final_idx > 0 ? String.format("%.3f", pm.getTotalThinkTimeMS() / final_totals[0].getTotalThinkTimeMS()) : ""); 
            j++;
        } // FOR
        
//        if (debug.get()) {
//            for (i = 0; i < rows.length; i++) {
//                LOG.debug("ROW[" + i + "]: " + Arrays.toString(rows[i]));
//            }
//        }
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
        HStoreThreadManager manager = hstore_site.getThreadManager();
        assert(manager != null);
        
        final Map<String, Object> m_thread = new ListOrderedMap<String, Object>();
        final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
        sortedThreads.clear();
        sortedThreads.addAll(threads.keySet());
        
        m_thread.put("Number of Threads", threads.size());
        for (Thread t : sortedThreads) {
            StackTraceElement stack[] = threads.get(t);
            
            String name = StringUtil.abbrv(t.getName(), 24, true);
            if (manager.isRegistered(t) == false) {
                name += " *UNREGISTERED*";
            }
            
            String trace = null;
            if (stack.length == 0) {
                trace = "<NO STACK TRACE>";
//            } else if (t.getName().startsWith("Thread-")) {
//                trace = Arrays.toString(stack);
            } else {
                // Find the first line that is interesting to us
//                trace = StringUtil.join("\n", stack);
                for (int i = 0; i < stack.length; i++) {
                    // if (THREAD_REGEX.matcher(stack[i].getClassName()).matches()) {
                        trace += stack[i].toString();
//                        break;
//                    }
                } // FOR
                if (trace == null) stack[0].toString();
            }
            m_thread.put(name, trace);
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
        // COLUMN DELIMITERS
        String last_prefix = null;
        String col_delimiters[] = new String[TransactionProfile.PROFILE_FIELDS.length + 2];
        int col_idx = 0;
        for (Field f : TransactionProfile.PROFILE_FIELDS) {
            String prefix = f.getName().split("_")[1];
            assert(prefix.isEmpty() == false);
            if (last_prefix != null && col_idx > 0 && prefix.equals(last_prefix) == false) {
                col_delimiters[col_idx+1] = " | ";        
            }
            col_idx++;
            last_prefix = prefix;
        } // FOR
        this.txn_profile_format = new TableUtil.Format("   ", col_delimiters, null, true, false, true, false, false, false, true, true, "-");
        
        // TABLE HEADER
        int idx = 0;
        this.txn_profiler_header = new String[TransactionProfile.PROFILE_FIELDS.length + 2];
        this.txn_profiler_header[idx++] = "";
        this.txn_profiler_header[idx++] = "txns";
        for (int i = 0; i < TransactionProfile.PROFILE_FIELDS.length; i++) {
            String name = TransactionProfile.PROFILE_FIELDS[i].getName()
                                .replace("pm_", "")
                                .replace("_total", "");
            this.txn_profiler_header[idx++] = name;
        } // FOR
        
        // PROCEDURE TOTALS
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.txn_profile_queues.put(catalog_proc, new LinkedBlockingDeque<long[]>());
            
            long totals[] = new long[TransactionProfile.PROFILE_FIELDS.length + 1];
            for (int i = 0; i < totals.length; i++) {
                totals[i] = 0;
            } // FOR
            this.txn_profile_totals.put(catalog_proc, totals);
        } // FOR
    }
    
    /**
     * 
     * @param tp
     */
    public void addTxnProfile(Procedure catalog_proc, TransactionProfile tp) {
        assert(catalog_proc != null);
        assert(tp.isStopped());
        if (trace.get()) LOG.info("Calculating TransactionProfile information");

        long tuple[] = tp.getTuple();
        assert(tuple != null);
        if (trace.get()) LOG.trace(String.format("Appending TransactionProfile: %s", tp, Arrays.toString(tuple)));
        this.txn_profile_queues.get(catalog_proc).offer(tuple);
    }
    
    private void calculateTxnProfileTotals(Procedure catalog_proc) {
        long totals[] = this.txn_profile_totals.get(catalog_proc);
        
        long tuple[] = null;
        LinkedBlockingDeque<long[]> queue = this.txn_profile_queues.get(catalog_proc); 
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
     * @return
     */
    private Object[][] generateTxnProfileSnapshot() {
        // TABLE ROWS
        List<Object[]> rows = new ArrayList<Object[]>(); 
        for (Entry<Procedure, long[]> e : this.txn_profile_totals.entrySet()) {
            this.calculateTxnProfileTotals(e.getKey());
            long totals[] = e.getValue();
            if (totals[0] == 0) continue;

            int col_idx = 0;
            Object row[] = new String[this.txn_profiler_header.length];
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
        Object rows_arr[][] = rows.toArray(new String[rows.size()][this.txn_profiler_header.length]);
        assert(rows_arr.length == rows.size());
        return (rows_arr);
    }
    
    public Map<String, String> txnProfileInfo() {
        Object rows[][] = this.generateTxnProfileSnapshot();
        if (rows == null) return (null);
        return (TableUtil.tableMap(this.txn_profile_format, this.txn_profiler_header, rows));
    }
    
    public String txnProfileCSV() {
        Object rows[][] = this.generateTxnProfileSnapshot();
        if (rows == null) return (null);
        
        if (debug.get()) {
            for (int i = 0; i < rows.length; i++) {
                if (i == 0) LOG.debug("HEADER: " + Arrays.toString(this.txn_profiler_header));
                LOG.debug("ROW[" + i + "] " + Arrays.toString(rows[i]));
            } // FOR
        }
        TableUtil.Format f = TableUtil.defaultCSVFormat().clone();
        f.replace_null_cells = 0;
        f.prune_null_rows = true;
        return (TableUtil.table(f, this.txn_profiler_header, rows));
    }
    
    // ----------------------------------------------------------------------------
    // OBJECT POOL PROFILING
    // ----------------------------------------------------------------------------
    private Map<String, Object> poolInfo() {
        
        // HStoreObjectPools
        Map<String, StackObjectPool> pools = HStoreObjectPools.getAllPools(); 
        
        // MarkovPathEstimators
        pools.put("Estimators", (StackObjectPool)TransactionEstimator.POOL_ESTIMATORS); 

        // TransactionEstimator.States
        pools.put("EstimationStates", (StackObjectPool)TransactionEstimator.POOL_STATES);
        
        final Map<String, Object> m_pool = new ListOrderedMap<String, Object>();
        for (String key : pools.keySet()) {
            StackObjectPool pool = pools.get(key);
            TypedPoolableObjectFactory<?> factory = (TypedPoolableObjectFactory<?>)pool.getFactory();
            if (factory.getCreatedCount() > 0) m_pool.put(key, this.formatPoolCounts(pool, factory));
        } // FOR

//        // Partition Specific
//        String labels[] = new String[] {
//            "LocalTxnState",
//            "RemoteTxnState",
//        };
//        int total_active[] = new int[labels.length];
//        int total_idle[] = new int[labels.length];
//        int total_created[] = new int[labels.length];
//        int total_passivated[] = new int[labels.length];
//        int total_destroyed[] = new int[labels.length];
//        for (int i = 0, cnt = labels.length; i < cnt; i++) {
//            total_active[i] = total_idle[i] = total_created[i] = total_passivated[i] = total_destroyed[i] = 0;
//            pool = (StackObjectPool)(i == 0 ? HStoreObjectPools.STATES_TXN_LOCAL : HStoreObjectPools.STATES_TXN_REMOTE);   
//            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
//            
//            total_active[i] += pool.getNumActive();
//            total_idle[i] += pool.getNumIdle(); 
//            total_created[i] += factory.getCreatedCount();
//            total_passivated[i] += factory.getPassivatedCount();
//            total_destroyed[i] += factory.getDestroyedCount();
//            i += 1;
//        } // FOR
//        
//        for (int i = 0, cnt = labels.length; i < cnt; i++) {
//            m_pool.put(labels[i], String.format(POOL_FORMAT, total_active[i], total_idle[i], total_created[i], total_destroyed[i], total_passivated[i]));
//        } // FOR
        
        return (m_pool);
    }
    
    
    // ----------------------------------------------------------------------------
    // SNAPSHOT PRETTY PRINTER
    // ----------------------------------------------------------------------------
    
    public synchronized String snapshot(boolean show_txns, boolean show_exec, boolean show_threads, boolean show_poolinfo) {
        // ----------------------------------------------------------------------------
        // Transaction Information
        // ----------------------------------------------------------------------------
        Map<String, String> m_txn = (show_txns ? this.txnExecInfo() : null);
        
        // ----------------------------------------------------------------------------
        // Executor Information
        // ----------------------------------------------------------------------------
        Map<String, Object> m_exec = (show_exec ? this.executorInfo() : null);

        // ----------------------------------------------------------------------------
        // Batch Planner Information
        // ----------------------------------------------------------------------------
        Map<String, String> plannerInfo = (hstore_conf.site.planner_profiling ? this.batchPlannerInfo() : null);
        
        // ----------------------------------------------------------------------------
        // Thread Information
        // ----------------------------------------------------------------------------
        Map<String, Object> threadInfo = null;
        Map<String, Object> cpuThreads = null;
        if (show_threads) {
            threadInfo = this.threadInfo();
            
            cpuThreads = new ListOrderedMap<String, Object>();
            for (Entry<Integer, Set<Thread>> e : hstore_site.getThreadManager().getCPUThreads().entrySet()) {
                TreeSet<String> names = new TreeSet<String>();
                for (Thread t : e.getValue())
                    names.add(t.getName());
                cpuThreads.put("CPU #" + e.getKey(), StringUtil.columns(names.toArray(new String[0])));
            } // FOR
        }

        // ----------------------------------------------------------------------------
        // Transaction Profiling
        // ----------------------------------------------------------------------------
        Map<String, String> txnProfiles = (hstore_conf.site.txn_profiling ? this.txnProfileInfo() : null);
        
        // ----------------------------------------------------------------------------
        // Object Pool Information
        // ----------------------------------------------------------------------------
        Map<String, Object> poolInfo = null;
        if (show_poolinfo) poolInfo = this.poolInfo();
        
        String top = StringUtil.formatMaps(header, m_exec, m_txn, threadInfo, cpuThreads, txnProfiles, plannerInfo, poolInfo);
        String bot = "";
        Histogram<Integer> blockedDtxns = hstore_site.getTransactionQueueManager().getDebugContext().getBlockedDtxnHistogram(); 
        if (hstore_conf.site.status_show_txn_info && blockedDtxns != null && blockedDtxns.isEmpty() == false) {
            bot = "\nRejected Transactions by Remote Identifier:\n" + blockedDtxns;
//            bot += "\n" + hstore_site.getTransactionQueueManager().toString();
        }
        
        return (top + bot);
    }
    
    private String formatPoolCounts(StackObjectPool pool, TypedPoolableObjectFactory<?> factory) {
        return (String.format(POOL_FORMAT, pool.getNumActive(),
                                           pool.getNumIdle(),
                                           factory.getCreatedCount(),
                                           factory.getDestroyedCount(),
                                           factory.getPassivatedCount()));
    }
} // END CLASS