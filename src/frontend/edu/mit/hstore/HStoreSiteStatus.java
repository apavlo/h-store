package edu.mit.hstore;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.BatchPlanner;
import org.voltdb.ExecutionSite;

import edu.brown.markov.TransactionEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite.TxnCounter;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;
import edu.mit.hstore.interfaces.Shutdownable;

/**
 * 
 * @author pavlo
 */
public class HStoreSiteStatus implements Runnable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(HStoreSiteStatus.class);
    
    private static final String POOL_FORMAT = "Active:%-5d / Idle:%-5d / Created:%-5d / Passivated:%-5d / Destroyed:%-5d";
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private final int interval; // seconds
    private final boolean kill_when_hanging;
    private final Histogram<Integer> partition_txns = new Histogram<Integer>();
    private final TreeMap<Integer, ExecutionSite> executors;
    
    private Integer last_completed = null;
    
    private Integer inflight_min = null;
    private Integer inflight_max = null;
    
    private Thread self;

    final Map<String, Object> m_txn = new ListOrderedMap<String, Object>();
    final Map<String, Object> m_exec = new ListOrderedMap<String, Object>();
    final Map<String, Object> m_thread = new ListOrderedMap<String, Object>();
    final Map<String, Object> m_pool = new ListOrderedMap<String, Object>();
    final Map<String, Object> header = new ListOrderedMap<String, Object>();
    
    final boolean show_txns = false;
    final boolean show_exec = true;
    final boolean show_threads = false;
    final boolean show_poolinfo = false;;
    
    
    final TreeSet<Thread> sortedThreads = new TreeSet<Thread>(new Comparator<Thread>() {
        @Override
        public int compare(Thread o1, Thread o2) {
            return o1.getName().compareToIgnoreCase(o2.getName());
        }
    });
    
    public HStoreSiteStatus(HStoreSite hstore_site, int interval, boolean kill_when_hanging) {
        this.hstore_site = hstore_site;
        this.hstore_conf = HStoreConf.singleton();
        this.interval = interval;
        this.kill_when_hanging = kill_when_hanging;
        this.executors = new TreeMap<Integer, ExecutionSite>();
        
        this.partition_txns.setKeepZeroEntries(true);
        for (Integer partition : hstore_site.getLocalPartitionIds()) {
            this.partition_txns.put(partition, 0);
            this.executors.put(partition, hstore_site.getExecutionSite(partition));
        } // FOR
        
        this.header.put(String.format("%s Status", HStoreSite.class.getSimpleName()), hstore_site.getSiteName());
        this.header.put("Number of Partitions", this.executors.size());
    }
    
    @Override
    public void run() {
        self = Thread.currentThread();
        self.setName(this.hstore_site.getThreadName("mon"));

        if (LOG.isDebugEnabled()) LOG.debug("Starting HStoreCoordinator status monitor thread [interval=" + interval + " secs]");
        while (!self.isInterrupted() && this.hstore_site.isShuttingDown() == false) {
            try {
                Thread.sleep(interval * 1000);
            } catch (InterruptedException ex) {
                return;
            }
            if (this.hstore_site.isShuttingDown()) break;

            // Out we go!
            this.printSnapshot();
            
            // If we're not making progress, bring the whole thing down!
            int completed = TxnCounter.COMPLETED.get();
            if (this.kill_when_hanging && this.last_completed != null &&
                this.last_completed == completed && hstore_site.getInflightTxnCount() > 0) {
                String msg = String.format("HStoreSite #%d is hung! Killing the cluster!", hstore_site.getSiteId()); 
                LOG.fatal(msg);
                this.hstore_site.getMessenger().shutdownCluster(new RuntimeException(msg));
            }
            this.last_completed = completed;
        } // WHILE
    }
    
    private void printSnapshot() {
        LOG.info("\n" + StringUtil.box(this.snapshot(show_txns, show_exec, show_threads, show_poolinfo)));
    }
    
    @Override
    public void prepareShutdown() {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public void shutdown() {
        this.printSnapshot();
        if (this.self != null) this.self.interrupt();
    }
    
    @Override
    public boolean isShuttingDown() {
        return this.hstore_site.isShuttingDown();
    }
    
    public synchronized String snapshot(boolean show_txns, boolean show_exec, boolean show_threads, boolean show_poolinfo) {
        int inflight_cur = hstore_site.getInflightTxnCount();
        if (inflight_min == null || inflight_cur < inflight_min) inflight_min = inflight_cur;
        if (inflight_max == null || inflight_cur > inflight_max) inflight_max = inflight_cur;
        
        this.partition_txns.clearValues();
        for (Entry<Long, LocalTransactionState> e : hstore_site.getAllTransactions()) {
            this.partition_txns.put(e.getValue().getBasePartition());
        } // FOR
        
        // ----------------------------------------------------------------------------
        // Transaction Information
        // ----------------------------------------------------------------------------
        m_txn.clear();
        if (show_txns) {
            for (TxnCounter tc : TxnCounter.values()) {
                int cnt = tc.get();
                String val = Integer.toString(cnt);
                if (tc != TxnCounter.COMPLETED && tc != TxnCounter.EXECUTED) {
                    val += String.format(" [%.03f]", tc.ratio());
                }
                if (cnt > 0) val += "\n" + tc.getHistogram().toString(50);
                m_txn.put(tc.toString(), val + "\n");
            } // FOR
        }
        
        // ----------------------------------------------------------------------------
        // Executor Information
        // ----------------------------------------------------------------------------
        if (show_exec) {
            m_exec.put("InFlight Txn Ids", String.format("%d [totalMin=%d, totalMax=%d]", inflight_cur, inflight_min, inflight_max));
            m_exec.put("Throttling Mode", String.format("%s [releaseTrigger=%d]\n", this.hstore_site.isThrottlingEnabled(), hstore_conf.site.txn_queue_release));
            
            for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
                ExecutionSite es = e.getValue();
                int partition = e.getKey().intValue();
                TransactionState ts = es.getCurrentDtxn();
                String key = String.format("  Partition[%02d]", partition); 
                String val = String.format("%2d total / %2d queued / %2d blocked / %2d waiting\nCurrent DTXN: %s\nCurrent Mode: %s\n",
                                           this.partition_txns.get(partition),
                                           es.getWorkQueueSize(),
                                           es.getBlockedQueueSize(),
                                           es.getWaitingQueueSize(),
                                           (ts == null ? "" : ts),
                                           es.getExecutionMode());
                m_exec.put(key, val);
            } // FOR
        }

        // ----------------------------------------------------------------------------
        // Thread Information
        // ----------------------------------------------------------------------------
        m_thread.clear();
        if (show_threads) {
            final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            sortedThreads.clear();
            sortedThreads.addAll(threads.keySet());
            m_thread.put("Number of Threads", threads.size());
            for (Thread t : sortedThreads) {
                StackTraceElement stack[] = threads.get(t);
                String trace = null;
                if (stack.length == 0) {
                    trace = "<NONE>";
//                } else if (t.getName().startsWith("Thread-")) {
//                    trace = Arrays.toString(stack);
                } else {
                    trace = stack[0].toString();
                }
                m_thread.put(StringUtil.abbrv(t.getName(), 24, true), trace);
            } // FOR
        }

        // ----------------------------------------------------------------------------
        // Object Pool Information
        // ----------------------------------------------------------------------------
        m_pool.clear();
        if (show_poolinfo) {
            // BatchPlanners
            m_pool.put("BatchPlanners", ExecutionSite.batch_planners.size());

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
            
            // BatchPlans
            int active = 0;
            int idle = 0;
            int created = 0;
            int passivated = 0;
            int destroyed = 0;
            for (BatchPlanner bp : ExecutionSite.batch_planners.values()) {
                pool = (StackObjectPool)bp.getBatchPlanPool();
                factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
                
                active += pool.getNumActive();
                idle += pool.getNumIdle();
                created += factory.getCreatedCount();
                passivated += factory.getPassivatedCount();
                destroyed += factory.getDestroyedCount();
            } // FOR
            m_pool.put("BatchPlans", String.format(POOL_FORMAT, active, idle, created, passivated, destroyed));
            
            // Partition Specific
            String labels[] = new String[] {
                "LocalTxnState",
                "RemoteTxnState",
                "Procedures"
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
                m_pool.put(labels[i], String.format(POOL_FORMAT, total_active[i], total_idle[i], total_created[i], total_passivated[i], total_destroyed[i]));
            } // FOR
        }
        return (StringUtil.formatMaps(header, m_exec, m_txn, m_thread, m_pool));
    }
    
    private String formatPoolCounts(StackObjectPool pool, CountingPoolableObjectFactory<?> factory) {
        return (String.format(POOL_FORMAT, pool.getNumActive(),
                                           pool.getNumIdle(),
                                           factory.getCreatedCount(),
                                           factory.getPassivatedCount(),
                                           factory.getDestroyedCount()));
    }
} // END CLASS