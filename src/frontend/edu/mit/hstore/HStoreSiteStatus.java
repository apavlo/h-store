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
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreSite.TxnCounter;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;

/**
 * 
 * @author pavlo
 */
public class HStoreSiteStatus implements Runnable {
    private static final Logger LOG = Logger.getLogger(HStoreSiteStatus.class);
    
    private static final String POOL_FORMAT = "Active:%-5d / Idle:%-5d / Created:%-5d / Passivated:%-5d / Destroyed:%-5d";
    
    private final HStoreSite hstore_site;
    private final int interval; // seconds
    private final boolean kill_when_hanging;
    private final TreeMap<Integer, TreeSet<Long>> partition_txns = new TreeMap<Integer, TreeSet<Long>>();
    private final TreeMap<Integer, ExecutionSite> executors;
    
    private Integer last_completed = null;
    
    private Integer inflight_min = null;
    private Integer inflight_max = null;

    final Map<String, Object> m0 = new ListOrderedMap<String, Object>();
    final Map<String, Object> m1 = new ListOrderedMap<String, Object>();
    final Map<String, Object> m2 = new ListOrderedMap<String, Object>();
    final Map<String, Object> header = new ListOrderedMap<String, Object>();
    final TreeSet<Thread> sortedThreads = new TreeSet<Thread>(new Comparator<Thread>() {
        @Override
        public int compare(Thread o1, Thread o2) {
            return o1.getName().compareToIgnoreCase(o2.getName());
        }
    });
    
    public HStoreSiteStatus(HStoreSite hstore_site, int interval, boolean kill_when_hanging) {
        this.hstore_site = hstore_site;
        this.interval = interval;
        this.kill_when_hanging = kill_when_hanging;
        this.executors = new TreeMap<Integer, ExecutionSite>(hstore_site.executors);
        
        for (Integer partition : hstore_site.all_partitions) {
            this.partition_txns.put(partition, new TreeSet<Long>());
        } // FOR
        
        this.header.put(String.format("%s Status", HStoreSite.class.getSimpleName()), String.format("Site #%02d", hstore_site.catalog_site.getId()));
        this.header.put("Number of Partitions", this.executors.size());
    }
    
    @Override
    public void run() {
        final Thread self = Thread.currentThread();
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
            LOG.info("\n" + StringUtil.box(this.snapshot(true, false, false)));
            
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
    
    public synchronized String snapshot(boolean show_txns, boolean show_threads, boolean show_poolinfo) {
        int inflight_cur = hstore_site.inflight_txns.size();
        if (inflight_min == null || inflight_cur < inflight_min) inflight_min = inflight_cur;
        if (inflight_max == null || inflight_cur > inflight_max) inflight_max = inflight_cur;
        
        for (Integer partition : this.partition_txns.keySet()) {
            this.partition_txns.get(partition).clear();
        } // FOR
        for (Entry<Long, LocalTransactionState> e : hstore_site.inflight_txns.entrySet()) {
            this.partition_txns.get(e.getValue().getBasePartition()).add(e.getKey());
        } // FOR
        
        // ----------------------------------------------------------------------------
        // Transaction Information
        // ----------------------------------------------------------------------------
        m0.clear();
        if (show_txns) {
            for (TxnCounter tc : TxnCounter.values()) {
                int cnt = tc.get();
                String val = Integer.toString(cnt);
                if (tc != TxnCounter.COMPLETED && tc != TxnCounter.EXECUTED) {
                    val += String.format(" [%.03f]", tc.ratio());
                }
                if (cnt > 0) val += "\n" + tc.getHistogram().toString(50);
                m0.put(tc.toString(), val + "\n");
            } // FOR

            m0.put("InFlight Txn Ids", String.format("%d [min=%d, max=%d]", inflight_cur, inflight_min, inflight_max));
            for (Entry<Integer, ExecutionSite> e : this.executors.entrySet()) {
                int partition = e.getKey().intValue();
                String key = String.format("  Partition[%02d]", partition); 
                String val = String.format("%2d txns / %2d queue", this.partition_txns.get(partition).size(), e.getValue().getQueueSize());
                m0.put(key, val);
            } // FOR
            m0.put("Throttling Mode", this.hstore_site.isThrottlingEnabled());
        }

        // ----------------------------------------------------------------------------
        // Thread Information
        // ----------------------------------------------------------------------------
        m1.clear();
        if (show_threads) {
            final Map<Thread, StackTraceElement[]> threads = Thread.getAllStackTraces();
            sortedThreads.clear();
            sortedThreads.addAll(threads.keySet());
            m1.put("Number of Threads", threads.size());
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
                m1.put(StringUtil.abbrv(t.getName(), 24, true), trace);
            } // FOR
        }

        // ----------------------------------------------------------------------------
        // Object Pool Information
        // ----------------------------------------------------------------------------
        m2.clear();
        if (show_poolinfo) {
            // BatchPlanners
            m2.put("BatchPlanners", ExecutionSite.batch_planners.size());

            // MarkovPathEstimators
            StackObjectPool pool = (StackObjectPool)TransactionEstimator.getEstimatorPool();
            CountingPoolableObjectFactory<?> factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m2.put("Estimators", this.formatPoolCounts(pool, factory));

            // TransactionEstimator.States
            pool = (StackObjectPool)TransactionEstimator.getStatePool();
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m2.put("EstimationStates", this.formatPoolCounts(pool, factory));
            
            // DependencyInfos
            pool = (StackObjectPool)DependencyInfo.INFO_POOL;
            factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
            m2.put("DependencyInfos", this.formatPoolCounts(pool, factory));
            
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
            m2.put("BatchPlans", String.format(POOL_FORMAT, active, idle, created, passivated, destroyed));
            
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

                for (ObjectPool p : e.procPool.values()) {
                    pool = (StackObjectPool)p;
                    factory = (CountingPoolableObjectFactory<?>)pool.getFactory();
                    total_active[i] += p.getNumActive();
                    total_idle[i] += p.getNumIdle();
                    total_created[i] += factory.getCreatedCount();
                    total_passivated[i] += factory.getPassivatedCount();
                    total_destroyed[i] += factory.getDestroyedCount();
                } // FOR
            } // FOR
            
            for (int i = 0, cnt = labels.length; i < cnt; i++) {
                m2.put(labels[i], String.format(POOL_FORMAT, total_active[i], total_idle[i], total_created[i], total_passivated[i], total_destroyed[i]));
            } // FOR
        }
        return (StringUtil.formatMaps(header, m0, m1, m2));
    }
    
    private String formatPoolCounts(StackObjectPool pool, CountingPoolableObjectFactory<?> factory) {
        return (String.format(POOL_FORMAT, pool.getNumActive(),
                                           pool.getNumIdle(),
                                           factory.getCreatedCount(),
                                           factory.getPassivatedCount(),
                                           factory.getDestroyedCount()));
    }
} // END CLASS