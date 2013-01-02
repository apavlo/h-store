package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.utils.ThreadUtils;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

public class HStoreThreadManager {
    private static final Logger LOG = Logger.getLogger(HStoreThreadManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public enum ThreadGroupType {
        PROCESSING,
        EXECUTION,
        NETWORK,
        AUXILIARY
    };
    
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    
    private boolean disable;
    
    private final ScheduledThreadPoolExecutor periodicWorkExecutor;
    private final int num_partitions;
    private final int num_cores = ThreadUtil.getMaxGlobalThreads();
    private final boolean defaultAffinity[];
    private final Set<Thread> all_threads = new HashSet<Thread>();
    private final Map<Integer, Set<Thread>> cpu_threads = new HashMap<Integer, Set<Thread>>();
    private final int ee_core_offset;
    
    private final Map<ThreadGroupType, ThreadGroup> threadGroups = new HashMap<ThreadGroupType, ThreadGroup>();
    
    private final Map<String, boolean[]> utilityAffinities = new HashMap<String, boolean[]>();
    private final String utility_suffixes[] = {
        HStoreConstants.THREAD_NAME_COMMANDLOGGER,
        HStoreConstants.THREAD_NAME_PERIODIC,
        HStoreConstants.THREAD_NAME_COORDINATOR,
        HStoreConstants.THREAD_NAME_TXNCLEANER,
        HStoreConstants.THREAD_NAME_TXNQUEUE,
    };
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    public HStoreThreadManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.num_partitions = this.hstore_site.getLocalPartitionIds().size();
        
        // Periodic Work Thread
        String threadName = getThreadName(hstore_site, HStoreConstants.THREAD_NAME_PERIODIC);
        this.periodicWorkExecutor = ThreadUtil.getScheduledThreadPoolExecutor(threadName,
                                                                              hstore_site.getExceptionHandler(),
                                                                              1,
                                                                              1024 * 128);
        this.defaultAffinity = new boolean[this.num_cores];
        Arrays.fill(this.defaultAffinity, true);
        
        Host host = hstore_site.getHost();
        Collection<Site> sites = CatalogUtil.getSitesForHost(host);
        Collection<Partition> host_partitions = CatalogUtil.getPartitionsForHost(host);
        
        if (hstore_conf.site.cpu_affinity == false) {
            this.disable = true;
            this.ee_core_offset = 0;
        }
        else if (this.num_cores <= host_partitions.size()) {
//            if (debug.val)
                LOG.warn(String.format("Unable to set CPU affinity on %s because there are %d partitions " +
                		               "but only %d available cores",
                                       host.getIpaddr(), host_partitions.size(), this.num_cores));
            this.disable = true;
            this.ee_core_offset = 0;
        }
        else {
            // IMPORTANT: There is a funkiness with the JVM on linux where the first core
            // is always used for internal system threads. So if we put anything
            // import on the first core, then it will always run slower.
            int ee_core_offset = 0;
            if (this.num_cores > 4 && this.num_partitions < this.num_cores) {
                ee_core_offset = 1;
            }
            for (int i = 0; i < host_partitions.size(); i++) {
                this.defaultAffinity[i+ee_core_offset] = false;
            } // FOR
            
            // IMPORTANT: If there are multiple sites at this node, then we need
            // to make sure that we offset ourselves so that we don't overlap.
            if (sites.size() != 1) {
                for (Site site : sites) {
                    if (site != hstore_site.getSite()) {
                        ee_core_offset += site.getPartitions().size();
                    } else {
                        break;
                    }
                } // FOR
            }
            if (debug.val) LOG.debug("EE CPU Core Offset: " + ee_core_offset);
            this.ee_core_offset = ee_core_offset;
            
            // Reserve the lowest cores for the various utility threads
            if ((this.num_cores - this.num_partitions) > this.utility_suffixes.length) {
                for (int i = 0; i < this.utility_suffixes.length; i++) {
                    boolean affinity[] = this.utilityAffinities.get(this.utility_suffixes[i]);
                    if (affinity == null) {
                        affinity = new boolean[this.num_cores];
                        Arrays.fill(affinity, false);
                    }
                    int core = this.num_cores - (i+1); 
                    affinity[core] = true;
                    this.defaultAffinity[core] = false;
                    this.utilityAffinities.put(this.utility_suffixes[i], affinity);
                } // FOR
            }
            if (debug.val) LOG.debug("Default CPU Affinity: " + Arrays.toString(this.defaultAffinity));
        }
    }
    
    public synchronized ThreadGroup getThreadGroup(ThreadGroupType type) {
        ThreadGroup group = this.threadGroups.get(type);
        if (group == null) {
            String name = StringUtil.title(type.name()) + " Threads";
            group = new ThreadGroup(name);
            this.threadGroups.put(type, group);
        }
        return (group);
    }
    
    // ----------------------------------------------------------------------------
    // PERIODIC WORK EXECUTOR
    // ----------------------------------------------------------------------------
    
    /**
     * Internal method to register our single periodic thread with ourself.
     * This is a blocking call that will wait until the initialization
     * thread is succesfully executed.
     */
    protected void initPerioidicThread() {
        final CountDownLatch latch = new CountDownLatch(1);
        Runnable r = new Runnable() {
            @Override
            public void run() {
                HStoreThreadManager.this.registerProcessingThread();
                latch.countDown();
            }
        };
        this.scheduleWork(r);
        
        // Wait until it's finished 
        boolean ret = false;
        try {
            ret = latch.await(1000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            // Ignore...
        }
        assert(ret) : "Failed to initialize perioidic thread";
    }
    
    public ScheduledThreadPoolExecutor getPeriodicWorkExecutor() {
        return (this.periodicWorkExecutor);
    }
    
    /**
     * From VoltDB
     * @param work
     * @param initialDelay
     * @param delay
     * @param unit
     * @return
     */
    public ScheduledFuture<?> schedulePeriodicWork(Runnable work, long initialDelay, long delay, TimeUnit unit) {
        assert(delay > 0);
        return this.periodicWorkExecutor.scheduleWithFixedDelay(work, initialDelay, delay, unit);
    }

    /**
     * Schedule a Runnable to be run once and only once with no initial dealy
     * @param work
     * @return
     */
    public ScheduledFuture<?> scheduleWork(Runnable work) {
        return this.periodicWorkExecutor.schedule(work, 0, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Schedule a Runnable to be run once and only once. The initialDelay specifies
     * how long the scheduler should wait before invoking the Runnable
     * @param work
     * @param initialDelay
     * @param unit
     * @return
     */
    public ScheduledFuture<?> scheduleWork(Runnable work, long initialDelay, TimeUnit unit) {
        return this.periodicWorkExecutor.schedule(work, initialDelay, unit);
    }
    
    
    // ----------------------------------------------------------------------------
    // THREAD-TO-CPU AFFINITY METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Set the CPU affinity for the EE thread executing for the given partition
     * @param partition
     */
    public boolean registerEEThread(Partition partition) {
        if (this.disable) return (false);
        
        Thread t = Thread.currentThread();
        boolean affinity[] = null;
        try {
            affinity = org.voltdb.utils.ThreadUtils.getThreadAffinity();
        // This doesn't seem to work on newer versions of OSX, so we'll just disable it
        } catch (UnsatisfiedLinkError ex) {
            LOG.warn("Unable to set CPU affinity for the ExecutionEngine thread for partition" + partition + ". " +
            		 "Disabling feature in ExecutionEngine", ex);
            this.disable = true;
            return (false);
        }
        assert(affinity != null);
        Arrays.fill(affinity, false);
        
        // Only allow this EE to execute on a single core
        if (hstore_site.getHStoreConf().site.cpu_affinity_one_partition_per_core) {
            int core = partition.getRelativeIndex()-1 % affinity.length; 
            affinity[core+this.ee_core_offset] = true;
        }
        // Allow this EE to run on any of the lower cores
        else {
            for (int i = 0; i < this.num_partitions; i++) {
                affinity[i+this.ee_core_offset] = true;
            } // FOR
        }
        
        if (debug.val)
            LOG.debug(String.format("Registering EE Thread %s to execute on CPUs %s",
                                    t.getName(), this.getCPUIds(affinity)));
        
        this.disable = (ThreadUtils.setThreadAffinity(affinity) == false);
        if (this.disable) {
            LOG.warn("Unable to set CPU affinity for thread '" + t.getName() + "'. Disabling feature");
            return (false);
        }
        this.registerThread(affinity);
        
        final boolean endingAffinity[] = ThreadUtils.getThreadAffinity();
        for (int ii = 0; ii < endingAffinity.length; ii++) {
            if (trace.val && endingAffinity[ii]) LOG.trace(String.format("NEW AFFINITY %s -> CPU[%d]", partition, ii));
            affinity[ii] = false;
        } // FOR
        if (debug.val) LOG.debug(String.format("Successfully set affinity for thread '%s' on CPUs %s",
                                   t.getName(), this.getCPUIds(affinity)));
        return (true);
    }
    
    /**
     * Set the CPU affinity for a non-EE thread
     */
    public boolean registerProcessingThread() {
        if (this.disable) return (false);
        
        boolean affinity[] = this.defaultAffinity;
        Thread t = Thread.currentThread();
        String suffix = CollectionUtil.last(t.getName().split("\\-"));
        if (this.utilityAffinities.containsKey(suffix)) {
            affinity = this.utilityAffinities.get(suffix); 
        }
        
        if (debug.val)
            LOG.debug(String.format("Registering Processing Thread %s to execute on CPUs %s",
                                    t.getName(), this.getCPUIds(affinity)));
        
        // This thread cannot run on the EE's cores
        // If this fails (such as on OS X for some weird reason), we'll
        // just print a warning rather than crash
        try {
            this.disable = (ThreadUtils.setThreadAffinity(affinity) == false);
        } catch (UnsatisfiedLinkError ex) {
            LOG.warn("Unable to set CPU affinity for thread '" + t.getName() + "'. Disabling feature", ex);
            this.disable = true;
            return (false);
        }
        if (this.disable) {
            LOG.warn("Unable to set CPU affinity for thread '" + t.getName() + "'. Disabling feature");
            return (false);
        }
        this.registerThread(this.defaultAffinity);
        
        if (debug.val) LOG.debug(String.format("Successfully set affinity for thread '%s' on CPUs %s",
                                   t.getName(), this.getCPUIds(affinity)));
        return (true);
    }
    
    private synchronized void registerThread(boolean affinity[]) {
        Thread t = Thread.currentThread();
        for (int i = 0; i < affinity.length; i++) {
            if (affinity[i]) {
                Set<Thread> s = this.cpu_threads.get(i);
                if (s == null) {
                    s = new HashSet<Thread>();
                    this.cpu_threads.put(i, s);
                }
                s.add(t);
            }
        } // FOR
        this.all_threads.add(t);
    }
    
    private Collection<Integer> getCPUIds(boolean affinity[]) {
        Collection<Integer> cpus = new ArrayList<Integer>();
        for (int i = 0; i < affinity.length; i++) {
            if (affinity[i]) cpus.add(i);
        }
        return (cpus);
    }
    
    /**
     * Returns all the CPU ids that the given Thread is allowed to execute on  
     * @param t
     * @return
     */
    public Collection<Integer> getCPUIds(Thread t) {
        Collection<Integer> cpus = new HashSet<Integer>();
        for (Integer cpu : this.cpu_threads.keySet()) {
            if (this.cpu_threads.get(cpu).contains(t)) {
                cpus.add(cpu);
            }
        } // FOR
        return (cpus);
    }

    public boolean isRegistered(Thread t) {
        return (this.all_threads.contains(t));
    }
    
    public Map<Integer, Set<Thread>> getCPUThreads() {
        return Collections.unmodifiableMap(this.cpu_threads);
    }
    
    /**
     * Return the total number of cores at this host
     * Note that this does not take into consideration other sites that may 
     * be running at the same host
     * @return
     */
    public int getNumCores() {
        return (this.num_cores);
    }

    /**
     * Returns true if cpu affinity pinning is enabled
     * @return
     */
    public boolean isEnabled() {
        return (this.disable == false);
    }

    // ----------------------------------------------------------------------------
    // THREAD NAME FORMATTERS
    // ----------------------------------------------------------------------------
    
    public static final String formatSiteName(Integer site_id) {
        if (site_id == null) return (null);
        return (getThreadName(site_id, null));
    }
    
    public static final String formatPartitionName(int site_id, int partition) {
        return (getThreadName(site_id, partition));
    }

    public static final String getThreadName(HStoreSite hstore_site, Integer partition) {
        return (getThreadName(hstore_site.getSiteId(), partition));
    }

    public static final String getThreadName(HStoreSite hstore_site, String...suffixes) {
        return (getThreadName(hstore_site.getSiteId(), null, suffixes));
    }

    public static final String getThreadName(HStoreSite hstore_site, Integer partition, String...suffixes) {
        return (getThreadName(hstore_site.getSiteId(), partition, suffixes));
    }

    /**
     * Formatted site name
     * @param site_id
     * @param partition - Can be null
     * @param suffix - Can be null
     * @return
     */
    public static final String getThreadName(int site_id, Integer partition, String...suffixes) {
        String suffix = null;
        if (suffixes != null && suffixes.length > 0) suffix = StringUtil.join("-", suffixes);
        if (suffix == null) suffix = "";
        if (suffix.isEmpty() == false) {
            suffix = "-" + suffix;
            if (partition != null) suffix = String.format("-%03d%s", partition.intValue(), suffix);
        } else if (partition != null) {
            suffix = String.format("-%03d", partition.intValue());
        }
        return (String.format("H%02d%s", site_id, suffix));
    }
    
}
