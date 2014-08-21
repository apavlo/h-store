package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.utils.ThreadUtils;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

/**
 * The thread manager is used to schedule periodic work and assign threads to
 * individual CPU cores down in the EE.
 * @author pavlo
 */
public class HStoreThreadManager {
    private static final Logger LOG = Logger.getLogger(HStoreThreadManager.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public enum ThreadGroupType {
        PROCESSING,
        EXECUTION,
        NETWORK,
        AUXILIARY,
        CLEANER
    };
    
    private static final Pattern THREAD_NAME_SLITTER = Pattern.compile("\\-");
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    @SuppressWarnings("unused")
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    
    private boolean disable;
    
    private final ScheduledThreadPoolExecutor periodicWorkExecutor;
    private final int num_cores = ThreadUtil.getMaxGlobalThreads();
    private final boolean defaultAffinity[];
    private final Set<Thread> all_threads = new HashSet<Thread>();
    
    /**
     * Set of CPU ids that the PartitionExecutor threads will not be 
     * allowed to execute on.
     * @see HStoreConf.site.cpu_partition_blacklist
     */
    private final Set<Integer> partitionBlacklist = new HashSet<Integer>();
    
    /**
     * Set of CPU ids that the utility threads will not be 
     * allowed to execute on.
     * @see HStoreConf.site.cpu_partition_blacklist
     */
    private final Set<Integer> utilityBlacklist = new HashSet<Integer>();

    /**
     * Mapping from Partition to individual CPU id
     * Note that this will contain all of the Partitions at this host
     */
    private final Map<Partition, Integer> partitionCPUs = new HashMap<Partition, Integer>(); 
    
    /**
     * Mapping from the CPU Id# to the Threads that pinned to it.
     * This is just for debugging purposes. If you modify this map, the threads
     * will not automatically be pinned to those CPUs. The real assignment is performed down
     * in the EE. 
     */
    private final Map<Integer, Set<Thread>> cpu_threads = new TreeMap<Integer, Set<Thread>>();
    
    /**
     * Internal mapping from ThreadGroupType to the ThreadGroup handle
     */
    private final Map<ThreadGroupType, ThreadGroup> threadGroups = new HashMap<ThreadGroupType, ThreadGroup>();
    
    private final Map<String, boolean[]> utilityAffinities = new HashMap<String, boolean[]>();
    private final String utility_suffixes[] = {
        HStoreConstants.THREAD_NAME_COMMANDLOGGER,
        HStoreConstants.THREAD_NAME_PERIODIC,
        HStoreConstants.THREAD_NAME_COORDINATOR,
        HStoreConstants.THREAD_NAME_QUEUE_MGR,
        HStoreConstants.THREAD_NAME_QUEUE_MGR,
        HStoreConstants.THREAD_NAME_QUEUE_RESTART,
        HStoreConstants.THREAD_NAME_TXNCLEANER,
        HStoreConstants.THREAD_NAME_POSTPROCESSOR,
    };
    
    // ----------------------------------------------------------------------------
    // CONSTRUCTOR
    // ----------------------------------------------------------------------------
    
    public HStoreThreadManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        
        // Partition Blacklist
        // Note that we assume that all sites on the same node have the same blacklist
        if (hstore_conf.site.cpu_partition_blacklist != null) {
            for (String part : hstore_conf.site.cpu_partition_blacklist.split(",")) {
                part = part.trim();
                if (part.isEmpty()) continue;
                
                int cpuId = -1;
                try {
                    cpuId = Integer.parseInt(part);
                    assert(cpuId >= 0);
                } catch (Throwable ex) {
                    LOG.error("Invalid CPU Id for partition blacklist '" + part + "'", ex);
                    break;
                }
                this.partitionBlacklist.add(cpuId);
            } // FOR
            if (debug.val)
                LOG.debug("Partition CPU Blacklist: " + this.partitionBlacklist);
        }
        // Partition Blacklist
        if (hstore_conf.site.cpu_utility_blacklist != null) {
            for (String part : hstore_conf.site.cpu_utility_blacklist.split(",")) {
                part = part.trim();
                if (part.isEmpty()) continue;
                
                int cpuId = -1;
                try {
                    cpuId = Integer.parseInt(part);
                    assert(cpuId >= 0);
                } catch (Throwable ex) {
                    LOG.error("Invalid CPU Id for utility blacklist '" + part + "'", ex);
                    break;
                }
                this.utilityBlacklist.add(cpuId);
            } // FOR
            if (debug.val)
                LOG.debug("Utility CPU Blacklist: " + this.utilityBlacklist);
        }
        
        
        // Periodic Work Thread
        String threadName = getThreadName(hstore_site, HStoreConstants.THREAD_NAME_PERIODIC);
        this.periodicWorkExecutor = ThreadUtil.getScheduledThreadPoolExecutor(threadName,
                                                                              hstore_site.getExceptionHandler(),
                                                                              1,
                                                                              1024 * 128);
        this.defaultAffinity = new boolean[this.num_cores];
        Arrays.fill(this.defaultAffinity, true);
        for (int cpu : this.utilityBlacklist) {
            this.defaultAffinity[cpu] = false;
        } // FOR
        
        Host host = hstore_site.getHost();
        Collection<Partition> host_partitions = CatalogUtil.getPartitionsForHost(host);
        
        if (hstore_conf.site.cpu_affinity == false) {
            this.disable = true;
        }
        else if (this.num_cores <= host_partitions.size()) {
            LOG.warn(String.format("Unable to set CPU affinity on %s because there are %d partitions " +
                     "but only %d available CPU cores",
                     host.getIpaddr(), host_partitions.size(), this.num_cores));
            this.disable = true;
        }
        
        // Calculate what cores the partitions + utility threads are allowed
        // to execute on at this HStoreSite. We have to be careful about considering
        // other sites that may be on the same host (for testing).
        else {
            
            // Now figure out where the partition threads are allowed to execute
            // Note that we are doing this for all of the partitions at this host
            int cpuId = 0;
            for (Partition partition : host_partitions) {
                while (this.partitionBlacklist.contains(cpuId)) {
                    cpuId++;
                } // WHILE
                this.partitionCPUs.put(partition, cpuId);
                this.defaultAffinity[cpuId] = false;
                cpuId++;
            } // FOR

            // Reserve the highest cores for the various utility threads
            // We want to pin these threads to a single core to make it easier to identify
            // what when one of them eats too much of it.
            if ((this.num_cores - host_partitions.size()) > this.utility_suffixes.length+2) {
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
            if (debug.val)
                LOG.debug("Default CPU Affinity: " + Arrays.toString(this.defaultAffinity));
        }
        
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
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
    public void scheduleWork(Runnable work) {
        //LOG.info("We've scheduled that!");
        this.periodicWorkExecutor.execute(work);
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
    public synchronized boolean registerEEThread(Partition partition) {
        if (this.disable) return (false);
        
        Thread t = Thread.currentThread();
        boolean affinity[] = null;
        try {
            affinity = ThreadUtils.getThreadAffinity();
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
        if (hstore_conf.site.cpu_affinity_one_partition_per_core) {
            int core = this.partitionCPUs.get(partition);
            affinity[core] = true;
        }
        // Allow this EE to run on any of the lower cores allocated for this Site
        else {
            for (Partition p : this.partitionCPUs.keySet()) {
                if (p.getParent().equals(partition.getParent())) {
                    int core = this.partitionCPUs.get(p);
                    affinity[core] = true;    
                }
            } // FOR
        }
        
        if (debug.val)
            LOG.debug(String.format("Registering EE Thread %s to execute on CPUs %s",
                      t.getName(), this.getCPUIds(affinity)));
        if (this.registerThread(t, affinity) == false) {
            return (false);
        }
        
//        final boolean endingAffinity[] = ThreadUtils.getThreadAffinity();
//        for (int ii = 0; ii < endingAffinity.length; ii++) {
//            if (trace.val && endingAffinity[ii])
//                LOG.trace(String.format("NEW AFFINITY %s -> CPU[%d]", partition, ii));
//            affinity[ii] = false;
//        } // FOR
        if (debug.val)
            LOG.debug(String.format("Successfully set affinity for thread '%s' on CPUs %s\n%s",
                      t.getName(), this.getCPUIds(affinity), this.debug()));
        return (true);
    }
    
    /**
     * Set the CPU affinity for the current non-EE thread
     * This thread cannot run on the EE's cores
     */
    public synchronized boolean registerProcessingThread() {
        if (this.disable) return (false);
        
        boolean affinity[] = this.defaultAffinity;
        Thread t = Thread.currentThread();
        
        // Check whether this is as utility thread that we want to pin
        // to a certain number of cores
        String nameParts[] = THREAD_NAME_SLITTER.split(t.getName());
        String suffix = (nameParts.length > 1 ? nameParts[1] : nameParts[0]); 
        if (this.utilityAffinities.containsKey(suffix)) {
            affinity = this.utilityAffinities.get(suffix); 
            if (trace.val)
                LOG.trace("Using utility affinity for '" + suffix + "'");
        }
        
        if (debug.val)
            LOG.debug(String.format("Registering Processing Thread %s to execute on CPUs %s",
                      t.getName(), this.getCPUIds(affinity)));
        if (this.registerThread(t, affinity) == false) {
            return (false);
        }
        
        if (debug.val)
            LOG.debug(String.format("Successfully set affinity for thread '%s' on CPUs %s\n%s",
                      t.getName(), this.getCPUIds(affinity), this.debug()));
        return (true);
    }
    
    private boolean registerThread(Thread t, boolean affinity[]) {
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
        
        return (true);
    }
    
    /**
     * For the given affinity mapping, return the corresponding CPU Ids.
     * This is for debugging
     * @param affinity
     * @return
     */
    private Collection<Integer> getCPUIds(boolean affinity[]) {
        Collection<Integer> cpus = new ArrayList<Integer>();
        for (int i = 0; i < affinity.length; i++) {
            if (affinity[i]) cpus.add(i);
        }
        return (cpus);
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
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    public synchronized String debug() {
        Map<String, Object> m = new LinkedHashMap<String, Object>();
        for (Entry<Integer, Set<Thread>> e : this.cpu_threads.entrySet()) {
            TreeSet<String> names = new TreeSet<String>();
            for (Thread t : e.getValue()) {
                names.add(t.getName());
            } // FOR
            m.put("CPU #" + e.getKey(), names.toString());
        } // FOR
        return (StringUtil.formatMaps(m));
    }
    
    public class Debug implements DebugContext {
        public Map<Integer, Set<Thread>> getCPUThreads() {
            return Collections.unmodifiableMap(cpu_threads);
        }
        public boolean isRegistered(Thread t) {
            return (all_threads.contains(t));
        }
        /**
         * Returns all the CPU ids that the given Thread is allowed to execute on  
         * @param t
         * @return
         */
        public Collection<Integer> getCPUIds(Thread t) {
            Collection<Integer> cpus = new HashSet<Integer>();
            for (Integer cpu : cpu_threads.keySet()) {
                if (cpu_threads.get(cpu).contains(t)) {
                    cpus.add(cpu);
                }
            } // FOR
            return (cpus);
        }
    }
    
    private Debug cachedDebugContext;
    public Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            this.cachedDebugContext = new Debug();
        }
        return (this.cachedDebugContext);
    }
    
}
