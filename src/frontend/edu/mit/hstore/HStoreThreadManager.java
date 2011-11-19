package edu.mit.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Partition;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class HStoreThreadManager {
    private static final Logger LOG = Logger.getLogger(HStoreThreadManager.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final HStoreSite hstore_site;
    private final boolean disable;
    private final int num_partitions;
    private final int num_cores = ThreadUtil.getMaxGlobalThreads();
    private final boolean processing_affinity[];
    private final Set<Thread> all_threads = new HashSet<Thread>();
    private final Map<Integer, Set<Thread>> cpu_threads = new HashMap<Integer, Set<Thread>>(); 
    
    public HStoreThreadManager(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.num_partitions = this.hstore_site.getLocalPartitionIds().size();
        this.processing_affinity = new boolean[this.num_cores];
        for (int i = 0; i < this.processing_affinity.length; i++) {
            this.processing_affinity[i] = true;
        } // FOR
        
        this.disable = (this.num_cores <= this.num_partitions);
        if (this.disable) {
            LOG.warn(String.format("Unable to set CPU affinity - There are %d partitions but only %d available cores",
                                   this.num_partitions, this.num_cores));
        } else {
            for (int i = 0; i < this.num_partitions; i++) {
                this.processing_affinity[i] = false;
            } // FOR
        }
    }
    
    /**
     * Set the CPU affinity for the EE thread executing for the given partition
     * @param partition
     */
    public void registerEEThread(Partition partition) {
        if (this.disable) return;
        final boolean affinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
        for (int ii = 0; ii < affinity.length; ii++) {
            affinity[ii] = false;
        } // FOR

        // Only allow this EE to execute on a single core
        if (hstore_site.getHStoreConf().site.cpu_affinity_one_partition_per_core) {
            affinity[partition.getRelativeIndex()-1 % affinity.length] = true;
        }
        // Allow this EE to run on any of the lower cores
        else {
            for (int i = 0; i < this.num_partitions; i++) {
                affinity[i] = true;
            } // FOR
        }
        if (debug.get())
            LOG.debug("Registering EE Thread for " + partition + " to execute on CPUs " + getCPUIds(affinity));
        org.voltdb.utils.ThreadUtils.setThreadAffinity(affinity);
        this.registerThread(affinity);
        
        final boolean endingAffinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
        for (int ii = 0; ii < endingAffinity.length; ii++) {
            if (trace.get() && endingAffinity[ii]) LOG.trace(String.format("NEW AFFINITY %s -> CPU[%d]", partition, ii));
            affinity[ii] = false;
        } // FOR
    }
    
    /**
     * Set the CPU affinity for a non-EE thread
     */
    public void registerProcessingThread() {
        if (this.disable) return;
        if (debug.get())
            LOG.debug("Registering Processing Thread to execute on CPUs " + getCPUIds(this.processing_affinity));
        // This thread cannot run on the EE's cores
        org.voltdb.utils.ThreadUtils.setThreadAffinity(this.processing_affinity);
        this.registerThread(this.processing_affinity);
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

    public boolean isRegistered(Thread t) {
        return (this.all_threads.contains(t));
    }
    
    public Map<Integer, Set<Thread>> getCPUThreads() {
        return Collections.unmodifiableMap(this.cpu_threads);
    }
}
