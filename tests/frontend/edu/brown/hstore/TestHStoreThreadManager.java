package edu.brown.hstore;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.EELibraryLoader;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

public class TestHStoreThreadManager extends BaseTestCase {

    private static final int NUM_PARTITIONS = 2;
    private static final int NUM_CORES = 8;

    private HStoreConf hstore_conf;
    private Site catalog_site;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        ThreadUtil.setMaxGlobalThreads(NUM_CORES);
        EELibraryLoader.loadExecutionEngineLibrary(true);
        
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.cpu_affinity = true;
        this.hstore_conf.site.cpu_partition_blacklist = null;

        this.catalog_site = CollectionUtil.first(catalogContext.sites);
    }
    
    /**
     * testRegisterEEThreadBlacklist
     */
    public void testRegisterEEThreadBlacklist() throws Exception {
        Thread self = Thread.currentThread();
        
        Set<Integer> whitelist = new HashSet<Integer>();
        for (int cpu = 0; cpu < NUM_CORES; cpu++) {
            if (cpu % 3 == 0) {
                whitelist.add(cpu);
            }
            if (whitelist.size() == NUM_PARTITIONS) break;
        } // FOR
        Set<Integer> blacklist = new HashSet<Integer>();
        for (int cpu = 0; cpu < NUM_CORES; cpu++) {
            if (whitelist.contains(cpu) == false) {
                blacklist.add(cpu);
            }
        } // FOR
        
        this.hstore_conf.site.cpu_affinity = true;
        this.hstore_conf.site.cpu_affinity_one_partition_per_core = false;
        this.hstore_conf.site.cpu_partition_blacklist = StringUtil.join(" ,", blacklist);
        
        MockHStoreSite hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, hstore_conf);
        HStoreThreadManager manager = hstore_site.getThreadManager();
        HStoreThreadManager.Debug managerDebug = manager.getDebugContext();

        // Check whether we can register the thread and it doesn't
        // come back as disabled
        Partition partition = CollectionUtil.first(this.catalog_site.getPartitions());
        boolean ret = manager.registerEEThread(partition);
        assertTrue(ret);
        assertTrue(manager.isEnabled());
        assertTrue(managerDebug.isRegistered(self));
        
        // It should be allowed to execute on every CPU except for the first two
        Collection<Integer> cpuIds = managerDebug.getCPUIds(self);
        assertEquals(NUM_PARTITIONS, cpuIds.size());
        for (int cpu = 0; cpu < NUM_CORES; cpu++) {
            if (blacklist.contains(cpu)) {
                assertFalse(Integer.toString(cpu), cpuIds.contains(cpu));
            } else {
                assertTrue(Integer.toString(cpu), cpuIds.contains(cpu));
                assertTrue(Integer.toString(cpu), whitelist.contains(cpu));
            }
        } // FOR
    }
    
    /**
     * testRegisterEEThreadOnePerPartition
     */
    public void testRegisterEEThreadOnePerPartition() throws Exception {
        Thread self = Thread.currentThread();
        
        this.hstore_conf.site.cpu_affinity = true;
        this.hstore_conf.site.cpu_affinity_one_partition_per_core = true;
        
        MockHStoreSite hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, hstore_conf);
        HStoreThreadManager manager = hstore_site.getThreadManager();
        HStoreThreadManager.Debug managerDebug = manager.getDebugContext();

        // Check whether we can register the thread and it doesn't
        // come back as disabled
        Partition partition = CollectionUtil.first(this.catalog_site.getPartitions());
        boolean ret = manager.registerEEThread(partition);
        assertTrue(ret);
        assertTrue(manager.isEnabled());
        assertTrue(managerDebug.isRegistered(self));
        
        // It should be allowed to execute on every CPU except for the first two
        Collection<Integer> cpuIds = managerDebug.getCPUIds(self);
        assertEquals(1, cpuIds.size());
    }
    
    /**
     * testRegisterEEThread
     */
    public void testRegisterEEThread() throws Exception {
        Thread self = Thread.currentThread();
        
        this.hstore_conf.site.cpu_affinity = true;
        this.hstore_conf.site.cpu_affinity_one_partition_per_core = false;
        
        MockHStoreSite hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, hstore_conf);
        HStoreThreadManager manager = hstore_site.getThreadManager();
        HStoreThreadManager.Debug managerDebug = manager.getDebugContext();

        // Check whether we can register the thread and it doesn't
        // come back as disabled
        Partition partition = CollectionUtil.first(this.catalog_site.getPartitions());
        boolean ret = manager.registerEEThread(partition);
        assertTrue(ret);
        assertTrue(manager.isEnabled());
        assertTrue(managerDebug.isRegistered(self));
        
        // It should be allowed to execute on every CPU except for the first two
        Collection<Integer> cpuIds = managerDebug.getCPUIds(self);
        assertEquals(NUM_PARTITIONS, cpuIds.size());
        for (int cpu = 0; cpu < NUM_PARTITIONS; cpu++) {
            assertTrue(Integer.toString(cpu), cpuIds.contains(cpu));
        } // FOR
    }
    
    /**
     * testRegisterProcessingThread
     */
    public void testRegisterProcessingThread() throws Exception {
        Thread self = Thread.currentThread();
        
        MockHStoreSite hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, hstore_conf);
        HStoreThreadManager manager = hstore_site.getThreadManager();
        HStoreThreadManager.Debug managerDebug = manager.getDebugContext();

        // Check whether we can register the thread and it doesn't
        // come back as disabled
        boolean ret = manager.registerProcessingThread();
        assertTrue(ret);
        assertTrue(manager.isEnabled());
        assertTrue(managerDebug.isRegistered(self));
        
        // It should be allowed to execute on every CPU except for the first two
        Collection<Integer> cpuIds = managerDebug.getCPUIds(self);
        assertEquals(NUM_CORES - NUM_PARTITIONS, cpuIds.size());
        for (int cpu = 0; cpu < NUM_PARTITIONS; cpu++) {
            assertFalse(Integer.toString(cpu), cpuIds.contains(cpu));
        } // FOR
    }
    
}
