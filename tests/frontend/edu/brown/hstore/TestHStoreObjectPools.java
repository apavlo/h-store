package edu.brown.hstore;

import java.util.Map;

import org.junit.Test;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.pools.TypedObjectPool;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * H-Store Object Pool Tests
 * @author pavlo
 */
public class TestHStoreObjectPools extends BaseTestCase {

    private HStoreSite hstore_site;
    private HStoreObjectPools objectPools;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        initializeCatalog(1, 1, 2);
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        HStoreConf hstore_conf = HStoreConf.singleton();
        this.hstore_site = createHStoreSite(catalog_site, hstore_conf);
        this.objectPools = this.hstore_site.getObjectPools();
        assertNotNull(this.objectPools);
    }
 
    @Override
    protected void tearDown() throws Exception {
        if (this.hstore_site != null) this.hstore_site.shutdown();
    }

//    /**
//     * testGetGlobalPools
//     */
//    @Test
//    public void testGetGlobalPools() throws Exception {
//        Map<String, TypedObjectPool<?>> globalPools = this.objectPools.getGlobalPools();
//        assertNotNull(globalPools);
//        assertFalse(globalPools.isEmpty());
//        for (String name : globalPools.keySet()) {
//            TypedObjectPool<?> pool = globalPools.get(name);
//            assertNotNull(name, pool);
//        } // FOR
//    }
    
    /**
     * testGetPartitionedPools
     */
    @Test
    public void testGetPartitionedPools() throws Exception {
        Map<String, TypedObjectPool<?>[]> partitionedPools = this.objectPools.getPartitionedPools();
        assertNotNull(partitionedPools);
        assertFalse(partitionedPools.isEmpty());
        
        for (String name : partitionedPools.keySet()) {
            TypedObjectPool<?> pools[] = partitionedPools.get(name);
            assertNotNull(name, pools);
            assertNotSame(0, pools.length);
        } // FOR
    }
    
}
