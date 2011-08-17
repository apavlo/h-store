/**
 * 
 */
package edu.brown.benchmark.auctionmark;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreConf;

/**
 * @author pavlo
 *
 */
public class TestAuctionMarkLoader extends BaseTestCase {
    protected static final Logger LOG = Logger.getLogger(TestAuctionMarkLoader.class);
    
//    protected static final int SCALE_FACTOR = 20000;
	protected static final double SCALE_FACTOR = 1000;
    protected static final String LOADER_ARGS[] = {
        "CLIENT.SCALEFACTOR=" + SCALE_FACTOR, 
        "HOST=localhost",
        "NUMCLIENTS=1",
//        "CATALOG=" + BaseTestCase.getCatalogJarPath(ProjectType.AUCTIONMARK).getAbsolutePath(),
        "NOCONNECTIONS=true",
    };
    
    /**
     * Tables to show debug output for in MockAuctionMarkLoader.loadTable()
     */
    protected static final HashSet<String> DEBUG_TABLES = new HashSet<String>();
    static {
//        DEBUG_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM);
        DEBUG_TABLES.add(AuctionMarkConstants.TABLENAME_ITEM_IMAGE);
    } // STATIC
    
    private static final Map<String, Long> EXPECTED_TABLESIZES = new HashMap<String, Long>();
    private static final Map<String, Integer> EXPECTED_BATCHSIZES = new HashMap<String, Integer>();

    
    /**
     * We have to use a single loader in order to ensure that locks are properly released
     */
    protected class MockAuctionMarkLoader extends AuctionMarkLoader {
        
        public MockAuctionMarkLoader(String args[]) {
            super(args);
        }
        
        @Override
        public Catalog getCatalog() {
            return (BaseTestCase.catalog);
        }
        
        @Override
        protected void loadTable(String tableName, VoltTable table, Long expectedTotal) {
            assertEquals(SCALE_FACTOR, HStoreConf.singleton().client.scalefactor, 0.01);
            
            boolean debug = DEBUG_TABLES.contains(tableName);
            if (debug) LOG.debug("loadTable() called for " + tableName);
            long expected_tableSize = TestAuctionMarkLoader.EXPECTED_TABLESIZES.get(tableName);
            int expected_batchSize = TestAuctionMarkLoader.EXPECTED_BATCHSIZES.get(tableName);
            
            // Make sure that we do this here because AuctionMarkLoader.generateTableData() doesn't do this anymore
            int current_batchSize = table.getRowCount();
            profile.addToTableSize(tableName, current_batchSize);
            long current_tableSize = profile.getTableSize(tableName);
            
            if (debug) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Expected TableSize", expected_tableSize);
                m.put("Expected BatchSize", expected_batchSize);
                m.put("Current TableSize", current_tableSize);
                m.put("Current BatchSize", current_batchSize);
                LOG.debug("LOAD TABLE: " + tableName + "\n" + StringUtil.formatMaps(m));
            }
            assertNotNull("Got null VoltTable object for table '" + tableName + "'", table);
            
            // Simple checks
            assert(current_batchSize > 0) : "The number of tuples to be inserted is zero for table '" + tableName + "'";
            assert(current_batchSize <= expected_batchSize) : String.format("%s: %d <= %d", tableName, current_batchSize, expected_batchSize);
            assert(current_tableSize <= expected_tableSize) : String.format("%s: %d <= %d", tableName, current_tableSize, expected_tableSize);

            // Debug Output
            if (debug) LOG.debug(table);
        }
    };
    
    protected static MockAuctionMarkLoader loader;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        
        if (loader == null) {
            this.addPartitions(10);
            loader = new MockAuctionMarkLoader(LOADER_ARGS);
        }
        
        if (EXPECTED_BATCHSIZES.isEmpty()) {
            loader.setCatalog(catalog);
            for (String tableName : AuctionMarkConstants.TABLENAMES) {
                initTable(tableName);
            } // FOR
        }
    }
    
    protected static void initTable(String tableName) throws Exception {
        String field_name = null;
        Field field_handle = null;
        
        Long tablesize = Long.MAX_VALUE;
        Long batchsize = Long.MAX_VALUE;
        
        // Not all tables will have a table size
    	if (AuctionMarkConstants.DATAFILE_TABLES.contains(tableName) == false &&
    	    AuctionMarkConstants.DYNAMIC_TABLES.contains(tableName) == false &&
    	    tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM) == false) {
	        LOG.debug("Retrieving TABLESIZE attribute for table '" + tableName + "'");
	        field_name = "TABLESIZE_" + tableName;
	        field_handle = AuctionMarkConstants.class.getField(field_name);
	        assertNotNull(field_handle);
	        tablesize = (Long)field_handle.get(null);
	        if (!AuctionMarkConstants.FIXED_TABLES.contains(tableName)) tablesize = Math.round(tablesize / SCALE_FACTOR);
    	}
    	
    	// But all tables should have a batch size
        field_name = "BATCHSIZE_" + tableName;
        field_handle = AuctionMarkConstants.class.getField(field_name);
        assertNotNull(field_handle);
        batchsize = (Long)field_handle.get(null);

        // Make sure we reset the total number of rows we have loaded so far
        EXPECTED_TABLESIZES.put(tableName, tablesize);
        EXPECTED_BATCHSIZES.put(tableName, batchsize.intValue());
    }
    
//    public void testRunLoop() throws Exception {
//        loader.runLoop();
//    }
    
    /**
     * testGenerateRegion
     */
    public void testGenerateRegion() throws Exception {
        loader.generateTableData(AuctionMarkConstants.TABLENAME_REGION);
    }
    
    /**
     * testCategory
     */
    public void testGenerateCategory() throws Exception {
        loader.generateTableData(AuctionMarkConstants.TABLENAME_CATEGORY);
    }
    
    /**
     * testGenerateGlobalAttributeGroup
     */
    public void testGenerateGlobalAttributeGroup() throws Exception {
        loader.generateTableData(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
    }

    /**
     * testGenerateGlobalAttributeValue
     */
    public void testGenerateGlobalAttributeValue() throws Exception {
        loader.generateTableData(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
    }

    
    /**
     * testGenerateUser
     */
    public void testGenerateUser() throws Exception {
        // FIXME loader.generateTableData(AuctionMarkConstants.TABLENAME_USER);
    }    

    /**
     * testGenerateUserAttributes
     */
    public void testGenerateUserAttributes() throws Exception {
        // FIXME loader.generateTableData(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES);
    }        
    
    /**
     * testGenerateItem
     */
//    public void testGenerateItem() throws Exception {
//        loader.generateTableData(AuctionMarkConstants.TABLENAME_ITEM);
//    }
}
