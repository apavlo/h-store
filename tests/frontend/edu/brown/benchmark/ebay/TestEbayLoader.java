/**
 * 
 */
package edu.brown.benchmark.ebay;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import edu.brown.BaseTestCase;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 *
 */
public class TestEbayLoader extends BaseTestCase {
    protected static final Logger LOG = Logger.getLogger(TestEbayLoader.class);
    
//    protected static final int SCALE_FACTOR = 20000;
	protected static final int SCALE_FACTOR = 1000;
    protected static final String LOADER_ARGS[] = {
        "scalefactor=" + SCALE_FACTOR, 
        "HOST=localhost"
    };
    
    /**
     * Tables to show debug output for in MockEbayLoader.loadTable()
     */
    protected static final HashSet<String> DEBUG_TABLES = new HashSet<String>();
    static {
//        DEBUG_TABLES.add(EbayConstants.TABLENAME_ITEM);
        DEBUG_TABLES.add(EbayConstants.TABLENAME_ITEM_IMAGE);
    } // STATIC
    
    private static final Map<String, Long> EXPECTED_TABLESIZES = new HashMap<String, Long>();
    private static final Map<String, Long> EXPECTED_BATCHSIZES = new HashMap<String, Long>();
    private static final Map<String, Long> TOTAL_ROWS = new HashMap<String, Long>();

    
    /**
     * We have to use a single loader in order to ensure that locks are properly released
     */
    protected static class MockEbayLoader extends EbayLoader {
        
        public MockEbayLoader(String args[]) {
            super(args);
        }
        
        @Override
        protected void loadTable(String tablename, VoltTable table) {
            boolean debug = DEBUG_TABLES.contains(tablename);
            LOG.info("loadTable() called for " + tablename);
            long current_tablesize = TestEbayLoader.EXPECTED_TABLESIZES.get(tablename);
            long current_batchsize = TestEbayLoader.EXPECTED_BATCHSIZES.get(tablename);
            long total_rows = TestEbayLoader.TOTAL_ROWS.get(tablename);
            
            if (debug) {
                LOG.debug("LOAD TABLE: " + tablename + " [" +
                          "tablesize="  + current_tablesize + "," +
                          "batchsize="  + current_batchsize + "," +
                          "num_rows="   + table.getRowCount() + "," + 
                          "total_rows=" + total_rows + "]");
            }
            assertNotNull("Got null VoltTable object for table '" + tablename + "'", table);
            
            // Simple checks
            int num_rows = table.getRowCount();
            total_rows += num_rows;
            assert(num_rows > 0) : "The number of tuples to be inserted is zero for table '" + tablename + "'";
            assert(num_rows <= current_batchsize);
            assert(total_rows <= current_tablesize);

            // Debug Output
            if (debug) LOG.debug(table);
            
            // Make sure that we do this here because EbayLoader.generateTableData() doesn't do this anymore
            MockEbayLoader.this.profile.addToTableSize(tablename, num_rows);
            
            TestEbayLoader.TOTAL_ROWS.put(tablename, total_rows);
        }
    };
    
    protected static final MockEbayLoader loader = new MockEbayLoader(LOADER_ARGS);
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.EBAY);
        
        if (EXPECTED_BATCHSIZES.isEmpty()) {
            for (String tableName : EbayConstants.TABLENAMES) {
                initTable(tableName);
            } // FOR
        }
    }
    
    protected static void initTable(String tablename) throws Exception {
        String field_name = null;
        Field field_handle = null;
        
        Long tablesize = Long.MAX_VALUE;
        Long batchsize = Long.MAX_VALUE;
        
        // Not all tables will have a table size
    	if (!EbayConstants.DATAFILE_TABLES.contains(tablename) && !EbayConstants.DYNAMIC_TABLES.contains(tablename)) {
	        LOG.debug("Retrieving TABLESIZE attribute for table '" + tablename + "'");
	        field_name = "TABLESIZE_" + tablename;
	        field_handle = EbayConstants.class.getField(field_name);
	        assertNotNull(field_handle);
	        tablesize = (Long)field_handle.get(null);
	        if (!EbayConstants.FIXED_TABLES.contains(tablename)) tablesize /= SCALE_FACTOR;
    	}
    	
    	// But all tables should have a batch size
        field_name = "BATCHSIZE_" + tablename;
        field_handle = EbayConstants.class.getField(field_name);
        assertNotNull(field_handle);
        batchsize = (Long)field_handle.get(null);

        // Make sure we reset the total number of rows we have loaded so far
        EXPECTED_TABLESIZES.put(tablename, tablesize);
        EXPECTED_BATCHSIZES.put(tablename, batchsize);
        TOTAL_ROWS.put(tablename, 0l);
    }
    
    /**
     * testGenerateRegion
     */
    public void testGenerateRegion() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_REGION);
    }
    
    /**
     * testCategory
     */
    public void testGenerateCategory() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_CATEGORY);
    }
    
    /**
     * testGenerateGlobalAttributeGroup
     */
    public void testGenerateGlobalAttributeGroup() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
    }

    /**
     * testGenerateGlobalAttributeValue
     */
    public void testGenerateGlobalAttributeValue() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
    }

    
    /**
     * testGenerateUser
     */
    public void testGenerateUser() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_USER);
    }    

    /**
     * testGenerateUserAttributes
     */
    public void testGenerateUserAttributes() throws Exception {
        loader.generateTableData(EbayConstants.TABLENAME_USER_ATTRIBUTES);
    }        
    
    /**
     * testGenerateItem
     */
//    public void testGenerateItem() throws Exception {
//        loader.generateTableData(EbayConstants.TABLENAME_ITEM);
//    }
}
