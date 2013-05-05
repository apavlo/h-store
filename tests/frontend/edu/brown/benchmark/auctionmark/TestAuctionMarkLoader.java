/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
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
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.benchmark.auctionmark.AuctionMarkLoader.AbstractTableGenerator;
import edu.brown.hstore.conf.HStoreConf;

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
        
        private final Histogram<String> tableSizes = new ObjectHistogram<String>(true);
        
        public MockAuctionMarkLoader(String args[]) {
            super(args);
        }
        
        @Override
        public CatalogContext getCatalogContext() {
            return (BaseTestCase.catalogContext);
        }
        
        @Override
        public ClientResponse loadVoltTable(String tableName, VoltTable table) {
            assertEquals(SCALE_FACTOR, HStoreConf.singleton().client.scalefactor, 0.01);
            
            boolean debug = DEBUG_TABLES.contains(tableName);
            if (debug) LOG.debug("loadTable() called for " + tableName);
            long expected_tableSize = TestAuctionMarkLoader.EXPECTED_TABLESIZES.get(tableName);
            int expected_batchSize = TestAuctionMarkLoader.EXPECTED_BATCHSIZES.get(tableName);
            
            // Make sure that we do this here because AuctionMarkLoader.generateTableData() doesn't do this anymore
            int current_batchSize = table.getRowCount();
            this.tableSizes.put(tableName, current_batchSize);
            long current_tableSize = tableSizes.get(tableName);
            
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
            
            return (null);
        }
    };
    
    protected static MockAuctionMarkLoader loader;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        
        if (isFirstSetup()) {
            this.addPartitions(10);
            loader = new MockAuctionMarkLoader(LOADER_ARGS);
            loader.setCatalogContext(catalogContext);
            
            for (String tableName : AuctionMarkConstants.TABLENAMES) {
                initTable(tableName);
            } // FOR
        }
        assertNotNull(loader);
        assertFalse(EXPECTED_BATCHSIZES.isEmpty());
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
        AbstractTableGenerator generator = loader.getGenerator(AuctionMarkConstants.TABLENAME_CATEGORY);
        assertNotNull(generator);
        assertEquals(AuctionMarkConstants.TABLENAME_CATEGORY, generator.getTableName());
        generator.init();
        loader.generateTableData(generator.getTableName());
    }
    
    /**
     * testGenerateGlobalAttributeGroup
     */
    public void testGenerateGlobalAttributeGroup() throws Exception {
        AbstractTableGenerator generator = loader.getGenerator(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        assertNotNull(generator);
        assertEquals(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, generator.getTableName());
        generator.init();
        loader.generateTableData(generator.getTableName());
    }

    /**
     * testGenerateGlobalAttributeValue
     */
    public void testGenerateGlobalAttributeValue() throws Exception {
        AbstractTableGenerator generator = loader.getGenerator(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        assertNotNull(generator);
        assertEquals(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE, generator.getTableName());
        generator.init();
        EXPECTED_TABLESIZES.put(generator.getTableName(), generator.getTableSize()); // HACK
        loader.generateTableData(generator.getTableName());
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
