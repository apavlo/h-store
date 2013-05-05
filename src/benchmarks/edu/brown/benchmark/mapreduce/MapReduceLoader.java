/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.mapreduce;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.conf.HStoreConf;

public class MapReduceLoader extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(MapReduceLoader.class);
    
    // scale all table cardinalities by this factor
    private final double m_scalefactor;
    
    // When set to true, all operations will run single-threaded
    private boolean debug = true;
    
    // Data Generator Classes
    // TableName -> AbstactTableGenerator
    private final Map<String, AbstractTableGenerator> generators = new HashMap<String, AbstractTableGenerator>();
    
    // Table Sizes
    // TableName -> Tuple Count
    private final Map<String, AtomicLong> table_sizes = new HashMap<String, AtomicLong>();
    
    public static void main(String args[]) throws Exception {
        edu.brown.api.BenchmarkComponent.main(MapReduceLoader.class, args, true);
    }
    
    /**
     * Constructor
     * @param args
     */
    public MapReduceLoader(String[] args) {
        super(args);
        
        double scaleFactor = HStoreConf.singleton().client.scalefactor; 
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Scale Factor
            if (key.equalsIgnoreCase("CLIENT.SCALEFACTOR")) { // FIXME
                scaleFactor = Double.parseDouble(value);
            }
        } // FOR
        m_scalefactor = scaleFactor;    
        LOG.debug("m_scalefactor = " + m_scalefactor + "\n" + StringUtil.formatMaps(m_extraParams));
        
        // Histograms + Table Sizes + Generators
        for (String tableName : MapReduceConstants.TABLENAMES) {
            this.table_sizes.put(tableName, new AtomicLong(0l));
            
            if (tableName.equals(MapReduceConstants.TABLENAME_TABLEA)) {
                this.generators.put(tableName, new TABLEAGenerator());
            } else if (tableName.equals(MapReduceConstants.TABLENAME_TABLEB)) {
                this.generators.put(tableName, new TABLEBGenerator());
            }
        } // FOR
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    /**
     * Main execution loop for invoking all the data generator threads
     */
    @Override
    public void runLoop() {
        List<Thread> load_threads = new ArrayList<Thread>();
        for (final String tableName : MapReduceConstants.TABLENAMES) {
            load_threads.add(new Thread() { 
                @Override
                public void run() {
                    generateTableData(tableName);
                }
            });
        } // FOR
        
        try {
            for (Thread thread : load_threads) {
                thread.start();
                if (this.debug) thread.join();
            }
            if (!this.debug) {
                for (Thread thread : load_threads) thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        LOG.info("Finished generating data for all tables");
    }
    
    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        LOG.debug("Starting data generator for '" + tableName + "'");
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert(generator != null);
        long tableSize = generator.getTableSize();
        long batchSize = generator.getBatchSize();
        final AtomicLong table_ctr = this.table_sizes.get(tableName); 
        VoltTable table = generator.getVoltTable();
        
        LOG.info("Loading " + tableSize + " tuples for table '" + tableName + "'");
        while (generator.hasMore()) {
            generator.addRow();
            if (table.getRowCount() >= batchSize) {
                if (table_ctr.get() % 100000 == 0) LOG.info(String.format(tableName + ": loading %d rows (id %d of %d)", table.getRowCount(), generator.getCount(), tableSize));
                loadVoltTable(tableName, table);
                table_ctr.addAndGet(table.getRowCount());
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            LOG.info(tableName + ": loading final " + table.getRowCount() + " rows.");
            loadVoltTable(tableName, table);
            this.table_sizes.get(tableName).addAndGet(table.getRowCount());
            table.clearRowData();
        }
        LOG.info(tableName + ": Inserted " + this.table_sizes.get(tableName) + " tuples");
    }

    // ----------------------------------------------------------------
    // DATA GENERATION
    // ----------------------------------------------------------------
    
    protected abstract class AbstractTableGenerator {
        protected final String tableName;
        protected final Table catalog_tbl;
        protected final VoltTable table;
        protected Long tableSize;
        protected Long batchSize;
        
        protected final Object[] row;
        protected long count = 0;
        
        public AbstractTableGenerator(String tableName) {
            this.tableName = tableName;
            this.catalog_tbl = getCatalogContext().database.getTables().get(this.tableName);
            assert(this.catalog_tbl != null);
            this.table = CatalogUtil.getVoltTable(this.catalog_tbl);
            this.row = new Object[this.table.getColumnCount()];
            
            // Initialize dynamic parameters
            try {
                String field_name = "TABLESIZE_" + tableName;
                Field field_handle = MapReduceConstants.class.getField(field_name);
                assert(field_handle != null);
                this.tableSize = Math.round((Long)field_handle.get(null) * MapReduceLoader.this.m_scalefactor);

                field_name = "BATCHSIZE_" + tableName;
                field_handle = MapReduceConstants.class.getField(field_name);
                assert(field_handle != null);
                this.batchSize = (Long)field_handle.get(null);
            } catch (Exception ex) {
                LOG.error(ex);
                System.exit(1);
            }
            LOG.info("Preparing to load " + this.tableSize + " tuples for '" + this.tableName + "' [batchSize=" + this.batchSize + "]");
        }
        
        public boolean hasMore() {
            return (this.count < this.tableSize);
        }
        
        public VoltTable getVoltTable() {
            return this.table;
        }
        
        public Long getTableSize() {
            return this.tableSize;
        }
        
        public Long getBatchSize() {
            return this.batchSize;
        }
        
        public String getTableName() {
            return this.tableName;
        }
        
        public long getCount() {
            return this.count;
        }
        
        /**
         * Invoked by generateTableData() to create a new row in our temporary
         * table. We don't need to worry about batches, counts, or anything else. Just makin' tuples
         */
        public void addRow() {
            this.populateRow();
            this.count++;
            this.table.addRow(this.row);
        }
        
        protected abstract void populateRow();
    } // END CLASS
    
    /**
     * TABLEA Generator
     */
    protected class TABLEAGenerator extends AbstractTableGenerator {
        
        final Random rand = new Random();
        
        public TABLEAGenerator() {
            super(MapReduceConstants.TABLENAME_TABLEA);
        }
        
        @Override
        protected void populateRow() {
            int col = 0;
            
            // A_ID
            row[col++] = new Integer((int)this.count);
            
            // A_NAME
            row[col++] = String.format("%s-%04d", MapReduceConstants.NAME_PREFIX,
                                                  rand.nextInt(MapReduceConstants.NUM_UNIQUE_NAMES));
            
            // A_AGE
            row[col++] = rand.nextInt(MapReduceConstants.MAX_AGE);

            assert (col == this.table.getColumnCount());
        }
    } // END CLASS

    /**
     * TABLEB Generator
     */
    protected class TABLEBGenerator extends AbstractTableGenerator {
        private long current_a_id = 0;
        private long current_b_id = 0;
        
        public TABLEBGenerator() {
            super(MapReduceConstants.TABLENAME_TABLEB);
        }
        
        @Override
        protected void populateRow() {
            int col = 0;
            
            // B_ID
            row[col++] = new Integer((int)this.current_b_id);
            
            // B_A_ID
            row[col++] = new Integer((int)this.current_a_id);

            // B_VALUE
            row[col++] = "DEF456"; // FIXME
            
            assert (col == this.table.getColumnCount());
            
            if (++this.current_b_id > MapReduceConstants.TABLESIZE_TABLEB_MULTIPLIER) {
                this.current_b_id = 0;
                this.current_a_id++;
            }
        }
    } // END CLASS
}

