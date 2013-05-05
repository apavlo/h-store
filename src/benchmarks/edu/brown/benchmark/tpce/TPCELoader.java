/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *  http://www.cs.brown.edu/~zhe/                                          *
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
package edu.brown.benchmark.tpce;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tpce.generators.TPCEGenerator;
import edu.brown.benchmark.tpce.generators.TradeGenerator;

/**
 * @author pavlo
 * @author akalinin
 */
public class TPCELoader extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCELoader.class);
    protected final TPCEGenerator generator;
 
    /**
     * Constructor
     * 
     * @param args
     */
    public TPCELoader(String[] args) {
        super(args);

        // Cannot work without flat TPC-E files
        if (!m_extraParams.containsKey("TPCE_LOADER_FILES")) {
            LOG.error("Unable to start benchmark. Missing 'TPCE_LOADER_FILES' parameter");
            System.exit(1);
        }
        File flatFilesPath = new File(m_extraParams.get("TPCE_LOADER_FILES") + File.separator);
        
        long total_customers = TPCEConstants.DEFAULT_NUM_CUSTOMERS;
        if (m_extraParams.containsKey("TPCE_TOTAL_CUSTOMERS")) {
            total_customers = Long.valueOf(m_extraParams.get("TPCE_TOTAL_CUSTOMERS"));
        }
        
        int scale_factor = TPCEConstants.DEFAULT_SCALE_FACTOR;
        if (m_extraParams.containsKey("TPCE_SCALE_FACTOR")) {
            scale_factor = Integer.valueOf(m_extraParams.get("TPCE_SCALE_FACTOR"));
        }
        
        int initial_days = TPCEConstants.DEFAULT_INITIAL_DAYS;
        if (m_extraParams.containsKey("TPCE_INITIAL_DAYS")) {
            initial_days = Integer.valueOf(m_extraParams.get("TPCE_INITIAL_DAYS"));
        }
        
        // validating parameters
        if (total_customers <= 0 || total_customers % TPCEConstants.DEFAULT_LOAD_UNIT != 0) {
            throw new IllegalArgumentException("The total number of customers must be a positive integer multiple of the load unit size");
        }
        
        /*
         * Completed trades in 8 hours must be a non-zero integral multiple of 100
         * so that exactly 1% extra trade ids can be assigned to simulate aborts.
         */
        if ((8 * 3600 * TPCEConstants.DEFAULT_LOAD_UNIT / scale_factor) % 100 != 0) { // 8 hours per work day
            throw new IllegalArgumentException("Wrong scale factor: 8 * 3600 * Load Unit Size (" +
                    TPCEConstants.DEFAULT_LOAD_UNIT + ") / Scale Factor(" + scale_factor + ") must be " +
                    "integral multiple of 100 to simulate trades");
        }
        
        if (initial_days <= 0) {
            throw new IllegalArgumentException("The number of initial trade days must be a positive integer");
        }
        
        LOG.info("TPC-E parameters are: flat_in = '" + flatFilesPath + "', total_customers = " + total_customers +
                ", scale_factor = " + scale_factor + ", initial_days = " + initial_days);
        
        this.generator = new TPCEGenerator(flatFilesPath, total_customers, scale_factor, initial_days);
    }

    public static void main(String[] args) {
        edu.brown.api.BenchmarkComponent.main(TPCELoader.class, args, true);
    }

    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    @Override
    public void runLoop() {
        LOG.info("Begin to load tables...");
        
        LOG.info("Parsing input files...");
        generator.parseInputFiles();
        LOG.info("Finished parsing input files...");

        CatalogContext catalogContext = null;
        try {
            catalogContext = this.getCatalogContext();
        } catch (Exception ex) {
            LOG.error("Failed to retrieve already compiled catalog", ex);
            System.exit(1);
        }
//        Database catalog_db = catalog.getClusters().get(0).getDatabases().get(0); // NASTY!
                                                                                  // CatalogUtil.getDatabase(catalog);
        Database catalog_db = catalogContext.database;

        //
        // Fixed-sized Tables
        //
        LOG.info("Generating and loading fixed-sized TPC-E tables");
        try {
            for (String table_name : TPCEConstants.FIXED_TABLES) {
                Table catalog_tbl = catalog_db.getTables().get(table_name);
                assert (catalog_tbl != null);
                this.loadTable(catalog_tbl, 1000);
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to generate and load fixed-sized tables", ex);
            System.exit(1);
        }

        //
        // Scaling Tables
        // Load them in batches based on the customer ids
        //
        LOG.info("Generating and loading scaling TPC-E tables");
        try {
            for (long start_idx = 0, cnt = this.generator.getTotalCustomers(); start_idx < cnt; start_idx += TPCEConstants.DEFAULT_LOAD_UNIT) {
                this.generator.changeSessionParams(TPCEConstants.DEFAULT_LOAD_UNIT, start_idx + 1);
                for (String table_name : TPCEConstants.SCALING_TABLES) {
                    Table catalog_tbl = catalog_db.getTables().get(table_name);
                    assert (catalog_tbl != null);
                    this.loadTable(catalog_tbl, 100);
                } // FOR
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to generate and load scaling tables", ex);
            System.exit(1);
        }

        //
        // Growing Tables
        // Load all of them at once in batches based on the customer ids
        //
        LOG.info("Generating and loading growing TPC-E tables");
        try {
            for (long start_idx = 0, cnt = this.generator.getTotalCustomers(); start_idx < cnt; start_idx += TPCEConstants.DEFAULT_LOAD_UNIT) {
                this.generator.changeSessionParams(TPCEConstants.DEFAULT_LOAD_UNIT, start_idx + 1);
                this.loadGrowingTables(catalog_db, 100);
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to generate and load growing tables", ex);
            System.exit(1);
        }
        
        LOG.info("TPCE loader done.");
    }
    
    /**
     * Dedicated function for loading growing tables. These tables
     * are loaded via a single generator, hence the weird logic
     * 
     * @param catalog Database catalog to retrieve Table
     * @param batchSize Batch size for loading tuples
     */
    private void loadGrowingTables(Database catalog, int batchSize) {
        LOG.debug("Loading records for growing tables in batches of " + batchSize);
        boolean debug = true;
        Map<String, VoltTable> growingVoltTables = new HashMap<String, VoltTable>();
        
        for (String tableName: TPCEConstants.GROWING_TABLES) {
            VoltTable vt = CatalogUtil.getVoltTable(catalog.getTables().get(tableName));
            growingVoltTables.put(tableName, vt);
        }
        
        try {
            TradeGenerator tableGen = new TradeGenerator(this.generator, catalog);
            while (tableGen.hasNext()) {
                Object tuple[] = tableGen.next(); // tuple
                String tableName = tableGen.getCurrentTable(); // table name the tuple is for
                VoltTable vt = growingVoltTables.get(tableName);
                int row_idx = vt.getRowCount();
                
                if (debug) {
                    StringBuilder sb = new StringBuilder();
                    for (Object o: tuple) {
                        sb.append(o.toString());
                        sb.append('|');
                    }
                    LOG.trace("Table[" + tableName + "], Tuple[" + row_idx + "]: "+ sb);
                }

                vt.addRow(tuple);
                row_idx++;
                
                if (row_idx % batchSize == 0) {
                    LOG.debug("Storing batch of " + batchSize + " tuples for " + tableName + " [total=" + row_idx + "]");
                    this.loadVoltTable(tableName, vt);
                    vt.clearRowData();
                }
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load growing tables", ex);
            System.exit(1);
        }
        
        // loading remaining (out-of-batch) tuples
        for (Entry<String, VoltTable> table: growingVoltTables.entrySet()) {
            String tableName = table.getKey();
            VoltTable vt = table.getValue();
            int row_idx = vt.getRowCount();
            
            if (row_idx > 0)
                this.loadVoltTable(tableName, vt);
            
            LOG.debug("Finished loading " + row_idx + " final tuples for " + tableName);
        }
    }

    /**
     * @param catalog_tbl
     */
    private void loadTable(Table catalog_tbl, int batch_size) {
        LOG.debug("Loading records for table " + catalog_tbl.getName() + " in batches of " + batch_size);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int row_idx = 0;
        boolean debug = true;
        
        try {
            Iterator <Object[]> table_gen = this.generator.getTableGen(catalog_tbl.getName(), catalog_tbl);
            while (table_gen.hasNext()) {
                Object tuple[] = table_gen.next();
                
                if (debug) {
                    StringBuilder sb = new StringBuilder();
                    for (Object o: tuple) {
                        sb.append(o.toString());
                        sb.append('|');
                    }
                    LOG.trace("Table[" + catalog_tbl.getName() + "], Tuple[" + row_idx + "]: "+ sb);
                }

                vt.addRow(tuple);
                row_idx++;
                if (row_idx % batch_size == 0) {
                    LOG.debug("Storing batch of " + batch_size + " tuples for " + catalog_tbl.getName() + " [total=" + row_idx + "]");
                    this.loadVoltTable(catalog_tbl.getName(), vt);
                    vt.clearRowData();
                }
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load table " + catalog_tbl.getName(), ex);
            System.exit(1);
        }
        if (vt.getRowCount() > 0)
            this.loadVoltTable(catalog_tbl.getName(), vt);
        LOG.debug("Finished loading " + row_idx + " tuples for " + catalog_tbl.getName());
        return;
    }
}
