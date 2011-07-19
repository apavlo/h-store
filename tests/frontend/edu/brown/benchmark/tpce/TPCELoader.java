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

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.VoltTable;

import edu.brown.benchmark.BenchmarkComponent;

/**
 * 
 * @author pavlo
 *
 */
public class TPCELoader extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(TPCELoader.class);
    protected final EGenLoader egenloader;
    
    /**
     * Constructor
     * @param args
     */
    public TPCELoader(String[] args) {
		super(args);
		
		//
		// We need to also be given the path to where the TPC-E EGenLoader binaries are
		//
		// System.out.println("EXTRA PARAMS: " + m_extraParams);
		if (!m_extraParams.containsKey(TPCEConstants.PARAM_EGENLOADER_HOME.toLowerCase())) {
		    LOG.error("Unable to start benchmark. Missing '" + TPCEConstants.PARAM_EGENLOADER_HOME + "' parameter");
		    System.exit(1);
		}
		int total_customers = TPCEConstants.DEFAULT_NUM_CUSTOMERS;
		int scale_factor = TPCEConstants.DEFAULT_SCALE_FACTOR;
		int initial_days = TPCEConstants.DEFAULT_INITIAL_DAYS;
	    this.egenloader = new EGenLoader(m_extraParams.get(TPCEConstants.PARAM_EGENLOADER_HOME), total_customers, scale_factor, initial_days);
	}

	public static void main(String[] args) {
		edu.brown.benchmark.BenchmarkComponent.main(TPCELoader.class, args, true);
	}

	@Override
	public String[] getTransactionDisplayNames() {
		return new String[] {};
	}
	
	@Override
	public void runLoop() {
		LOG.info("Begin to load tables...");
		
		Catalog catalog = null;
		try {
		    catalog = this.getCatalog();
		} catch (Exception ex) {
		    LOG.error("Failed to retrieve already compiled catalog", ex);
		    System.exit(1);
		}
		Database catalog_db = catalog.getClusters().get(0).getDatabases().get(0); // NASTY! CatalogUtil.getDatabase(catalog);
		
		//
		// Fixed-sized Tables
		//
		LOG.info("Generating and loading fixed-sized TPC-E tables");
        try {
            this.egenloader.generateFixedTables();
    		for (String table_name : TPCEConstants.FIXED_TABLES) {
    	        Table catalog_tbl = catalog_db.getTables().get(table_name);
    	        assert(catalog_tbl != null);
    	        this.loadTable(catalog_tbl, 1000);
    		} // FOR
    		//this.egenloader.clearTables();
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
            for (int start_idx = 0, cnt = this.egenloader.getTotalCustomers(); start_idx < cnt; start_idx += 1000) {
                this.egenloader.generateScalingTables(start_idx);
                for (String table_name : TPCEConstants.SCALING_TABLES) {
                    Table catalog_tbl = catalog_db.getTables().get(table_name);
                    assert(catalog_tbl != null);
                    this.loadTable(catalog_tbl, 100);
                } // FOR
                //this.egenloader.clearTables();
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to generate and load scaling tables", ex);
            System.exit(1);
        }
        
        //
        // Growing Tables
        // Load them in batches based on the customer ids
        //
        LOG.info("Generating and loading growing TPC-E tables");
        try {
            for (int start_idx = 0, cnt = this.egenloader.getTotalCustomers(); start_idx < cnt; start_idx += 1000) {
                this.egenloader.generateGrowingTables(start_idx);
                for (String table_name : TPCEConstants.GROWING_TABLES) {
                    Table catalog_tbl = catalog_db.getTables().get(table_name);
                    assert(catalog_tbl != null);
                    this.loadTable(catalog_tbl, 100);
                } // FOR
                //this.egenloader.clearTables();
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to generate and load growing tables", ex);
            System.exit(1);
        }
        
		LOG.info("TPCE loader done.");
	}
	
	/**
	 * 
	 * @param catalog_tbl
	 */
	public void loadTable(Table catalog_tbl, int batch_size) {
	    LOG.debug("Loading records for table " + catalog_tbl.getName() + " in batches of " + batch_size);
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int row_idx = 0;
        boolean debug = false; //catalog_tbl.getName().equals("NEWS_ITEM");
        try {
            for (Object tuple[] : this.egenloader.getTable(catalog_tbl)) {
                if (debug) {
                    for (int i = 0; i < tuple.length; i++) {
                        System.out.println("[" + i + "]: " + tuple[i].toString().length());
                    } // FOR
                } // IF
                
                vt.addRow(tuple);
                row_idx++;
                if (row_idx % batch_size == 0) {
                    LOG.debug("Storing batch of " + batch_size + " tuples for " + catalog_tbl.getName() + " [total=" + row_idx + "]");
                    //System.out.println(vt.toString());
                    this.loadVoltTable(catalog_tbl.getName(), vt);
                    vt.clearRowData();
                }
            } // FOR
        } catch (Exception ex) {
            LOG.error("Failed to load table " + catalog_tbl.getName(), ex);
            System.exit(1);
        }
        if (vt.getRowCount() > 0) this.loadVoltTable(catalog_tbl.getName(), vt);
        LOG.debug("Finished loading " + row_idx + " tuples for " + catalog_tbl.getName());
        return;
	}

	public void loadVoltTable(String tablename, VoltTable table) {
		//Client.SyncCallback cb = new Client.SyncCallback();
		try {
		    String target_proc = "@LoadMultipartitionTable";
		    LOG.debug("Loading VoltTable for " + tablename + " using " + target_proc);
		    this.getClientHandle().callProcedure(target_proc, tablename, table); 
			//this.getClientHandle().callProcedure(cb, target_proc, tablename, table);
			//cb.waitForResponse();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}