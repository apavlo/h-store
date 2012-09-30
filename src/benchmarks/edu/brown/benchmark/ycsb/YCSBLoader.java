/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *								   
 *                                                                         *
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

package edu.brown.benchmark.ycsb;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;

import edu.brown.api.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;

public class YCSBLoader extends BenchmarkComponent {
	
	private static final Logger LOG = Logger.getLogger(YCSBLoader.class);
    private static final boolean d = LOG.isDebugEnabled();

    public static void main(String args[]) throws Exception {
		
		if (d)
            LOG.debug("MAIN: " + YCSBLoader.class.getName());
		
        BenchmarkComponent.main(YCSBLoader.class, args, true);
    }

    public YCSBLoader(String[] args) {
		
        super(args);
				
		if (d)
            LOG.debug("CONSTRUCTOR: " + YCSBLoader.class.getName());
		
        for (String key : m_extraParams.keySet()) {
            // TODO: Retrieve extra configuration parameters
        } // FOR
    }

    @Override
    public void runLoop() {
		
		if (d)
            LOG.debug("Starting YCSBLoader");
		
        Catalog catalog = this.getCatalog();
        Client client = this.getClientHandle();
		
        for (Table catalog_tbl : CatalogUtil.getDatabase(catalog).getTables()) {
            // Create an empty VoltTable handle and then populate it in batches
            // to be sent to the DBMS
            VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
			
			Object row[] = new Object[table.getColumnCount()];
			//String sql = SQLUtil.getInsertSQL(catalog_tbl);
			long total = 0;
			int batch = 0;
			
			for (int i = 0; i < YCSBConstants.NUM_RECORDS; i++) {
				
				//row[0] = new String("" + i); // set row id
				
				row[0] = i; 
				
				// randomly generate strings for each column
				for (int col = 2; col < YCSBConstants.NUM_COLUMNS; col++) {
					row[col] = YCSBUtil.astring(1, 50); 
				}
				
				//assert col == table.getColumnCount();
				table.addRow(row); 
				total++;
				
				// insert this batch of tuples
				if(table.getRowCount() >= YCSBConstants.BATCH_SIZE) {

					loadVoltTable(YCSBConstants.TABLE_NAME, table);
					table.clearRowData();
					
					if(d)
						LOG.debug(String.format("Records Loaded: %6d / %d", total, YCSBConstants.NUM_RECORDS));
				}
			} // FOR
			
			// load remaining records
			if(table.getRowCount() > 0) {
				
				loadVoltTable(YCSBConstants.TABLE_NAME, table);
				table.clearRowData();
				
				if(d)
					LOG.debug(String.format("Records Loaded: %6d / %d", total, YCSBConstants.NUM_RECORDS));
			}
			
			if (d) 
				LOG.info("Finished loading " + catalog_tbl.getName());
						
            try {
                // Makes a blocking call to @LoadMultipartitionTable sysproc in
                // order to load the contents of the VoltTable into the cluster
               // this.loadVoltTable(catalog_tbl.getName(), table);
            } catch (Exception e) {
				System.out.println(e.getMessage()); 
                throw new RuntimeException("Failed to load data for " + catalog_tbl, e);
            }
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        // IGNORE: Only needed for Client
        return new String[] {};
    }
}
