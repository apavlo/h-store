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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;

public class YCSBLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(YCSBClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static void main(String args[]) throws Exception {
        if (debug.val)
            LOG.debug("MAIN: " + YCSBLoader.class.getName());
        BenchmarkComponent.main(YCSBLoader.class, args, true);
    }

    public YCSBLoader(String[] args) {
        super(args);
        if (debug.val)
            LOG.debug("CONSTRUCTOR: " + YCSBLoader.class.getName());

//        for (String key : m_extraParams.keySet()) {
//            // TODO: Retrieve extra configuration parameters
//        } // FOR
    }

    @Override
    public void load() {
        if (debug.val)
            LOG.debug("Starting YCSBLoader");

        final Table catalog_tbl = this.getCatalogContext().getTableByName(YCSBConstants.TABLE_NAME);
        final AtomicLong total = new AtomicLong(0);
        final int init_record_count = (int)Math.round(YCSBConstants.NUM_RECORDS * this.getScaleFactor());
        
        // Multi-threaded loader
        final int num_cores = ThreadUtil.availableProcessors();
        final int rows_per_thread = (int)Math.ceil(init_record_count / (double)num_cores);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < num_cores; i++) {
            final int thread_id = i;
            final int start = rows_per_thread * i;
            final int stop = start + rows_per_thread;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    // Create an empty VoltTable handle and then populate it in batches
                    // to be sent to the DBMS
                    VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
                    Object row[] = new Object[table.getColumnCount()];

                    for (int i = start; i < stop; i++) {
                        row[0] = i;

                        // randomly generate strings for each column
                        for (int col = 2; col < YCSBConstants.NUM_COLUMNS; col++) {
                            row[col] = YCSBUtil.astring(50, 50);
                        } // FOR
                        table.addRow(row);

                        // insert this batch of tuples
                        if (table.getRowCount() >= YCSBConstants.BATCH_SIZE) {
                            loadVoltTable(YCSBConstants.TABLE_NAME, table);
                            total.addAndGet(table.getRowCount());
                            table.clearRowData();
                            if (debug.val)
                                LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                          thread_id, total.get(), YCSBConstants.NUM_RECORDS));
                        }
                    } // FOR

                    // load remaining records
                    if (table.getRowCount() > 0) {
                        loadVoltTable(YCSBConstants.TABLE_NAME, table);
                        total.addAndGet(table.getRowCount());
                        table.clearRowData();
                        if (debug.val)
                            LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                      thread_id, total.get(), YCSBConstants.NUM_RECORDS));
                    }
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        if (debug.val)
            LOG.info("Finished loading " + catalog_tbl.getName());
    }
}
