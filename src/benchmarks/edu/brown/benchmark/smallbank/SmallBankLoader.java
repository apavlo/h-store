/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
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

package edu.brown.benchmark.smallbank;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;

import edu.brown.api.Loader;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.rand.RandomDistribution.DiscreteRNG;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.utils.ThreadUtil;

public class SmallBankLoader extends Loader {
    private static final Logger LOG = Logger.getLogger(SmallBankLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    public static void main(String args[]) throws Exception {
        Loader.main(SmallBankLoader.class, args, true);
    }
    
    private final long numAccounts;
    private int loadthreads = ThreadUtil.availableProcessors();
    private final int acctNameLength;
    
    public SmallBankLoader(String[] args) {
        super(args);
        
        this.numAccounts = (int)Math.round(SmallBankConstants.NUM_ACCOUNTS * 
                                           this.getScaleFactor());
        
        // Calculate account name length
        CatalogContext catalogContext = this.getCatalogContext();
        Table catalog_tbl = catalogContext.getTableByName(SmallBankConstants.TABLENAME_ACCOUNTS);
        int acctNameLength = -1;
        for (Column col : catalog_tbl.getColumns()) {
            if (col.getType() == VoltType.STRING.getValue()) {
                acctNameLength = col.getSize();
                break;
            }
        } // FOR
        assert(acctNameLength > 0);
        this.acctNameLength = acctNameLength;
    }

    @Override
    public void load() {
        final CatalogContext catalogContext = this.getCatalogContext();
        final int rows_per_thread = (int)Math.ceil(this.numAccounts / (double)this.loadthreads);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < this.loadthreads; i++) {
            final int start = rows_per_thread * i;
            final int stop = start + rows_per_thread;
            runnables.add(new Generator(catalogContext, start, stop));
        } // FOR
        ThreadUtil.runGlobalPool(runnables);
        
        if (debug.val)
            LOG.debug("Table Counts:\n" + this.getTableTupleCounts());
    }
    
    /**
     * Thread that can generate a range of accounts
     */
    private class Generator implements Runnable {
        private final VoltTable acctsTable;
        private final VoltTable savingsTable;
        private final VoltTable checkingTable;
        private final int start;
        private final int stop;
        private final DefaultRandomGenerator rand = new DefaultRandomGenerator(); 
        private final DiscreteRNG randBalance;
        
        public Generator(CatalogContext catalogContext, int start, int stop) {
            this.acctsTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_ACCOUNTS));
            this.savingsTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_SAVINGS));
            this.checkingTable = CatalogUtil.getVoltTable(catalogContext.getTableByName(SmallBankConstants.TABLENAME_CHECKING));
            this.start = start;
            this.stop = stop;
            this.randBalance = new Gaussian(this.rand,
                                            SmallBankConstants.MIN_BALANCE,
                                            SmallBankConstants.MAX_BALANCE);
        }
        
        public void run() {
            final String acctNameFormat = "%0"+acctNameLength+"d";
            int batchSize = 0;
            for (int acctId = this.start; acctId < this.stop; acctId++) {
                // ACCOUNT
                String acctName = String.format(acctNameFormat, acctId);
                this.acctsTable.addRow(acctId, acctName);
                
                // CHECKINGS
                this.checkingTable.addRow(acctId, this.randBalance.nextInt());
                
                // SAVINGS
                this.savingsTable.addRow(acctId, this.randBalance.nextInt());
                
                if (++batchSize >= SmallBankConstants.BATCH_SIZE) {
                    this.loadTables();
                    batchSize = 0;
                }
            } // FOR
            if (batchSize > 0) {
                this.loadTables();
            }
        }
        
        private void loadTables() {
            loadVoltTable(SmallBankConstants.TABLENAME_ACCOUNTS, this.acctsTable);
            this.acctsTable.clearRowData();
            loadVoltTable(SmallBankConstants.TABLENAME_SAVINGS, this.savingsTable);
            this.savingsTable.clearRowData();
            loadVoltTable(SmallBankConstants.TABLENAME_CHECKING, this.checkingTable);
            this.checkingTable.clearRowData();
        }
    };
}
