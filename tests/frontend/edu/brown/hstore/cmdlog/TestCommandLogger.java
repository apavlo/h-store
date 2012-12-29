/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
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
package edu.brown.hstore.cmdlog;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.cmdlog.CommandLogReader;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.cmdlog.LogEntry;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

/**
 * @author mkirsch
 * @author pavlo
 */
public class TestCommandLogger extends BaseTestCase {
    
    static final AtomicLong TXN_ID = new AtomicLong(1000);
    static final int BASE_PARTITION = 0;
    
    @SuppressWarnings("unchecked")
    static final Class<? extends VoltProcedure>[] TARGET_PROCS = (Class<? extends VoltProcedure>[])new Class<?>[]{
        UpdateLocation.class,
        UpdateSubscriberData.class
    };
    static final Object TARGET_PARAMS[][] = new Object[][]{
        { 12345l, "ABCDEF"},
        { 666l, 777l, 888l, 999l}
    };
    
    HStoreSite hstore_site; 
    CommandLogWriter logger;
    Thread loggerThread;
    Procedure catalog_procs[];
    File outputFile;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);

        this.catalog_procs = new Procedure[TARGET_PROCS.length];
        for (int i = 0; i < this.catalog_procs.length; i++) {
            this.catalog_procs[i] = this.getProcedure(TARGET_PROCS[i]);
        } // FOR

        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_conf.site.commandlog_enable = false;
        hstore_conf.site.commandlog_timeout = 1000;

        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, hstore_conf);
        assert(hstore_site.isLocalPartition(0));
        
        outputFile = FileUtil.getTempFile("log");
        logger = new CommandLogWriter(hstore_site, outputFile);
        loggerThread = new Thread(this.logger);
        loggerThread.setDaemon(true);
        loggerThread.start();
    }

    @Override
    public void tearDown() throws Exception {
        if (outputFile != null && outputFile.exists())
            outputFile.delete();
    }
    
    @Test
    public void testWithGroupCommit() throws Exception {
        // Write out a new txn invocation to the log
        int num_txns = 1000;
        long txnId[] = new long[num_txns];
        for (int i = 0; i < num_txns; i++) {
            LocalTransaction ts = new LocalTransaction(hstore_site);
            txnId[i] = TXN_ID.incrementAndGet(); 
            ts.testInit(txnId[i],
                        BASE_PARTITION,
                        new PartitionSet(BASE_PARTITION),
                        catalog_procs[i % 2],
                        TARGET_PARAMS[i % 2]);
            
            ClientResponseImpl cresponse = new ClientResponseImpl(txnId[i],
                                                                  0l,
                                                                  BASE_PARTITION,
                                                                  Status.OK,
                                                                  HStoreConstants.EMPTY_RESULT,
                                                                  "");
            boolean ret = logger.appendToLog(ts, cresponse);
            assertFalse(ret);
        }
        logger.flush(); //This makes sure everything is written to the file
        logger.shutdown(); // This closes the file
        
        // Now read in the file back in and check to see that we have two
        // entries that have our expected information
        CommandLogReader reader = new CommandLogReader(outputFile.getAbsolutePath());
        int ctr = 0;
        for (LogEntry entry : reader) {
            assertNotNull(entry);
            assertEquals(txnId[ctr], entry.getTransactionId().longValue());
            assertEquals(catalog_procs[ctr % 2].getId(), entry.getProcedureId());
            
            Object[] entryParams = entry.getProcedureParams().toArray();
            assertEquals(TARGET_PARAMS[ctr % 2].length, entryParams.length);
            for (int i = 0; i < TARGET_PARAMS[ctr % 2].length; i++)
                assertEquals(TARGET_PARAMS[ctr % 2][i], entryParams[i]);
            
            ctr++;
        }
        assertEquals(txnId.length, ctr);
    }
}
