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
package edu.brown.hstore.wal;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.InsertSubscriber;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

/**
 * @author mkirsch
 * @author pavlo
 */
public class TestCommandLogger extends BaseTestCase {
    
    static final AtomicLong TXN_ID = new AtomicLong(1000);
    static final int BASE_PARTITION = 0;
    static final Class<? extends VoltProcedure>[] TARGET_PROC = (Class<? extends VoltProcedure> []) new Class[2];
    static final Object TARGET_PARAMS[][] = new Object[][]{{ 12345l, "ABCDEF"},{ 666l, 777l, 888l, 999l}};
    
    HStoreSite hstore_site; 
    CommandLogWriter logger;
    Procedure catalog_proc[];
    File outputFile;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        TARGET_PROC[0] = UpdateLocation.class;
        TARGET_PROC[1] = UpdateSubscriberData.class;
        this.catalog_proc = new Procedure[2];
        this.catalog_proc[0] = this.getProcedure(TARGET_PROC[0]);
        this.catalog_proc[1] = this.getProcedure(TARGET_PROC[1]);
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_conf.site.exec_command_logging_group_commit = 2;
        hstore_conf.site.exec_command_logging_group_commit_timeout = 5000000;
        hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        assert(hstore_site.isLocalPartition(0));
        
        outputFile = FileUtil.getTempFile("log");
        logger = new CommandLogWriter(hstore_site, outputFile);
    }

    @Override
    public void tearDown() throws Exception {
        if (outputFile != null && outputFile.exists())
            outputFile.delete();
    }
    
    @Test
    public void testWithGroupCommit() {
        // Write out a new txn invocation to the log
        long txnId[] = new long[2];
        for (int i = 0; i < 2; i++) {
            LocalTransaction ts = new LocalTransaction(hstore_site);
            txnId[i] = TXN_ID.incrementAndGet(); 
            ts.testInit(new Long(txnId[i]),
                        BASE_PARTITION,
                        Collections.singleton(BASE_PARTITION),
                        catalog_proc[i],
                        TARGET_PARAMS[i]);
            
            ClientResponseImpl cresponse = new ClientResponseImpl(txnId[i],
                                                                  0l,
                                                                  BASE_PARTITION,
                                                                  Status.OK,
                                                                  HStoreConstants.EMPTY_RESULT,
                                                                  "");
            boolean ret = logger.appendToLog(ts, cresponse);
            assertFalse(ret);
        }
        logger.finishAndPrepareShutdown(); //This makes sure everything is written to the file
        logger.shutdown(); // This closes the file
        
        // Now read in the file back in and check to see that we have two
        // entries that have our expected information
        CommandLogReader reader = new CommandLogReader(outputFile.getAbsolutePath());
        int ctr = 0;
        for (LogEntry entry : reader) {
            assertNotNull(entry);
            assertEquals(txnId[ctr], entry.txnId.longValue());
            assertEquals(catalog_proc[ctr].getId(), entry.procId);
            
            Object[] entryParams = entry.procParams.toArray();
            assertEquals(TARGET_PARAMS[ctr].length, entryParams.length);
            for (int i = 0; i < TARGET_PARAMS[ctr].length; i++)
                assertEquals(TARGET_PARAMS[ctr][i], entryParams[i]);
            
            ctr++;
        }
        assertEquals(txnId.length, ctr);
    }
}
