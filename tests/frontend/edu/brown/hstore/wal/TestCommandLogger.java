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

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
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
 */
public class TestCommandLogger extends BaseTestCase {
    
    static final AtomicLong TXN_ID = new AtomicLong(1000);
    static final int BASE_PARTITION = 0;
    static final Class<? extends VoltProcedure> TARGET_PROC = UpdateLocation.class;
    static final Object TARGET_PARAMS[] = new Object[]{ 12345l, "ABCDEF" };
    
    HStoreSite hstore_site; 
    CommandLogWriter logger;
    Procedure catalog_proc;
    File outputFile;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.catalog_proc = this.getProcedure(TARGET_PROC);
        
        Site catalog_site = CollectionUtil.first(CatalogUtil.getCluster(catalog).getSites());
        HStoreConf hstore_conf = HStoreConf.singleton();
        hstore_site = new MockHStoreSite(catalog_site, hstore_conf);
        
        outputFile = FileUtil.getTempFile("log"); //"/research/hstore/mkirsch/testwal.log";
        logger = new CommandLogWriter(hstore_site, outputFile);
    }

    @Override
    public void tearDown() throws Exception {
        if (outputFile != null && outputFile.exists())
            outputFile.delete();
    }
    
    @Test
    public void testSimpleTest() {
        // Write out a new txn invocation to the log
        LocalTransaction ts = new LocalTransaction(hstore_site);
        long txnId = TXN_ID.incrementAndGet(); 
        ts.testInit(new Long(txnId),
                    BASE_PARTITION,
                    Collections.singleton(BASE_PARTITION),
                    catalog_proc,
                    TARGET_PARAMS);
        ClientResponseImpl cresponse = new ClientResponseImpl(txnId,
                                                              12345l,
                                                              BASE_PARTITION,
                                                              Status.OK,
                                                              HStoreConstants.EMPTY_RESULT,
                                                              "");
        boolean ret = logger.appendToLog(ts, cresponse);
        assertTrue(ret);
        logger.shutdown(); // This closes the file
        
        // Now read in the file back in and check to see that we have one
        // entry that has our expected information
        CommandLogReader reader = new CommandLogReader(outputFile.getAbsolutePath());
        int ctr = 0;
        for (LogEntry entry : reader) {
            assertNotNull(entry);
            assertEquals(txnId, entry.txnId.longValue());
            assertEquals(catalog_proc.getId(), entry.procId);
            
            Object[] entryParams = entry.procParams.toArray();
            assertEquals(TARGET_PARAMS.length, entryParams.length);
            for (int i = 0; i < TARGET_PARAMS.length; i++)
                assertEquals(TARGET_PARAMS[i], entryParams[i]);
            
            // TODO: Do this check for all the others
            
            ctr++;
        }
        assertEquals(1, ctr);
    }
}
