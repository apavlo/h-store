/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltdb.regressionsuites;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.saverestore.SaveRestoreTestProjectBuilder;
import org.voltdb.utils.SnapshotVerifier;

import com.sun.net.httpserver.Authenticator.Success;

import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBLoader;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.benchmark.ycsb.procedures.ReadRecord;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.cmdlog.CommandLogReader;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.cmdlog.LogEntry;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures for YCSB
 */
public class TestSnapshotRecovery extends RegressionSuite {

    private static final String TMPDIR = "./snapshot";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;
    
    private static int NUM_SITES = -1;    
    private static int NUM_PARTITIONS = -1;    

    // YCSB
    private static final String PREFIX = "ycsb";
    private static final int NUM_TUPLES = 16;

    public TestSnapshotRecovery(String name) {
        super(name);
    }

    @Override
    public void setUp() {
        deleteTestFiles();
        super.setUp();
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Override
    public void tearDown() {
        try {
            deleteTestFiles();
            super.tearDown();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteTestFiles() {
        FilenameFilter cleaner = new FilenameFilter() {
            public boolean accept(File dir, String file) {
                return file.startsWith(TESTNONCE) || file.endsWith(".vpt") || file.endsWith(".digest") || file.endsWith(".tsv") || file.endsWith(".csv");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files) {
            tmp_file.delete();
        }
    }

    private static synchronized void setUpSnapshotDir(){
        try {
            File snapshotDir = new File(TMPDIR);

            if (!snapshotDir.exists()) {
                boolean result = snapshotDir.mkdir();
                if (result) {
                    System.out.println("Created snapshot directory.");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }    
    
    private VoltTable[] saveTables(Client client) {
        VoltTable[] results = null;
        try {
            results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1).getResults();
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }

    private void validateSnapshot(boolean expectSuccess) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] { TESTNONCE, "--dir", TMPDIR };
            SnapshotVerifier.main(args);
            ps.flush();
            String reportString = baos.toString("UTF-8");

            //System.err.println("Validate Snapshot :"+reportString);

            if (expectSuccess) {
                assertTrue(reportString.startsWith("Snapshot valid\n"));
                System.err.println("Validate Snapshot :" + "Snapshot valid");
            } else {
                assertTrue(reportString.startsWith("Snapshot corrupted\n"));
                System.err.println("Validate Snapshot :" + "Snapshot corrupted");
            }
        } catch (UnsupportedEncodingException e) {
        } finally {
            System.setOut(original);
        }
    }    
        
    private void initializeDatabase(final Client client, final int num_tuples) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.FIXED_SIZE=true",
            "BENCHMARK.NUM_RECORDS="+num_tuples,
            "BENCHMARK.LOADTHREADS=1",
        };
        final CatalogContext catalogContext = this.getCatalogContext();
        YCSBLoader loader = new YCSBLoader(args) {
            {
                this.setCatalogContext(catalogContext);
                this.setClientHandle(client);
            }
            @Override
            public CatalogContext getCatalogContext() {
                return (catalogContext);
            }
        };
        loader.load();
    }    
        
    public void testSaveAndRestoreYCSB() throws IOException, InterruptedException, ProcCallException {
        
        System.out.println("Starting testSaveAndRestoreYCSB");
        
        deleteTestFiles();
        setUpSnapshotDir();
    
        VoltTable results[] = null;
        ClientResponse cresponse = null;
        Client client = this.getClient();
        
        try{
            this.initializeDatabase(client, NUM_TUPLES);
        }
        catch(Exception e){
            e.printStackTrace();
        }        
        
        // Stream of Adhoc Select Queries
        /*
        for (int q_itr = 0; q_itr < 2; q_itr++) {
            String query = "SELECT COUNT(*) FROM " + YCSBConstants.TABLE_NAME;

            cresponse = client.callProcedure("@AdHoc", query);
            assertEquals(Status.OK, cresponse.getStatus());
            results = cresponse.getResults();

            assertEquals(1, results.length);
            assertEquals(NUM_TUPLES, results[0].asScalarLong());
        }
        */
        
        // Read Record
        
        long key = NUM_TUPLES / 2;
        String procName = ReadRecord.class.getSimpleName();
        Object params[] = { key };
        
        cresponse = client.callProcedure(procName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(1, cresponse.getResults().length);
        
        VoltTable vt = cresponse.getResults()[0];
        boolean adv = vt.advanceRow();
        assert(adv);
        assertEquals(key, vt.getLong(0));
        
        System.out.println("Read Record Test Passed");
        
        // Snapshot
        results = null;
        results = saveTables(client);

        validateSnapshot(true);

        VoltTable[] results_tmp = null;
        results_tmp = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics after saveTables :");
        System.out.println(results_tmp[0]);

        
        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        client = getClient();

        try {
            /*
            results = client.callProcedure("@Undo").getResults();

            System.out.println(results[0]);
            
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
            */
            
            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWEXPORT).getResults();

            System.out.println(results[0]);
            
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        System.out.println("@Statistics after restore:");
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);
               
        validateSnapshot(true);        
        
        checkYCSBTable(client, 2);              
        
        parseAndApplyCommandLog();
    }
    
    void parseAndApplyCommandLog() throws NoConnectionsException, IOException, ProcCallException{
        File outputFile = new File("./obj"+File.separator+
                "cmdlog" +
                File.separator +
                "h00" +
                CommandLogWriter.LOG_OUTPUT_EXT);
        
        System.err.println("parseCommandLog :"+outputFile);        
        
        // Now read in the file back in and check to see that we have two
        // entries that have our expected information
        CommandLogReader reader = null;

        try{
            reader = new CommandLogReader(outputFile.getAbsolutePath());
        }
        catch(Exception e){
            e.printStackTrace();
        }
        
        int ctr = 0;
        Iterator<LogEntry> log_itr = reader.iterator();
        ClientResponse cresponse = null;
        Client client = this.getClient();
        CatalogContext cc = this.getCatalogContext();
        VoltTable results[] = null;
        
        while(log_itr.hasNext()) {
            LogEntry entry = log_itr.next();

            if(entry.getType() == false){            
                assertNotNull(entry);
                System.err.println("REDO :: TXN ID :" + entry.getTransactionId().longValue());
                System.err.println("REDO :: PROC ID :" + entry.getProcedureId());

                Object[] entryParams = entry.getProcedureParams().toArray();
                for (Object obj : entryParams) {
                    System.err.println(obj);
                }

                String procName = cc.getProcedureById(entry.getProcedureId()).fullName();
                System.out.println("Invoking procedure ::" + procName);

                cresponse = client.callProcedure(procName, entry.getProcedureParams().toArray());
                assertEquals(Status.OK, cresponse.getStatus());
                results = cresponse.getResults();

                assertEquals(1, results.length);
                // assertEquals(NUM_TUPLES, results[0].asScalarLong());
                // System.out.println("Results for procedure ::" + procName + " " + results[0]);
            }
            else{
                assertNotNull(entry);                
                System.err.println("UNDO :: TXN ID :" + entry.getTransactionId().longValue());
                System.err.println("UNDO :: PROC ID :" + entry.getProcedureId());                               
            }
            
            ctr++;
        }
        
        System.err.println("WAL LOG entries :"+ctr);                        
    }
    
    
    private void checkYCSBTable(Client client, int numTuples) {
        long key_itr, key;
        String procName;
        ClientResponse cresponse = null;
        VoltTable vt = null;
        boolean adv = true;
        
        for(key_itr = 0 ; key_itr < numTuples ; key_itr++){
            procName = ReadRecord.class.getSimpleName();
            Object params[] = { key_itr };
            
            try{
                cresponse = client.callProcedure(procName, params);
            }
            catch(Exception e){
                e.printStackTrace();
            }
                
            assertNotNull(cresponse);
            assertEquals(Status.OK, cresponse.getStatus());
            assertEquals(1, cresponse.getResults().length);
            
            vt = cresponse.getResults()[0];
            adv = vt.advanceRow();
            if(adv == false)
                System.err.println("key :"+key_itr+" no result");
            //else
            //    System.err.println("key :"+key_itr+" result:"+vt.getLong(0));

            assert(adv);
            assertEquals(key_itr, vt.getLong(0));            
        }                               
        
        System.out.println("checkYCSBTable Passed");
    }    
    

    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSnapshotRecovery.class);
        builder.setGlobalConfParameter("site.commandlog_enable", true);
        builder.setGlobalConfParameter("site.commandlog_timeout", 10);
        builder.setGlobalConfParameter("site.status_enable", true);
        builder.setGlobalConfParameter("site.status_exec_info", true);
                
        YCSBProjectBuilder project = new YCSBProjectBuilder();

        project.addAllDefaults();

        VoltServerConfig m_config = null;        
        boolean success = false;

        setUpSnapshotDir();

        // CONFIG #1: 2 Local Site with 4 Partitions running on JNI backend
        /*
        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster("snapshot-"+PREFIX+"-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);
        */
        
        // CONFIG #2: 1 Local Site with 1 Partitions running on JNI backend
        NUM_SITES = 1;
        NUM_PARTITIONS = 1;        
        m_config = new LocalSingleProcessServer("snapshot-"+PREFIX+"-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);

        return builder;
    }
}
