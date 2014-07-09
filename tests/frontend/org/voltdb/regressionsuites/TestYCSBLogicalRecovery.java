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
import java.util.Calendar;
import java.util.Iterator;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.SnapshotVerifier;

import edu.brown.benchmark.ycsb.YCSBClient;
import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBLoader;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.benchmark.ycsb.procedures.ReadRecord;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.cmdlog.CommandLogReader;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.cmdlog.LogEntry;

/**
 * Test logical recovery
 */
public class TestYCSBLogicalRecovery extends RegressionSuite {

    private static final String TMPDIR = "./obj/snapshot";
    private static final String CMDLOGDIR = "./obj/cmdlog";

    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;

    private static int NUM_SITES = -1;
    private static int NUM_PARTITIONS = -1;

    // YCSB
    private static final String PREFIX = "ycsb";
    private static int NUM_TRANSACTIONS = 500;
    private static final String projectJAR = "logical_" + PREFIX + ".jar";


    public TestYCSBLogicalRecovery(String name) {
        super(name);
    }

    @Override
    public void setUp() {
        deleteTestFiles();
        deleteCommandLogDir();
        super.setUp();
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Override
    public void tearDown() {
        try {
            deleteTestFiles();
            deleteCommandLogDir();
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
    
    private static synchronized void deleteCommandLogDir() {
        try {
            File cmdlogDir = new File(CMDLOGDIR);

            if (cmdlogDir.exists()) {
                File[] log_files = cmdlogDir.listFiles();
                for (File log_file : log_files) {
                    log_file.delete();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static synchronized void setUpSnapshotDir() {
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

            // System.err.println("Validate Snapshot :"+reportString);

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
    
    private void initializeYCSBDatabase(final CatalogContext catalogContext, final Client client, final int num_tuples) throws Exception {
        String args[] = { 
                "NOCONNECTIONS=true", 
                "BENCHMARK.FIXED_SIZE=true", 
                "BENCHMARK.NUM_RECORDS=" + num_tuples, 
                "BENCHMARK.LOADTHREADS=4", };
        
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

    public void testYCSB() throws IOException, InterruptedException, ProcCallException {

        System.out.println("Starting testYCSB - Logical Recovery");                

        deleteTestFiles();
        setUpSnapshotDir();

        VoltTable results[] = null;
        Client client = this.getClient();
        CatalogContext cc = this.getCatalogContext();       
        
        // Load database
        try {
            //initializeYCSBDatabase(cc, client, YCSBConstants.NUM_RECORDS);
            initializeYCSBDatabase(cc, client, 10000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        final String MOCK_ARGS[] = { "HOST=localhost", "NUMCLIENTS=1",
                // XXX HACK to find catalog jar
                "CATALOG=" + "./obj/release/testobjects/" + projectJAR, "" };

        MOCK_ARGS[MOCK_ARGS.length - 1] = HStoreConstants.BENCHMARK_PARAM_PREFIX;

        YCSBClient yscbClient = new YCSBClient(MOCK_ARGS);

        // Run transactions
        long k_itr = 0;
        long numTransactions = NUM_TRANSACTIONS;
        long period = numTransactions / 10;

        for (k_itr = 0; k_itr < numTransactions; k_itr++) {
            boolean response = yscbClient.runOnce();
            assertEquals(response, true);

            if (k_itr % period == 0)
                System.out.println(String.format("Transactions Processed: %6d / %d", k_itr, numTransactions));
        }
        
        // Statistics         
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);

        // Take Snapshot
        results = null;
        results = saveTables(client);

        validateSnapshot(true);

        VoltTable[] results_tmp = null;
        results_tmp = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics before restart :");
        System.out.println(results_tmp[0]);

        
        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        client = getClient();

        Calendar calendar;
        long t1,t2;

        calendar = Calendar.getInstance();                    
        t1 = calendar.getTimeInMillis();
        
        // LOGICAL : SNAPSHOT + CMD LOG
        try {
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
        
        validateSnapshot(true);
        
        System.out.println("@Statistics after LOGICAL snapshot restore:");
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);
                
        File logDir = new File("./obj" + File.separator + "cmdlog");                               
        
        // Parse WAL logs for all sites
        CatalogMap<Site> sites = cc.sites;

        for(Site site : sites){
            String siteName = site.getName().toLowerCase();
            siteName = "h0" + siteName;            
            System.err.println("Site Prefix : "+siteName);
            
            parseAndApplyCommandLog(logDir, siteName);            
        }

        calendar = Calendar.getInstance();                    
        t2 = calendar.getTimeInMillis();

        System.err.println("Recovery Latency :" + (t2-t1)+ " ms"); 
        
        // checkYCSBTable(client, NUM_TUPLES);
    }
    
    // COMMAND LOG        
    File scanForLatestLogFile(File logDir, String prefix){
        System.err.println("Scanning logDir :" + logDir.getAbsolutePath());           

        final String logPrefix = prefix;
        FilenameFilter cleaner = new FilenameFilter() {
            public boolean accept(File dir, String file) {
                return  file.startsWith(logPrefix) && file.endsWith(CommandLogWriter.LOG_OUTPUT_EXT);
            }
        };

        File[] logFiles = logDir.listFiles(cleaner);
        File latestFile = null, prevFile = null;        
        long maxTimeStamp = Long.MIN_VALUE;
        
        for (File logFile : logFiles) {
            String name = logFile.getName();
            String delims = "_|\\.";
            String[] tokens = name.split(delims);

            //System.err.println("Tokens :"+Arrays.toString(tokens));

            if(tokens.length <= 1)
                continue;
            
            long fileTimestamp = Long.parseLong(tokens[1]) ;
            if(fileTimestamp > maxTimeStamp){
                maxTimeStamp = fileTimestamp;
                prevFile = latestFile;
                latestFile = logFile;         
            }
        }       

        if(prevFile != null){
            System.err.println("Using logfile :" + prevFile.getAbsolutePath());           
        }

        if(latestFile != null){
            System.err.println("Found latest logfile :" + latestFile.getAbsolutePath());           
        }
        
        return prevFile;
    }
    
    void parseAndApplyCommandLog(File logDir, String hostPrefix) throws NoConnectionsException, IOException, ProcCallException {

        if(logDir == null){
            System.err.println("logDir null ");           
            return;
        }

        File latestFile = scanForLatestLogFile(logDir, hostPrefix);
        
        if(latestFile == null){
            System.err.println("Command log not found :" + logDir.getAbsolutePath());           
            return;
        }
        
        System.out.println("parseCommandLog :" + latestFile.getAbsolutePath());

        // Now read in the file back in and check to see that we have two
        // entries that have our expected information
        CommandLogReader reader = null;

        try {
            reader = new CommandLogReader(latestFile.getAbsolutePath());
        } catch (Exception e) {
            System.err.println("Command log not found :" + latestFile.getAbsolutePath());
            return;
        }

        int ctr = 0;
        Iterator<LogEntry> log_itr = reader.iterator();
        ClientResponse cresponse = null;
        Client client = this.getClient();
        CatalogContext cc = this.getCatalogContext();
        VoltTable results[] = null;

        while (log_itr.hasNext()) {
            LogEntry entry = log_itr.next();

            assert(entry != null);
            //System.err.println("REDO :: TXN ID :" + entry.getTransactionId().longValue());
            //System.err.println("REDO :: PROC ID :" + entry.getProcedureId());

            Object[] entryParams = entry.getProcedureParams().toArray();
        
            String procName = cc.getProcedureById(entry.getProcedureId()).fullName();
            Procedure catalog_proc = cc.procedures.getIgnoreCase(procName);

            if(catalog_proc.getReadonly() == false){
                // System.out.println("Invoking procedure ::" + procName);

                cresponse = client.callProcedure(procName, entryParams);
                assertEquals(cresponse.getStatus(), Status.OK);
                
                // results = cresponse.getResults();
                // assertEquals(results.length, 1);
            }

            ctr++;
        }

        System.out.println("################################# WAL LOG entries :" + ctr);
    }


    private void checkYCSBTable(Client client, int numTuples) {
        long key_itr, key;
        String procName;
        ClientResponse cresponse = null;
        VoltTable vt = null;
        boolean adv = true;

        for (key_itr = 0; key_itr < numTuples; key_itr++) {
            procName = ReadRecord.class.getSimpleName();
            Object params[] = { key_itr };

            try {
                cresponse = client.callProcedure(procName, params);
            } catch (Exception e) {
                e.printStackTrace();
            }

            assertNotNull(cresponse);
            assertEquals(Status.OK, cresponse.getStatus());
            assertEquals(1, cresponse.getResults().length);

            vt = cresponse.getResults()[0];
            adv = vt.advanceRow();
            if (adv == false)
                System.err.println("key :" + key_itr + " no result");
            else
                System.err.println("key :" + key_itr + " result:" + vt.getLong(0));

            // assert(adv);
            // assertEquals(key_itr, vt.getLong(0));
        }

        System.out.println("checkYCSBTable Passed");
    }

    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestYCSBLogicalRecovery.class);

        // COMMAND LOG
        builder.setGlobalConfParameter("site.commandlog_enable", true);
        builder.setGlobalConfParameter("site.commandlog_timeout", 10);
        builder.setGlobalConfParameter("site.anticache_enable", false);     

        builder.setGlobalConfParameter("site.snapshot", true);

        YCSBProjectBuilder project = new YCSBProjectBuilder();

        project.addAllDefaults();

        VoltServerConfig m_config = null;
        boolean success = false;

        setUpSnapshotDir();
        
        // CONFIG #1: 2 Local Site with 4 Partitions running on JNI backend
        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster(projectJAR, NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);
                   
        
        // CONFIG #2: 1 Local Site with 1 Partitions running on JNI backend  
        /*
        NUM_SITES = 1;
        NUM_PARTITIONS = 1;
        m_config = new LocalSingleProcessServer(projectJAR, NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);     
        */
        
        return builder;
    }
}
