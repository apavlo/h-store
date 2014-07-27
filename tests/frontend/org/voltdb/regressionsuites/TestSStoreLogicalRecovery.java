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
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.ProcedureRef;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.sysprocs.SetConfiguration;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.benchmark.recovery.RecoveryClient;
import edu.brown.benchmark.recovery.RecoveryLoader;
import edu.brown.benchmark.recovery.RecoveryProjectBuilder;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.cmdlog.CommandLogReader;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.cmdlog.LogEntry;

/**
 * Test logical recovery
 */
public class TestSStoreLogicalRecovery extends RegressionSuite {

    private static final String TMPDIR = "./obj/snapshot";
    private static final String CMDLOGDIR = "./obj/cmdlog";

    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;

    private static int NUM_SITES = -1;
    private static int NUM_PARTITIONS = -1;

    // Recovery benchmark
    private static final String PREFIX = "recovery";//"tpcc";
    private static int NUM_TRANSACTIONS = 5;
    private static final String projectJAR = "logical_" + PREFIX + ".jar";

    public TestSStoreLogicalRecovery(String name) {
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
            // added by hawk to ensure we have latest command log records
            //deleteCommandLogDir();
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

    public void initializeRecoveryDatabase(final CatalogContext catalogContext, final Client client, boolean scaleItems) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
//            "BENCHMARK.WAREHOUSE_PER_PARTITION=true",
//            "BENCHMARK.NUM_LOADTHREADS=4",
//            "BENCHMARK.SCALE_ITEMS="+scaleItems,
        };

        RecoveryLoader loader = new RecoveryLoader(args)
        {
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

    boolean weak_recovery = true;

    public void testRecovery() throws IOException, InterruptedException, ProcCallException {

        System.out.println("Starting testRecovery - Logical Recovery");

        // set weak recovery true or false
        weak_recovery = false; // weak_recovery is false means that we will use strong recovery

        deleteTestFiles();
        setUpSnapshotDir();

        VoltTable results[] = null;
        Client client = this.getClient();
        CatalogContext cc = this.getCatalogContext();

        setFrontendTriggerWorking(true);

        if(weak_recovery == true)
            setWeakRecovery(true);
        else
            setWeakRecovery(false);

        // Load database
        try {
            initializeRecoveryDatabase(cc, client, false);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // check S1 and S2 before taking snapshot
        boolean s1_hasTuple = hasTuple("S1");
        boolean s2_hasTuple = hasTuple("S2");
        boolean w1_hasTuple = hasTuple("W1");
        boolean t2_hasTuple = hasTuple("T2");
        // Take Snapshot
        results = null;
        results = saveTables(client);
        long checkpoint_timestamp = EstTime.currentTimeMillis();

        validateSnapshot(true);

        final String MOCK_ARGS[] = { "HOST=localhost", "NUMCLIENTS=1",
                // XXX HACK to find catalog jar
                "CATALOG=" + "./obj/release/testobjects/" + projectJAR, "" };

        MOCK_ARGS[MOCK_ARGS.length - 1] = HStoreConstants.BENCHMARK_PARAM_PREFIX;

        RecoveryClient recoveryClient = new RecoveryClient(MOCK_ARGS);

        // Run transactions for record the log
        long k_itr = 0;
        long numTransactions = NUM_TRANSACTIONS;
        //long period = numTransactions / 10;
        long period = numTransactions / 5;
        long startpoint = 101;

        for (k_itr = 0; k_itr < numTransactions; k_itr++) {
            boolean response = recoveryClient.runWithTuple((int)(k_itr + startpoint));
            assertEquals(response, true);

            if (k_itr % period == 0)
                System.out.println(String.format("Transactions Processed: %6d / %d", k_itr, numTransactions));
        }

        // check S1 and S2
        s1_hasTuple = hasTuple("S1");
        s2_hasTuple = hasTuple("S2");
        w1_hasTuple = hasTuple("W1");
        t2_hasTuple = hasTuple("T2");

        // Statistics
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);

        VoltTable[] results_tmp = null;
        results_tmp = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics before restart :");
        System.out.println(results_tmp[0]);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        // now, we enter the recovery phrase
        m_config.startUp();
        client = getClient();

        // used for testing to check if the SnapshotRestore destroy window behavior
//        client.callProcedure("SimpleCall", 1000);
//        s1_hasTuple = hasTuple("S1");
//        s2_hasTuple = hasTuple("S2");
//        w1_hasTuple = hasTuple("W1");
//        t2_hasTuple = hasTuple("T2");
//
//        DisplayWindowAttribute("W1");
        // end of testing

        Calendar calendar;
        long t1,t2;

        calendar = Calendar.getInstance();
        t1 = calendar.getTimeInMillis();




        if(weak_recovery == true)
        {
            setWeakRecovery(true);
            setFrontendTriggerWorking(true);
        }
        else // for the strong recovery, we will turn off the frontend trigger mechanism
        {
            setWeakRecovery(false);
            setFrontendTriggerWorking(false);
        }


        // modified by hawk, 2014/6/17
        // for S-Store, we should re-execute the related frontend trigger,
        // if there are some tuple alive in the streams after SNAPSHOT recovery
        // LOGICAL : SNAPSHOT + FRONTEND TRIGGER + CMD LOG
        // STEP ONE: SNAPSHOT recovery
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

        //DisplayWindowAttribute("W1");

        validateSnapshot(true);

        System.out.println("@Statistics after LOGICAL snapshot restore:");
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);

        // check S1 and S2
        s1_hasTuple = hasTuple("S1");
        s2_hasTuple = hasTuple("S2");
        w1_hasTuple = hasTuple("W1");
        t2_hasTuple = hasTuple("T2");

        // STEP TWO: FRONTEND TRIGGER re-execution
        // if we use weak recovery mechanism, we should make frontend trigger recovery with some existing tuples in streams
        if(weak_recovery == true)
            doFrontendTriggerRecovery();

        // check S1 and S2
        s1_hasTuple = hasTuple("S1");
        s2_hasTuple = hasTuple("S2");
        w1_hasTuple = hasTuple("W1");
        t2_hasTuple = hasTuple("T2");

        // STEP THREE: CMD LOG re-execution
        //File logDir = new File("/mnt/pmfs" + File.separator + "cmdlog");
        File logDir = new File("./obj" + File.separator + "cmdlog");

        // Parse WAL logs for all sites
        CatalogMap<Site> sites = cc.sites;

        for(Site site : sites){
            String siteName = site.getName().toLowerCase();
            siteName = "h0" + siteName;
            System.err.println("Site Prefix : "+siteName);

            parseAndApplyCommandLog(logDir, siteName, checkpoint_timestamp);
        }

        // check S1 and S2
        s1_hasTuple = hasTuple("S1");
        s2_hasTuple = hasTuple("S2");
        w1_hasTuple = hasTuple("W1");
        t2_hasTuple = hasTuple("T2");

        // for strong recovery, we should turn the frontend trigger mechanism on back
        if(weak_recovery == false)
        {
            setFrontendTriggerWorking(true);
            doFrontendTriggerRecovery();
        }


        calendar = Calendar.getInstance();
        t2 = calendar.getTimeInMillis();

        System.err.println("Recovery Latency :" + (t2-t1)+ " ms");

        // checkYCSBTable(client, NUM_TUPLES);
    }

    private void DisplayWindowAttribute(String strWindow)
    {
        CatalogContext catalogContext = this.getCatalogContext();
        Table catalog_tbl = catalogContext.getTableByName(strWindow);

        boolean isWindow = catalog_tbl.getIswindow();
        boolean isRows = catalog_tbl.getIsrows();
        int winSize = catalog_tbl.getSize();
        int winSlide = catalog_tbl.getSlide();
        String streamName = catalog_tbl.getStreamname();

        System.out.println(
                String.format("window : %B, rows : %B, size : %d, slide : %d, stream : %S",
                        isWindow, isRows, winSize, winSlide, streamName)
                );

    }

    // FRONTEND TRIGGER
    private void doFrontendTriggerRecovery() throws NoConnectionsException, IOException, ProcCallException {
        //FIXME, should I care about the sequence of streams ?
        CatalogContext catalogContext = this.getCatalogContext();
        Client client = this.getClient();

        // for streams
        for (Table catalog_tbl : catalogContext.getStreamTables()) {

            System.out.println("Dealing with stream - " + catalog_tbl.fullName());

            CatalogMap<ProcedureRef> procedures = catalog_tbl.getTriggerprocedures();

            if( (procedures==null) || (procedures.isEmpty()==true))
                continue;

            // should I check if the stream is empty or not? yes
            String streamName = catalog_tbl.fullName();
            System.out.println( "Checking if there is tuples in stream - " + streamName );
            boolean hasTuple = hasTuple(streamName);
            if (hasTuple == true){
                // FIXME, should I care about the sequence of procedures
                for(ProcedureRef procedureRef : procedures)
                {
                    Procedure procedure = procedureRef.getProcedure();
                    // FIXME, I need batchId and clienthandle
                    //this.invocationTriggerProcedureProcess(ts.getBatchId(), ts.getClientHandle(), EstTime.currentTimeMillis(), procedure);
                    System.out.println( "Recovery: re-execute frontend trigger - " + procedure.getName() );
                    //Object[] entryParams = null;

                    //this.invocationTriggerProcedureProcess(-1, 0, EstTime.currentTimeMillis(), procedure);
                    ClientResponse cresponse = client.callProcedure(procedure.getName());
                    assertEquals(cresponse.getStatus(), Status.OK);
                }
            }
        }
    }

    private void setFrontendTriggerWorking(Boolean flag)
    {

        try{

            Client client = this.getClient();

            String procName = VoltSystemProcedure.procCallName(SetConfiguration.class);
            String confParams[] = {"global.sstore_frontend_trigger"};
            String confValues[] = {flag.toString()};
            ClientResponse cresponse = client.callProcedure(procName, confParams, confValues);
            assert(cresponse.getStatus() == Status.OK) : cresponse.toString();

            client.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void setWeakRecovery(Boolean flag)
    {

        try{

            Client client = this.getClient();

            String procName = VoltSystemProcedure.procCallName(SetConfiguration.class);
            String confParams[] = {"global.weak_recovery"};
            String confValues[] = {flag.toString()};
            ClientResponse cresponse = client.callProcedure(procName, confParams, confValues);
            assert(cresponse.getStatus() == Status.OK) : cresponse.toString();

            client.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private boolean hasTuple(String streamName)
    {
        boolean result = false;

        try{

            Client client = this.getClient();
            //CatalogContext cc = this.getCatalogContext();

            String query = "select * from " + streamName;
            ClientResponse cresponse = client.callProcedure("@AdHoc", query);

            if (cresponse != null) {
                if (cresponse.getStatus() == Status.OK) {
                    System.out.println(streamName + " - " + this.formatResult(cresponse));
                    VoltTable results[] = cresponse.getResults();
                    if(results[0].getRowCount() != 0)
                        result = true;
                } else {
                    System.out.printf("Server Response: %s / %s\n",
                                      cresponse.getStatus(),
                                      cresponse.getStatusString());
                }
            }

            client.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        return result;
    }

    private String formatResult(ClientResponse cr) {

        final VoltTable results[] = cr.getResults();
        //final int num_results = results.length;
        //System.out.println("num of results: " + String.valueOf(num_results));
        StringBuilder sb = new StringBuilder();
        sb.append(VoltTableUtil.format(results));

        VoltTable vt = results[0];
        //int num_rows = vt.getRowCount();
        //System.out.println("num of rows: " + String.valueOf(num_rows));

        return (sb.toString());
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
        //long maxTimeStamp = Long.MIN_VALUE;
        String maxTimeStamp = "0";

        for (File logFile : logFiles) {
            String name = logFile.getName();
            String delims = "_|\\.";
            String[] tokens = name.split(delims);

            //System.err.println("Tokens :"+Arrays.toString(tokens));

            if(tokens.length <= 1)
                continue;

            //long fileTimestamp = Long.parseLong(tokens[1]) ;
            String fileTimestamp = (tokens[1]) ;
            if(fileTimestamp.compareTo(maxTimeStamp) > 0){
                maxTimeStamp = fileTimestamp;
                prevFile = latestFile;
                latestFile = logFile;
            }
            else
            {
                prevFile = logFile;
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

    void parseAndApplyCommandLog(File logDir, String hostPrefix, long checkpoint_timestamp) throws NoConnectionsException, IOException, ProcCallException {

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

            // determine if the entry timestamp is less then checkpoint timestamp, if yes, do not deal with it
            if(entry.getTimestamp() < checkpoint_timestamp)
                continue;

            //System.err.println("REDO :: TXN ID :" + entry.getTransactionId().longValue());
            //System.err.println("REDO :: PROC ID :" + entry.getProcedureId());

            Object[] entryParams = entry.getProcedureParams().toArray();

            String procName = cc.getProcedureById(entry.getProcedureId()).fullName();
            Procedure catalog_proc = cc.procedures.getIgnoreCase(procName);

            // here we will see that only normal volt procedures will be re-executed.
            if((catalog_proc.getReadonly() == false) && ((this.weak_recovery==false) || (catalog_proc.getBedefault() == false)))
            {
                // System.out.println("Invoking procedure ::" + procName);

                cresponse = client.callProcedure(procName, entryParams);
                assertEquals(cresponse.getStatus(), Status.OK);

                ctr++;

                // results = cresponse.getResults();
                // assertEquals(results.length, 1);
            }


        }

        System.out.println("################################# Real dealed WAL LOG entries :" + ctr);
    }

    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSStoreLogicalRecovery.class);

        // COMMAND LOG
        builder.setGlobalConfParameter("global.sstore", true);

        builder.setGlobalConfParameter("site.commandlog_enable", true);
        builder.setGlobalConfParameter("site.commandlog_timeout", 1000);
        builder.setGlobalConfParameter("site.commandlog_dir", "./obj/cmdlog");

        builder.setGlobalConfParameter("site.anticache_enable", false);

        RecoveryProjectBuilder project = new RecoveryProjectBuilder();

        project.addAllDefaults();

        VoltServerConfig m_config = null;
        boolean success = false;

        setUpSnapshotDir();

        // CONFIG #1: 2 Local Site with 4 Partitions running on JNI backend
        /*
        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster(projectJAR, NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);
        */


        // CONFIG #2: 1 Local Site with 1 Partitions running on JNI backend
        NUM_SITES = 1;
        NUM_PARTITIONS = 1;
        m_config = new LocalSingleProcessServer(projectJAR, NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);

        return builder;
    }
}
