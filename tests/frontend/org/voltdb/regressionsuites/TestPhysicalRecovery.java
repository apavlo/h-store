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

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Stack;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;

import edu.brown.benchmark.ycsb.YCSBConstants;
import edu.brown.benchmark.ycsb.YCSBLoader;
import edu.brown.benchmark.ycsb.YCSBProjectBuilder;
import edu.brown.benchmark.ycsb.YCSBUtil;
import edu.brown.benchmark.ycsb.procedures.DeleteRecord;
import edu.brown.benchmark.ycsb.procedures.InsertRecord;
import edu.brown.benchmark.ycsb.procedures.ReadRecord;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.cmdlog.CommandLogReader;
import edu.brown.hstore.cmdlog.CommandLogWriter;
import edu.brown.hstore.cmdlog.LogEntry;

/**
 * Test physical recovery
 */
public class TestPhysicalRecovery extends RegressionSuite {

    private static final String ARIESDIR = "./obj/aries";    
        
    private static int NUM_SITES = -1;    
    private static int NUM_PARTITIONS = -1;    

    // YCSB
    private static final String PREFIX = "ycsb";
    private static final int NUM_TUPLES = 100000;

    public TestPhysicalRecovery(String name) {
        super(name);
    }

    @Override
    public void setUp() {
        super.setUp();
    }

    @Override
    public void tearDown() {
        try {
            super.tearDown();
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    // Recursively delete all files in a dir 
    // XXX Java 7 has direct support
    public static void removeRecursive(File startDir) throws IOException {
        File dir = startDir;
        File[] currList;

        Stack<File> stack = new Stack<File>();
        stack.push(dir);

        while (!stack.isEmpty()) {
            if (stack.lastElement().isDirectory()) {
                currList = stack.lastElement().listFiles();
                if (currList == null) {
                    stack.pop();
                    continue;
                }

                if (currList.length > 0) {
                    for (File curr : currList) {
                        stack.push(curr);
                    }
                } else {
                    System.out.println("Deleting file "+stack.lastElement().getAbsolutePath());
                    stack.pop().delete();
                }
            } else {
                System.out.println("Deleting file "+stack.lastElement().getAbsolutePath());
                stack.pop().delete();
            }
        }
    }
   
          
    private void initializeDatabase(final Client client, final int num_tuples) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.FIXED_SIZE=true",
            "BENCHMARK.NUM_RECORDS="+num_tuples,
            "BENCHMARK.LOADTHREADS=4",
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
        
    public void testYCSB() throws IOException, InterruptedException, ProcCallException {
        
        System.out.println("Starting testYCSB - Physical Recovery");                
       
        VoltTable results[] = null;
        ClientResponse cresponse = null;
        Client client = this.getClient();
        
        try{
            this.initializeDatabase(client, NUM_TUPLES);
        }
        catch(Exception e){
            e.printStackTrace();
        }        
                
        // Read Record
        
        long key = NUM_TUPLES/2;
        String procName = ReadRecord.class.getSimpleName();
        Object params[] ;
        params = new Object[]{ key };
        
        cresponse = client.callProcedure(procName, params);
        assertNotNull(cresponse);
        assertEquals(Status.OK, cresponse.getStatus());
        assertEquals(1, cresponse.getResults().length);
        
        VoltTable vt = cresponse.getResults()[0];
        boolean adv = vt.advanceRow();
        assert(adv);
        assertEquals(key, vt.getLong(0));
        
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println(results[0]);
        
        // Delete, then Insert these many tuples back
        int numTestTuples = NUM_TUPLES/4;
        
        for (long k_itr = 0; k_itr < numTestTuples; k_itr++) {
            procName = DeleteRecord.class.getSimpleName();
            key = k_itr;
            params =  new Object[]{ key };
            
            cresponse = client.callProcedure(procName, params);
            assertEquals(Status.OK, cresponse.getStatus());
            results = cresponse.getResults();

            assertEquals(1, results.length);
            assertNotNull(cresponse);
        }

        System.out.println("Delete Record Test Passed");

        for (long k_itr = 0; k_itr < numTestTuples; k_itr++) {
            procName = InsertRecord.class.getSimpleName();
            key = k_itr;
            String fields[] = new String[YCSBConstants.NUM_COLUMNS];
            for (int i = 0; i < fields.length; i++) {
                fields[i] = YCSBUtil.astring(YCSBConstants.COLUMN_LENGTH, YCSBConstants.COLUMN_LENGTH);
            } // FOR
            params = new Object[]{ key, fields };
                        
            cresponse = client.callProcedure(procName, params);
            assertEquals(Status.OK, cresponse.getStatus());
            results = cresponse.getResults();

            assertEquals(1, results.length);
            assertNotNull(cresponse);
        }

        System.out.println("Insert Record Test Passed");

        VoltTable[] results_tmp = null;
        results_tmp = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics before RESTART :");
        System.out.println(results_tmp[0]);      
        
        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        client = getClient();        
        
        results_tmp = null;
        results_tmp = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics after PHYSICAL restore :");
        System.out.println(results_tmp[0]);      
        
        //checkYCSBTable(client, NUM_TUPLES);              

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
            // System.out.println("Results for procedure ::" + procName + " " +
            // results[0]);
            
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
            else
                System.err.println("key :"+key_itr+" result:"+vt.getLong(0));

            //assert(adv);
            //assertEquals(key_itr, vt.getLong(0));            
        }                               
        
        System.out.println("checkYCSBTable Passed");
    }    
    

    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestPhysicalRecovery.class);
        
        // PHYSICAL
        builder.setGlobalConfParameter("site.aries", true);        
        builder.setGlobalConfParameter("site.aries_forward_only", false);     
                   
        YCSBProjectBuilder project = new YCSBProjectBuilder();

        project.addAllDefaults();

        VoltServerConfig m_config = null;        
        boolean success = false;

        // ARIES
        File aries_dir = new File(ARIESDIR);
        try {
            removeRecursive(aries_dir);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        
        // CONFIG #1: 2 Local Site with 4 Partitions running on JNI backend
        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster("aries-"+PREFIX+"-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);
        
        
        /*
        // CONFIG #2: 1 Local Site with 1 Partitions running on JNI backend
        NUM_SITES = 1;
        NUM_PARTITIONS = 2;        
        m_config = new LocalSingleProcessServer("aries-"+PREFIX+"-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);
        */
        
        return builder;
    }
}
