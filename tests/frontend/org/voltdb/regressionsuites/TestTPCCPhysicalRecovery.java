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
import java.util.Stack;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.benchmark.tpcc.TPCCClient;
import org.voltdb.benchmark.tpcc.TPCCLoader;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

import edu.brown.hstore.HStoreConstants;

/**
 * Test physical recovery
 */
public class TestTPCCPhysicalRecovery extends RegressionSuite {

    //private static final String ARIESDIR = "/mnt/pmfs/aries";    
    private static final String ARIESDIR = "./obj/aries";    
        
    private static int NUM_SITES = -1;    
    private static int NUM_PARTITIONS = -1;    

    // TPCC
    private static final String PREFIX = "tpcc";
    // XXX ARIES logger fsync's a lot - so reduced this to 10 txns
    // Either change it to group commit or use PMFS which has lightweight fsync
    private static int NUM_TRANSACTIONS = 10;
    private static final String projectJAR = "physical_" + PREFIX + ".jar";    

    public TestTPCCPhysicalRecovery(String name) {
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
            //e.printStackTrace();
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
             
    public void initializeTPCCDatabase(final CatalogContext catalogContext, final Client client, boolean scaleItems) throws Exception {
        String args[] = {
            "NOCONNECTIONS=true",
            "BENCHMARK.WAREHOUSE_PER_PARTITION=true",
            "BENCHMARK.NUM_LOADTHREADS=4",
            "BENCHMARK.SCALE_ITEMS="+scaleItems,
        };
        TPCCLoader loader = new TPCCLoader(args) {
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
        
    public void testTPCC() throws IOException, InterruptedException, ProcCallException {
        
        System.out.println("Starting testTPCC - Physical Recovery");                
       
        VoltTable results[] = null;
        ClientResponse cresponse = null;
        Client client = this.getClient();
        CatalogContext cc = this.getCatalogContext();       

        // Load database        
        try{
            initializeTPCCDatabase(cc, client, false);
        }
        catch(Exception e){
            e.printStackTrace();
        }        
        
        final String MOCK_ARGS[] = { "HOST=localhost", "NUMCLIENTS=1",
                // XXX HACK to find catalog jar
                "CATALOG=" + "./obj/release/testobjects/" + projectJAR, "" };

        MOCK_ARGS[MOCK_ARGS.length - 1] = HStoreConstants.BENCHMARK_PARAM_PREFIX;

        TPCCClient tpccClient = new TPCCClient(MOCK_ARGS);

        // Run transactions
        long k_itr = 0;
        long numTransactions = NUM_TRANSACTIONS;
        long period = numTransactions / 10;

        for (k_itr = 0; k_itr < numTransactions; k_itr++) {
            boolean response = tpccClient.runOnce();
            assertEquals(response, true);

            if (k_itr % period == 0)
                System.out.println(String.format("Transactions Processed: %6d / %d", k_itr, numTransactions));
        }            
                
        // Statistics 
        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println("@Statistics before RESTART :");               
        System.out.println(results[0]);        
        
        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        client = getClient();        

        results = client.callProcedure("@Statistics", "table", 0).getResults();
        System.out.println("@Statistics after PHYSICAL restore :");
        System.out.println(results[0]);      
        
    }
    
    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestTPCCPhysicalRecovery.class);
        
        // PHYSICAL
        builder.setGlobalConfParameter("site.aries", true);        
        builder.setGlobalConfParameter("site.aries_forward_only", false);     
        builder.setGlobalConfParameter("site.anticache_enable", false);     
        
        //builder.setGlobalConfParameter("site.aries_dir", "/mnt/pmfs/aries");     
        
        TPCCProjectBuilder project = new TPCCProjectBuilder();

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
        /*
        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster(projectJAR, NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);        
        */
                           
        // CONFIG #2: 1 Local Site with 2 Partitions running on JNI backend          
        NUM_SITES = 1;
        NUM_PARTITIONS = 1;
        m_config = new LocalSingleProcessServer(projectJAR, NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);
        success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);      
                
            
        return builder;
    }
}
