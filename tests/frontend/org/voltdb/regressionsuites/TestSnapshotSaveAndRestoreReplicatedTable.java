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
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
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
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSnapshotSaveAndRestoreReplicatedTable extends RegressionSuite {

    private static final String TMPDIR = "./snapshot";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;
    
    private static int NUM_SITES = -1;    
    private static int NUM_PARTITIONS = -1;    

    public TestSnapshotSaveAndRestoreReplicatedTable(String name) {
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
    
    private VoltTable createReplicatedTable(int numberOfItems, int indexBase, StringBuilder sb) {
        return createReplicatedTable(numberOfItems, indexBase, sb, false);
    }

    private VoltTable createReplicatedTable(int numberOfItems, int indexBase, StringBuilder sb, boolean generateCSV) {
        VoltTable repl_table = new VoltTable(new ColumnInfo("RT_ID", VoltType.INTEGER), new ColumnInfo("RT_NAME", VoltType.STRING), new ColumnInfo("RT_INTVAL", VoltType.INTEGER), new ColumnInfo(
                "RT_FLOATVAL", VoltType.FLOAT));
        char delimeter = generateCSV ? ',' : '\t';
        for (int i = indexBase; i < numberOfItems + indexBase; i++) {
            String stringVal = null;
            String escapedVal = null;

            if (sb != null) {
                if (generateCSV) {
                    int escapable = i % 5;
                    switch (escapable) {
                        case 0:
                            stringVal = "name_" + i;
                            escapedVal = "name_" + i;
                            break;
                        case 1:
                            stringVal = "na,me_" + i;
                            escapedVal = "\"na,me_" + i + "\"";
                            break;
                        case 2:
                            stringVal = "na\"me_" + i;
                            escapedVal = "\"na\"\"me_" + i + "\"";
                            break;
                        case 3:
                            stringVal = "na\rme_" + i;
                            escapedVal = "\"na\rme_" + i + "\"";
                            break;
                        case 4:
                            stringVal = "na\nme_" + i;
                            escapedVal = "\"na\nme_" + i + "\"";
                            break;
                    }
                } else {
                    int escapable = i % 5;
                    switch (escapable) {
                        case 0:
                            stringVal = "name_" + i;
                            escapedVal = "name_" + i;
                            break;
                        case 1:
                            stringVal = "na\tme_" + i;
                            escapedVal = "na\\tme_" + i;
                            break;
                        case 2:
                            stringVal = "na\nme_" + i;
                            escapedVal = "na\\nme_" + i;
                            break;
                        case 3:
                            stringVal = "na\rme_" + i;
                            escapedVal = "na\\rme_" + i;
                            break;
                        case 4:
                            stringVal = "na\\me_" + i;
                            escapedVal = "na\\\\me_" + i;
                            break;
                    }
                }
            } else {
                stringVal = "name_" + i;
            }

            Object[] row = new Object[] { i, stringVal, i, new Double(i) };
            if (sb != null) {
                sb.append(i).append(delimeter).append(escapedVal).append(delimeter);
                sb.append(i).append(delimeter).append(new Double(i).toString()).append('\n');
            }
            repl_table.addRow(row);
        }
        return repl_table;
    }

    private VoltTable createPartitionedTable(int numberOfItems, int indexBase) {
        VoltTable partition_table = new VoltTable(new ColumnInfo("PT_ID", VoltType.INTEGER), new ColumnInfo("PT_NAME", VoltType.STRING), new ColumnInfo("PT_INTVAL", VoltType.INTEGER), new ColumnInfo(
                "PT_FLOATVAL", VoltType.FLOAT));

        for (int i = indexBase; i < numberOfItems + indexBase; i++) {
            Object[] row = new Object[] { i, "name_" + i, i, new Double(i) };
            partition_table.addRow(row);
        }

        return partition_table;
    }

    private VoltTable[] loadTable(Client client, String tableName, VoltTable table) {
        VoltTable[] results = null;
        int allowExport = 0;
        try {
            client.callProcedure("@LoadMultipartitionTable", tableName, table);
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }

    private void loadLargeReplicatedTable(Client client, String tableName, int itemsPerChunk, int numChunks) {
        loadLargeReplicatedTable(client, tableName, itemsPerChunk, numChunks, false, null);
    }

    private void loadLargeReplicatedTable(Client client, String tableName, int itemsPerChunk, int numChunks, boolean generateCSV, StringBuilder sb) {
        for (int i = 0; i < numChunks; i++) {
            VoltTable repl_table = createReplicatedTable(itemsPerChunk, i * itemsPerChunk, sb, generateCSV);
            loadTable(client, tableName, repl_table);
        }
        if (sb != null) {
            sb.trimToSize();
        }
    }

    private void loadLargePartitionedTable(Client client, String tableName, int itemsPerChunk, int numChunks) {
        for (int i = 0; i < numChunks; i++) {
            VoltTable part_table = createPartitionedTable(itemsPerChunk, i * itemsPerChunk);
            loadTable(client, tableName, part_table);
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

    private void checkTable(Client client, String tableName, String orderByCol, int expectedRows) {
        if (expectedRows > 200000) {
            System.out.println("Table too large to retrieve with select *");
            System.out.println("Skipping integrity check");
        }
        VoltTable result = null;
        try {
            result = client.callProcedure("SaveRestoreSelect", tableName).getResults()[0];

        } catch (Exception e) {
            e.printStackTrace();
        }
        final int rowCount = result.getRowCount();
        assertEquals(expectedRows, rowCount);

        // System.out.println("Check table :: \n"+result);

        int i = 0;
        while (result.advanceRow()) {
            assertEquals(i, result.getLong(0));
            assertEquals("name_" + i, result.getString(1));
            assertEquals(i, result.getLong(2));
            assertEquals(new Double(i), result.getDouble(3));
            ++i;
        }
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

            System.err.println("Validate Snapshot :"+reportString);

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

   
    public void testSaveAndRestoreReplicatedTable() throws IOException, InterruptedException, ProcCallException {
        
        System.out.println("Starting testSaveAndRestoreReplicatedTable");
        
        deleteTestFiles();
        setUpSnapshotDir();
        
        int num_replicated_items_per_chunk = 3;
        int num_replicated_chunks = 2;

        Client client = getClient();
        loadLargeReplicatedTable(client, "REPLICATED_TESTER", num_replicated_items_per_chunk, num_replicated_chunks);

        VoltTable[] results = null;
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

        checkTable(client, "REPLICATED_TESTER", "RT_ID", num_replicated_items_per_chunk * num_replicated_chunks);

        int foundItem = 0;
        while (results[0].advanceRow()) {
            if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER")) {
                ++foundItem;
                assertEquals((num_replicated_chunks * num_replicated_items_per_chunk), results[0].getLong("TUPLE_COUNT"));
            }
        }

        // make sure all sites were loaded 
        //assertEquals(3, foundItem);
        validateSnapshot(true);
    }              
    
    /**
     * Build a list of the tests to be run. Use the regression suite helpers to
     * allow multiple back ends. JUnit magic that uses the regression suite
     * helper classes.
     */
    static public Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSnapshotSaveAndRestoreReplicatedTable.class);
                
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder("snapshot-VoltDB-project");
        
        project.addAllDefaults();

        VoltServerConfig m_config = null;        
        setUpSnapshotDir();

        NUM_SITES = 2;
        NUM_PARTITIONS = 2;
        m_config = new LocalCluster("snapshot-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_SITES, NUM_PARTITIONS, 1, BackendTarget.NATIVE_EE_JNI);
        
        //NUM_SITES = 1;
        //NUM_PARTITIONS = 2;        
        //m_config = new LocalSingleProcessServer("snapshot-"+NUM_SITES+"-site-"+NUM_PARTITIONS+"-partition.jar", NUM_PARTITIONS, BackendTarget.NATIVE_EE_JNI);

        boolean success = m_config.compile(project);
        assert (success);
        builder.addServerConfig(m_config);

        return builder;
    }
}
