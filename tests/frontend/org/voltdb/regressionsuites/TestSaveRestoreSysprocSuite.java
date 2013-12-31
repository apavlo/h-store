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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.sysprocs.saverestore.SaveRestoreTestProjectBuilder;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSysprocSuite extends RegressionSuite {

    private static final String TMPDIR = "/home/parallels/git/h-store/snapshot";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWEXPORT = 0;

    public TestSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void setUp()
    {
        deleteTestFiles();
        super.setUp();
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Override
    public void tearDown() 
    {	
	try{
	  deleteTestFiles();
	  super.tearDown();
	}
	catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteTestFiles()
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) ||
                file.endsWith(".vpt") ||
                file.endsWith(".digest") ||
                file.endsWith(".tsv") ||
                file.endsWith(".csv");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            tmp_file.delete();
        }
    }
    
    private VoltTable createPartitionedTable(int numberOfItems,
                                             int indexBase)
    {
        VoltTable partition_table =
                new VoltTable(new ColumnInfo("PT_ID", VoltType.INTEGER),
                              new ColumnInfo("PT_NAME", VoltType.STRING),
                              new ColumnInfo("PT_INTVAL", VoltType.INTEGER),
                              new ColumnInfo("PT_FLOATVAL", VoltType.FLOAT));

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            Object[] row = new Object[] {i,
                                         "name_" + i,
                                         i,
                                         new Double(i)};
            partition_table.addRow(row);
        }
        return partition_table;
    }

    private VoltTable[] loadTable(Client client, String tableName,
                                  VoltTable table)
    {
        VoltTable[] results = null;
        int allowExport = 0;
        try
        {
            client.callProcedure("@LoadMultipartitionTable", tableName,
                                 table);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }

    private void loadLargePartitionedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable part_table =
                createPartitionedTable(itemsPerChunk, i * itemsPerChunk);
            loadTable(client, tableName, part_table);
        }
    }

    private VoltTable[] saveTables(Client client)
    {
        VoltTable[] results = null;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
                                           TESTNONCE,
                                           (byte)1).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }

    private void checkTable(Client client, String tableName, String orderByCol,
                            int expectedRows)
    {
        if (expectedRows > 200000)
        {
            System.out.println("Table too large to retrieve with select *");
            System.out.println("Skipping integrity check");
        }
        VoltTable result = null;
        try
        {
            result = client.callProcedure("SaveRestoreSelect", tableName).getResults()[0];
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        final int rowCount = result.getRowCount();
        assertEquals(expectedRows, rowCount);

        int i = 0;
        while (result.advanceRow())
        {
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
            String args[] = new String[] {
                    TESTNONCE,
                    "--dir",
                    TMPDIR
            };
            SnapshotVerifier.main(args);
            ps.flush();
            String reportString = baos.toString("UTF-8");

            if (expectSuccess) {
                assertTrue(reportString.startsWith("Snapshot valid\n"));
            } else {
                assertTrue(reportString.startsWith("Snapshot corrupted\n"));
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
    }
    
    /*
     * Also does some basic smoke tests
     * of @SnapshotStatus, @SnapshotScan and @SnapshotDelete
     */
    public void testSnapshotSave() throws Exception
    {
        System.out.println("Starting testSnapshotSave");
        Client client = getClient();

        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        VoltTable[] results = null;

        results = client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1).getResults();

        //validateSnapshot(true);

        /*
        // Check that snapshot status returns a reasonable result
        
        VoltTable statusResults[] = client.callProcedure("@SnapshotStatus").getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);
        assertEquals( 13, statusResults[0].getColumnCount());
        assertTrue(statusResults[0].advanceRow());
        assertTrue(TMPDIR.equals(statusResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(statusResults[0].getString("NONCE")));
        assertFalse( 0 == statusResults[0].getLong("END_TIME"));
        assertTrue("SUCCESS".equals(statusResults[0].getString("RESULT")));

        VoltTable scanResults[] = client.callProcedure("@SnapshotScan", new Object[] { null }).getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults.length);
        assertEquals( 1, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue( "ERR_MSG".equals(scanResults[0].getColumnName(0)));

        scanResults = client.callProcedure("@SnapshotScan", "/doesntexist").getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults[1].getRowCount());
        assertTrue( scanResults[1].advanceRow());
        assertTrue( "FAILURE".equals(scanResults[1].getString("RESULT")));

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        
        // We can't assert that all snapshot files are generated by this test.
        // There might be leftover snapshot files from other runs.
         
        int count = 0;
        String completeStatus = null;
        do {
            if (TESTNONCE.equals(scanResults[0].getString("NONCE"))) {
                assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
                count++;
                completeStatus = scanResults[0].getString("COMPLETE");
            }
        } while (scanResults[0].advanceRow());
        assertEquals(1, count);
        assertNotNull(completeStatus);
        assertTrue("TRUE".equals(completeStatus));

        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) && file.endsWith("vpt");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        tmp_files[0].delete();

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        count = 0;
        String missingTableName = null;
        do {
            if (TESTNONCE.equals(scanResults[0].getString("NONCE"))
                && "FALSE".equals(scanResults[0].getString("COMPLETE"))) {
                assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
                count++;
                missingTableName = scanResults[0].getString("TABLES_MISSING");
            }
        } while (scanResults[0].advanceRow());
        assertEquals(1, count);
        assertNotNull(missingTableName);
        assertTrue(tmp_files[0].getName().contains(missingTableName));

        // Instead of something exhaustive, let's just make sure that we get
        // the number of result rows corresponding to the number of ExecutionSites
        // that did save work
        Cluster cluster = VoltDB.instance().getCatalogContext().cluster;
        Database database = cluster.getDatabases().get("database");
        CatalogMap<Table> tables = database.getTables();
        CatalogMap<Site> sites = cluster.getSites();
        int num_hosts = cluster.getHosts().size();
        int replicated = 0;
        int total_tables = 0;
        int expected_entries = 0;

        for (Table table : tables)
        {
            // Ignore materialized tables
            if (table.getMaterializer() == null)
            {
                total_tables++;
                if (table.getIsreplicated())
                {
                    replicated++;
                }
            }
        }

        for (Site s : sites) {
            if (s.getIsexec()) {
                expected_entries++;
            }
        }
        assertEquals(expected_entries, results[0].getRowCount());

        while (results[0].advanceRow())
        {
            assertEquals(results[0].getString("RESULT"), "SUCCESS");
        }

        // Now, try the save again and verify that we fail (since all the save
        // files will still exist. This will return one entry per table
        // per host
        expected_entries =
            ((total_tables - replicated) * num_hosts) + replicated;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
                                           TESTNONCE, (byte)1).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        assertEquals(expected_entries, results[0].getRowCount());
        while (results[0].advanceRow())
        {
            if (!tmp_files[0].getName().contains(results[0].getString("TABLE"))) {
                assertEquals(results[0].getString("RESULT"), "FAILURE");
                assertTrue(results[0].getString("ERR_MSG").contains("SAVE FILE ALREADY EXISTS"));
            }
        }

        VoltTable deleteResults[] =
            client.callProcedure(
                "@SnapshotDelete",
                new String[] {TMPDIR},
                new String[]{TESTNONCE}).getResults();
        assertNotNull(deleteResults);
        assertEquals( 1, deleteResults.length);
        assertEquals( 9, deleteResults[0].getColumnCount());
        assertEquals( 8, deleteResults[0].getRowCount());
        tmp_files = tmp_dir.listFiles(cleaner);
        assertEquals( 0, tmp_files.length);

        validateSnapshot(false);
        */
    }

    private void generateAndValidateTextFile(StringBuilder expectedText, boolean csv) throws Exception {
        String args[] = new String[] {
                TESTNONCE,
               "--dir",
               TMPDIR,
               "--table",
               "REPLICATED_TESTER",
               "--type",
               csv ? "CSV" : "TSV",
               "--outdir",
               TMPDIR
        };
        //SnapshotConverter.main(args);
        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + "REPLICATED_TESTER" + (csv ? ".csv" : ".tsv"));
        try {
            int filesize = (int)fis.getChannel().size();
            ByteBuffer expectedBytes = ByteBuffer.wrap(expectedText.toString().getBytes("UTF-8"));
            ByteBuffer readBytes = ByteBuffer.allocate(filesize);
            while (readBytes.hasRemaining()) {
                int read = fis.getChannel().read(readBytes);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            // this throws an exception on failure
            new String(readBytes.array(), "UTF-8");

            readBytes.flip();
            assertTrue(expectedBytes.equals(readBytes));
        } finally {
            fis.close();
        }
    }

    /*
    public void testSaveAndRestorePartitionedTable()
    throws IOException, InterruptedException, ProcCallException
    {
        System.out.println("Starting testSaveAndRestorePartitionedTable");
        int num_partitioned_items_per_chunk = 120; // divisible by 3
        int num_partitioned_chunks = 10;
        Client client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);
        VoltTable[] results = null;

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = true;
        results = saveTables(client);
        deleteTestFiles();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("FAILURE"));
        }

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;

        validateSnapshot(false);

        results = saveTables(client);

        validateSnapshot(true);

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            results = client.callProcedure("@SnapshotStatus").getResults();
            assertTrue(results[0].advanceRow());
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
            assertEquals( 7, results[0].getRowCount());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE, ALLOWEXPORT).getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TABLE_ACTIVE_TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        deleteTestFiles();

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = true;

        org.voltdb.sysprocs.SnapshotRegistry.clear();
        client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        results = saveTables(client);

        validateSnapshot(false);

        try
        {
            results = client.callProcedure("@SnapshotStatus").getResults();
            boolean hasFailure = false;
            while (results[0].advanceRow())
                hasFailure |= results[0].getString("RESULT").equals("FAILURE");
            assertTrue(hasFailure);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        deleteTestFiles();
        results = saveTables(client);

        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE, ALLOWEXPORT).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TABLE_ACTIVE_TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);
    }
    */
    
    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSaveRestoreSysprocSuite.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder("snapshot-VoltDB-project");
           
        System.out.println("SR URL :"+project.ddlFile);   
        project.addAllDefaults();

        config = new LocalSingleProcessServer("snapshot-1-sites.jar", 1,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
