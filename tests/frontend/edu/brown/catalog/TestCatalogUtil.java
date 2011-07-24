package edu.brown.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 *
 */
public class TestCatalogUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 6;
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
    }
    
    /**
     * testGetReadOnlyColumns
     */
    public void testGetReadOnlyColumns() throws Exception {
        // The columns that we know are modified for these tables
        Map<String, Collection<String>> modified = new HashMap<String, Collection<String>>();
        modified.put("WAREHOUSE", CollectionUtil.addAll(new HashSet<String>(), "W_YTD"));
        modified.put("CUSTOMER", CollectionUtil.addAll(new HashSet<String>(), "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA"));
        
        for (String tableName : modified.keySet()) {
            Table catalog_tbl = this.getTable(tableName);
            Collection<String> modifiedCols = modified.get(tableName);
            
            Set<Column> expected = new HashSet<Column>();
            for (Column catalog_col : catalog_tbl.getColumns()) {
                if (modifiedCols.contains(catalog_col.getName()) == false) {
                    expected.add(catalog_col);
                }
            } // FOR (col)
            
            Set<Column> readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, false);
            assertNotNull(readOnly);
            assertEquals(readOnly.toString(), expected.size(), readOnly.size());
            assertEquals(expected, readOnly);
        } // FOR (table)
        
        // We want to also check that excluding INSERT queries works
        Table catalog_tbl = this.getTable("HISTORY");
        Set<Column> all_readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, true);
        assertNotNull(all_readOnly);
        assertEquals(catalog_tbl.getColumns().size(), all_readOnly.size());
        assert(catalog_tbl.getColumns().containsAll(all_readOnly));
        
        Set<Column> noinsert_readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, false);
        assertNotNull(noinsert_readOnly);
        assertEquals(0, noinsert_readOnly.size());
    }
    
    
    /**
     * testGetTables
     */
    public void testGetReferencedTables() throws Exception {
        // Get the SLEV procedure and make sure that we get all of our tables back
        String expected[] = { "DISTRICT", "ORDER_LINE", "STOCK" };
        Procedure catalog_proc = this.getProcedure(slev.class);
        
        Collection<Table> tables = CatalogUtil.getReferencedTables(catalog_proc);
        assertEquals(expected.length, tables.size());
        for (String table_name : expected) {
            Table catalog_tbl = catalog_db.getTables().get(table_name);
            assertNotNull(catalog_tbl);
            assertTrue(tables.contains(catalog_tbl));
        } // FOR
        return;
    }

    /**
     * testGetColumnsSelect
     */
    public void testGetReferencedColumnsSelect() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getDistrict");
        Table catalog_tbl = this.getTable("DISTRICT");
        Column expected[] = {
            this.getColumn(catalog_tbl, "D_ID"),
            this.getColumn(catalog_tbl, "D_W_ID")
        };
        
        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }
    
    /**
     * testGetColumnsInsert
     */
    public void testGetReferencedColumnsInsert() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "createNewOrder");
        Table catalog_tbl = this.getTable("NEW_ORDER");
        Column expected[] = new Column[catalog_tbl.getColumns().size()];
        catalog_tbl.getColumns().toArray(expected);
        
        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }
    
    /**
     * testGetColumnsDelete
     */
    public void testGetReferencedColumnsDelete() throws Exception {
        Procedure catalog_proc = this.getProcedure(delivery.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "deleteNewOrder");
        Table catalog_tbl = this.getTable("NEW_ORDER");
        Column expected[] = {
            this.getColumn(catalog_tbl, "NO_D_ID"),
            this.getColumn(catalog_tbl, "NO_W_ID"),
            this.getColumn(catalog_tbl, "NO_O_ID")
        };
        
        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }

    /**
     * testGetColumnsUpdate
     */
    public void testGetReferencedColumnsUpdate() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "incrementNextOrderId");
        Table catalog_tbl = this.getTable("DISTRICT");
        Column expected[] = {
            this.getColumn(catalog_tbl, "D_NEXT_O_ID"),
            this.getColumn(catalog_tbl, "D_ID"),
            this.getColumn(catalog_tbl, "D_W_ID")
        };
        
        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
//        AbstractPlanNode node = QueryPlanUtil.deserializeStatement(catalog_stmt, true);
//        System.err.println(PlanNodeUtil.debug(node));
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert(columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }
    
    /**
     * testGetProcedures
     */
    public void testGetProcedures() throws Exception {
        Procedure expected[] = {
            this.getProcedure(delivery.class),
            this.getProcedure(neworder.class),
            this.getProcedure(ResetWarehouse.class),
        };
        Table catalog_tbl = this.getTable("NEW_ORDER");
        Collection<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_tbl);
        assertNotNull(procedures);
        assertEquals(procedures.toString(), expected.length, procedures.size());
        for (int i = 0; i < expected.length; i++) {
            assert(procedures.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }
    
    /**
     * testGetProceduresReplicatedColumn
     */
    public void testGetProceduresReplicatedColumn() throws Exception {
        Procedure expected[] = {
            this.getProcedure(delivery.class),
            this.getProcedure(neworder.class),
            this.getProcedure(ResetWarehouse.class),
        };
        Table catalog_tbl = this.getTable("NEW_ORDER");
        Column catalog_col = ReplicatedColumn.get(catalog_tbl);
        
        Collection<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_col);
        assertNotNull(procedures);
        assertEquals(procedures.toString(), expected.length, procedures.size());
        for (int i = 0; i < expected.length; i++) {
            assert(procedures.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }
    
    
    /**
     * testGetLocalPartitions
     */
    public void testGetLocalPartitions() throws Exception {
        //
        // Add in some fake partitions
        //
        Cluster cluster = CatalogUtil.getCluster(catalog);
        assertNotNull(cluster);
        Map<Host, Set<Partition>> host_partitions = new HashMap<Host, Set<Partition>>();
        
        for (Host catalog_host : cluster.getHosts()) {
            host_partitions.put(catalog_host, new HashSet<Partition>());
            for (Site catalog_site : CatalogUtil.getSitesForHost(catalog_host)) {
                for (Partition catalog_part : catalog_site.getPartitions()) {
                    host_partitions.get(catalog_host).add(catalog_part);    
                } // FOR
            } // FOR
        } // FOR
        
        for (Host catalog_host : host_partitions.keySet()) {
            Set<Partition> partitions = host_partitions.get(catalog_host);
            Partition catalog_part = CollectionUtil.getFirst(partitions);
            int base_partition = catalog_part.getId();
            Set<Partition> local_partitions = CatalogUtil.getLocalPartitions(catalog_db, base_partition);
            assertEquals(partitions.size(), local_partitions.size());
            
            for (Partition other_part : local_partitions) {
                assertNotNull(other_part);
//                assertFalse(catalog_part.equals(other_part));
                assertTrue(host_partitions.get(catalog_host).contains(other_part));
            } // FOR
        } // FOR
    }
    
    /**
     * testGetSitesForHost
     */
    public void testGetSitesForHost() throws Exception {
        Map<Host, List<Site>> host_sites = new HashMap<Host, List<Site>>();
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        
        for (Site catalog_site : catalog_clus.getSites()) {
            Host catalog_host = catalog_site.getHost();
            assertNotNull(catalog_host);
            if (!host_sites.containsKey(catalog_host)) {
                host_sites.put(catalog_host, new ArrayList<Site>());
            }
            host_sites.get(catalog_host).add(catalog_site);
        } // FOR
        
        for (Host catalog_host : catalog_clus.getHosts()) {
            List<Site> sites = CatalogUtil.getSitesForHost(catalog_host);
            List<Site> expected = host_sites.get(catalog_host);
            assertEquals(expected.size(), sites.size());
            assert(sites.containsAll(expected));
        } // FOR
    }
    
    /**
     * testGetCluster
     */
    public void testGetCluster() {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        assertNotNull(catalog_clus);
        
        Set<Cluster> clusters = new HashSet<Cluster>();
        for (Cluster c : catalog.getClusters()) {
            clusters.add(c);
        } // FOR
        assertEquals(1, clusters.size());
        assertEquals(catalog_clus, CollectionUtil.getFirst(clusters));
    }
    
    /**
     * testGetDatabase
     */
    public void testGetDatabase() {
        Database db0 = CatalogUtil.getDatabase(catalog);
        assertNotNull(db0);
        
        Set<Database> dbs = new HashSet<Database>();
        for (Database db1 : CatalogUtil.getCluster(catalog).getDatabases()) {
            dbs.add(db1);
        } // FOR
        assertEquals(1, dbs.size());
        assertEquals(db0, CollectionUtil.getFirst(dbs));
    }
    
    /**
     * testCatalogMapGet
     */
    public void testCatalogMapGet() {
        Table catalog_tbl = this.getTable("ORDER_LINE");
        CatalogMap<Column> catalog_cols = catalog_tbl.getColumns();
        List<Column> sorted_cols = org.voltdb.utils.CatalogUtil.getSortedCatalogItems(catalog_cols, "index");
        assertEquals(catalog_cols.size(), sorted_cols.size());
        for (int i = 0, cnt = catalog_cols.size(); i < cnt; i++) {
            Column expected = sorted_cols.get(i);
            Column actual = catalog_cols.get(i);
            assertEquals("Failed get(" + i + "): " + expected, expected, actual);
        } // FOR
    }
    
//    public static void main(String[] args) throws Exception {
//        TestCatalogUtil tc = new TestCatalogUtil();
//        tc.setUp();
//        tc.testGetLocalPartitions();
//        javax.swing.SwingUtilities.invokeLater(new Runnable() {
//            public void run() {
//                new CatalogViewer(catalog, "fake").setVisible(true);
//                System.out.println("???");
//            } // RUN
//         });
//    }
}
