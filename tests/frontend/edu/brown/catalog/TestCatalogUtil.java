package edu.brown.catalog;

import java.lang.reflect.*;
import java.util.*;

import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 *
 */
public class TestCatalogUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 6;
    
    /**
     * ClassName.FieldName
     */
    protected final Set<String> CHECK_FIELDS_EXCLUDE = new HashSet<String>();
    {
        CHECK_FIELDS_EXCLUDE.add("Cluster.hosts");
        CHECK_FIELDS_EXCLUDE.add("Cluster.sites");
        CHECK_FIELDS_EXCLUDE.add("Cluster.partitions");
        CHECK_FIELDS_EXCLUDE.add("Database.users");
        CHECK_FIELDS_EXCLUDE.add("Database.groups");
        CHECK_FIELDS_EXCLUDE.add("Procedure.procparameter");
        CHECK_FIELDS_EXCLUDE.add("Procedure.partitioncolumn");
        // CHECK_FIELDS_EXCLUDE.add("Statement.fullplan");
        // CHECK_FIELDS_EXCLUDE.add("PlanFragment.plannodetree");
    };
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
    }
    
    /**
     * Recursively walkthrough the catalog and make sure that all the fields match between the two items 
     * @param <T>
     * @param base_class
     * @param item0
     * @param item1
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    protected <T extends CatalogType> void checkFields(Class<T> base_class, CatalogType item0, CatalogType item1) throws Exception {
        String base_name = base_class.getSimpleName();
        boolean debug = false;
        
        assertNotNull("Null item for " + base_name, item0);
        assertNotNull("Null item for " + base_name, item1);
        
        if (debug) System.err.println(CatalogUtil.getDisplayName(item0, true));
        for (Method method_handle : base_class.getDeclaredMethods()) {
            String method_name = method_handle.getName();
            if (!method_name.startsWith("get")) continue;
            String field_name = base_name + "." + method_name.substring(3).toLowerCase();
            if (debug) System.err.print("  " + field_name + ": ");
            if (CHECK_FIELDS_EXCLUDE.contains(field_name)) {
                if (debug) System.err.println("SKIP");
                continue;
            }
            
            Object field_val0 = method_handle.invoke(item0);
            Object field_val1 = method_handle.invoke(item1);
            
            // CatalogMap
            if (field_val0 instanceof CatalogMap) {
                assert(field_val1 instanceof CatalogMap);
                
                CatalogMap map0 = (CatalogMap)field_val0;
                CatalogMap map1 = (CatalogMap)field_val1;
                if (debug) System.err.println(CatalogUtil.debug(map0)  + " <-> " + CatalogUtil.debug(map1));
                assertEquals("Mismatched CatalogMap sizes for " + field_name, map0.size(), map1.size());
                
                Class<? extends CatalogType> generic_class = (Class<? extends CatalogType>)map0.getGenericClass();
                assert(generic_class != null);
                
                CatalogType map0_values[] = map0.values(); 
                CatalogType map1_values[] = map0.values();
                for (int i = 0, cnt = map0.size(); i < cnt; i++) {
                    CatalogType child0 = map0_values[i];
                    assertNotNull("Null child element at index " + i + " for " + field_name, child0);
                    CatalogType child1 = map1_values[i];
                    assertNotNull("Null child element at index " + i + " for " + field_name, child1);
                    this.checkFields(generic_class, child0, child1);
                } // FOR
            // ConstraintRefs
            } else if (field_val0 instanceof ConstraintRef) {
                ConstraintRef ref0 = (ConstraintRef)field_val0;
                ConstraintRef ref1 = (ConstraintRef)field_val1;
                if (debug) System.err.println(CatalogUtil.getDisplayName(ref0) + " <-> " + CatalogUtil.getDisplayName(ref1));
                this.checkFields(Column.class, ref0.getConstraint(), ref1.getConstraint());
            // ColumnRefs
            } else if (field_val0 instanceof ColumnRef) {
                ColumnRef ref0 = (ColumnRef)field_val0;
                ColumnRef ref1 = (ColumnRef)field_val1;
                if (debug) System.err.println(CatalogUtil.getDisplayName(ref0) + " <-> " + CatalogUtil.getDisplayName(ref1));
                this.checkFields(Column.class, ref0.getColumn(), ref1.getColumn());
            // CatalogMap
            } else if (field_val0 != null && ClassUtil.getSuperClasses(field_val0.getClass()).contains(CatalogType.class)) {
                CatalogType type0 = (CatalogType)field_val0;
                CatalogType type1 = (CatalogType)field_val1;
                if (debug) System.err.println(CatalogUtil.getDisplayName(type0) + " <-> " + CatalogUtil.getDisplayName(type1));
                assertEquals("Mismatched values for " + field_name, type0.getName(), type1.getName());
            // Scalar
            } else { 
                if (debug) System.err.println(field_val0  + " <-> " + field_val1);
                assertEquals("Mismatched values for " + field_name, field_val0, field_val1);
            }
        } // FOR (field)
    }
    
    /**
     * testCloneDatabase
     */
    public void testCloneDatabase() throws Exception {
        assertEquals(NUM_PARTITIONS, CatalogUtil.getNumberOfPartitions(catalog_db));
        Collection<Integer> all_partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        assertNotNull(all_partitions);
        
        // We want to make sure that it clones MultiProcParameters too!
        Procedure target_proc = this.getProcedure(neworder.class);
        List<MultiProcParameter> expected = new ArrayList<MultiProcParameter>();
        int cnt = 3;
        for (int i = 0; i < cnt; i++) {
            ProcParameter col0 = target_proc.getParameters().get(i);
            for (int ii = i+1; ii < cnt; ii++) {
                ProcParameter col1 = target_proc.getParameters().get(ii);
                MultiProcParameter mpp = MultiProcParameter.get(col0, col1);
                assertNotNull(mpp);
                expected.add(mpp);
            } // FOR
        } // FOR
        assertFalse(expected.isEmpty());
        
        Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
        assertNotNull(clone_db);

        Cluster catalog_cluster = CatalogUtil.getCluster(catalog_db);
        assertNotNull(catalog_cluster);
        Cluster clone_cluster = CatalogUtil.getCluster(clone_db);
        assertNotNull(clone_cluster);
        for (Host catalog_host : catalog_cluster.getHosts()) {
            Host clone_host = clone_cluster.getHosts().get(catalog_host.getName());
            assertNotNull(clone_host);
            checkFields(Host.class, catalog_host, clone_host);
        } // FOR
        
        for (Site catalog_site : catalog_cluster.getSites()) {
            Site clone_site = clone_cluster.getSites().get(catalog_site.getName());
            assertNotNull(clone_site);
            checkFields(Site.class, catalog_site, clone_site);
        }
        
        assertEquals(NUM_PARTITIONS, CatalogUtil.getNumberOfPartitions(clone_db));
        Collection<Integer> clone_partitions = CatalogUtil.getAllPartitionIds(clone_db);
        assertNotNull(clone_partitions);
        assertEquals(all_partitions, clone_partitions);
        
        for (Table catalog_tbl : catalog_db.getTables()) {
            Table clone_tbl = clone_db.getTables().get(catalog_tbl.getName());
            assertNotNull(catalog_tbl.toString(), clone_tbl);
            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column clone_col = clone_tbl.getColumns().get(catalog_col.getName());
                assertNotNull(CatalogUtil.getDisplayName(catalog_col), clone_col);
                assertEquals(CatalogUtil.getDisplayName(clone_col), clone_tbl, (Table)clone_col.getParent());
                assertEquals(CatalogUtil.getDisplayName(clone_col), clone_tbl.hashCode(), clone_col.getParent().hashCode());
            } // FOR
        } // FOR
        
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            Procedure clone_proc = clone_db.getProcedures().get(catalog_proc.getName());
            assertNotNull(catalog_proc.toString(), clone_proc);
            assertEquals(clone_proc.getParameters().toString(), catalog_proc.getParameters().size(), clone_proc.getParameters().size());
            
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                ProcParameter clone_param = clone_proc.getParameters().get(catalog_param.getIndex());
                assertNotNull(CatalogUtil.getDisplayName(catalog_param), clone_param);
                assertEquals(CatalogUtil.getDisplayName(clone_param), clone_proc, (Procedure)clone_param.getParent());
                assertEquals(CatalogUtil.getDisplayName(clone_param), clone_proc.hashCode(), clone_param.getParent().hashCode());
            } // FOR
        }
    }
    
    /**
     * testCloneDatabaseMultiColumn
     */
    public void testCloneDatabaseMultiColumn() throws Exception {
        // We want to make sure that it clones MultiColumns too!
        Table catalog_tbl = this.getTable("WAREHOUSE");
        Column columns[] = new Column[] {
            this.getColumn(catalog_tbl, "W_NAME"),
            this.getColumn(catalog_tbl, "W_YTD"),
        };
        MultiColumn mc = MultiColumn.get(columns);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);
        
        Database clone_db = CatalogUtil.cloneDatabase(catalog_db);
        assertNotNull(clone_db);
        
        Table clone_tbl = this.getTable(clone_db, catalog_tbl.getName());
        Column clone_col = clone_tbl.getPartitioncolumn();
        assertNotNull(clone_col);
        assert(clone_col instanceof MultiColumn);
        MultiColumn clone_mc = (MultiColumn)clone_col;
        assertEquals(mc.size(), clone_mc.size());
        
        for (int i = 0; i < mc.size(); i++) {
            Column catalog_col = mc.get(i);
            assertNotNull(catalog_col);
            clone_col = clone_mc.get(i);
            assertNotNull(clone_col);
            assertEquals(catalog_col, clone_col);
            assertNotSame(catalog_col.hashCode(), clone_col.hashCode());
        } // FOR
    }
    
    /**
     * testCloneCatalog
     */
    public void testCloneCatalog() throws Exception {
        Catalog clone = CatalogUtil.cloneBaseCatalog(catalog);
        assertNotNull(clone);
        
        for (Cluster catalog_clus : catalog.getClusters()) {
            Cluster clone_clus = clone.getClusters().get(catalog_clus.getName());
            assertNotNull(clone_clus);
            this.checkFields(Cluster.class, catalog_clus, clone_clus);
        } // FOR (Cluster)
    }

    /**
     * testClone
     */
    public void testCloneItem() throws Exception {
        Catalog clone = CatalogUtil.cloneBaseCatalog(catalog);
        assertNotNull(clone);
        CHECK_FIELDS_EXCLUDE.add("Table.constraints");
        CHECK_FIELDS_EXCLUDE.add("Column.constraints");
        
        // Test Table
        Table catalog_tbl = catalog_db.getTables().get("DISTRICT");
        assertNotNull(catalog_tbl);
        Table clone_tbl = CatalogUtil.clone(catalog_tbl, clone);
        assertNotNull(clone_tbl);
        this.checkFields(Table.class, catalog_tbl, clone_tbl);
    }
    
    /**
     * testGetTables
     */
    public void testGetReferencedTables() throws Exception {
        //
        // Get the SLEV procedure and make sure that we get all of our tables back
        //
        String expected[] = { "DISTRICT", "ORDER_LINE", "STOCK" };
        Procedure catalog_proc = this.getProcedure(slev.class);
        
        Set<Table> tables = CatalogUtil.getReferencedTables(catalog_proc);
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
        
        Set<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
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
        
        Set<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
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
        
        Set<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
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
        
        Set<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
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
        Set<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_tbl);
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
        
        Set<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_col);
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
