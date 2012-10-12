package edu.brown.catalog;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.compiler.VoltCompiler;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProjectType;

public class TestCatalogCloner extends BaseTestCase {

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

    protected final String VERTICAL_PARTITION_TABLE = "CUSTOMER";
    protected final List<String> VERTICAL_PARTITION_COLUMNS = new ArrayList<String>();
    {
        VERTICAL_PARTITION_COLUMNS.add("C_ID");
        VERTICAL_PARTITION_COLUMNS.add("C_FIRST");
        VERTICAL_PARTITION_COLUMNS.add("C_LAST");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);

        if (isFirstSetup()) {
            this.addPartitions(NUM_PARTITIONS);
            VoltCompiler.addVerticalPartition(catalog_db, VERTICAL_PARTITION_TABLE, VERTICAL_PARTITION_COLUMNS, true);
        }
    }

    /**
     * Recursively walkthrough the catalog and make sure that all the fields
     * match between the two items
     * 
     * @param <T>
     * @param base_class
     * @param item0
     * @param item1
     * @throws Exception
     */
    protected <T extends CatalogType> void checkFields(Class<T> base_class, CatalogType item0, CatalogType item1) throws Exception {
        String base_name = base_class.getSimpleName();
        boolean debug = false;

        assertNotNull("Null item for " + base_name, item0);
        assertNotNull("Null item for " + base_name, item1);

        if (debug)
            System.err.println(CatalogUtil.getDisplayName(item0, true));
        for (Method method_handle : base_class.getDeclaredMethods()) {
            String method_name = method_handle.getName();
            if (!method_name.startsWith("get"))
                continue;
            String field_name = base_name + "." + method_name.substring(3).toLowerCase();
            if (debug)
                System.err.print("  " + field_name + ": ");
            if (CHECK_FIELDS_EXCLUDE.contains(field_name)) {
                if (debug)
                    System.err.println("SKIP");
                continue;
            }

            Object field_val0 = method_handle.invoke(item0);
            Object field_val1 = method_handle.invoke(item1);

            // CatalogMap
            if (field_val0 instanceof CatalogMap) {
                assert (field_val1 instanceof CatalogMap);

                CatalogMap<?> map0 = (CatalogMap<?>) field_val0;
                CatalogMap<?> map1 = (CatalogMap<?>) field_val1;
                if (debug)
                    System.err.println(CatalogUtil.debug(map0) + " <-> " + CatalogUtil.debug(map1));
                assertEquals("Mismatched CatalogMap sizes for " + field_name, map0.size(), map1.size());

                Class<? extends CatalogType> generic_class = (Class<? extends CatalogType>) map0.getGenericClass();
                assert (generic_class != null);

                CatalogType map0_values[] = map0.values();
                CatalogType map1_values[] = map0.values();
                for (int i = 0, cnt = map0.size(); i < cnt; i++) {
                    CatalogType child0 = map0_values[i];
                    assertNotNull("Null child element at index " + i + " for " + field_name, child0);
                    CatalogType child1 = map1_values[i];
                    assertNotNull("Null child element at index " + i + " for " + field_name, child1);
                    this.checkFields(generic_class, child0, child1);
                } // FOR
            }
            // ConstraintRefs
            else if (field_val0 instanceof ConstraintRef) {
                ConstraintRef ref0 = (ConstraintRef) field_val0;
                ConstraintRef ref1 = (ConstraintRef) field_val1;
                if (debug)
                    System.err.println(CatalogUtil.getDisplayName(ref0) + " <-> " + CatalogUtil.getDisplayName(ref1));
                this.checkFields(Column.class, ref0.getConstraint(), ref1.getConstraint());
            }
            // TableRefs
            else if (field_val0 instanceof TableRef) {
                TableRef ref0 = (TableRef) field_val0;
                TableRef ref1 = (TableRef) field_val1;
                if (debug)
                    System.err.println(CatalogUtil.getDisplayName(ref0) + " <-> " + CatalogUtil.getDisplayName(ref1));
                this.checkFields(Table.class, ref0.getTable(), ref1.getTable());
            }
            // ColumnRefs
            else if (field_val0 instanceof ColumnRef) {
                ColumnRef ref0 = (ColumnRef) field_val0;
                ColumnRef ref1 = (ColumnRef) field_val1;
                if (debug)
                    System.err.println(CatalogUtil.getDisplayName(ref0) + " <-> " + CatalogUtil.getDisplayName(ref1));
                this.checkFields(Column.class, ref0.getColumn(), ref1.getColumn());
            }
            // CatalogMap
            else if (field_val0 != null && ClassUtil.getSuperClasses(field_val0.getClass()).contains(CatalogType.class)) {
                CatalogType type0 = (CatalogType) field_val0;
                CatalogType type1 = (CatalogType) field_val1;
                if (debug)
                    System.err.println(CatalogUtil.getDisplayName(type0) + " <-> " + CatalogUtil.getDisplayName(type1));
                assertEquals("Mismatched values for " + field_name, type0.getName(), type1.getName());
            }
            // Scalar
            else {
                if (debug)
                    System.err.println(field_val0 + " <-> " + field_val1);
                assertEquals("Mismatched values for " + field_name, field_val0, field_val1);
            }
        } // FOR (field)
    }
    
    private void checkProcedureConflicts(Procedure catalog_proc, CatalogMap<ConflictSet> conflicts0, CatalogMap<ConflictSet> conflicts1) {
        assertEquals(catalog_proc.toString(), conflicts0.size(), conflicts1.size());
        for (String procName : conflicts0.keySet()) {
            assertNotNull(procName);
            ConflictSet cs0 = conflicts0.get(procName);
            assertNotNull(cs0);
            ConflictSet cs1 = conflicts1.get(procName);
            assertNotNull(cs1);
            
            assertEquals(cs0.getReadwriteconflicts().size(), cs1.getReadwriteconflicts().size());
            for (ConflictPair conflict0 : cs0.getReadwriteconflicts()) {
                ConflictPair conflict1 = cs1.getReadwriteconflicts().get(conflict0.getName());
                assertNotNull(conflict0.fullName(), conflict1);
                assertEquals(conflict0.fullName(), conflict0.getStatement0().getName(), conflict1.getStatement0().getName());
                assertEquals(conflict0.fullName(), conflict0.getStatement1().getName(), conflict1.getStatement1().getName());
                assertEquals(conflict0.fullName(), conflict0.getAlwaysconflicting(), conflict1.getAlwaysconflicting());
                assertEquals(conflict0.fullName(), conflict0.getConflicttype(), conflict1.getConflicttype());
                for (TableRef ref0 : conflict0.getTables()) {
                    TableRef ref1 = conflict1.getTables().get(ref0.getName());
                    assertNotNull(ref0.fullName(), ref1);
                    assertEquals(ref0.fullName(), ref0.getTable().getName(), ref1.getTable().getName());
                } // FOR
            } // FOR
            
            assertEquals(cs0.getWritewriteconflicts().size(), cs1.getWritewriteconflicts().size());
            for (ConflictPair conflict0 : cs0.getWritewriteconflicts()) {
                ConflictPair conflict1 = cs1.getWritewriteconflicts().get(conflict0.getName());
                assertNotNull(conflict0.fullName(), conflict1);
                assertEquals(conflict0.fullName(), conflict0.getStatement0().getName(), conflict1.getStatement0().getName());
                assertEquals(conflict0.fullName(), conflict0.getStatement1().getName(), conflict1.getStatement1().getName());
                assertEquals(conflict0.fullName(), conflict0.getAlwaysconflicting(), conflict1.getAlwaysconflicting());
                assertEquals(conflict0.fullName(), conflict0.getConflicttype(), conflict1.getConflicttype());
                for (TableRef ref0 : conflict0.getTables()) {
                    TableRef ref1 = conflict1.getTables().get(ref0.getName());
                    assertNotNull(ref0.fullName(), ref1);
                    assertEquals(ref0.fullName(), ref0.getTable().getName(), ref1.getTable().getName());
                } // FOR
            } // FOR

        } // FOR
    }

    /**
     * testCloneDatabase
     */
    public void testCloneDatabase() throws Exception {
        assertEquals(NUM_PARTITIONS, catalogContext.numberOfPartitions);
        Collection<Integer> all_partitions = catalogContext.getAllPartitionIds();
        assertNotNull(all_partitions);

        // We want to make sure that it clones MultiProcParameters too!
        Procedure target_proc = this.getProcedure(neworder.class);
        List<MultiProcParameter> expected = new ArrayList<MultiProcParameter>();
        int cnt = 3;
        for (int i = 0; i < cnt; i++) {
            ProcParameter col0 = target_proc.getParameters().get(i);
            for (int ii = i + 1; ii < cnt; ii++) {
                ProcParameter col1 = target_proc.getParameters().get(ii);
                MultiProcParameter mpp = MultiProcParameter.get(col0, col1);
                assertNotNull(mpp);
                expected.add(mpp);
            } // FOR
        } // FOR
        assertFalse(expected.isEmpty());
        
        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
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
        assertEquals(all_partitions.size(), clone_partitions.size());
        assert (all_partitions.containsAll(clone_partitions));

        for (Table catalog_tbl : catalog_db.getTables()) {
            Table clone_tbl = clone_db.getTables().get(catalog_tbl.getName());
            assertNotNull(catalog_tbl.toString(), clone_tbl);
            checkFields(Table.class, catalog_tbl, clone_tbl);

            for (Column catalog_col : catalog_tbl.getColumns()) {
                Column clone_col = clone_tbl.getColumns().get(catalog_col.getName());
                assertNotNull(CatalogUtil.getDisplayName(catalog_col), clone_col);
                assertEquals(CatalogUtil.getDisplayName(clone_col), clone_tbl, (Table) clone_col.getParent());
                assertEquals(CatalogUtil.getDisplayName(clone_col), clone_tbl.hashCode(), clone_col.getParent().hashCode());
            } // FOR
            for (MaterializedViewInfo catalog_view : catalog_tbl.getViews()) {
                assert (catalog_view.getGroupbycols().size() > 0);
                MaterializedViewInfo clone_view = clone_tbl.getViews().get(catalog_view.getName());
                checkFields(MaterializedViewInfo.class, catalog_view, clone_view);
            } // FOR

        } // FOR

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            Procedure clone_proc = clone_db.getProcedures().get(catalog_proc.getName());
            assertNotNull(catalog_proc.toString(), clone_proc);
            assertEquals(clone_proc.getParameters().toString(), catalog_proc.getParameters().size(), clone_proc.getParameters().size());

            // Procedure Conflicts
            this.checkProcedureConflicts(catalog_proc, catalog_proc.getConflicts(), clone_proc.getConflicts());
            this.checkProcedureConflicts(catalog_proc, catalog_proc.getConflicts(), clone_proc.getConflicts());
            
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                ProcParameter clone_param = clone_proc.getParameters().get(catalog_param.getIndex());
                assertNotNull(CatalogUtil.getDisplayName(catalog_param), clone_param);
                assertEquals(CatalogUtil.getDisplayName(clone_param), clone_proc, (Procedure) clone_param.getParent());
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
        Column columns[] = new Column[] { this.getColumn(catalog_tbl, "W_NAME"), this.getColumn(catalog_tbl, "W_YTD"), };
        MultiColumn mc = MultiColumn.get(columns);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);

        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
        assertNotNull(clone_db);

        Table clone_tbl = this.getTable(clone_db, catalog_tbl.getName());
        Column clone_col = clone_tbl.getPartitioncolumn();
        assertNotNull(clone_col);
        assert (clone_col instanceof MultiColumn);
        MultiColumn clone_mc = (MultiColumn) clone_col;
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
        Catalog clone = CatalogCloner.cloneBaseCatalog(catalog);
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
        Catalog clone = CatalogCloner.cloneBaseCatalog(catalog);
        assertNotNull(clone);
        CHECK_FIELDS_EXCLUDE.add("Table.constraints");
        CHECK_FIELDS_EXCLUDE.add("Column.constraints");

        // Test Table
        Table catalog_tbl = catalog_db.getTables().get("DISTRICT");
        assertNotNull(catalog_tbl);
        Table clone_tbl = CatalogCloner.clone(catalog_tbl, clone);
        assertNotNull(clone_tbl);
        this.checkFields(Table.class, catalog_tbl, clone_tbl);
    }
}
