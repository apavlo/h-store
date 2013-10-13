package edu.brown.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.procedures.GetTableCounts;
import org.voltdb.benchmark.tpcc.procedures.ResetWarehouse;
import org.voltdb.benchmark.tpcc.procedures.SelectAll;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.utils.Pair;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class TestCatalogUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 6;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
    }
    
    /**
     * testCatalogMapValues
     */
    public void testCatalogMapValues() {
        Database catalog_db = CatalogUtil.getDatabase(CatalogCloner.cloneBaseCatalog(this.getCatalog()));
        Procedure catalog_proc = this.getProcedure(catalog_db, neworder.class);
        CatalogMap<Statement> stmts = catalog_proc.getStatements();
        List<Statement> orig = new ArrayList<Statement>(stmts);
        assertFalse(stmts.isEmpty());
        int expected = stmts.size();
        assertEquals(expected, stmts.values().length);
        assertEquals(orig.size(), stmts.values().length);
        
        stmts.clear();
        
        // Check that we can still iterate
        int ctr = 0;
        for (Statement stmt : stmts) {
            assertNotNull(stmt);
            ctr++;
        }
        assertEquals(0, ctr);
        for (Statement stmt : stmts.values()) {
            assertNotNull(stmt);
            ctr++;
        }
        assertEquals(0, ctr);
        
        assertEquals(0, stmts.size());
        assertEquals(0, stmts.values().length);

        // Add them back
        stmts.addAll(orig);
        assertEquals(expected, stmts.size());
        assertEquals(expected, stmts.values().length);
    }
    
    /**
     * testGetPlanFragment
     */
    public void testGetPlanFragment() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assert (catalog_stmt != null);

        for (PlanFragment expected : catalog_stmt.getFragments()) {
            int id = expected.getId();
            PlanFragment actual = CatalogUtil.getPlanFragment(expected, id);
            assertNotNull(actual);
            assertEquals(expected, actual);
        } // FOR
        for (PlanFragment expected : catalog_stmt.getMs_fragments()) {
            int id = expected.getId();
            PlanFragment actual = CatalogUtil.getPlanFragment(expected, id);
            assertNotNull(actual);
            assertEquals(expected, actual);
        } // FOR
    }

    /**
     * testGetOrderByColumns
     */
    public void testGetOrderByColumns() throws Exception {
        Procedure catalog_proc = this.getProcedure(ostatByCustomerId.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getLastOrder");
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_ORDERS);
        Column expected[] = { this.getColumn(catalog_tbl, "O_ID") };

        Collection<Column> cols = CatalogUtil.getOrderByColumns(catalog_stmt);
        assertNotNull(cols);
        assertEquals(cols.toString(), expected.length, cols.size());
        for (Column col : expected) {
            assert (cols.contains(col)) : "Unexpected " + col;
        } // FOR
    }

    /**
     * testCopyQueryPlans
     */
    public void testCopyQueryPlans() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getWarehouseTaxRate");

        Map<String, Object> orig_values = new HashMap<String, Object>();
        for (String f : catalog_stmt.getFields()) {
            orig_values.put(f, catalog_stmt.getField(f));
        } // FOR
        Map<String, Map<String, Object>> orig_frag_values = new HashMap<String, Map<String, Object>>();
        for (PlanFragment catalog_frag : CollectionUtils.union(catalog_stmt.getFragments(), catalog_stmt.getMs_fragments())) {
            Map<String, Object> m = new HashMap<String, Object>();
            for (String f : catalog_frag.getFields()) {
                Object val = catalog_frag.getField(f);
                if (val instanceof String)
                    val = StringUtil.md5sum(val.toString());
                m.put(f, val);
            } // FOR
            orig_frag_values.put(catalog_frag.fullName(), m);
        } // FOR
        // System.err.println(StringUtil.formatMaps(orig_frag_values));
        // System.err.println(StringUtil.SINGLE_LINE);

        Statement copy = catalog_proc.getStatements().add("TestStatement");
        assertNotNull(copy);
        assertFalse(copy.getHas_singlesited());
        assertEquals(0, copy.getFragments().size());
        assertTrue(copy.getFullplan().isEmpty());
        assertTrue(copy.getExptree().isEmpty());
        assertFalse(copy.getHas_multisited());
        assertEquals(0, copy.getMs_fragments().size());
        assertTrue(copy.getMs_fullplan().isEmpty());
        assertTrue(copy.getMs_exptree().isEmpty());

        CatalogUtil.copyQueryPlans(catalog_stmt, copy);

        // Make sure we didn't change the original
        for (String f : orig_values.keySet()) {
            assertEquals(f, orig_values.get(f), catalog_stmt.getField(f));
        } // FOR
        for (PlanFragment catalog_frag : CollectionUtils.union(catalog_stmt.getFragments(), catalog_stmt.getMs_fragments())) {
            Map<String, Object> m = orig_frag_values.get(catalog_frag.fullName());
            // System.err.println(catalog_frag.fullName());
            for (String f : m.keySet()) {
                Object orig_val = m.get(f);
                Object new_val = catalog_frag.getField(f);
                if (new_val instanceof String)
                    new_val = StringUtil.md5sum(new_val.toString());
                // System.err.println(String.format("\t%s = %s", f, new_val));
                assertEquals(catalog_frag.fullName() + " - " + f, orig_val, new_val);
            } // FOR
            // System.err.println(StringUtil.SINGLE_LINE);
        } // FOR

        // And then check that the destination was updated
        assertEquals(catalog_stmt.getHas_singlesited(), copy.getHas_singlesited());
        assertEquals(catalog_stmt.getFragments().size(), copy.getFragments().size());
        assertEquals(catalog_stmt.getFullplan(), copy.getFullplan());
        assertEquals(catalog_stmt.getExptree(), copy.getExptree());
        assertEquals(catalog_stmt.getHas_multisited(), copy.getHas_multisited());
        assertEquals(catalog_stmt.getMs_fragments().size(), copy.getMs_fragments().size());
        assertEquals(catalog_stmt.getMs_fullplan(), copy.getMs_fullplan());
        assertEquals(catalog_stmt.getMs_exptree(), copy.getMs_exptree());

        // Make sure the PlanFragments are the same
        for (boolean sp : new boolean[] { true, false }) {
            CatalogMap<PlanFragment> copy_src_fragments = null;
            CatalogMap<PlanFragment> copy_dest_fragments = null;
            if (sp) {
                copy_src_fragments = catalog_stmt.getFragments();
                copy_dest_fragments = copy.getFragments();
            } else {
                copy_src_fragments = catalog_stmt.getMs_fragments();
                copy_dest_fragments = copy.getMs_fragments();
            }
            assert (copy_src_fragments != null);
            assert (copy_dest_fragments != null);

            for (PlanFragment copy_src_frag : copy_src_fragments) {
                assertNotNull(copy_src_frag);
                PlanFragment copy_dest_frag = copy_dest_fragments.get(copy_src_frag.getName());
                assertNotNull(copy_dest_frag);

                for (String f : copy_src_frag.getFields()) {
                    assertEquals(f, copy_src_frag.getField(f), copy_dest_frag.getField(f));
                } // FOR

                AbstractPlanNode src_root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(copy_src_frag);
                assertNotNull(copy_src_frag.fullName(), src_root);
                AbstractPlanNode dest_root = PlanNodeUtil.getPlanNodeTreeForPlanFragment(copy_dest_frag);
                assertNotNull(copy_src_frag.fullName(), dest_root);
                //
                // System.err.println(StringUtil.columns(PlanNodeUtil.debug(src_root),
                // PlanNodeUtil.debug(dest_root)));
            }
        } // FOR
    }

    /**
     * testGetReadOnlyColumns
     */
    public void testGetReadOnlyColumns() throws Exception {
        // The columns that we know are modified for these tables
        Map<String, Collection<String>> modified = new HashMap<String, Collection<String>>();
        modified.put(TPCCConstants.TABLENAME_WAREHOUSE, CollectionUtil.addAll(new HashSet<String>(), "W_YTD"));
        modified.put(TPCCConstants.TABLENAME_CUSTOMER, CollectionUtil.addAll(new HashSet<String>(), "C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DATA"));

        for (String tableName : modified.keySet()) {
            Table catalog_tbl = this.getTable(tableName);
            Collection<String> modifiedCols = modified.get(tableName);

            Set<Column> expected = new HashSet<Column>();
            for (Column catalog_col : catalog_tbl.getColumns()) {
                if (modifiedCols.contains(catalog_col.getName()) == false) {
                    expected.add(catalog_col);
                }
            } // FOR (col)

            Collection<Column> readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, false);
            assertNotNull(readOnly);
            assertEquals(readOnly.toString(), expected.size(), readOnly.size());
            assertEquals(expected, readOnly);
        } // FOR (table)

        // We want to also check that excluding INSERT queries works
        Table catalog_tbl = this.getTable("HISTORY");
        Collection<Column> all_readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, true);
        assertNotNull(all_readOnly);
        assertEquals(catalog_tbl.getColumns().size(), all_readOnly.size());
        assert (catalog_tbl.getColumns().containsAll(all_readOnly));

        Collection<Column> noinsert_readOnly = CatalogUtil.getReadOnlyColumns(catalog_tbl, false);
        assertNotNull(noinsert_readOnly);
        assertEquals(0, noinsert_readOnly.size());
    }

    /**
     * testGetReferencedTablesProcedure
     */
    public void testGetReferencedTablesProcedure() throws Exception {
        // Get the SLEV procedure and make sure that we get all of our tables back
        String expected[] = { TPCCConstants.TABLENAME_DISTRICT, TPCCConstants.TABLENAME_ORDER_LINE, "STOCK" };
        Procedure catalog_proc = this.getProcedure(slev.class);

        Collection<Table> tables = CatalogUtil.getReferencedTables(catalog_proc);
        assertEquals(expected.length, tables.size());
        for (String table_name : expected) {
            Table catalog_tbl = this.getTable(table_name);
            assertTrue(table_name, tables.contains(catalog_tbl));
        } // FOR
        return;
    }
    
    /**
     * testGetReferencedTablesPlanFragment
     */
    public void testGetReferencedTablesPlanFragment() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        String expected[] = null;
        Statement stmt = null;
        
        // SELECT
        stmt = this.getStatement(catalog_proc, "getDistrict");
        expected = new String[]{ TPCCConstants.TABLENAME_DISTRICT };
        for (PlanFragment frag : stmt.getFragments()) {
            Collection<Table> tables = CatalogUtil.getReferencedTables(frag);
            assertEquals(frag.fullName(), expected.length, tables.size());
            for (String tableName : expected) {
                Table tbl = this.getTable(tableName);
                assertTrue(tableName, tables.contains(tbl));
            } // FOR
        } // FOR
        
        // INSERT
        stmt = this.getStatement(catalog_proc, "createNewOrder");
        expected = new String[]{ TPCCConstants.TABLENAME_NEW_ORDER };
        for (PlanFragment frag : stmt.getFragments()) {
            Collection<Table> tables = CatalogUtil.getReferencedTables(frag);
            assertEquals(frag.fullName(), expected.length, tables.size());
            for (String tableName : expected) {
                Table tbl = this.getTable(tableName);
                assertTrue(tableName, tables.contains(tbl));
            } // FOR
        } // FOR
        
        // UPDATE
        stmt = this.getStatement(catalog_proc, "updateStock");
        expected = new String[]{ TPCCConstants.TABLENAME_STOCK };
        for (PlanFragment frag : stmt.getFragments()) {
            Collection<Table> tables = CatalogUtil.getReferencedTables(frag);
            assertEquals(frag.fullName(), expected.length, tables.size());
            for (String tableName : expected) {
                Table tbl = this.getTable(tableName);
                assertTrue(tableName, tables.contains(tbl));
            } // FOR
        } // FOR
    }

    /**
     * testGetColumnsSelect
     */
    public void testGetReferencedColumnsSelect() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getDistrict");
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        Column expected[] = { this.getColumn(catalog_tbl, "D_ID"), this.getColumn(catalog_tbl, "D_W_ID") };

        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert (columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }

    /**
     * testGetColumnsInsert
     */
    public void testGetReferencedColumnsInsert() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "createNewOrder");
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_NEW_ORDER);
        Column expected[] = new Column[catalog_tbl.getColumns().size()];
        catalog_tbl.getColumns().toArray(expected);

        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert (columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }

    /**
     * testGetColumnsDelete
     */
    public void testGetReferencedColumnsDelete() throws Exception {
        Procedure catalog_proc = this.getProcedure(delivery.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "deleteNewOrder");
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_NEW_ORDER);
        Column expected[] = { this.getColumn(catalog_tbl, "NO_D_ID"), this.getColumn(catalog_tbl, "NO_W_ID"), this.getColumn(catalog_tbl, "NO_O_ID") };

        Collection<Column> columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expected.length, columns.size());
        for (int i = 0; i < expected.length; i++) {
            assert (columns.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }

    /**
     * testGetColumnsUpdate
     */
    public void testGetReferencedColumnsUpdate() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "incrementNextOrderId");
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        Column expectedReadOnly[] = { this.getColumn(catalog_tbl, "D_ID"), this.getColumn(catalog_tbl, "D_W_ID") };
        Column expectedModified[] = { this.getColumn(catalog_tbl, "D_NEXT_O_ID"), };
        Column expectedAll[] = new Column[expectedReadOnly.length + expectedModified.length];
        int i = 0;
        for (Column c : expectedReadOnly)
            expectedAll[i++] = c;
        for (Column c : expectedModified)
            expectedAll[i++] = c;

        Collection<Column> columns = null;

        // READ-ONLY
        columns = CatalogUtil.getReadOnlyColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expectedReadOnly.length, columns.size());
        for (i = 0; i < expectedReadOnly.length; i++) {
            assert (columns.contains(expectedReadOnly[i])) : "Missing " + expectedReadOnly[i];
        } // FOR

        // MODIFIED
        columns = CatalogUtil.getModifiedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expectedModified.length, columns.size());
        for (i = 0; i < expectedModified.length; i++) {
            assert (columns.contains(expectedModified[i])) : "Missing " + expectedModified[i];
        } // FOR

        // ALL REFERENCED
        columns = CatalogUtil.getReferencedColumns(catalog_stmt);
        assertNotNull(columns);
        assertEquals(columns.toString(), expectedAll.length, columns.size());
        for (i = 0; i < expectedAll.length; i++) {
            assert (columns.contains(expectedAll[i])) : "Missing " + expectedAll[i];
        } // FOR
    }

    /**
     * GetReferencingProcedures
     */
    public void testGetReferencingProcedures() throws Exception {
        Procedure expected[] = {
            this.getProcedure(delivery.class),
            this.getProcedure(neworder.class),
            this.getProcedure(ResetWarehouse.class),
            this.getProcedure(GetTableCounts.class),
            this.getProcedure(SelectAll.class),
            
        };
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_NEW_ORDER);
        Collection<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_tbl);
        assertNotNull(procedures);
        
        // HACK: Remove MR procedures
        procedures.removeAll(catalogContext.getMapReduceProcedures());
        
        assertEquals(procedures.toString(), expected.length, procedures.size());
        for (int i = 0; i < expected.length; i++) {
            assert (procedures.contains(expected[i])) : "Missing " + expected[i];
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
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_NEW_ORDER);
        Column catalog_col = ReplicatedColumn.get(catalog_tbl);

        Collection<Procedure> procedures = CatalogUtil.getReferencingProcedures(catalog_col);
        assertNotNull(procedures);
        
        // HACK: Remove MR procedures
        procedures.removeAll(catalogContext.getMapReduceProcedures());
        
        assertEquals(procedures.toString(), expected.length, procedures.size());
        for (int i = 0; i < expected.length; i++) {
            assert (procedures.contains(expected[i])) : "Missing " + expected[i];
        } // FOR
    }

    /**
     * testGetLocalPartitions
     */
    public void testGetLocalPartitions() throws Exception {
        //
        // Add in some fake partitions
        //
        Cluster cluster = catalogContext.cluster;
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
            Partition catalog_part = CollectionUtil.first(partitions);
            int base_partition = catalog_part.getId();
            Collection<Partition> local_partitions = CatalogUtil.getLocalPartitions(catalogContext.database, base_partition);
            assertEquals(partitions.size(), local_partitions.size());

            for (Partition other_part : local_partitions) {
                assertNotNull(other_part);
                // assertFalse(catalog_part.equals(other_part));
                assertTrue(host_partitions.get(catalog_host).contains(other_part));
            } // FOR
        } // FOR
    }

    /**
     * testGetSitesForHost
     */
    public void testGetSitesForHost() throws Exception {
        Map<Host, List<Site>> host_sites = new HashMap<Host, List<Site>>();
        Cluster catalog_clus = catalogContext.cluster;

        for (Site catalog_site : catalog_clus.getSites()) {
            Host catalog_host = catalog_site.getHost();
            assertNotNull(catalog_host);
            if (!host_sites.containsKey(catalog_host)) {
                host_sites.put(catalog_host, new ArrayList<Site>());
            }
            host_sites.get(catalog_host).add(catalog_site);
        } // FOR

        for (Host catalog_host : catalog_clus.getHosts()) {
            Collection<Site> sites = CatalogUtil.getSitesForHost(catalog_host);
            Collection<Site> expected = host_sites.get(catalog_host);
            assertEquals(expected.size(), sites.size());
            assert (sites.containsAll(expected));
        } // FOR
    }

    /**
     * testGetCluster
     */
    @SuppressWarnings("deprecation")
    public void testGetCluster() {
        Cluster catalog_clus = catalogContext.cluster;
        assertNotNull(catalog_clus);

        Set<Cluster> clusters = new HashSet<Cluster>();
        for (Cluster c : catalog.getClusters()) {
            clusters.add(c);
        } // FOR
        assertEquals(1, clusters.size());
        assertEquals(catalog_clus, CollectionUtil.first(clusters));
    }

    /**
     * testGetDatabase
     */
    @SuppressWarnings("deprecation")
    public void testGetDatabase() {
        Database db0 = CatalogUtil.getDatabase(catalog);
        assertNotNull(db0);

        Set<Database> dbs = new HashSet<Database>();
        for (Database db1 : catalogContext.cluster.getDatabases()) {
            dbs.add(db1);
        } // FOR
        assertEquals(1, dbs.size());
        assertEquals(db0, CollectionUtil.first(dbs));
    }

    /**
     * testCatalogMapGet
     */
    public void testCatalogMapGet() {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_ORDER_LINE);
        CatalogMap<Column> catalog_cols = catalog_tbl.getColumns();
        List<Column> sorted_cols = org.voltdb.utils.CatalogUtil.getSortedCatalogItems(catalog_cols, "index");
        assertEquals(catalog_cols.size(), sorted_cols.size());
        for (int i = 0, cnt = catalog_cols.size(); i < cnt; i++) {
            Column expected = sorted_cols.get(i);
            Column actual = catalog_cols.get(i);
            assertEquals("Failed get(" + i + "): " + expected, expected, actual);
        } // FOR
    }

    /**
     * Checks that the ColumnSet has the proper mapping from Columns to
     * StmtParameters
     * 
     * @param cset
     * @param expected
     */
    private void checkColumnSet(PredicatePairs cset, Collection<Pair<Column, Integer>> expected) {
        for (CatalogPair entry : cset) {
            int column_idx = (entry.getFirst() instanceof StmtParameter ? 1 : 0);
            int param_idx = (column_idx == 0 ? 1 : 0);

            assertEquals(entry.get(column_idx).getClass(), Column.class);
            assertEquals(entry.get(param_idx).getClass(), StmtParameter.class);

            Column catalog_col = (Column) entry.get(column_idx);
            StmtParameter catalog_param = (StmtParameter) entry.get(param_idx);

            Pair<Column, Integer> found = null;
            // System.err.println("TARGET: " + catalog_col.fullName() + " <=> "
            // + catalog_param.fullName());
            for (Pair<Column, Integer> pair : expected) {
                // System.err.println(String.format("  COMPARE: %s <=> %d",
                // pair.getFirst().fullName(), pair.getSecond()));
                if (pair.getFirst().equals(catalog_col) && pair.getSecond() == catalog_param.getIndex()) {
                    found = pair;
                    break;
                }
            } // FOR
            if (found == null)
                System.err.println("Failed to find " + entry + "\n" + cset.debug());
            assertNotNull("Failed to find " + entry, found);
            expected.remove(found);
        } // FOR
        assertEquals(0, expected.size());
    }

    /**
     * testExtractStatementColumnSet
     */
    public void testExtractStatementColumnSet() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        assertNotNull(catalog_tbl);
        Procedure catalog_proc = this.getProcedure("neworder");
        Statement catalog_stmt = this.getStatement(catalog_proc, "getDistrict");
        PredicatePairs cset = CatalogUtil.extractStatementPredicates(catalog_stmt, false, catalog_tbl);

        // Column -> StmtParameter Index
        Set<Pair<Column, Integer>> expected_columns = new HashSet<Pair<Column, Integer>>();
        expected_columns.add(Pair.of(catalog_tbl.getColumns().get("D_ID"), 0));
        expected_columns.add(Pair.of(catalog_tbl.getColumns().get("D_W_ID"), 1));
        assertEquals(catalog_stmt.getParameters().size(), expected_columns.size());

        this.checkColumnSet(cset, expected_columns);
    }

    /**
     * testExtractUpdateColumnSet
     */
    public void testExtractUpdateColumnSet() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "incrementNextOrderId");

        PredicatePairs cset = CatalogUtil.extractUpdateColumnSet(catalog_stmt, false, catalog_tbl);
        // System.out.println(cset.debug());

        // Column -> StmtParameter Index
        Set<Pair<Column, Integer>> expected_columns = new HashSet<Pair<Column, Integer>>();
        expected_columns.add(Pair.of(catalog_tbl.getColumns().get("D_NEXT_O_ID"), 0));

        this.checkColumnSet(cset, expected_columns);
    }

    /**
     * testExtractInsertColumnSet
     */
    public void testExtractInsertColumnSet() throws Exception {
        Table catalog_tbl = this.getTable("HISTORY");
        Procedure catalog_proc = this.getProcedure(paymentByCustomerId.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "insertHistory");

        assertNotNull(catalog_stmt);
        PredicatePairs cset = CatalogUtil.extractStatementPredicates(catalog_stmt, false, catalog_tbl);
        // System.out.println(cset.debug());

        // Column -> StmtParameter Index
        Set<Pair<Column, Integer>> expected_columns = new ListOrderedSet<Pair<Column, Integer>>();
        for (Column catalog_col : CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index")) {
            assertNotNull(catalog_col);
            expected_columns.add(Pair.of(catalog_col, catalog_col.getIndex()));
        } // FOR
        System.err.println(StringUtil.columns("EXPECTED:\n" + StringUtil.join("\n", expected_columns), "ACTUAL:\n" + StringUtil.join("\n", cset)));

        this.checkColumnSet(cset, expected_columns);
    }

    /**
     * testExtractPlanNodeColumnSet
     */
    public void testExtractPlanNodeColumnSet() throws Exception {
        Procedure catalog_proc = this.getProcedure(slev.class);
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("GetStockCount");

        AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        assertNotNull(root_node);
        // System.out.println(PlanNodeUtil.debug(root_node));

        PredicatePairs cset = new PredicatePairs();
        Table tables[] = new Table[] {
                this.getTable(TPCCConstants.TABLENAME_ORDER_LINE),
                this.getTable("STOCK")
        };
        for (Table catalog_tbl : tables) {
            Set<Table> table_set = new HashSet<Table>();
            table_set.add(catalog_tbl);
            CatalogUtil.extractPlanNodePredicates(catalog_stmt, catalogContext.database, cset, root_node, false, table_set);
        }
        // System.err.println(cset.debug() + "\n-------------------------\n");

        // Column -> StmtParameter Index
        List<Pair<Column, Integer>> expected_columns = new ArrayList<Pair<Column, Integer>>();
        expected_columns.add(Pair.of(tables[0].getColumns().get("OL_W_ID"), 0));
        expected_columns.add(Pair.of(tables[0].getColumns().get("OL_D_ID"), 1));
        expected_columns.add(Pair.of(tables[0].getColumns().get("OL_O_ID"), 2));
        expected_columns.add(Pair.of(tables[0].getColumns().get("OL_O_ID"), 3));
        expected_columns.add(Pair.of(tables[0].getColumns().get("OL_O_ID"), 3)); // Need
                                                                                 // two
                                                                                 // of
                                                                                 // these
                                                                                 // because
                                                                                 // of
                                                                                 // indexes
        expected_columns.add(Pair.of(tables[1].getColumns().get("S_W_ID"), 4));
        expected_columns.add(Pair.of(tables[1].getColumns().get("S_QUANTITY"), 5));

        this.checkColumnSet(cset, expected_columns);
    }

    // public static void main(String[] args) throws Exception {
    // TestCatalogUtil tc = new TestCatalogUtil();
    // tc.setUp();
    // tc.testGetLocalPartitions();
    // javax.swing.SwingUtilities.invokeLater(new Runnable() {
    // public void run() {
    // new CatalogViewer(catalog, "fake").setVisible(true);
    // System.out.println("???");
    // } // RUN
    // });
    // }
}
