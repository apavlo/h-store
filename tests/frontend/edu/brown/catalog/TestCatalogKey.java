package edu.brown.catalog;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.catalog.special.MultiAttributeCatalogType;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.utils.ProjectType;

public class TestCatalogKey extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }

    private void Tester_createKey(CatalogType catalog_item) {
        assertNotNull(catalog_item);
        String key = CatalogKey.createKey(catalog_item);
        assertFalse(key.isEmpty());
        assertTrue(key, key.contains(catalog_item.getName()));
    }

    private void Tester_getNameFromKey(CatalogType catalog_item) {
        String key = CatalogKey.createKey(catalog_item);
        String name = CatalogKey.getNameFromKey(key);
        assert (name != null);
        assertFalse(key, name.isEmpty());
        assertEquals(key, catalog_item.getName(), name);
    }

    private void Tester_getFromKey(CatalogType catalog_item) {
        assertNotNull(catalog_item);
        String key = CatalogKey.createKey(catalog_item);
        assertFalse(key.isEmpty());
        CatalogType clone = CatalogKey.getFromKey(catalog_db, key, catalog_item.getClass());
        assertNotNull(clone);
        assertEquals(catalog_item, clone);
    }

    /**
     * testCreateKeyFromStrings
     */
    public void testCreateKeyFromStrings() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING);
        Column catalog_col = this.getColumn(catalog_tbl, 0);

        String expected = CatalogKey.createKey(catalog_col);
        assertNotNull(expected);
        assertFalse(expected.isEmpty());

        String actual = CatalogKey.createKey(catalog_tbl.getName(), catalog_col.getName());
        assertNotNull(actual);
        assertFalse(actual.isEmpty());
        assertEquals(expected, actual);
    }

    /**
     * testCreateKey
     */
    public void testCreateKey() throws Exception {
        for (Table catalog_tbl : catalog_db.getTables()) {
            this.Tester_createKey(catalog_tbl);

            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                this.Tester_createKey(catalog_idx);
            } // FOR (Index)
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.Tester_createKey(catalog_col);
            } // FOR (Column)
        } // FOR (Table)

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            this.Tester_createKey(catalog_proc);

            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                this.Tester_createKey(catalog_stmt);
                for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                    this.Tester_createKey(catalog_stmt_param);
                } // FOR (StmtParameter)
            } // FOR (Statement)

            for (ProcParameter catalog_proc_param : catalog_proc.getParameters()) {
                if ((catalog_proc_param instanceof MultiAttributeCatalogType<?>) == false) {
                    this.Tester_createKey(catalog_proc_param);
                }
            } // FOR (ProcParameter)
        } // FOR (Procedure)
    }

    /**
     * testGetNameFromKey
     */
    public void testGetNameFromKey() throws Exception {
        for (Table catalog_tbl : catalog_db.getTables()) {
            this.Tester_getNameFromKey(catalog_tbl);

            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                this.Tester_getNameFromKey(catalog_idx);
            } // FOR (Index)
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.Tester_getNameFromKey(catalog_col);
            } // FOR (Column)
        } // FOR (Table)

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            this.Tester_getNameFromKey(catalog_proc);

            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                this.Tester_getNameFromKey(catalog_stmt);
                for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                    this.Tester_getNameFromKey(catalog_stmt_param);
                } // FOR (StmtParameter)
            } // FOR (Statement)

            for (ProcParameter catalog_proc_param : catalog_proc.getParameters()) {
                if ((catalog_proc_param instanceof MultiAttributeCatalogType<?>) == false) {
                    this.Tester_getNameFromKey(catalog_proc_param);
                }
            } // FOR (ProcParameter)
        } // FOR (Procedure)
    }

    /**
     * testGetFromKey
     */
    public void testGetFromKey() throws Exception {
        for (Table catalog_tbl : catalog_db.getTables()) {
            this.Tester_getFromKey(catalog_tbl);

            for (Index catalog_idx : catalog_tbl.getIndexes()) {
                this.Tester_getFromKey(catalog_idx);
            } // FOR (Index)
            for (Column catalog_col : catalog_tbl.getColumns()) {
                this.Tester_getFromKey(catalog_col);
            } // FOR (Column)
        } // FOR (Table)

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            this.Tester_getFromKey(catalog_proc);

            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                this.Tester_getFromKey(catalog_stmt);
                for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
                    this.Tester_getFromKey(catalog_stmt_param);
                } // FOR (StmtParameter)
            } // FOR (Statement)

            for (ProcParameter catalog_proc_param : catalog_proc.getParameters()) {
                if ((catalog_proc_param instanceof MultiAttributeCatalogType<?>) == false) {
                    this.Tester_getFromKey(catalog_proc_param);
                }
            } // FOR (ProcParameter)
        } // FOR (Procedure)
    }

    /**
     * testMultiColumn
     */
    public void testMultiColumn() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column catalog_cols[] = { this.getColumn(catalog_tbl, "S_ID"), this.getColumn(catalog_tbl, "SUB_NBR"), this.getColumn(catalog_tbl, "VLR_LOCATION"), };

        MultiColumn item0 = MultiColumn.get(catalog_cols);
        assertNotNull(item0);
        assertEquals(catalog_cols.length, item0.size());

        String key = CatalogKey.createKey(item0);
        assertNotNull(key);
        assertFalse(key.isEmpty());
        // System.err.println("----------------------------------");
        // System.err.println(mc + " ==> " + key);

        MultiColumn clone = (MultiColumn) CatalogKey.getFromKey(catalog_db, key, Column.class);
        assertNotNull(clone);
        assertEquals(catalog_cols.length, clone.size());
        for (int i = 0; i < item0.size(); i++) {
            assertEquals(item0.get(i), item0.get(i));
        } // FOR
    }

    /**
     * testVerticalPartitionColumn
     */
    public void testVerticalPartitionColumn() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column orig_hp_col = this.getColumn(catalog_tbl, "S_ID");
        MultiColumn orig_vp_col = MultiColumn.get(this.getColumn(catalog_tbl, "S_ID"), this.getColumn(catalog_tbl, "SUB_NBR"), this.getColumn(catalog_tbl, "VLR_LOCATION"));

        VerticalPartitionColumn item0 = VerticalPartitionColumn.get(orig_hp_col, orig_vp_col);
        assertNotNull(item0);

        String key = CatalogKey.createKey(item0);
        assertNotNull(key);
        assertFalse(key.isEmpty());
        // System.err.println(item0 + "\n" + key);

        MultiColumn clone = (MultiColumn) CatalogKey.getFromKey(catalog_db, key, Column.class);
        assertNotNull(clone);
        assertEquals(item0.size(), clone.size());
    }

    /**
     * testMultiProcParameter
     */
    public void testMultiProcParameter() throws Exception {
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        ProcParameter params[] = { catalog_proc.getParameters().get(0), catalog_proc.getParameters().get(1), };
        MultiProcParameter item0 = MultiProcParameter.get(params);
        assertNotNull(item0);

        String key = CatalogKey.createKey(item0);
        assertNotNull(key);
        assertFalse(key.isEmpty());
        // System.err.println("----------------------------------");
        // System.err.println(mpp + " ==> " + key);

        MultiProcParameter clone = (MultiProcParameter) CatalogKey.getFromKey(catalog_db, key, ProcParameter.class);
        assertNotNull(clone);
        for (int i = 0; i < item0.size(); i++) {
            assertEquals(item0.get(i), item0.get(i));
        } // FOR
    }
}