package edu.brown.catalog;

import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.utils.*;

public class TestCatalogKey extends BaseTestCase {
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }

    private void Tester_createKey(CatalogType catalog_item) {
        assertNotNull(catalog_item);
        String key = CatalogKey.createKey(catalog_item);
        assertFalse(key.isEmpty());
        assertTrue(key.contains(catalog_item.getName()));
    }
    
    private void Tester_getNameFromKey(CatalogType catalog_item) {
        String key = CatalogKey.createKey(catalog_item);
        String name = CatalogKey.getNameFromKey(key);
        assert(name != null);
        assertFalse(name.isEmpty());
        assertEquals(catalog_item.getName(), name);
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
                this.Tester_createKey(catalog_proc_param);
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
                this.Tester_getNameFromKey(catalog_proc_param);
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
                this.Tester_getFromKey(catalog_proc_param);
            } // FOR (ProcParameter)
        } // FOR (Procedure)
    }
    
    /**
     * testMultiColumn
     */
    public void testMultiColumn() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column col0 = this.getColumn(catalog_tbl, "S_ID");
        Column col1 = this.getColumn(catalog_tbl, "SUB_NBR");
        assertNotSame(col0, col1);
        
        MultiColumn mc = MultiColumn.get(col0, col1);
        assertNotNull(mc);
        
        String key = CatalogKey.createKey(mc);
        assertNotNull(key);
        assertFalse(key.isEmpty());
//        System.err.println("----------------------------------");
//        System.err.println(mc + " ==> " + key);
        
        MultiColumn clone = (MultiColumn)CatalogKey.getFromKey(catalog_db, key, Column.class);
        assertNotNull(clone);
        for (int i = 0; i < mc.size(); i++) {
            assertEquals(mc.get(i), mc.get(i));
        } // FOR
    }
    
    /**
     * testMultiProcParameter
     */
    public void testMultiProcParameter() throws Exception {
        Procedure catalog_proc = this.getProcedure(UpdateSubscriberData.class);
        ProcParameter params[] = {
            catalog_proc.getParameters().get(0),
            catalog_proc.getParameters().get(1),
        };
        MultiProcParameter mpp = MultiProcParameter.get(params);
        assertNotNull(mpp);
        
        String key = CatalogKey.createKey(mpp);
        assertNotNull(key);
        assertFalse(key.isEmpty());
//        System.err.println("----------------------------------");
//        System.err.println(mpp + " ==> " + key);
        
        MultiProcParameter clone = (MultiProcParameter)CatalogKey.getFromKey(catalog_db, key, ProcParameter.class);
        assertNotNull(clone);
        for (int i = 0; i < mpp.size(); i++) {
            assertEquals(mpp.get(i), mpp.get(i));
        } // FOR
    }
}