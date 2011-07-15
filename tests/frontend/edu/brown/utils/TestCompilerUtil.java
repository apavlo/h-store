package edu.brown.utils;

import junit.framework.TestCase;

import java.util.*;

import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.*;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.designer.DependencyGraph;

/**
 * 
 * @author pavlo
 *
 */
public class TestCompilerUtil extends TestCase {

    protected static Catalog catalog;
    
    @Override
    protected void setUp() throws Exception {
        if (catalog == null) {
            catalog = TPCCProjectBuilder.getTPCCSchemaCatalog();
            assertNotNull(catalog);
        }
    }

    
    /**
     * testCompileCatalog
     */
    public void testCompileCatalog() throws Exception {
        TPCCProjectBuilder builder = new TPCCProjectBuilder();
        Catalog new_catalog = CompilerUtil.compileCatalog(builder.getDDLURL(true).getFile());
        assertNotNull(new_catalog);
        
        for (Cluster orig_cluster : catalog.getClusters()) {
            Cluster new_cluster = new_catalog.getClusters().get(orig_cluster.getName());
            assertNotNull(new_cluster);
            
            for (Database orig_db : orig_cluster.getDatabases()) {
                Database new_db = new_cluster.getDatabases().get(orig_db.getName());
                assertNotNull(new_db);
                
                for (Table orig_tbl : orig_db.getTables()) {
                    Table new_tbl = new_db.getTables().get(orig_tbl.getName());
                    assertNotNull(new_tbl);
                    
                    for (Column orig_col : orig_tbl.getColumns()) {
                        Column new_col = new_tbl.getColumns().get(orig_col.getName());
                        assertNotNull(new_col);
                    } // FOR
                } // FOR
            } // FOR
        } // FOR
        CatalogUtil.saveCatalog(new_catalog, "new_catalog.txt");
    }
}
