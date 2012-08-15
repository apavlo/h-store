package edu.brown.designer.generators;

import org.voltdb.catalog.*;

import java.util.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogKey;
import edu.brown.designer.*;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;

/**
 * 
 * @author pavlo
 *
 */
public class TestDependencyGraphGenerator extends BaseTestCase {

    protected DesignerInfo info;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        this.applyParameterMappings(ProjectType.TPCC);
        
        Workload workload = new Workload(catalog);
        info = new DesignerInfo(catalogContext, workload);
    }

    /**
     * testGenerate
     */
    public void testGenerate() throws Exception {
        DependencyGraph dgraph = new DependencyGraph(catalog_db);
        new DependencyGraphGenerator(info).generate(dgraph);
        
        Table item_table = catalog_db.getTables().get("ITEM");
        Table warehouse_table = catalog_db.getTables().get("WAREHOUSE");
        Table district_table = catalog_db.getTables().get("DISTRICT");
        
        //
        // Make sure that ITEM and WAREHOUSE are the roots
        //
        Set<String> expected = new HashSet<String>();
        expected.add(CatalogKey.createKey(item_table));
        expected.add(CatalogKey.createKey(warehouse_table));
        
        for (DesignerVertex v : dgraph.getRoots()) {
            // Skip any internal system tables
            Table catalog_tbl = v.getCatalogItem();
            if (catalog_tbl.getSystable()) continue;
            
            //System.out.println(v.getCatalogItem());
            assertTrue(expected.contains(v.getCatalogKey()));
        } // FOR
        assertEquals(expected.size(), dgraph.getRoots().size());
        
        //
        // Then make sure that DISTRICT is attached to WAREHOUSE
        //
        DesignerVertex warehouse_v = dgraph.getVertex(warehouse_table);
        assertNotNull(warehouse_v);
        DesignerVertex district_v = dgraph.getVertex(district_table);
        assertNotNull(district_v);
        
        assertTrue(dgraph.getSuccessors(warehouse_v).contains(district_v));
    }
    
}
