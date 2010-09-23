package edu.brown.designer.generators;

import org.voltdb.catalog.*;

import java.util.*;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogKey;
import edu.brown.designer.*;
import edu.brown.utils.ProjectType;
import edu.brown.workload.WorkloadTraceFileOutput;

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
        
        WorkloadTraceFileOutput workload = new WorkloadTraceFileOutput(catalog);
        info = new DesignerInfo(catalog_db, workload);
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
        
        for (Vertex v : dgraph.getRoots()) {
            //System.out.println(v.getCatalogItem());
            assertTrue(expected.contains(v.getCatalogKey()));
        } // FOR
        assertEquals(expected.size(), dgraph.getRoots().size());
        
        //
        // Then make sure that DISTRICT is attached to WAREHOUSE
        //
        Vertex warehouse_v = dgraph.getVertex(warehouse_table);
        assertNotNull(warehouse_v);
        Vertex district_v = dgraph.getVertex(district_table);
        assertNotNull(district_v);
        
        assertTrue(dgraph.getSuccessors(warehouse_v).contains(district_v));
    }
    
}
