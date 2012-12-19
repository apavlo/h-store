package edu.brown;

import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.sysprocs.Statistics;

import edu.brown.utils.ProjectType;

import junit.framework.TestCase;

public class TestBaseTestCase extends TestCase {
    
    private BaseTestCase bt;
    private final ProjectType types[] = {
            ProjectType.TPCC,
            ProjectType.TM1,
            ProjectType.SEATS
    };
        
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.bt = new BaseTestCase() {
            // Nothing...
        };
    }
    
    /**
     * testGetProcedure
     */
    @Test
    public void testGetProcedure() throws Exception {
        ProjectType type = ProjectType.TPCC;
        this.bt.setUp(type);
        CatalogContext catalogContext = this.bt.getCatalogContext();
        assertNotNull(type.toString(), catalogContext);
        
        Procedure proc0, proc1;
        
        // Regular Procedure
        proc0 = this.bt.getProcedure(catalogContext.database, neworder.class);
        assertNotNull(proc0);
        assertTrue(proc0.getName(), proc0.getName().endsWith(neworder.class.getSimpleName()));
        assertFalse(proc0.getName(), proc0.getSystemproc());
        proc1 = this.bt.getProcedure(proc0.getName());
        assertNotNull(proc1);
        assertEquals(proc0, proc1);
        
        // System Procedure
        proc0 = this.bt.getProcedure(catalogContext.database, Statistics.class);
        assertNotNull(proc0);
        assertTrue(proc0.getName(), proc0.getName().endsWith(Statistics.class.getSimpleName()));
        assertTrue(proc0.getName(), proc0.getSystemproc());
        proc1 = this.bt.getProcedure(proc0.getName());
        assertNotNull(proc1);
        assertEquals(proc0, proc1);
    }
    
    /**
     * testParameterMappings
     */
    @Test
    public void testParameterMappings() throws Exception {
        // Check to make sure that we can get the ParameterMappingsSet
        // from the CatalogContexts for the different projects
        for (ProjectType type : types) {
            this.bt.setUp(type);
            CatalogContext catalogContext = this.bt.getCatalogContext();
            assertNotNull(type.toString(), catalogContext);
            assertEquals(type.name().toLowerCase(), catalogContext.database.getProject());
            assertNotNull(type.toString(), catalogContext.paramMappings);
        } // FOR
    }

}
