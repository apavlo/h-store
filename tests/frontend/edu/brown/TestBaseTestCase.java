package edu.brown;

import org.junit.Test;
import org.voltdb.CatalogContext;

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
