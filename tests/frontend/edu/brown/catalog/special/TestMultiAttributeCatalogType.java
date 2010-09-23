package edu.brown.catalog.special;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.utils.ProjectType;

public class TestMultiAttributeCatalogType extends BaseTestCase {

    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }
    
    /**
     * testMultiColumn
     */
    public void testMultiColumn() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column columns[] = {
            this.getColumn(catalog_tbl, "S_ID"),
            this.getColumn(catalog_tbl, "SUB_NBR"),
        };
        MultiColumn mc = MultiColumn.get(columns);
        assertNotNull(mc);
        
        for (int i = 0; i < columns.length; i++) {
            assertNotNull(columns[i].toString(), mc.get(i));
            assertEquals(columns[i], mc.get(i));
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
        int num_params = catalog_proc.getParameters().size(); 
        MultiProcParameter mc = MultiProcParameter.get(params);
        assertNotNull(mc);
        assertEquals(num_params, mc.getIndex());
        
        for (int i = 0; i < params.length; i++) {
            assertNotNull(params[i].toString(), mc.get(i));
            assertEquals(params[i], mc.get(i));
        } // FOR
    }
}
