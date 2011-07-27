package edu.brown.catalog.special;

import java.util.Collection;
import java.util.HashSet;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;
import edu.brown.utils.CollectionUtil;
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
        
        MultiColumn item0 = MultiColumn.get(columns);
        assertNotNull(item0);
        assertEquals(catalog_tbl, item0.getParent());
        for (int i = 0; i < columns.length; i++) {
            assertNotNull(columns[i].toString(), item0.get(i));
            assertEquals(columns[i], item0.get(i));
        } // FOR
        
        // Make another and make sure it's the same 
        MultiColumn item1 = MultiColumn.get(columns);
        assertNotNull(item1);
        assert(item0 == item1);
        assertEquals(item0.hashCode(), item1.hashCode());
        assertEquals(item0, item1);
    }
    
    /**
     * testVerticalPartitionColumn
     */
    @SuppressWarnings("unchecked")
    public void testVerticalPartitionColumn() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column orig_hp_col = this.getColumn(catalog_tbl, "S_ID");
        MultiColumn orig_vp_col = MultiColumn.get(this.getColumn(catalog_tbl, "S_ID"),
                                                  this.getColumn(catalog_tbl, "SUB_NBR")); 
        
        VerticalPartitionColumn item0 = VerticalPartitionColumn.get(orig_hp_col, orig_vp_col);
        assertNotNull(item0);
        assertEquals(catalog_tbl, item0.getParent());
        assertEquals(2, item0.size());
        
        Collection<Column> expected[] = new Collection[]{
            CollectionUtil.addAll(new HashSet<Column>(), orig_hp_col),
            orig_vp_col.getAttributes()
        };
        Collection<Column> actual[] = new Collection[]{
            CollectionUtil.addAll(new HashSet<Column>(), item0.getHorizontalColumn()),
            item0.getVerticalPartitionColumns()
        };
        String labels[] = { "Horizontal", "Vertical" };
        for (int i = 0; i < expected.length; i++) {
            assertNotNull(labels[i], actual[i]);
            assertEquals(labels[i], expected[i].size(), actual[i].size());
            assertTrue(labels[i], actual[i].containsAll(expected[i]));
        } // FOR
        
        // Make another and make sure it's the same
        VerticalPartitionColumn item1 = VerticalPartitionColumn.get(orig_hp_col, orig_vp_col);
        assertNotNull(item1);
        assert(item0 == item1);
        assertEquals(item0.hashCode(), item1.hashCode());
        assertEquals(item0, item1);
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
        MultiProcParameter item0 = MultiProcParameter.get(params);
        assertNotNull(item0);
        assertEquals(catalog_proc, item0.getParent());
        assertEquals(num_params, item0.getIndex());
        for (int i = 0; i < params.length; i++) {
            assertNotNull(params[i].toString(), item0.get(i));
            assertEquals(params[i], item0.get(i));
        } // FOR
        
        // Make another and make sure it's the same
        MultiProcParameter item1 = MultiProcParameter.get(params);
        assertNotNull(item1);
        assert(item0 == item1);
        assertEquals(item0.hashCode(), item1.hashCode());
        assertEquals(item0, item1);
    }
}
