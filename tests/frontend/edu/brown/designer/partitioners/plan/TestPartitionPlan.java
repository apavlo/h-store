package edu.brown.designer.partitioners.plan;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.benchmark.tpcc.procedures.delivery;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.ostatByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

public class TestPartitionPlan extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
    }
    
    /**
     * testGetChangedEntries
     */
    @Test
    public void testGetChangedEntries() {
        Table catalog_tbl = this.getTable("WAREHOUSE");
        Column catalog_col = this.getColumn(catalog_tbl, "W_NAME");

        // First check they are the same
        final PartitionPlan pplan0 = PartitionPlan.createFromCatalog(catalog_db);
        assertNotNull(pplan0);
        final PartitionPlan pplan1 = PartitionPlan.createFromCatalog(catalog_db);
        assertNotNull(pplan1);
        Collection<CatalogType> changed = pplan0.getChangedEntries(pplan1);
        assertNotNull(changed);
        assert(changed.isEmpty()) : changed;
        
        // Now change the table's partitioning column and check that it comes back as changed
        TableEntry pentry = pplan1.getTableEntry(catalog_tbl);
        pentry.setAttribute(catalog_col);
        changed = pplan0.getChangedEntries(pplan1);
        assertNotNull(changed);
        assertEquals(1, changed.size());
        assertEquals(catalog_tbl, CollectionUtil.first(changed));
        changed = pplan1.getChangedEntries(pplan0);
        assertNotNull(changed);
        assertEquals(1, changed.size());
        assertEquals(catalog_tbl, CollectionUtil.first(changed));

        // Remove the entry from pplan1 and make sure that it comes back as changed for pplan0 but not pplan1
        pplan1.getTableEntries().remove(catalog_tbl);
        changed = pplan0.getChangedEntries(pplan1);
        assertNotNull(changed);
        assertEquals(1, changed.size());
        assertEquals(catalog_tbl, CollectionUtil.first(changed));
        changed = pplan1.getChangedEntries(pplan0);
        assertNotNull(changed);
        assert(changed.isEmpty()) : changed;
    }
    
    /**
     * testMultiColumn
     */
    @Test
    public void testMultiColumn() throws Exception {
        Map<CatalogType, CatalogType> m = new HashMap<CatalogType, CatalogType>();
        
        // Replication!
        m.put(this.getTable("WAREHOUSE"), ReplicatedColumn.get(this.getTable("WAREHOUSE")));
        m.put(this.getTable("ITEM"), ReplicatedColumn.get(this.getTable("ITEM")));
        
        // Straight-up!
        m.put(this.getTable("STOCK"), this.getColumn("STOCK", "S_W_ID"));
        
        // Multi-Column!
        m.put(this.getTable("DISTRICT"),    MultiColumn.get(this.getColumn("DISTRICT", "D_W_ID"),
                                                            this.getColumn("DISTRICT", "D_ID")));
        m.put(this.getTable("CUSTOMER"),    MultiColumn.get(this.getColumn("CUSTOMER", "C_W_ID"),
                                                            this.getColumn("CUSTOMER", "C_D_ID")));
        m.put(this.getTable("ORDERS"),      MultiColumn.get(this.getColumn("ORDERS", "O_W_ID"),
                                                            this.getColumn("ORDERS", "O_D_ID")));
        m.put(this.getTable("ORDER_LINE"),  MultiColumn.get(this.getColumn("ORDER_LINE", "OL_W_ID"),
                                                            this.getColumn("ORDER_LINE", "OL_D_ID")));
        m.put(this.getTable("NEW_ORDER"),   MultiColumn.get(this.getColumn("NEW_ORDER", "NO_W_ID"),
                                                            this.getColumn("NEW_ORDER", "NO_D_ID")));
        m.put(this.getTable("HISTORY"),     MultiColumn.get(this.getColumn("HISTORY", "H_W_ID"),
                                                            this.getColumn("HISTORY", "H_D_ID")));

        // Procedures!
        m.put(this.getProcedure(delivery.class), this.getProcParameter(delivery.class, 0));
        
        // MultiProcParameters!
        // IMPORTANT: The ordering of columns in the tables is <D_ID, W_ID> but the ordering of the params (according
        // to their index values) is <W_ID, D_ID>. We need to make the two orders match! 
        m.put(this.getProcedure(neworder.class),
                MultiProcParameter.get(this.getProcParameter(neworder.class, 1),
                                       this.getProcParameter(neworder.class, 0)));
        m.put(this.getProcedure(ostatByCustomerId.class),
                MultiProcParameter.get(this.getProcParameter(ostatByCustomerId.class, 1),
                                       this.getProcParameter(ostatByCustomerId.class, 0)));
        m.put(this.getProcedure(ostatByCustomerName.class),
                MultiProcParameter.get(this.getProcParameter(ostatByCustomerName.class, 1),
                                       this.getProcParameter(ostatByCustomerName.class, 0)));
        m.put(this.getProcedure(paymentByCustomerId.class),
                MultiProcParameter.get(this.getProcParameter(paymentByCustomerId.class, 1),
                                       this.getProcParameter(paymentByCustomerId.class, 0)));
//        m.put(this.getProcedure(paymentByCustomerIdC.class),
//                MultiProcParameter.get(this.getProcParameter(paymentByCustomerIdC.class, 1),
//                                       this.getProcParameter(paymentByCustomerIdC.class, 0)));
//        m.put(this.getProcedure(paymentByCustomerIdW.class),
//                MultiProcParameter.get(this.getProcParameter(paymentByCustomerIdW.class, 1),
//                                       this.getProcParameter(paymentByCustomerIdW.class, 0)));
        m.put(this.getProcedure(paymentByCustomerName.class),
                MultiProcParameter.get(this.getProcParameter(paymentByCustomerName.class, 1),
                                       this.getProcParameter(paymentByCustomerName.class, 0)));
//        m.put(this.getProcedure(paymentByCustomerNameC.class),
//                MultiProcParameter.get(this.getProcParameter(paymentByCustomerNameC.class, 1),
//                                       this.getProcParameter(paymentByCustomerNameC.class, 0)));
//        m.put(this.getProcedure(paymentByCustomerNameW.class),
//                MultiProcParameter.get(this.getProcParameter(paymentByCustomerNameW.class, 1),
//                                       this.getProcParameter(paymentByCustomerNameW.class, 0)));
        m.put(this.getProcedure(slev.class),
                MultiProcParameter.get(this.getProcParameter(slev.class, 1),
                                       this.getProcParameter(slev.class, 0)));

        PartitionPlan pplan = PartitionPlan.createFromMap(m);
        assertNotNull(pplan);
        // pplan.save("/tmp/tpcc.50w.manual.pplan");
        
        String json = pplan.toJSONString();
        assertFalse(json.isEmpty());
        JSONObject json_object = new JSONObject(json);
        assertNotNull(json_object);
        
        PartitionPlan clone = new PartitionPlan();
        clone.fromJSON(json_object, catalog_db);
        if (pplan.equals(clone) == false) {
            System.err.println(StringUtil.columns("ORIGINAL:\n" + pplan, "CLONE:\n" + clone));
        }
        assertEquals("Clone failed", pplan, clone);
        // System.err.println(clone);
    }
}
