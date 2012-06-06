package edu.brown.designer.partitioners.plan;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.catalog.CatalogType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.*;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.utils.ProjectType;

public class TestPartitionPlanTM1 extends BaseTestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }
    
    /**
     * testMultiColumn
     */
    @Test
    public void testMultiColumn() throws Exception {
        Map<CatalogType, CatalogType> m = new HashMap<CatalogType, CatalogType>();
        
        // Replication
        m.put(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER), this.getColumn(TM1Constants.TABLENAME_SUBSCRIBER, "S_ID"));
        
        m.put(this.getTable(TM1Constants.TABLENAME_ACCESS_INFO),
                ReplicatedColumn.get(this.getTable(TM1Constants.TABLENAME_ACCESS_INFO)));
        
        // Multi-Column!
        m.put(this.getTable(TM1Constants.TABLENAME_SPECIAL_FACILITY),
                MultiColumn.get(this.getColumn(TM1Constants.TABLENAME_SPECIAL_FACILITY, "S_ID"),
                                this.getColumn(TM1Constants.TABLENAME_SPECIAL_FACILITY, "SF_TYPE")));
        m.put(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING),
                MultiColumn.get(this.getColumn(TM1Constants.TABLENAME_CALL_FORWARDING, "S_ID"),
                                this.getColumn(TM1Constants.TABLENAME_CALL_FORWARDING, "SF_TYPE")));

        // Procedures!
        m.put(this.getProcedure(DeleteCallForwarding.class), this.getProcParameter(DeleteCallForwarding.class, 0));
        m.put(this.getProcedure(GetAccessData.class), this.getProcParameter(GetAccessData.class, 0));
        m.put(this.getProcedure(GetSubscriberData.class), this.getProcParameter(GetSubscriberData.class, 0));
        m.put(this.getProcedure(InsertCallForwarding.class), this.getProcParameter(InsertCallForwarding.class, 0));
//        m.put(this.getProcedure(InsertSubscriber.class), this.getProcParameter(InsertSubscriber.class, 0));
        m.put(this.getProcedure(UpdateLocation.class), this.getProcParameter(UpdateLocation.class, 1));
        
        // MultiProcParameters!
        m.put(this.getProcedure(GetNewDestination.class),
                MultiProcParameter.get(this.getProcParameter(GetNewDestination.class, 1),
                                       this.getProcParameter(GetNewDestination.class, 0)));
        m.put(this.getProcedure(UpdateSubscriberData.class),
                MultiProcParameter.get(this.getProcParameter(UpdateSubscriberData.class, 0),
                                       this.getProcParameter(UpdateSubscriberData.class, 3)));

        PartitionPlan pplan = PartitionPlan.createFromMap(m);
        assertNotNull(pplan);
//        pplan.save("/tmp/tm1.manual.pplan");
        
        String json = pplan.toJSONString();
        assertFalse(json.isEmpty());
        JSONObject json_object = new JSONObject(json);
        assertNotNull(json_object);
        
        PartitionPlan clone = new PartitionPlan();
        clone.fromJSON(json_object, catalog_db);
        System.err.println(clone);
        
    }
}
