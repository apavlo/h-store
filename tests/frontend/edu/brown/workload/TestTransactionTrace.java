package edu.brown.workload;

import java.util.*;

import org.json.JSONObject;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTypeUtil;

import com.sun.tools.javac.util.Pair;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestTransactionTrace extends BaseTestCase {

    protected final static Random rand = new Random(1);
    protected Procedure catalog_proc;
    protected TransactionTrace xact;
    protected Object params[];
    protected boolean is_array[];
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, false);
        catalog_proc = this.getProcedure("neworder");
        
        List<ProcParameter> catalog_params = CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index");
        Pair<Object[], boolean[]> param_pair = this.makeParams(catalog_params, "type");
        params = param_pair.fst;
        /*
        for (Object obj : params) {
            if (obj instanceof Object[]) {
                System.out.print("[");
                String add = "";
                for (Object inner : (Object[])obj) {
                    System.out.print(add + inner);
                    add = ", ";
                }
                System.out.println("]");
            } else {
                System.out.println(obj);
            }
        }
        System.exit(1);*/
        is_array = param_pair.snd;
        xact = new TransactionTrace("XYZ", catalog_proc, params);
        assertNotNull(xact);
//        System.out.println("CREATED: " + xact);
    }
    
    protected <T extends CatalogType> Pair<Object[], boolean[]> makeParams(List<T> catalog_params, String type_name) {
        Object params[] = new Object[catalog_params.size()];
        boolean is_array[] = new boolean[catalog_params.size()];
        int array_size = rand.nextInt(10);
        for (int i = 0; i < params.length; i++) {
            VoltType type = VoltType.get(((Integer)catalog_params.get(i).getField(type_name)).byteValue());
            //System.out.println(i + "[" + type_name + "-" + catalog_params.get(i).getField(type_name) + "]: " + type);
            Object param_is_array = catalog_params.get(i).getField("isarray"); 
            if (param_is_array != null && (Boolean)param_is_array) {
                is_array[i] = true;
                Object inner[] = new Object[array_size];
                for (int j = 0; j < inner.length; j++) {
                    inner[j] = VoltTypeUtil.getRandomValue(type);
                } // FOR
                params[i] = inner;
            } else {
                is_array[i] = false;
                params[i] = VoltTypeUtil.getRandomValue(type);
            }
        } // FOR
        return (new Pair<Object[], boolean[]>(params, is_array));
    }
    
    protected void compare(Object param0, Object param1) {
        // Workaround for comparing dates...
        if (param0 instanceof TimestampType) {
            assertEquals(((TimestampType)param0).toString(), ((TimestampType)param1).toString());
        } else {
            assertEquals(param0.toString(), param1.toString());
//            assertEquals(param0, param1);
        }
        return;
    }

    public void testToJSONString() throws Exception {
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        for (AbstractTraceElement.Members element : AbstractTraceElement.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
        for (TransactionTrace.Members element : TransactionTrace.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }

    public void testFromJSONString() throws Exception {
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        TransactionTrace copy = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
        
        assertEquals(xact.catalog_item_name, copy.catalog_item_name);
        assertEquals(xact.id, copy.id);
        assertEquals(xact.xact_id, copy.xact_id);
        assertEquals(xact.start_timestamp, copy.start_timestamp);
        
        assertEquals(xact.params.length, copy.params.length);
        for (int i = 0; i < xact.params.length; i++) {
            if (is_array[i]) {
                Object inner0[] = (Object[])xact.params[i];
                Object inner1[] = (Object[])copy.params[i];
                for (int j = 0; j < inner0.length; j++) {
                    this.compare(inner0[j], inner1[j]);
                } // FOR
            } else {
                this.compare(xact.params[i], copy.params[i]);    
            }
        } // FOR
    }
    
}
