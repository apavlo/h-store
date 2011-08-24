package edu.brown.workload;

import java.util.*;

import org.json.JSONObject;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.*;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.utils.JSONUtil;
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
        params = param_pair.getFirst();
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
        is_array = param_pair.getSecond();
        xact = new TransactionTrace(12345, catalog_proc, params);
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

    /**
     * testSetOutputObjectArrays
     */
    public void testSetOutputObjectArrays() throws Exception {
        Object output[][] = new Object[][] {
            { new Long(1), new Long(2), new Float(2.0f) },
            { new Long(3), new Long(4), new Float(6.0f) },
        };
        assertFalse(xact.hasOutput());
        xact.setOutput(output);
        assert(xact.hasOutput());
        
        VoltType expected[] = { VoltType.BIGINT, VoltType.BIGINT, VoltType.FLOAT };
        VoltType actual[] = xact.getOutputTypes(0);
        assertNotNull(actual);
        assertEquals(expected.length, actual.length);
        for (int i = 0; i < expected.length; i++) {
            assertEquals(Integer.toString(i), expected[i], actual[i]);
        } // FOR
    }
    
    /**
     * testSetOutputNull
     */
    public void testSetOutputNull() throws Exception {
        assertFalse(xact.hasOutput());
//        xact.setOutput((Object[][])null);
        
        xact.setOutput((VoltTable)null);
        
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
        System.err.println(JSONUtil.format(jsonObject));
    }
    
    /**
     * testSetOutputVoltTable
     */
    public void testSetOutputVoltTable() throws Exception {
        int num_rows = 20;
        int num_cols = 4;
        
        VoltTable vt = new VoltTable(
            new ColumnInfo("col0", VoltType.BIGINT),
            new ColumnInfo("col1", VoltType.BIGINT),
            new ColumnInfo("col2", VoltType.TIMESTAMP),
            new ColumnInfo("col3", VoltType.FLOAT)
        );
        assertEquals(num_cols, vt.getColumnCount());
        
        for (int i = 0; i < num_rows; i++) {
            Object row[] = new Object[num_cols];
            for (int j = 0; j < num_cols; j++) {
                row[j] = VoltTypeUtil.getRandomValue(vt.getColumnType(j));
            } // FOR
            vt.addRow(row);
        } // FOR
        assertEquals(num_rows, vt.getRowCount());
        
        assertFalse(xact.hasOutput());
        xact.setOutput(vt);
        assert(xact.hasOutput());
        
        VoltType types[] = xact.getOutputTypes(0);
        assertNotNull(types);
        Object output[][] = xact.getOutput(0);
        assertNotNull(output);
        for (int j = 0; j < num_cols; j++) {
            assertEquals(vt.getColumnName(j), vt.getColumnType(j), types[j]);
            assertNotNull(vt.getColumnName(j), output[0][j]);
        }
        
//        String json = xact.toJSONString(catalog_db);
//        System.err.println(JSONUtil.format(json));
    }
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        for (AbstractTraceElement.Members element : AbstractTraceElement.Members.values()) {
            if (element != AbstractTraceElement.Members.WEIGHT) {
                assertTrue(element.toString(), json.indexOf(element.name()) != -1);
            }
        } // FOR
        for (TransactionTrace.Members element : TransactionTrace.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }

    /**
     * testFromJSONString
     */
    public void testFromJSONString() throws Exception {
        Object output[][] = new Object[][] {
            { new Long(1), new String("ABC"), new Long(2), new Float(2.0f) },
            { new Long(3), new String("XYZ"), new Long(4), new Float(6.0f) },
            { new Long(5), new String("123"), new Long(2), new Float(18.0f) },
        };
        assertFalse(xact.hasOutput());
        xact.setOutput(output);
        assert(xact.hasOutput());
        
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
//        System.err.println(JSONUtil.format(jsonObject));
        
        TransactionTrace copy = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
        
        assertEquals(xact.catalog_item_name, copy.catalog_item_name);
        assertEquals(xact.getTransactionId(), copy.getTransactionId());
        assertEquals(xact.start_timestamp, copy.start_timestamp);
        assertEquals(xact.getQueryCount(), copy.getQueryCount());
        assertEquals(xact.getBatchCount(), copy.getBatchCount());
        
        // OUTPUT
        assertEquals(xact.hasOutput(), copy.hasOutput());
        Object copy_output[][] = copy.getOutput(0);
        assertNotNull(copy_output);
        assertEquals(output.length, copy_output.length);
        for (int i = 0; i < output.length; i++) {
            Object orig_row[] = output[i];
            assertNotNull(orig_row);
            Object copy_row[] = copy_output[i];
            assertNotNull(copy_row);
            assertEquals(orig_row.length, copy_row.length);
            for (int j = 0; j < orig_row.length; j++) {
                assertEquals(String.format("[%d, %d]", i, j), orig_row[j].toString(), copy_row[j].toString());
            } // FOR
        } // FOR
        
        // OUTPUT TYPES
        VoltType expected_types[] = xact.getOutputTypes(0);
        assertNotNull(expected_types);
        VoltType actual_types[] = xact.getOutputTypes(0);
        assertNotNull(actual_types);
        assertEquals(expected_types.length, actual_types.length);
        for (int i = 0; i < expected_types.length; i++) {
            assertEquals(Integer.toString(i), expected_types[i], actual_types[i]);
        } // FOR
        
        // PARAMS
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
