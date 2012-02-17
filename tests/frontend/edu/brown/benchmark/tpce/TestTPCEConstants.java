package edu.brown.benchmark.tpce;

import java.lang.reflect.Field;

import junit.framework.TestCase;

public class TestTPCEConstants extends TestCase {

    /**
     * testProcedureFrequencies
     */
    public void testProcedureFrequencies() throws Exception {
        // Gather the list of txn frequencies and make sure it adds up to 100
        int total = 0;
        for (Field field_handle : TPCEConstants.class.getFields()) {
            String field_name = field_handle.getName();
            if (field_name.startsWith("FREQUENCY_")) {
                Integer field_val = (Integer) field_handle.get(null);
                assertNotNull("Null value for " + field_name, field_val);
                if (field_val >= 0)
                    total += field_val;
            }
        } // FOR
        assertEquals(100, total);
    }

}
