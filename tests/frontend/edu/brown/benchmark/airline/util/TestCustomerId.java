package edu.brown.benchmark.airline.util;

import junit.framework.TestCase;

public class TestCustomerId extends TestCase {

    private final long base_ids[]    = { 66666, 77777, 88888 };  
    private final long airport_ids[] = { 123, 1234, 12345 };
    
    /**
     * testCustomerId
     */
    public void testCustomerId() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                CustomerId customer_id = new CustomerId(base_id, airport_id);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }
    
    /**
     * testCustomerIdEncode
     */
    public void testCustomerIdEncode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                long values[] = { base_id, airport_id };
                long encoded = CustomerId.encode(values);
//                System.err.println("base_id=" + base_id);
//                System.err.println("airport_id=" + airport_id);
//                System.err.println("encodd=" + encoded);
//                System.exit(1);
                assert(encoded >= 0);
                
                CustomerId customer_id = new CustomerId(encoded);
                assertNotNull(customer_id);
                assertEquals(base_id, customer_id.getId());
                assertEquals(airport_id, customer_id.getDepartAirportId());
            } // FOR
        } // FOR
    }
    
    /**
     * testCustomerIdDecode
     */
    public void testCustomerIdDecode() {
        for (long base_id : this.base_ids) {
            for (long airport_id : this.airport_ids) {
                long values[] = { base_id, airport_id };
                long encoded = CustomerId.encode(values);
                assert(encoded >= 0);

                long new_values[] = CustomerId.decode(encoded);
                assertEquals(values.length, new_values.length);
                for (int i = 0; i < new_values.length; i++) {
                    assertEquals(values[i], new_values[i]);
                } // FOR
            } // FOR
        } // FOR
    }
}