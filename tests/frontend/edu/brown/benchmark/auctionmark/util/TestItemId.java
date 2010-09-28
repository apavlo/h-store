package edu.brown.benchmark.auctionmark.util;

import junit.framework.TestCase;

public class TestItemId extends TestCase {

    private final long user_ids[]    = { 66666, 77777, 88888 };
    private final int num_items = 10;
    
    /**
     * testItemId
     */
    public void testItemId() {
        for (long user_id : this.user_ids) {
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                ItemId customer_id = new ItemId(user_id, item_ctr);
                assertNotNull(customer_id);
                assertEquals(user_id, customer_id.getUserId());
                assertEquals(item_ctr, customer_id.getItemCtr());
            } // FOR
        } // FOR
    }
    
    /**
     * testItemIdEncode
     */
    public void testItemIdEncode() {
        for (long user_id : this.user_ids) {
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                long values[] = { user_id, item_ctr };
                long encoded = ItemId.encode(values);
//                System.err.println("user_id=" + user_id);
//                System.err.println("item_ctr=" + item_ctr);
//                System.err.println("encoded=" + encoded);
//                System.exit(1);
                assert(encoded >= 0);
                
                ItemId customer_id = new ItemId(encoded);
                assertNotNull(customer_id);
                assertEquals(user_id, customer_id.getUserId());
                assertEquals(item_ctr, customer_id.getItemCtr());
            } // FOR
        } // FOR
    }
    
    /**
     * testItemIdDecode
     */
    public void testItemIdDecode() {
        for (long user_id : this.user_ids) {
            for (int item_ctr = 0; item_ctr < num_items; item_ctr++) {
                long values[] = { user_id, item_ctr };
                long encoded = ItemId.encode(values);
                assert(encoded >= 0);

                long new_values[] = ItemId.decode(encoded);
                assertEquals(values.length, new_values.length);
                for (int i = 0; i < new_values.length; i++) {
                    assertEquals(values[i], new_values[i]);
                } // FOR
            } // FOR
        } // FOR
    }
}