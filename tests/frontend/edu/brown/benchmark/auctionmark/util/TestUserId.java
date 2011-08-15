package edu.brown.benchmark.auctionmark.util;

import java.util.Random;

import junit.framework.TestCase;

public class TestUserId extends TestCase {

    private static final Random rand = new Random();
    
    /**
     * testUserId
     */
    public void testUserId() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                UserId user_id = new UserId(size, offset);
                assertNotNull(user_id);
                assertEquals(size, user_id.getSize());
                assertEquals(offset, user_id.getOffset());
            } // FOR
        } // FOR
    }
    
    /**
     * testUserIdEncode
     */
    public void testUserIdEncode() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                long encoded = new UserId(size, offset).encode();
                assert(encoded >= 0);
                
                UserId user_id = new UserId(encoded);
                assertNotNull(user_id);
                assertEquals(size, user_id.getSize());
                assertEquals(offset, user_id.getOffset());
            } // FOR
        } // FOR
    }
    
    /**
     * testUserIdDecode
     */
    public void testUserIdDecode() {
        for (int i = 0; i < 100; i++) {
            int size = rand.nextInt(10000);
            for (int offset = 0; offset < 10; offset++) {
                long values[] = { size, offset };
                long encoded = new UserId(size, offset).encode();
                assert(encoded >= 0);
                
                long new_values[] = new UserId(encoded).toArray();
                assertEquals(values.length, new_values.length);
                for (int j = 0; j < new_values.length; j++) {
                    assertEquals(values[j], new_values[j]);
                } // FOR
            } // FOR
        } // FOR
    }
}