package edu.brown.benchmark.auctionmark.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import junit.framework.TestCase;

import org.junit.Before;

import edu.brown.benchmark.auctionmark.AuctionMarkConstants;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;

/**
 * 
 * @author pavlo
 */
public class TestUserIdGenerator extends TestCase {

    private static final int NUM_CLIENTS = 10;
    private static final int NUM_USERS = 1000;
    
    private static final Zipf randomNumItems = new Zipf(new Random(),
            AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
            AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER,
            1.0001);
    
    private final Histogram<Integer> users_per_item_count = new Histogram<Integer>();
	
	@Before
	public void setUp() throws Exception {
        for (long i = 0; i < NUM_USERS; i++) {
            this.users_per_item_count.put(randomNumItems.nextInt());
        } // FOR
        assertEquals(NUM_USERS, this.users_per_item_count.getSampleCount());
	}
	
	/**
	 * testAllUsers
	 */
	public void testAllUsers() throws Exception {
	    UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS);
	    Set<UserId> seen = new HashSet<UserId>();
	    assert(generator.hasNext());
	    for (UserId u_id : CollectionUtil.wrapIterator(generator)) {
	        assertNotNull(u_id);
	        assert(seen.contains(u_id) == false) : "Duplicate UserId " + u_id;
	        seen.add(u_id);
//	        System.err.println(u_id);
	    } // FOR
	    assertEquals(NUM_USERS, seen.size());
	}

	/**
	 * 
	 * @throws Exception
	 */
	public void testPerClient() throws Exception {
	    Histogram<Integer> clients_h = new Histogram<Integer>();
	    Set<UserId> all_seen = new HashSet<UserId>();
	    for (int client = 0; client < NUM_CLIENTS; client++) {
    	    UserIdGenerator generator = new UserIdGenerator(users_per_item_count, NUM_CLIENTS, client);
            Set<UserId> seen = new HashSet<UserId>();
            assert(generator.hasNext());
            for (UserId u_id : CollectionUtil.wrapIterator(generator)) {
                assertNotNull(u_id);
                assert(seen.contains(u_id) == false) : "Duplicate UserId " + u_id;
                assert(all_seen.contains(u_id) == false) : "Duplicate UserId " + u_id;
                seen.add(u_id);
                all_seen.add(u_id);
            } // FOR
            assertThat(Integer.toString(client), NUM_USERS, not(equalTo(seen.size())));
            assertFalse(Integer.toString(client), seen.isEmpty());
            clients_h.put(client, seen.size());
	    } // FOR
	    assertEquals(NUM_USERS, all_seen.size());
	    
	    // Make sure that they all have the same number of UserIds
	    Long last_cnt = null; 
	    for (Integer client : clients_h.values()) {
	        if (last_cnt != null) {
	            assertEquals(client.toString(), last_cnt, clients_h.get(client));
	        }
	        last_cnt = clients_h.get(client);
	    } // FOR
	    System.err.println(clients_h);
	}

}
