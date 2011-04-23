package edu.brown.benchmark.auctionmark;

import java.util.Random;

import org.json.JSONObject;
import org.junit.Test;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestAuctionMarkBenchmarkProfile extends BaseTestCase {

    private final AuctionMarkBenchmarkProfile profile = new AuctionMarkBenchmarkProfile();
    private final Random rand = new Random();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        
        for (String table_name : AuctionMarkConstants.TABLENAMES) {
            long count = Math.abs(this.rand.nextInt()) + 1;
            assert(count > 0);
            this.profile.setTableSize(table_name, count);
            assertEquals(count, this.profile.getTableSize(table_name));
        } // FOR
    }
    
    /**
     * testAddToTableSize
     */
    @Test
    public void testAddToTableSize() {
        for (String table_name : AuctionMarkConstants.TABLENAMES) {
            long count = this.profile.getTableSize(table_name);
            assert(count > 0) : "Unexpected count for " + table_name;
            long delta = this.rand.nextInt();
            this.profile.addToTableSize(table_name, delta);
            assertEquals(count + delta, this.profile.getTableSize(table_name));
        } // FOR
    }
    
    /**
     * testSerialization
     */
    @Test
    public void testSerialization() throws Exception {
        String json_string = this.profile.toJSONString();
        
        AuctionMarkBenchmarkProfile clone = new AuctionMarkBenchmarkProfile();
        clone.fromJSON(new JSONObject(json_string), catalog_db);
        //assertNotNull(clone.items_per_user);
        assertNotNull(clone.item_category_histogram);

        for (String table_name : AuctionMarkConstants.TABLENAMES) {
            assertEquals(this.profile.getTableSize(table_name), clone.getTableSize(table_name));
        } // FOR
        
        for (Long value : this.profile.item_category_histogram.values()) {
            assert(clone.item_category_histogram.contains(value));
            assertEquals(this.profile.item_category_histogram.get(value), clone.item_category_histogram.get(value));
        } // FOR
        
        /*
        for (Object value : this.profile.items_per_user.values()) {
            assert(clone.items_per_user.containsKey(value));
            assertEquals(this.profile.items_per_user.get(value), clone.items_per_user.get(value));
        } // FOR
        */
    }
}
