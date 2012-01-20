package edu.brown.benchmark.auctionmark;

import java.util.LinkedList;
import java.util.Random;

import org.json.JSONObject;
import org.junit.Test;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestAuctionMarkProfile extends BaseTestCase {

    private static final int NUM_CLIENTS = 10;
    private static final AbstractRandomGenerator RAND = new DefaultRandomGenerator();
    
    private final AuctionMarkProfile profile = new AuctionMarkProfile(RAND, NUM_CLIENTS);
    private final Random rand = new Random();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.AUCTIONMARK);
        
        for (String table_name : AuctionMarkConstants.TABLENAMES) {
            long count = Math.abs(this.rand.nextInt()) + 1;
            assert(count > 0);
//            this.profile.setTableSize(table_name, count);
//            assertEquals(count, this.profile.getTableSize(table_name));
        } // FOR
        
        profile.setAndGetBenchmarkStartTime();
    }
    
    /**
     * testAddToTableSize
     */
//    @Test
//    public void testAddToTableSize() {
//        for (String table_name : AuctionMarkConstants.TABLENAMES) {
//            long count = this.profile.getTableSize(table_name);
//            assert(count > 0) : "Unexpected count for " + table_name;
//            long delta = this.rand.nextInt(10000);
//            this.profile.addToTableSize(table_name, delta);
//            assertEquals(count + delta, this.profile.getTableSize(table_name));
//        } // FOR
//    }
    
//    /**
//     * testSerialization
//     */
//    @SuppressWarnings("unchecked")
//    @Test
//    public void testSerialization() throws Exception {
//        // Throw in some ItemInfos
//        for (int i = 0; i < 100; i++) {
//            ItemId itemId = new ItemId(rand.nextInt(10000), rand.nextInt(1000));
//            TimestampType endDate = new TimestampType();
//            double currentPrice = rand.nextDouble();
//            short numBids = (short)rand.nextInt(100);
//            ItemInfo itemInfo = new ItemInfo(itemId, currentPrice, endDate, numBids);
//            this.profile.addItemToProperQueue(itemInfo, true);
//        } // FOR
//        
//        String json_string = this.profile.toJSONString();
//        assertNotNull(json_string);
//        assertFalse(json_string.isEmpty());
//        
//        AuctionMarkBenchmarkProfile clone = new AuctionMarkBenchmarkProfile(RAND, NUM_CLIENTS);
//        clone.fromJSON(new JSONObject(json_string), catalog_db);
//        //assertNotNull(clone.items_per_user);
//        assertNotNull(clone.item_category_histogram);
//
//        for (String table_name : AuctionMarkConstants.TABLENAMES) {
//            assertEquals(this.profile.getTableSize(table_name), clone.getTableSize(table_name));
//        } // FOR
//        
//        for (Long value : this.profile.item_category_histogram.values()) {
//            assert(clone.item_category_histogram.contains(value));
//            assertEquals(this.profile.item_category_histogram.get(value), clone.item_category_histogram.get(value));
//        } // FOR
//        
//        LinkedList<ItemInfo> expected[] = new LinkedList[]{
//            profile.items_available,
//            profile.items_endingSoon,
//            profile.items_waitingForPurchase,
//            profile.items_completed,
//        };
//        LinkedList<ItemInfo> actual[] = new LinkedList[]{
//            clone.items_available,
//            clone.items_endingSoon,
//            clone.items_waitingForPurchase,
//            clone.items_completed,
//        };
//        for (int i = 0; i < expected.length; i++) {
////            System.err.println(StringUtil.columns(StringUtil.join("\n", expected[i]),
////                                                  StringUtil.join("\n", actual[i])
////            ));
////            System.err.println(StringUtil.SINGLE_LINE);
//            
//            assertEquals("["+i+"]", expected[i].size(), actual[i].size());
//            
//            for (int ii = 0, cnt = expected[i].size(); ii < cnt; ii++) {
//                ItemInfo itemInfo0 = expected[i].get(ii);
//                assertNotNull(itemInfo0);
//                ItemInfo itemInfo1 = actual[i].get(ii);
//                assertNotNull(itemInfo1);
//                
//                String debug = String.format("[%d, %d]: %s <-> %s", i, ii, itemInfo0, itemInfo1); 
//                assertEquals(debug, itemInfo0.hashCode(), itemInfo1.hashCode());
//                assertTrue(debug, itemInfo0.equals(itemInfo0));
//                assertTrue(debug, itemInfo1.equals(itemInfo1));
//
//                assertEquals(debug, itemInfo0, itemInfo1);
//                assertEquals(debug, 0, itemInfo0.compareTo(itemInfo1));
//                assertEquals(debug, itemInfo0.hasCurrentPrice(), itemInfo1.hasCurrentPrice());
//                assertEquals(debug, itemInfo0.hasEndDate(), itemInfo1.hasEndDate());
//                assertEquals(debug, itemInfo0.getEndDate(), itemInfo1.getEndDate());
//                assertEquals(debug, (float)itemInfo0.getCurrentPrice(), (float)itemInfo1.getCurrentPrice(), 0.001);
//            } // FOR
//        } // FOR
//    }
}
