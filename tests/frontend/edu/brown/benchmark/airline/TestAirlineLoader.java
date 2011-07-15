package edu.brown.benchmark.airline;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.catalog.Table;

public class TestAirlineLoader extends AirlineBaseTestCase {

    protected static class MockAuctionMarkLoader extends AirlineLoader {

        public MockAuctionMarkLoader(String[] args) {
            super(args);
            // TODO Auto-generated constructor stub
        }
        
        @Override
        public void loadTable(Table catalogTbl, Iterable<Object[]> iterable, int batchSize) {
            // TODO Auto-generated method stub
            super.loadTable(catalogTbl, iterable, batchSize);
        }
        
    }
    
    /**
     * testLoader
     */
    public void testLoader() throws Exception {
        
    }
 
    
//    public void testTimeCode() throws Exception {
//        String code = "09:45";
//        Pattern time_pattern = Pattern.compile("([\\d]{2,2}):([\\d]{2,2})");
//        Matcher m = time_pattern.matcher(code);
//        assert(m.find());
//        System.out.println(m);
//        System.out.println("hour=" + m.group(1));
//        System.out.println("minute=" + m.group(2));
//    }
}