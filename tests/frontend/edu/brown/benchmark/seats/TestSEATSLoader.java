/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.seats;

import java.util.HashSet;
import java.util.Random;

import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.types.TimestampType;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;

public class TestSEATSLoader extends SEATSBaseTestCase {

    private MockSEATSLoader loader;

    private final double scale_factor = 1000;
    private final int num_airports = 10;
    private final int num_customers[] = new int[this.num_airports];
    private final int max_num_customers = 4;
    private final Random rand = new Random(0);
    private final HashSet<Long> customer_ids = new HashSet<Long>();
    private final HashSet<Long> flight_ids = new HashSet<Long>();
    private final long num_flights = 10l;
    private final TimestampType flightStartDate = new TimestampType(1262630005000l); // Monday 01.04.2010 13:33:25
    private final int flightPastDays = 7;
    private final int flightFutureDays = 14;
    
    
    class MockSEATSLoader extends SEATSLoader {
        public MockSEATSLoader(String[] args) {
            super(args);
        }
        @Override
        public CatalogContext getCatalogContext() {
            return (BaseTestCase.catalogContext);
        }
        @Override
        public ClientResponse loadVoltTable(String tableName, VoltTable vt) {
            assertNotNull(vt);
            return (null);
        }
        public int getHistogramCount() {
            return (this.profile.histograms.size());
        }
        public Histogram<String> getHistogram(String name) {
            return (this.profile.histograms.get(name));
        }
        public long getCustomerIdCount() {
            return (this.profile.getCustomerIdCount());
        }
        public Long getCustomerIdCount(Long airport_id) {
            return (this.profile.airport_max_customer_id.get(airport_id));
        }
        
//        @Override
//        protected Iterable<Object[]> getScalingIterable(Table catalog_tbl) {
//            if (catalog_tbl.getName().equalsIgnoreCase(SEATSConstants.TABLENAME_AIRPORT_DISTANCE)) {
//                return new AirportDistanceIterable(catalog_tbl, 0) {
//                    @Override
//                    protected boolean hasNext() {
//                        return (false);
//                    }
//                };
//            } else {
//                return super.getScalingIterable(catalog_tbl);
//            }
//        }
    }
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        String loaderArgs[] = {
            "CLIENT.SCALEFACTOR=" + scale_factor, 
            "HOST=localhost",
            "NUMCLIENTS=1",
            "NOCONNECTIONS=true",
            "BENCHMARK.DATADIR=" + AIRLINE_DATA_DIR,
        };
        loader = new MockSEATSLoader(loaderArgs);
        assertNotNull(loader);
    }
    
    private void initializeLoader(final MockSEATSLoader loader) {
        loader.profile.setFlightPastDays(flightPastDays);
        loader.profile.setFlightFutureDays(flightFutureDays);
        loader.profile.setFlightStartDate(flightStartDate);
        loader.profile.setFlightUpcomingDate(flightStartDate);
        
        long next_id = 1000l;
        for (long airport_id = 0; airport_id < num_airports; airport_id++) {
            num_customers[(int)airport_id] = rand.nextInt(max_num_customers) + 1;
            for (int customer_id = 0; customer_id < num_customers[(int)airport_id]; customer_id++) {
                loader.profile.incrementAirportCustomerCount(airport_id);
                customer_ids.add(next_id++);
            } // FOR
//            System.err.println(airport_id + ": " + this.num_customers[(int)airport_id] + " customers");
        } // FOR
//        System.err.println("------------------------");
        
        // Add a bunch of FlightIds
        int count = 0;
        for (long depart_airport_id = 0; depart_airport_id < num_airports; depart_airport_id++) {
            for (long arrive_airport_id = 0; arrive_airport_id < num_airports; arrive_airport_id++) {
                if (depart_airport_id == arrive_airport_id) continue;
                // int time_offset = rand.nextInt(86400000 * (int)flightFutureDays);
                // TimestampType flight_date = new TimestampType(flightStartDate.getTime() + time_offset);
                long id = count++; // , depart_airport_id, arrive_airport_id, flightStartDate, flight_date);
                this.flight_ids.add(id);
                if (count >= this.num_flights) break;
            } // FOR
            if (count >= this.num_flights) break;
        } // FOR
        assertEquals(this.num_flights, this.flight_ids.size());
    }
    
    
    /**
     * testIncrementAirportCustomerCount
     */
    public void testIncrementAirportCustomerCount() {
        this.initializeLoader(loader);
        for (long airport_id = 0; airport_id < this.num_airports; airport_id++) {
            Long cnt = loader.getCustomerIdCount(airport_id);
            assertNotNull(cnt);
            assertEquals(this.num_customers[(int)airport_id], cnt.intValue());
        } // FOR
    }
    
    /**
     * testLoadHistograms
     */
    public void testLoadHistograms() throws Exception {
        loader.loadHistograms(catalogContext);
        assertEquals(SEATSConstants.HISTOGRAM_DATA_FILES.length, loader.getHistogramCount());
        for (String histogram_name : SEATSConstants.HISTOGRAM_DATA_FILES) {
            Histogram<String> h = loader.getHistogram(histogram_name);
            assertNotNull(h);
            assertTrue(h.getSampleCount() > 0);
        } // FOR
    }
    
    /**
     * testGetFixedIterable
     */
    public void testGetFixedIterable() throws Exception {
        for (String table_name : SEATSConstants.TABLES_DATAFILES) {
            Table catalog_tbl = this.getTable(table_name);
            Iterable<Object[]> it = loader.getFixedIterable(catalog_tbl);
            assertNotNull(catalog_tbl.getName(), it);
            assertTrue(catalog_tbl.getName(), it.iterator().hasNext());
        } // FOR
    }


    /**
     * testToJSONString
     */
//    public void testToJSONString() throws Exception {
//        this.initializeLoader(loader);
//        String jsonString = loader.toJSONString();
//        for (FlightId flight_id : this.flight_ids) {
//            String encoded = Long.toString(flight_id.encode());
//            assertTrue(jsonString.contains(encoded));
//        } // FOR
//    }
//    
//    /**
//     * testFromJSONString
//     */
//    public void testFromJSONString() throws Exception {
//        String jsonString = loader.toJSONString();
//        JSONObject jsonObject = new JSONObject(jsonString);
//        
//        MockSEATSLoader clone = new MockSEATSLoader(new String[]{ "BENCHMARK.DATADIR=" + AIRLINE_DATA_DIR });
//        clone.fromJSON(jsonObject, null);
//        
//        assertEquals(loader.getCustomerIdCount(), clone.getCustomerIdCount());
//        assertEquals(loader.getFlightIdCount(), clone.getFlightIdCount());
//        assertEquals(loader.getFlightStartDate(), clone.getFlightStartDate());
//        
//        for (FlightId clone_id : clone.getFlightIds()) {
//            assert(this.flight_ids.contains(clone_id)) : "Unknown flight id " + clone_id;
//        } // FOR
//        
//    }
    
}