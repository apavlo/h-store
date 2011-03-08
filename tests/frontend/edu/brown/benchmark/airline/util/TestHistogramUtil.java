package edu.brown.benchmark.airline.util;

import java.util.Map;
import java.util.regex.Pattern;

import edu.brown.benchmark.airline.AirlineBaseTestCase;
import edu.brown.benchmark.airline.AirlineConstants;
import edu.brown.statistics.Histogram;

public class TestHistogramUtil extends AirlineBaseTestCase {

    /**
     * testLoadAirportFlights
     */
    public void testLoadAirportFlights() throws Exception {
        Map<String, Histogram> histograms = HistogramUtil.loadAirportFlights(AIRLINE_DATA_DIR);
        assertNotNull(histograms);
        assertFalse(histograms.isEmpty());
        assert(histograms.size() >= 200);
        
        // Just some airports that we expect to be in there
        String airports[] = { "BWI", "LAX", "JFK", "MDW", "ATL", "SFO", "ORD" };
        for (String a : airports) {
            assertTrue(a, histograms.containsKey(a));
        } // FOR
        
//        System.err.println(StringUtil.formatMaps(histograms));
        
        // We expect ATL to be the max
//        assertEquals("ATL", histogram.getMaxCountValue());
        
        // Make sure the values are formatted correctly
        Pattern p = Pattern.compile("[\\d\\w]{3,3}");
        for (String s_airport : histograms.keySet()) {
            assert(p.matcher(s_airport).matches()) : "Invalid source airport: " + s_airport;
            for (Object value : histograms.get(s_airport).values()) {
                assert(p.matcher(value.toString()).matches()) : "Invalid destination airport: " + value;  
            } // FOR
        } // FOR
    }
    
    /**
     * testLoadFlightDepartTime
     */
    public void testLoadFlightDepartTime() throws Exception {
        Histogram histogram = HistogramUtil.loadHistogram(AirlineConstants.HISTOGRAM_FLIGHT_DEPART_TIMES, AIRLINE_DATA_DIR, true);
        assertFalse(histogram.values().isEmpty());
        // System.out.println(histogram);
        
        // We expect the times to be in 15 minute increments, therefore there should
        // be exactly 96 entries in the histogram
        assertEquals(96, histogram.values().size());
        
        // Make sure the values are formatted correctly
        Pattern p = Pattern.compile("[\\d]{2,2}:[\\d]{2,2}");
        for (Object value : histogram.values()) {
            assert(p.matcher(value.toString()).matches()) : "Invalid entry '" + value + "'";
        } // FOR
    }
    
    /**
     * testLoadPostalCodePopulations
     */
    public void testLoadPostalCodePopulations() throws Exception {
        Histogram histogram = HistogramUtil.loadHistogram(AirlineConstants.HISTOGRAM_POPULATION_PER_AIRPORT, AIRLINE_DATA_DIR, true);
        assertFalse(histogram.values().isEmpty());
        
        // There are 33178 zip codes in the USA
        // FIXME assertEquals(33178, histogram.values().size());
        
        // Make sure the values are formatted correctly
        /* FIXME
        Pattern p = Pattern.compile("[\\d\\w]{5,5}");
        for (Object value : histogram.values()) {
            assert(p.matcher(value.toString()).matches()) : "Invalid entry '" + value + "'";
        } // FOR
        */
    }
}
