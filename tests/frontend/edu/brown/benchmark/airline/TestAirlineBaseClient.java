package edu.brown.benchmark.airline;

import edu.brown.statistics.Histogram;

public class TestAirlineBaseClient extends AirlineBaseTestCase {

    /**
     * testLoadHistograms
     */
    public void testLoadHistograms() throws Exception {
        loader.loadHistograms();
        
        assertFalse(loader.histograms.isEmpty());
        assertEquals(AirlineConstants.HISTOGRAM_DATA_FILES.length, loader.histograms.size());
        for (String histogram_name : AirlineConstants.HISTOGRAM_DATA_FILES) {
            Histogram h = loader.histograms.get(histogram_name);
            assertNotNull(h);
            assertTrue(h.getSampleCount() > 0);
        } // FOR
    }
}
