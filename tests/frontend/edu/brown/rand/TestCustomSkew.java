package edu.brown.rand;

import java.util.Random;

import edu.brown.BaseTestCase;
import edu.brown.statistics.Histogram;
import edu.brown.benchmark.ycsb.distributions.CustomSkewGenerator;

public class TestCustomSkew extends BaseTestCase {
	
    private final Random rand = new Random(0);
    private final int max = 1000;
    
    /**
     * testSigmaValues
     */
    public void testSkewValues() throws Exception {
	
		int hot_data_size = 20; 
		int warm_data_size = 20;
		
		int hot_data_skew = 85; 
		int warm_data_skew = 15; 
		
		int delta = 5;
		
        for (int i = 0; i < 3; i++) {
	
			CustomSkewGenerator custom_skew = new CustomSkewGenerator(max, hot_data_skew, hot_data_size, warm_data_skew, warm_data_size); 
            Histogram<Integer> h = new Histogram<Integer>(); 

			System.out.println("Generating " + hot_data_skew + "/" + hot_data_size + "-" + warm_data_skew + "/" + warm_data_size + " distribution."); 
            for (int j = 0; j < 100000; j++) {
                h.put(custom_skew.nextInt());
            } // FOR
            System.out.println(h);
            System.out.println("==========================================");
			System.out.flush(); 
			
            hot_data_skew -= delta; 
			warm_data_skew -= delta; 
			
			
        } // FOR
    }
}