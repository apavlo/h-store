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
	
		int access_skew = 80; 
		int data_skew = 20;
		
		int delta = 5;
		
        for (int i = 0; i < 4; i++) {
	
			CustomSkewGenerator custom_skew = new CustomSkewGenerator(max, access_skew, data_skew); 
            Histogram<Integer> h = new Histogram<Integer>(); 

			System.out.println("Generating " + access_skew + "/" + data_skew + " distribution."); 
            for (int j = 0; j < 100000; j++) {
                h.put(custom_skew.nextInt());
            } // FOR
            System.out.println(h);
            System.out.println("==========================================");
			System.out.flush(); 
			
            access_skew += delta;
			data_skew -= delta; 
			
        } // FOR
    }
}