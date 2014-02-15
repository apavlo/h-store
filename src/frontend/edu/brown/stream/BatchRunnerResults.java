package edu.brown.stream;

import java.util.HashMap;
import java.util.Map;

import edu.brown.utils.MathUtil;


public class BatchRunnerResults {
    
    public final Map<Long, Integer> sizes = new HashMap<Long, Integer>();
    public final Map<Long, Integer> latencies = new HashMap<Long, Integer>();
    public final Map<Long, Double> throughputs = new HashMap<Long, Double>();
    
    private long length = 0;
    
    // min avg max for #tuples, latency and #/s
    public int minSize = Integer.MAX_VALUE;
    public int minLatency = Integer.MAX_VALUE;
    public int maxSize = Integer.MIN_VALUE; 
    public int maxLatency = Integer.MIN_VALUE;
    public double minThrouput = Float.MAX_VALUE;
    public double maxThrouput = Float.MIN_VALUE;
    public int totalSize = 0;
    public int totalLatency = 0;
    public double totalThrouput = (double) 0.0;
    public int averageSize = 0;
    public int averageLatency = 0;
    public double averageThrouput = (double) 0.0;
    public double stddevSize = (double) 0.0;
    public double stddevLatency = (double) 0.0;
    public double stddevThrouput = (double) 0.0;

    public BatchRunnerResults() {
        // TODO Auto-generated constructor stub
    }
    
    public void addOneBatchResult(long batchid, int size, int latency)
    {
        double throuput = (double)(size*1000/latency);

        sizes.put(batchid, size);
        latencies.put(batchid, latency);
        throughputs.put(batchid, throuput);
        
        length++;
        
        // update 
        totalSize += size;
        totalLatency += latency;
        
        if(size > maxSize)
            maxSize = size;
        if(latency > maxLatency)
            maxLatency = latency;
        
        if(size < minSize)
            minSize = size;
        if(latency < minLatency)
            minLatency = latency;
        
        //
        totalThrouput += throuput;
        
        if (throuput > maxThrouput)
            maxThrouput = throuput;
        if (throuput < minThrouput)
            minThrouput = throuput;

        //
        averageSize = (int)(totalSize/length);
        averageLatency = (int)(totalLatency/length);
        averageThrouput = (double)(totalThrouput/length);
        
    }
    
    public void generateStdev()
    {
        double sizes[] = new double[(int)length];
        double latencies[] = new double[(int)length];
        double throuputs[] = new double[(int)length];
        
        for(long i = 0; i<length; i++)
        {
            sizes[(int)i] = (double)this.sizes.get((Long)i);
            latencies[(int)i] = (double)this.latencies.get((Long)i);
            throuputs[(int)i] = (double)this.throughputs.get((Long)i);
        }

        this.stddevSize = MathUtil.stdev(sizes);
        this.stddevLatency = MathUtil.stdev(latencies);
        this.stddevThrouput = MathUtil.stdev(throuputs);
        
    }
    
    

}
