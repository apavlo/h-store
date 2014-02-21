package edu.brown.stream;

import java.util.HashMap;
import java.util.Map;

import edu.brown.utils.MathUtil;


public class BatchRunnerResults {
    
    public final Map<Long, Integer> sizes = new HashMap<Long, Integer>();
    public final Map<Long, Integer> latencies = new HashMap<Long, Integer>();
    public final Map<Long, Integer> clusterlatencies = new HashMap<Long, Integer>();
    public final Map<Long, Double> tuplelatencies = new HashMap<Long, Double>();
    public final Map<Long, Double> throughputs = new HashMap<Long, Double>();
    public final Map<Long, Double> batchthroughputs = new HashMap<Long, Double>();
    
    private long length = 0;
    
    // min avg max for #tuples, latency and #/s
    public int minSize = Integer.MAX_VALUE;
    public int minLatency = Integer.MAX_VALUE;
    public int minClusterLatency = Integer.MAX_VALUE;
    public double minTupleLatency = Double.MAX_VALUE;
    public double minThrouput = Double.MAX_VALUE;
    public double minBatchThrouput = Double.MAX_VALUE;

    public int maxSize = Integer.MIN_VALUE; 
    public int maxLatency = Integer.MIN_VALUE;
    public int maxClusterLatency = Integer.MIN_VALUE;
    public double maxTupleLatency = Double.MIN_VALUE;
    public double maxThrouput = Double.MIN_VALUE;
    public double maxBatchThrouput = Double.MIN_VALUE;

    public int totalSize = 0;
    public int totalLatency = 0;
    public int totalClusterLatency = 0;
    public double totalTupleLatency = (double) 0.0;
    public double totalThrouput = (double) 0.0;
    public double totalBatchThrouput = (double) 0.0;

    public int averageSize = 0;
    public int averageLatency = 0;
    public int averageClusterLatency = 0;
    public double averageTupleLatency = (double) 0.0;
    public double averageThrouput = (double) 0.0;
    public double averageBatchThrouput = (double) 0.0;

    public double stddevSize = (double) 0.0;
    public double stddevLatency = (double) 0.0;
    public double stddevClusterLatency = (double) 0.0;
    public double stddevTupleLatency = (double) 0.0;
    public double stddevThrouput = (double) 0.0;
    public double stddevBatchThrouput = (double) 0.0;

    public BatchRunnerResults() {
        // TODO Auto-generated constructor stub
    }
    
    public void addOneBatchResult(long batchid, int size, int latency, int clusterlatency)
    {
        //double throuput = (double)((double)size*1000/(double)latency);  // #/s
        double throuput = (double)((double)size*1000/(double)clusterlatency);  // #/s
        double tuplelatency = (double)((double)latency/(double)size);   // ms
        double batchthrouput = (double)((double)1000/(double)clusterlatency);  // #/s

        sizes.put(batchid, size);
        latencies.put(batchid, latency);
        clusterlatencies.put(batchid, clusterlatency);
        tuplelatencies.put(batchid, tuplelatency);
        throughputs.put(batchid, throuput);
        batchthroughputs.put(batchid, batchthrouput);
        
        length++;
        
        // update 
        totalSize += size;
        totalLatency += latency;
        totalClusterLatency += clusterlatency;
        totalTupleLatency += tuplelatency;
        
        if(size > maxSize)
            maxSize = size;
        if(latency > maxLatency)
            maxLatency = latency;
        if(clusterlatency > maxClusterLatency)
            maxClusterLatency = clusterlatency;
        if(tuplelatency > maxTupleLatency)
            maxTupleLatency = tuplelatency;
        
        if(size < minSize)
            minSize = size;
        if(latency < minLatency)
            minLatency = latency;
        if(clusterlatency < minClusterLatency)
            minClusterLatency = clusterlatency;
        if(tuplelatency < minTupleLatency)
            minTupleLatency = tuplelatency;
        
        //
        totalThrouput += throuput;
        
        if (throuput > maxThrouput)
            maxThrouput = throuput;
        if (throuput < minThrouput)
            minThrouput = throuput;

        totalBatchThrouput += batchthrouput;
        
        if (batchthrouput > maxBatchThrouput)
            maxBatchThrouput = batchthrouput;
        if (batchthrouput < minBatchThrouput)
            minBatchThrouput = batchthrouput;
        
        //
        averageSize = (int)(totalSize/length);
        averageLatency = (int)(totalLatency/length);
        averageClusterLatency = (int)(totalClusterLatency/length);
        averageTupleLatency = (double)(totalTupleLatency/length);
        averageThrouput = (double)(totalThrouput/length);
        averageBatchThrouput = (double)(totalBatchThrouput/length);
        
    }
    
    public void generateStdev()
    {
        double sizes[] = new double[(int)length];
        double latencies[] = new double[(int)length];
        double clusterlatencies[] = new double[(int)length];
        double tuplelatencies[] = new double[(int)length];
        double throuputs[] = new double[(int)length];
        double batchthrouputs[] = new double[(int)length];
        
        for(long i = 0; i<length; i++)
        {
            sizes[(int)i] = (double)this.sizes.get((Long)i);
            latencies[(int)i] = (double)this.latencies.get((Long)i);
            clusterlatencies[(int)i] = (double)this.clusterlatencies.get((Long)i);
            tuplelatencies[(int)i] = (double)this.tuplelatencies.get((Long)i);
            throuputs[(int)i] = (double)this.throughputs.get((Long)i);
            batchthrouputs[(int)i] = (double)this.batchthroughputs.get((Long)i);
        }

        this.stddevSize = MathUtil.stdev(sizes);
        this.stddevLatency = MathUtil.stdev(latencies);
        this.stddevClusterLatency = MathUtil.stdev(clusterlatencies);
        this.stddevTupleLatency = MathUtil.stdev(tuplelatencies);
        this.stddevThrouput = MathUtil.stdev(throuputs);
        this.stddevBatchThrouput = MathUtil.stdev(batchthrouputs);
        
    }
    
}
