package edu.brown.stream;

import java.util.LinkedHashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.brown.utils.StringUtil;

public class FinalResult {
    // used for format output
    private static final String RESULT_FORMAT = "%.2f";
    
    // throuput
    private final double minThrouput;
    private final double maxThrouput;
    private final double averageThrouput;
    private final double stddevThrouput;
    // batch cluster throuput
    private final double minBatchThrouput;
    private final double maxBatchThrouput;
    private final double averageBatchThrouput;
    private final double stddevBatchThrouput;
    // client throuput
    private final double minClientThrouput;
    private final double maxClientThrouput;
    private final double averageClientThrouput;
    private final double stddevClientThrouput;
    // batch client throuput
    private final double minClientBatchThrouput;
    private final double maxClientBatchThrouput;
    private final double averageClientBatchThrouput;
    private final double stddevClientBatchThrouput;
    // size
    private final double minSize;
    private final double maxSize;
    private final double averageSize;
    private final double stddevSize;
    
    // batch client latency
    private final double minLatency;
    private final double maxLatency;
    private final double averageLatency;
    private final double stddevLatency;

    // batch cluster latency
    private final double minClusterLatency;
    private final double maxClusterLatency;
    private final double averageClusterLatency;
    private final double stddevClusterLatency;
    
    // tuple latency
    private final double minTupleLatency;
    private final double maxTupleLatency;
    private final double averageTupleLatency;
    private final double stddevTupleLatency;
    
    
    
    public FinalResult(BatchRunnerResults batchresult) {
        
        batchresult.generateStdev();
        
        // initialize final result metrics
        minThrouput = batchresult.minThrouput;
        maxThrouput = batchresult.maxThrouput;
        averageThrouput = batchresult.averageThrouput;
        stddevThrouput = batchresult.stddevThrouput;

        minBatchThrouput = batchresult.minBatchThrouput;
        maxBatchThrouput = batchresult.maxBatchThrouput;
        averageBatchThrouput = batchresult.averageBatchThrouput;
        stddevBatchThrouput = batchresult.stddevBatchThrouput;
        
        minClientThrouput = batchresult.minClientThrouput;
        maxClientThrouput = batchresult.maxClientThrouput;
        averageClientThrouput = batchresult.averageClientThrouput;
        stddevClientThrouput = batchresult.stddevClientThrouput;

        minClientBatchThrouput = batchresult.minClientBatchThrouput;
        maxClientBatchThrouput = batchresult.maxClientBatchThrouput;
        averageClientBatchThrouput = batchresult.averageClientBatchThrouput;
        stddevClientBatchThrouput = batchresult.stddevClientBatchThrouput;

        minSize = batchresult.minSize;
        maxSize = batchresult.maxSize;
        averageSize = batchresult.averageSize;
        stddevSize = batchresult.stddevSize;
        
        minLatency = batchresult.minLatency;
        maxLatency = batchresult.maxLatency;
        averageLatency = batchresult.averageLatency;
        stddevLatency = batchresult.stddevLatency;
        
        minClusterLatency = batchresult.minClusterLatency;
        maxClusterLatency = batchresult.maxClusterLatency;
        averageClusterLatency = batchresult.averageClusterLatency;
        stddevClusterLatency = batchresult.stddevClusterLatency;

        minTupleLatency = batchresult.minTupleLatency;
        maxTupleLatency = batchresult.maxTupleLatency;
        averageTupleLatency = batchresult.averageTupleLatency;
        stddevTupleLatency = batchresult.stddevTupleLatency;
        
    }
    
    public String generateNormalOutputFormat()
    {
        // generate the result string with format
        StringBuilder sb = new StringBuilder();
        final int width = 80; 
        sb.append(String.format("\n%s\n\n", StringUtil.header("INPUTCLIENT BATCHRUNNER RESULTS", "=", width)));

        // batch cluster throuput
        StringBuilder batchthroughput = new StringBuilder();
        batchthroughput.append(String.format(RESULT_FORMAT + " #batch/s", this.averageBatchThrouput))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, this.minBatchThrouput))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, this.maxBatchThrouput))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevBatchThrouput))
             .append("]");

        // tuple throuput
        StringBuilder throughput = new StringBuilder();
        throughput.append(String.format(RESULT_FORMAT + " #tuple/s", this.averageThrouput))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, this.minThrouput))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, this.maxThrouput))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevThrouput))
             .append("]");

        // batch client throuput
        StringBuilder clientbatchthroughput = new StringBuilder();
        clientbatchthroughput.append(String.format(RESULT_FORMAT + " #batch/s", this.averageClientBatchThrouput))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, this.minClientBatchThrouput))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, this.maxClientBatchThrouput))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevClientBatchThrouput))
             .append("]");

        // tuple throuput
        StringBuilder clientthroughput = new StringBuilder();
        clientthroughput.append(String.format(RESULT_FORMAT + " #tuple/s", this.averageClientThrouput))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, this.minClientThrouput))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, this.maxClientThrouput))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevClientThrouput))
             .append("]");

        // size
        StringBuilder size = new StringBuilder();
        size.append(String.format(RESULT_FORMAT + " #", (double)this.averageSize))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, (double)this.minSize))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, (double)this.maxSize))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevSize))
             .append("]");
        
        // batch client latency
        StringBuilder latency = new StringBuilder();
        latency.append(String.format(RESULT_FORMAT + " ms", (double)this.averageLatency))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, (double)this.minLatency))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, (double)this.maxLatency))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevLatency))
             .append("]");

        // batch cluster latency
        StringBuilder clusterlatency = new StringBuilder();
        clusterlatency.append(String.format(RESULT_FORMAT + " ms", (double)this.averageClusterLatency))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, (double)this.minClusterLatency))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, (double)this.maxClusterLatency))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevClusterLatency))
             .append("]");
        
        // tuple latency
        StringBuilder tuplelatency = new StringBuilder();
        tuplelatency.append(String.format(RESULT_FORMAT + " ms", (double)this.averageTupleLatency))
             .append(" [")
             .append(String.format("min:" + RESULT_FORMAT, (double)this.minTupleLatency))
             .append(" / ")
             .append(String.format("max:" + RESULT_FORMAT, (double)this.maxTupleLatency))
             .append(" / ")
             .append(String.format("stdev:" + RESULT_FORMAT, this.stddevTupleLatency))
             .append("]");

        Map<String, Object> m = new LinkedHashMap<String, Object>();
        m.put("Batch Size", size.toString());
        m.put("Batch Throughput", batchthroughput.toString()); 
        m.put("Batch Throughput(client)", clientbatchthroughput.toString()); 
        m.put("Batch Latency", latency.toString());
        m.put("Batch Cluster Latency", clusterlatency.toString());
        m.put("Tuple Throughput", throughput.toString()); 
        m.put("Tuple Throughput(client)", clientthroughput.toString()); 
        m.put("Tuple Latency", tuplelatency.toString());

        sb.append(StringUtil.formatMaps(m));
        sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
        
        String strOutput = sb.toString();
        
        return  strOutput;

    }

    public String generateJSONOutputFormat() {
        
        String strOutput = "<json>\n";
        strOutput += this.toJSONString();
        strOutput += "\n</json>\n";
        return strOutput;
    }
    
    public JSONObject toJSONObject()
    {
        
        JSONObject jsonBatch = new JSONObject();
        try
        {
            jsonBatch.put("MINTHROUPUT", this.minThrouput);
            jsonBatch.put("MAXTHROUPUT", this.maxThrouput);
            jsonBatch.put("AVERAGETHROUPUT", this.averageThrouput);
            jsonBatch.put("STDDEVTHROUPUT", this.stddevThrouput);
            
            jsonBatch.put("MINBATCHTHROUPUT", this.minBatchThrouput);
            jsonBatch.put("MAXBATCHTHROUPUT", this.maxBatchThrouput);
            jsonBatch.put("AVERAGEBATCHTHROUPUT", this.averageBatchThrouput);
            jsonBatch.put("STDDEVBATCHTHROUPUT", this.stddevBatchThrouput);
            
            jsonBatch.put("MINCLIENTTHROUPUT", this.minClientThrouput);
            jsonBatch.put("MAXCLIENTTHROUPUT", this.maxClientThrouput);
            jsonBatch.put("AVERAGECLIENTTHROUPUT", this.averageClientThrouput);
            jsonBatch.put("STDDEVCLIENTTHROUPUT", this.stddevClientThrouput);
            
            jsonBatch.put("MINCLIENTBATCHTHROUPUT", this.minClientBatchThrouput);
            jsonBatch.put("MAXCLIENTBATCHTHROUPUT", this.maxClientBatchThrouput);
            jsonBatch.put("AVERAGECLIENTBATCHTHROUPUT", this.averageClientBatchThrouput);
            jsonBatch.put("STDDEVCLIENTBATCHTHROUPUT", this.stddevClientBatchThrouput);
            
            jsonBatch.put("MINSIZE", this.minSize);
            jsonBatch.put("MAXSIZE", this.maxSize);
            jsonBatch.put("AVERAGESIZE", this.averageSize);
            jsonBatch.put("STDDEVSIZE", this.stddevSize);
            
            jsonBatch.put("MINLATENCY", this.minLatency);
            jsonBatch.put("MAXLATENCY", this.maxLatency);
            jsonBatch.put("AVERAGELATENCY", this.averageLatency);
            jsonBatch.put("STDDEVLATENCY", this.stddevLatency);
            
            jsonBatch.put("MINCLUSTERLATENCY", this.minClusterLatency);
            jsonBatch.put("MAXCLUSTERLATENCY", this.maxClusterLatency);
            jsonBatch.put("AVERAGECLUSTERLATENCY", this.averageClusterLatency);
            jsonBatch.put("STDDEVCLUSTERLATENCY", this.stddevClusterLatency);
            
            jsonBatch.put("MINTUPLELATENCY", this.minTupleLatency);
            jsonBatch.put("MAXTUPLELATENCY", this.maxTupleLatency);
            jsonBatch.put("AVERAGETUPLELATENCY", this.averageTupleLatency);
            jsonBatch.put("STDDEVTUPLELATENCY", this.stddevTupleLatency);

        } catch (JSONException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }        
        return jsonBatch;
    }
    
    public String toJSONString()
    {
        JSONObject jsonBatch = this.toJSONObject();
        
        return jsonBatch.toString();
    }

}
