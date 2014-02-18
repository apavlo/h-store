package edu.brown.stream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;

public class BatchRunner implements Runnable{
    private static final Logger LOG = Logger.getLogger(BatchRunner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private final BatchRunnerResults m_batchStats = new BatchRunnerResults();
    
    private boolean stop = false;
    
    private class InputClientConnection {
        final Client client;
        final String hostname;
        final int port;
        
        public InputClientConnection(Client client, String hostname, int port) {
            this.client = client;
            this.hostname = hostname;
            this.port = port;
        }
    } // CLASS
    
    // ---------------------------------------------------------------
    // INSTANCE CONFIGURATION MEMBERS
    // ---------------------------------------------------------------
    
    private Catalog catalog;
    private Database catalog_db;
    private String hostname = null;
    private int port = HStoreConstants.DEFAULT_PORT;
    
    public BlockingQueue<BatchRunnerResults> batchResultQueue;
    public BlockingQueue<Batch> batchQueue = new LinkedBlockingQueue<Batch>();
    
    private static final Pattern SPLITTER = Pattern.compile("[ ]+");
    
    // ---------------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------------

    
    public BatchRunner(BlockingQueue<BatchRunnerResults> batchResultQueue)
    {
        this.batchResultQueue = batchResultQueue;
    }
    
    public void setCatalog(Catalog catalog) throws Exception{
        this.catalog = catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
    }
    
    public void setHost(String hostname)
    {
        this.hostname = hostname;
    }
    
    public void setPort(int port)
    {
        this.port = port;
    }
    
    public void run() {
        InputClientConnection icc = this.getClientConnection();
        
        long success_count = 0;
        
        try {
            
            // get transactions execution information before running benchmark 
            try {
                //this.preProcessBenchmark(icc.client);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            
            boolean finishOperation = false;
            
            do {
                try {
                    
                    //
                    if (this.stop==true)
                        break;
                    
                    Batch batch = this.batchQueue.take();
                    String batchJSONString = batch.toJSONString();
                    //System.out.println("InputClient consume: " + batchJSONString);
                    
                    // empty batch encountered, quit processing
                    if(batch==null || batch.getID()==-1)
                        break;
                    
                    int retries = 3;
                    boolean reconnect = false;
                    
                    while (retries-- > 0) {
                        try {
                            
                            //this.execQuery(icc.client, query);
                            boolean successful = this.execBatch(icc.client, "SimpleCall", batch, reconnect);
                            //boolean successful = this.execProcedure(icc.client, "SimpleCall", batchJSONString, reconnect);
                            if(successful==true)
                            {
                                success_count++;
                                //System.out.println("BatchRunner : successful execute #batchs - " + success_count);
                            }
                            
                        } catch (NoConnectionsException ex) {
                            LOG.warn("Connection lost. Going to try to connect again...");
                            icc = this.getClientConnection();
                            reconnect = true;
                            continue;
                        }
                        break;
                    } // WHILE
                    
                } catch (RuntimeException ex) {
                    throw ex;
                // Friendly Error
                } catch (Exception ex) {
                    LOG.error(ex.getMessage());
                    Throwable cause = ex.getCause();
                    if (cause != null) {
                        LOG.error(cause.getMessage());
                        if (debug.val) cause.printStackTrace();
                    }
                }
                
            } while (true);
            
            try {
                // get transactions execution information after running benchmark 
                // get metrics
                //this.postProcessBenchmark(icc.client);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
//            try {
//                if (icc != null) icc.client.close();
//            } catch (InterruptedException ex) {
//                // Ignore
//            }
        }
        
        // generating benchmark report
        
        
        //
        //System.out.println("BatchRunner : successful execute #batch - " + success_count);
    }
    
    private void increaseBatchCounter(Batch batch, int latency) throws InterruptedException {
        //System.out.println("BatchRunner : increaseBatchCounter " );
        latency = (int)(batch.getLatency());
        m_batchStats.addOneBatchResult(batch.getID(), batch.getSize(), latency);
        batchResultQueue.put(m_batchStats);
        
    }


    private void preProcessBenchmark(Client client) throws Exception {
        // DUMP PROFILING INFORMATION
        Map<SysProcSelector, String> map = new HashMap<SysProcSelector, String>();
        map.put(SysProcSelector.PROCEDURE, "s-store_proc_pre.csv");
        map.put(SysProcSelector.TRIGGER, "s-store_trigger_pre.csv");
        map.put(SysProcSelector.STREAM, "s-store_stream_pre.csv");
     
        for (Map.Entry<SysProcSelector, String> entry : map.entrySet()) {
            this.writeProfilingData(client, entry.getKey(), new File(entry.getValue()));
        }
    }

    private void postProcessBenchmark(Client client) throws Exception {
        if (debug.val) LOG.debug("Performing post-processing on benchmark");
        
        // Then tell the cluster to drain all txns
//        if (debug.val) LOG.debug("Draining execution queues on cluster");
//        ClientResponse cresponse = null;
//        String procName = VoltSystemProcedure.procCallName(Quiesce.class);
//        try {
//            cresponse = client.callProcedure(procName);
//        } catch (Exception ex) {
//            throw new Exception("Failed to execute " + procName, ex);
//        }
//        assert(cresponse.getStatus() == Status.OK) :
//            String.format("Failed to quiesce cluster!\n%s", cresponse);

        //Thread.sleep(10000);
        
        // DUMP PROFILING INFORMATION
        Map<SysProcSelector, String> map = new HashMap<SysProcSelector, String>();
        map.put(SysProcSelector.PROCEDURE, "s-store_proc_post.csv");
        map.put(SysProcSelector.TRIGGER, "s-store_trigger_post.csv");
        map.put(SysProcSelector.STREAM, "s-store_stream_post.csv");
     
        for (Map.Entry<SysProcSelector, String> entry : map.entrySet()) {
            this.writeProfilingData(client, entry.getKey(), new File(entry.getValue()));
        }
    }
    
    private void writeProfilingData(Client client, SysProcSelector sps, File outputPath) throws Exception {
        Object params[];
        String sysproc;
        
        sysproc = VoltSystemProcedure.procCallName(Statistics.class);
        params = new Object[]{ sps.name(), 0 };
        
        // Grab the data that we need from the cluster
        ClientResponse cresponse;
        try {
            cresponse = client.callProcedure(sysproc, params);
        } catch (Exception ex) {
            throw new Exception("Failed to execute " + sysproc, ex);
        }
        assert(cresponse.getStatus() == Status.OK) :
            String.format("Failed to get %s stats\n%s", sps, cresponse); 
        assert(cresponse.getResults().length == 1) :
            String.format("Failed to get %s stats\n%s", sps, cresponse);
        VoltTable vt = cresponse.getResults()[0];
        
        // Write out CSV
        FileWriter out = new FileWriter(outputPath);
        VoltTableUtil.csv(out, vt, true);
        out.close();
        LOG.info(String.format("Wrote %s information to '%s'", sps, outputPath));
        return;
    }
    
    
    /**
     * Get a client handle to a random site in the running cluster
     * The return value includes what site the client connected to
     * @return
     */
    private InputClientConnection getClientConnection() {
        String hostname = null;
        int port = -1;
        
        // Fixed hostname
        if (this.hostname != null) {
            if (this.hostname.contains(":")) {
                String split[] = this.hostname.split("\\:", 2);
                hostname = split[0];
                port = Integer.valueOf(split[1]);
            } else {
                hostname = this.hostname;
                port = this.port;
            }
        }
        // Connect to random host and using a random port that it's listening on
        else if (this.catalog != null) {
            Site catalog_site = CollectionUtil.random(CatalogUtil.getAllSites(this.catalog));
            hostname = catalog_site.getHost().getIpaddr();
            port = catalog_site.getProc_port();
        }
        assert(hostname != null);
        assert(port > 0);
        
        if (debug.val)
            LOG.debug(String.format("Creating new client connection to %s:%d",
                      hostname, port));
        System.out.println(String.format("Creating new client connection to %s:%d",
                      hostname, port));
        Client client = ClientFactory.createClient(128, null, false, null);
        try {
            client.createConnection(null, hostname, port, "user", "password");
            //System.out.println("BatchRunner: connection is ok ... ");
        } catch (Exception ex) {
            String msg = String.format("Failed to connect to HStoreSite at %s:%d", hostname, port);
            throw new RuntimeException(msg);
        }
        return new InputClientConnection(client, hostname, port);
    }
    
    /**
     * Execute the given query as an ad-hoc request on the server and
     * return the result.
     * @param client
     * @param query
     * @return
     * @throws Exception
     */
    private boolean execQuery(Client client, String query) throws Exception {
        //if (debug.val) LOG.debug("QUERY: " + query);
        System.out.println("QUERY: " + query);
        boolean result = true;
        result = client.asynCallProcedure(null, "@AdHoc", null, query);
        return result;
    }
    
    private boolean execBatch(Client client, String procName, Batch batch, boolean reconnect) throws Exception {
        String query = batch.toJSONString();
        
        Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(procName);
        if (catalog_proc == null) {
            throw new Exception("Invalid stored procedure name '" + procName + "'");
        }
        
        List<Object> procParams = new ArrayList<Object>();
        {
            // Extract the parameters and then convert them to their appropriate type
            List<String> params = BatchRunner.extractParams(query);
            if (debug.val) LOG.debug("PARAMS: " + params);
            if (params.size() != catalog_proc.getParameters().size()) {
                String msg = String.format("Expected %d params for '%s' but %d parameters were given",
                                           catalog_proc.getParameters().size(), catalog_proc.getName(), params.size());
                throw new Exception(msg);
            }
            int i = 0;
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                VoltType vtype = VoltType.get(catalog_param.getType());
                Object value = VoltTypeUtil.getObjectFromString(vtype, params.get(i));
                
                // HACK: Allow us to send one-element array parameters
                if (catalog_param.getIsarray()) {
                    switch (vtype) {
                        case BOOLEAN:
                            value = new boolean[]{ (Boolean)value };
                            break;
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                            value = new int[]{ (Integer)value };
                            break;
                        case BIGINT:
                            value = new long[]{ (Long)value };
                            break;
                        case FLOAT:
                        case DECIMAL:
                            value = new double[]{ (Double)value };
                            break;
                        case STRING:
                            value = new String[]{ (String)value };
                            break;
                        case TIMESTAMP:
                            value = new TimestampType[]{ (TimestampType)value };
                        default:
                            assert(false);
                    } // SWITCH
                }
                procParams.add(value);
                i++;
            } // FOR
        }

        Object params[] = procParams.toArray(); 
        boolean result = true;
        //result = client.asynCallProcedure(null, catalog_proc.getName(), null, params);
        ClientResponse response = client.callProcedure(catalog_proc.getName(), params);
        
        if(response.getStatus()!=Status.OK)
            result = false;
        else
        {
            long currentTimeStamp = System.currentTimeMillis();
            batch.setEndTimestamp(currentTimeStamp);            
            this.increaseBatchCounter(batch, response.getClusterRoundtrip());
        }
        
        return result;
    }
    
    protected static List<String> extractParams(String paramStr) throws Exception {
        List<String> params = new ArrayList<String>();
        int pos = -1;
        int len = paramStr.length();
        while (++pos < len) {
            char cur = paramStr.charAt(pos);
            
            // Skip if it's just a space
            if (cur == ' ') continue;
            
            // See if our current position is a quotation mark
            // If it is, then we know that we have a string parameter
            if (cur == '"') {
                // Keep going until we reach an unescaped quotation mark
                boolean escaped = false;
                boolean valid = false;
                StringBuilder sb = new StringBuilder();
                while (++pos < len) {
                    cur = paramStr.charAt(pos); 
                    if (cur == '\\') {
                        escaped = true;
                    } else if (cur == '"' && escaped == false) {
                        valid = true;
                        break;
                    } else {
                        escaped = false;
                    }
                    sb.append(cur);
                } // WHILE
                if (valid == false) {
                    throw new Exception("Invalid parameter string '" + sb + "'");
                }
                params.add(sb.toString());

            // Otherwise just grab the substring to the next space 
            } else {
                int next = paramStr.indexOf(" ", pos);
                if (next == -1) {
                    params.add(paramStr.substring(pos));
                    pos = len;
                } else {
                    params.add(paramStr.substring(pos, next));
                    pos = next;
                }
            }
        }
        return (params);
    }

    public void stop() {
        stop = true;
    }

}
