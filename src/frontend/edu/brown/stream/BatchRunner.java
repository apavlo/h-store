package edu.brown.stream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
    private boolean display = false;
    private int rounds = 10;
    
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
    private List<String> hostnames = new ArrayList<String>();
    private List<InputClientConnection> connections = new ArrayList<InputClientConnection>();
    
    private List<TransactionRunner> workers =  new ArrayList<TransactionRunner>();
    
    private int port = HStoreConstants.DEFAULT_PORT;
    
    public BlockingQueue<BatchRunnerResults> batchResultQueue;
    public BlockingQueue<Batch> batchQueue = new LinkedBlockingQueue<Batch>();
    
    private static final Pattern SPLITTER = Pattern.compile("[ ]+");
    
    // ---------------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------------

    
    public BatchRunner(BlockingQueue<BatchRunnerResults> batchResultQueue, int rounds, boolean display)
    {
        this.batchResultQueue = batchResultQueue;
        this.rounds = rounds;
        
        if(display == true)
            this.display = false;
        else
            this.display = true;
    }
    
    public void setCatalog(Catalog catalog) throws Exception{
        this.catalog = catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
    }
    
    public void setHosts(String names)
    {
        //this.hostname = hostname;
        String[] hosts = names.split(",");
        for (int i=0; i<hosts.length; i++)
        {
            hostnames.add(hosts[i]);
        }
    }
    
    public void setPort(int port)
    {
        this.port = port;
    }
    
    private void createConnections() 
    {
        if(this.hostnames.isEmpty())
            this.hostnames.add("localhost");
            
        
        for(int i=0; i<this.hostnames.size();i++)
        {
            InputClientConnection connection = this.getClientConnection(hostnames.get(i));
            connections.add(connection);
        }
    }
    
    private void closeConnections() throws InterruptedException
    {
        for(int i=0; i<this.connections.size();i++)
        {
            InputClientConnection connection = connections.get(i);
            connection.client.close();
            connections.remove(i);
        }
    }
    
    private void resetConnections() throws InterruptedException
    {
        closeConnections();
        createConnections();
    }
    
    private InputClientConnection getConnection(long batchid)
    {
        int length = connections.size();
        int index = (int)batchid % length;
        
        return connections.get(index);
    }
    
    private InputClientConnection getRandomConnection()
    {
        int length = connections.size();
        
        Random randomGenerator = new Random();
        int randomInt = randomGenerator.nextInt(length);
        
        return connections.get(randomInt);
    }
    
    private boolean isRoundFinished(long batchid)
    {
        int length = connections.size();
        int index = (int)batchid % length;
        if(index == (length-1))
            return true;
        else
            return false;
    }
    
    public void run() {
        
        createConnections();
        
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
                    
                    if (success_count == this.rounds)
                    {
                        batchQueue.clear();
                        break;
                    }
                    
                    Batch batch = this.batchQueue.take();
                    
                    // empty batch encountered, quit processing
                    if(batch==null || batch.getID()==-1)
                        break;
                    
                    int retries = 3;
                    
                    while (retries-- > 0) {
                        
                        InputClientConnection icc = this.getConnection(batch.getID());
                        
                        try {
                            
                            //if(this.display==true)
                            {
                                long currentTimeStamp = System.currentTimeMillis();
                                System.out.println("Sending batch-" + batch.getID() + " to node-" + icc.hostname + " at time: " + currentTimeStamp);
                            }
                            //boolean successful = this.execBatch(icc.client, "SimpleCall", batch);
                            TransactionRunner t = new TransactionRunner(this, batch, icc.client,"SimpleCall");
                            workers.add(t);
                            t.start();

                            //if(successful==true)
                            {
                                success_count++;
                            }
                            
                            // if round is over, then we get the result and print it out
                            //if(isRoundFinished(batch.getID()))
                            {
                                // print out
//                                if(this.display==true)
//                                {
//                                    // get one round result
//                                    InputClientConnection anothericc = this.getRandomConnection();
//                                    VoltTable table = getResult(anothericc.client, "GetResults");
//                                
//                                    System.out.println("Getting result from node -" + anothericc.hostname + "...");
//                                    if(table != null)
//                                    {
//                                        int rowsize = table.getRowCount();
//                                        System.out.println("batch:" + batch.getID() + " - total words: " + batch.getSize() + " - words:"+ rowsize);
//                                        System.out.println("--------------BEGIN------------");
//                                        int igroup = 5;
//                                        String groupoutput = "";
//                                        for(int rowindex=0; rowindex<rowsize; rowindex++)
//                                        {
//                                            String word = table.fetchRow(rowindex).getString(0);
//                                            int num = (int)table.fetchRow(rowindex).getLong(1);
//                                            word = String.format("%-15s - ", word);
//                                            String strNum = String.format("%5d    ", num);
//                                            groupoutput += word + strNum;
//                                            if(rowindex % igroup == (igroup-1)){
//                                                System.out.println( groupoutput );
//                                                groupoutput = "";
//                                            }
//                                        }
//                                        System.out.println("--------------END--------------");
//                                    }
//                                }
                            }
                            
                        } catch (Exception ex) {
                            //LOG.warn("Connection lost. Going to try to connect again...");
                            
                            //resetConnections();
                            //icc = this.getConnection(batch.getID());
                            
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
            try {
                for(int iWorker=0; iWorker<workers.size();iWorker++)
                {
                    TransactionRunner worker = workers.get(iWorker);
                    worker.join();
                }
                
                for(int iWorker=0; iWorker<workers.size();iWorker++)
                {
                    TransactionRunner worker = workers.get(iWorker);
                    this.increaseBatchCounter(worker.batchid, worker.batchsize, worker.clientlatency, worker.clusterlatency);
                }
                this.closeConnections();
                //if (icc != null) icc.client.close();
            } catch (InterruptedException ex) {
                // Ignore
            }
        }
        
        // generating benchmark report
        
        
        //
        //System.out.println("BatchRunner : successful execute #batch - " + success_count);
    }
    
    //private void increaseBatchCounter(Batch batch, int latency) throws InterruptedException {
    private void increaseBatchCounter(long batchid, int batchsize, int clientlatency, int latency) throws InterruptedException {
        //System.out.println("BatchRunner : increaseBatchCounter " );
        //m_batchStats.addOneBatchResult(batch.getID(), batch.getSize(), (int)(batch.getLatency()), latency);
        m_batchStats.addOneBatchResult(batchid, batchsize, clientlatency, latency);
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
    private InputClientConnection getClientConnection(String host) {
        String hostname = null;
        int port = -1;
        
        // Fixed hostname
        if (host != null) {
            if (host.contains(":")) {
                String split[] = host.split("\\:", 2);
                hostname = split[0];
                port = Integer.valueOf(split[1]);
            } else {
                hostname = host;
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
    
    private boolean execBatch(Client client, String procName, Batch batch) throws Exception {
        //String query = batch.toJSONString();
        
        Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(procName);
        if (catalog_proc == null) {
            throw new Exception("Invalid stored procedure name '" + procName + "'");
        }
        
        List<Object> procParams = new ArrayList<Object>();
        {
            // Extract the parameters and then convert them to their appropriate type
            //List<String> params = BatchRunner.extractParams(query);
            List<String> params = new ArrayList<String>();
            int parametersize = catalog_proc.getParameters().size();
            String parameters[] = Batch.splictToMultipleJSONString(batch, parametersize);
            //System.out.println("parametersize-" + parametersize);
            
            for (int iPar=0; iPar<parametersize; iPar++)
            {
                //System.out.println("Parameter - " + parameters[iPar]);
                params.add(parameters[iPar]);
                //byte[] strbytes = parameters[iPar].getBytes("UTF-8");
                //int len = strbytes.length;
                //System.out.println("Sending parameter:" + iPar + " - size:" + len);
            }
            
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
            this.increaseBatchCounter(batch.getID(), batch.getSize(), (int)batch.getLatency(), response.getClusterRoundtrip());
        }
        
        return result;
    }
    
    private VoltTable getResult(Client client, String procName) throws Exception 
    {
        VoltTable result = null;
        
        Procedure catalog_proc = this.catalog_db.getProcedures().getIgnoreCase(procName);
        if (catalog_proc == null) {
            throw new Exception("Invalid stored procedure name '" + procName + "'");
        }
        
        //result = client.asynCallProcedure(null, catalog_proc.getName(), null, params);
        ClientResponse response = client.callProcedure(catalog_proc.getName());
        
        if(response.getStatus()==Status.OK)
        {
            result = response.getResults()[0];
        }
        else
            System.out.println("Failed : " + catalog_proc.getName());
        
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
    
    public class TransactionRunner extends Thread{
        private BatchRunner runner;
        private Batch batch;
        private Client client;
        private String procedurename;
        
        // result information
        public long batchid;
        public int batchsize;
        public int clientlatency; 
        public int clusterlatency;
        
        public TransactionRunner(BatchRunner runner, Batch batch, Client client, String procedurename) {
            this.runner = runner;
            this.batch = batch;
            this.client = client;
            this.procedurename = procedurename;
        }
        
        public void run() {
            try {
                //client.callProcedure(callback, "SimpleCall", b.toJSONString());
                //ois.close();
                this.execBatch(client, procedurename, batch);
                
                displayResult();
            }
            catch(Exception e){
                e.printStackTrace();
            }
        }
        
        private void displayResult() throws Exception
        {
            {
                // print out
                if(runner.display==true)
                {
                    // get one round result
                    InputClientConnection anothericc = runner.getRandomConnection();
                    VoltTable table = getResult(anothericc.client, "GetResults");
                
                    System.out.println("Getting result from node -" + anothericc.hostname + "...");
                    if(table != null)
                    {
                        int rowsize = table.getRowCount();
                        System.out.println("batch:" + batch.getID() + " - total words: " + batch.getSize() + " - words:"+ rowsize);
                        System.out.println("--------------BEGIN------------");
                        int igroup = 5;
                        String groupoutput = "";
                        for(int rowindex=0; rowindex<rowsize; rowindex++)
                        {
                            String word = table.fetchRow(rowindex).getString(0);
                            int num = (int)table.fetchRow(rowindex).getLong(1);
                            word = String.format("%-15s - ", word);
                            String strNum = String.format("%5d    ", num);
                            groupoutput += word + strNum;
                            if(rowindex % igroup == (igroup-1)){
                                System.out.println( groupoutput );
                                groupoutput = "";
                            }
                        }
                        System.out.println("--------------END--------------");
                    }
                }
            }

        }
        
        private boolean execBatch(Client client, String procName, Batch batch) throws Exception {
            //String query = batch.toJSONString();
            long currentTimeStamp = System.currentTimeMillis();
            //batch.setTimestamp(currentTimeStamp);
            
            Procedure catalog_proc = runner.catalog_db.getProcedures().getIgnoreCase(procName);
            if (catalog_proc == null) {
                throw new Exception("Invalid stored procedure name '" + procName + "'");
            }
            
            List<Object> procParams = new ArrayList<Object>();
            {
                // Extract the parameters and then convert them to their appropriate type
                //List<String> params = BatchRunner.extractParams(query);
                List<String> params = new ArrayList<String>();
                int parametersize = catalog_proc.getParameters().size();
                String parameters[] = Batch.splictToMultipleJSONString(batch, parametersize);
                //System.out.println("parametersize-" + parametersize);
                
                for (int iPar=0; iPar<parametersize; iPar++)
                {
                    //System.out.println("Parameter - " + parameters[iPar]);
                    params.add(parameters[iPar]);
                    //byte[] strbytes = parameters[iPar].getBytes("UTF-8");
                    //int len = strbytes.length;
                    //System.out.println("Sending parameter:" + iPar + " - size:" + len);
                }
                
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
            //ClientResponse response = client.callProcedure(catalog_proc.getName(), params);
            ClientResponse response = client.callStreamProcedure(catalog_proc.getName(), (int)batch.getID(), params);
            
            if(response.getStatus()!=Status.OK)
                result = false;
            else
            {
                currentTimeStamp = System.currentTimeMillis();
                batch.setEndTimestamp(currentTimeStamp);      
                System.out.println("finishing batch-" + batch.getID() 
                        + " at time: " + currentTimeStamp 
                        + " with return batchid: " + response.getBatchId());
                batchid = batch.getID();
                batchsize = batch.getSize();
                clientlatency = (int)batch.getLatency(); 
                clusterlatency = response.getClusterRoundtrip();
                //runner.increaseBatchCounter(batch.getID(), batch.getSize(), (int)batch.getLatency(), response.getClusterRoundtrip());
            }
            
            return result;
        }
        
        private VoltTable getResult(Client client, String procName) throws Exception 
        {
            VoltTable result = null;
            
            Procedure catalog_proc = runner.catalog_db.getProcedures().getIgnoreCase(procName);
            if (catalog_proc == null) {
                throw new Exception("Invalid stored procedure name '" + procName + "'");
            }
            
            //result = client.asynCallProcedure(null, catalog_proc.getName(), null, params);
            ClientResponse response = client.callProcedure(catalog_proc.getName());
            
            if(response.getStatus()==Status.OK)
            {
                result = response.getResults()[0];
            }
            else
                System.out.println("Failed : " + catalog_proc.getName());
            
            return result;
        }

    }

}
