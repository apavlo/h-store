package edu.brown.stream;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
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
import org.voltdb.sysprocs.DatabaseDump;
import org.voltdb.sysprocs.EvictHistory;
import org.voltdb.sysprocs.EvictedAccessHistory;
import org.voltdb.sysprocs.Quiesce;
import org.voltdb.sysprocs.Statistics;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.VoltTableUtil;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.terminal.HStoreTerminal;
import edu.brown.terminal.HStoreTerminal.Command;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringUtil;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.text.*;
import java.util.*;


public class InputClient implements Runnable {
    //
    private BlockingQueue<BatchRunnerResults> batchResultQueue = new LinkedBlockingQueue<BatchRunnerResults>();

    private int batchRounds = 10;

    private boolean json = false;
    private boolean display = false;
    
    // ---------------------------------------------------------------
    // CONSTRUCTOR
    // ---------------------------------------------------------------
    
    public InputClient() throws Exception{

    }
    
    private void setBatchRounds(int rounds) {
        this.batchRounds  = rounds;
    }
    
    private void setResultFormat(boolean json) {
        this.json  = json;
    }
    
    private void setDisplay(boolean display)
    {
        this.display = display;
    }
    
    @Override
    public void run() {
        int batchlimit = this.batchRounds;
        try {
            long i = 0;
            BatchRunnerResults batchresult = null;

            StringBuilder sb = new StringBuilder();
            final int width = 80;
            sb.append(String.format("\n%s\n", StringUtil.repeat("=", width)));
            String strOutput = sb.toString();
            System.out.println(strOutput);

            
            while (true) {
                
                if (i == batchlimit)
                    break;

                batchresult = batchResultQueue.take();
                if(batchresult!=null)
                {
                    if(display==true)
                    {
                        int size = batchresult.sizes.get((Long)i);
                        int latency = batchresult.latencies.get((Long)i);
                        int clusterlatency = batchresult.clusterlatencies.get((Long)i);
                        double batchthroughput = batchresult.batchthroughputs.get((Long)i);
                        double throughput = batchresult.throughputs.get((Long)i);
                        strOutput = " batch id : " + String.format("%4d", i);
                        strOutput += " - tuple size : " + String.format("%5d", size);
                        strOutput += " - client latency : " + String.format("%5d", latency) + " ms";
                        strOutput += " - cluster latency : " + String.format("%5d", clusterlatency) + " ms";
                        strOutput += " - cluster #batch/s :" + String.format("%8.2f", batchthroughput);
                        strOutput += " - #tuple/s :" + String.format("%8.2f", throughput);
                        strOutput += " ";
                        System.out.println(strOutput);
                    }
    
                    i++;
                }
                else
                    System.out.println("InputClient: run empty result - strange !");
            }

            if(display==true)
                outputFinalResult(batchresult);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void outputFinalResult(BatchRunnerResults batchresult)
    {
        
        FinalResult finalResult = new FinalResult(batchresult);
        
        String strOutput = finalResult.generateNormalOutputFormat();
        
        // print out the final result
        System.out.println(strOutput);
        
        // if needed we can generate json results for experiments
        if(this.json==true)
        {
            strOutput = "\n\n" + finalResult.generateJSONOutputFormat();
            System.out.println(strOutput);
        }
        
    }

    public static void main(String vargs[]) throws Exception {

//        Batch batch = new Batch();
//        Tuple tuple = new Tuple();
//        tuple.addField("name", "hawk");
//        tuple.addField("sex", "male");
//        batch.addTuple(tuple);
//        String strJSON = batch.toJSONString();
//        System.out.println(strJSON);
//        byte[] objJSON = StringCompressor.compress(strJSON);
//        String strRetrieved = StringCompressor.decompress(objJSON);
//        System.out.println(strRetrieved);
        
        
        AnotherArgumentsParser args = AnotherArgumentsParser.load( vargs );
        
        InputClient ic = new InputClient();

        boolean display = false; 
        if (args.hasParam(AnotherArgumentsParser.PARAM_RESULT_DISPLAY)) {
            display = args.getBooleanParam(AnotherArgumentsParser.PARAM_RESULT_DISPLAY);
        }
        
        int inverval = 1000; // ms
        if (args.hasParam(AnotherArgumentsParser.PARAM_BATCH_INTERVAL)) {
            inverval = args.getIntParam(AnotherArgumentsParser.PARAM_BATCH_INTERVAL);
        }

        int rounds = 10; // ms
        if (args.hasParam(AnotherArgumentsParser.PARAM_BATCH_ROUNDS)) {
            rounds = args.getIntParam(AnotherArgumentsParser.PARAM_BATCH_ROUNDS);
        }

        String filename = "word.txt";
        if (args.hasParam(AnotherArgumentsParser.PARAM_SOURCE_FILE)) {
            filename = args.getParam(AnotherArgumentsParser.PARAM_SOURCE_FILE);
        }

        int sendrate = 1000; // tuple/s
        if (args.hasParam(AnotherArgumentsParser.PARAM_SOURCE_SENDRATE)) {
            sendrate = args.getIntParam(AnotherArgumentsParser.PARAM_SOURCE_SENDRATE);
        }

        boolean sendstop = false; 
        if (args.hasParam(AnotherArgumentsParser.PARAM_SOURCE_SENDSTOP)) {
            sendstop = args.getBooleanParam(AnotherArgumentsParser.PARAM_SOURCE_SENDSTOP);
        }
        
        boolean json = false; 
        if (args.hasParam(AnotherArgumentsParser.PARAM_RESULT_JSON)) {
            json = args.getBooleanParam(AnotherArgumentsParser.PARAM_RESULT_JSON);
        }

        BatchRunner batchRunner = new BatchRunner(ic.batchResultQueue, rounds, display);
        batchRunner.setCatalog(args.catalog);
        
        // HOSTNAME
        if (args.hasParam(AnotherArgumentsParser.ORIGIN_TERMINAL_HOST)) {
            batchRunner.setHosts(args.getParam(AnotherArgumentsParser.ORIGIN_TERMINAL_HOST));
        }
        // PORT
        if (args.hasParam(AnotherArgumentsParser.ORIGIN_TERMINAL_PORT)) {
            batchRunner.setPort(args.getIntParam(AnotherArgumentsParser.ORIGIN_TERMINAL_PORT));
        }
        
        BatchProducer batchProducer = new BatchProducer(batchRunner.batchQueue, inverval);
        TupleProducer tupleProducer = new TupleProducer(batchProducer.queue, filename, sendrate, sendstop);
        
        //starting producer to produce messages in queue
        new Thread(tupleProducer).start();
        
        // starting batch producer to manager tuples in batch
        new Thread(batchProducer).start();

        // starting batch runner
        new Thread(batchRunner).start();
        
        // start inputclient monitor
        ic.setBatchRounds(rounds);
        ic.setResultFormat(json);
        ic.setDisplay(display);
        
        ic.run();
        
        tupleProducer.stop();
        //batchRunner.stop();
        
    }




}
