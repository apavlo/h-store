package edu.brown.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.voltdb.catalog.Database;
import org.voltdb.utils.Pair;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.Filter.FilterResult;

public abstract class WorkloadUtil {
    private static final Logger LOG = Logger.getLogger(WorkloadUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final int ELEMENT_CTR_IDX            = 0;
    private static final int TXN_CTR_IDX                = ELEMENT_CTR_IDX + 1;
    private static final int QUERY_CTR_IDX              = ELEMENT_CTR_IDX + 2;
    private static final int WEIGHTED_TXN_CTR_IDX       = ELEMENT_CTR_IDX + 3;
    private static final int WEIGHTED_QUERY_CTR_IDX     = ELEMENT_CTR_IDX + 4;


    /**
     * READ THREAD
     */
    public static class ReadThread implements Runnable {
        final File input_path;
        final List<ProcessingThread> processingThreads = new ArrayList<ProcessingThread>();
        final AtomicInteger processingThreadId = new AtomicInteger(0);
        final LinkedBlockingDeque<Pair<Integer, String>> lines;
        final Pattern pattern;
        boolean stop = false;
        Thread self;
        
        public ReadThread(File input_path, Pattern pattern, int num_threads) {
            this.input_path = input_path;
            this.pattern = pattern;
            this.lines = new LinkedBlockingDeque<Pair<Integer, String>>(num_threads * 1000);
        }
        
        @Override
        public void run() {
            self = Thread.currentThread();
            self.setName(this.getClass().getSimpleName());
            if (debug.val) LOG.debug(String.format("Starting thread to read workload '%s' [processingThreads=%d]",
                                       this.input_path.getAbsolutePath(), this.processingThreads.size()));
            
            BufferedReader in = null;
            try {
                in = FileUtil.getReader(this.input_path);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            
            int totalCtr = 0;
            int procCtr = 0;
            int fastCtr = 0;
            try {
                while (in.ready() && this.stop == false) {
                    totalCtr++;
                    if (debug.val && totalCtr % 10000 == 0)
                        LOG.debug(String.format("Read in %d lines from '%s' [queue=%d, procCtr=%d, fastCtr=%d]",
                                                totalCtr, this.input_path.getName(), this.lines.size(), procCtr, fastCtr));
                    
                    String line = in.readLine().trim();
                    if (line.isEmpty()) continue;
                    if (this.pattern != null && this.pattern.matcher(line).find() == false) {
                        fastCtr++;
                        continue;
                    }
                    this.lines.put(Pair.of(procCtr, line)); // , 100, TimeUnit.SECONDS);
                    procCtr++;
                } // WHILE
                in.close();
                if (debug.val) LOG.debug("Finished reading file. Telling all ProcessingThreads to stop when their queue is empty");
            } catch (InterruptedException ex) {
                if (this.stop == false) throw new RuntimeException(ex);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                // Tell all the load threads to stop before we finish
                for (ProcessingThread lt : this.processingThreads) {
                    lt.stop();
                } // FOR
            }
            if (debug.val) LOG.debug(String.format("Read %d lines [fast_filter=%d]", procCtr, fastCtr));
        }
        public synchronized void stop() {
            if (this.stop == false) {
                if (debug.val) LOG.debug("ReadThread told to stop by LoadThread [queue_size=" + this.lines.size() + "]");
                this.stop = true;
                this.lines.clear();
                this.self.interrupt();
            }
        }
    } // END CLASS
    
    /**
     * PROCESSING THREAD
     */
    public static class ProcessingThread implements Runnable {
        final int id;
        final Workload workload;
        final File input_path;
        final ReadThread reader;
        final Database catalog_db;
        final Filter filter;
        final AtomicInteger counters[];
         
        boolean stop = false;
        
        public ProcessingThread(Workload workload, File input_path, ReadThread reader, Database catalog_db, Filter filter, AtomicInteger counters[]) {
            this.workload = workload;
            this.input_path = input_path;
            this.reader = reader;
            this.catalog_db = catalog_db;
            this.filter = filter;
            this.counters = counters;
            this.id = reader.processingThreadId.getAndIncrement();
        }
        
        @Override
        public void run() {
            Thread self = Thread.currentThread();
            self.setName(String.format("%s-%d", this.getClass().getSimpleName(), this.id));
            if (debug.val) LOG.debug(String.format("Starting %s [%d / %d]",
                                       this.getClass().getSimpleName(), this.id, this.reader.processingThreads.size()));

            AtomicInteger element_ctr = this.counters[ELEMENT_CTR_IDX];
            AtomicInteger xact_ctr = this.counters[TXN_CTR_IDX];
            AtomicInteger query_ctr = this.counters[QUERY_CTR_IDX];
            AtomicInteger weightedTxn_ctr = this.counters[WEIGHTED_TXN_CTR_IDX];
            AtomicInteger weightedQuery_ctr = this.counters[WEIGHTED_QUERY_CTR_IDX];
            
            while (true) {
                String line = null;
                Integer line_ctr = null;
                JSONObject jsonObject = null;
                Pair<Integer, String> p = null;
                
                try {
                    p = this.reader.lines.poll(100, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    // IGNORE
                    break;
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
    
                if (p == null) {
                    if (this.stop) {
                        if (debug.val) LOG.debug("Queue is empty and we were told to stop!");
                        break;
                    }
                    if (debug.val) LOG.debug("Queue is empty but we haven't been told to stop yet");
                    continue;
                }
                
                line_ctr = p.getFirst();
                line = p.getSecond();
                if (trace.val)
                    LOG.trace(String.format("Processing TransactionTrace on line %d [queueSize=%d, bytes=%d]",
                                             line_ctr, reader.lines.size(), line.length()));
                try {
                    try {
                        jsonObject = new JSONObject(line);
                    } catch (JSONException ex) {
                        String msg = String.format("Ignoring invalid TransactionTrace on line %d of '%s'", (line_ctr+1), input_path);
                        if (debug.val) {
                            LOG.warn(msg, ex);
                        } else {
                            LOG.warn(msg); 
                        }
                        continue;
                    }
                
                    // TransactionTrace
                    if (jsonObject.has(TransactionTrace.Members.TXN_ID.name())) {
                        
                        // If we have already loaded in up to our limit, then we don't need to
                        // do anything else. But we still have to keep reading because we need
                        // be able to load in our index structures that are at the bottom of the file
                        //
                        // NOTE: If we ever load something else but the straight trace dumps, then the following
                        // line should be a continue and not a break.
                        
                        // Load the xact from the jsonObject
                        TransactionTrace xact = null;
                        try {
                            xact = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
                        } catch (Throwable ex) {
                            LOG.warn(ex.getMessage());
                            continue;
                        }
                        if (xact == null) {
                            throw new Exception("Failed to deserialize transaction trace on line " + xact_ctr);
                        } else if (filter != null) {
                            FilterResult result = null;
                            
                            // It's ok to do this because the real CPU bottleneck is 
                            // the JSON deserialization
                            synchronized (filter) {
                                result = filter.apply(xact);
                            } // SYNCH
                            if (trace.val) LOG.trace(xact + " Filter Result: " + result);
                            
                            if (result == FilterResult.HALT) {
                                // We have to tell the ReadThread to stop too!
                                if (debug.val) LOG.debug("Got HALT response from filter! Telling ReadThread to stop!");
                                this.reader.stop();
                                break;
                            }
                            else if (result == FilterResult.SKIP) continue;
                            if (trace.val) LOG.trace(result + ": " + xact);
                        }
    
                        // Keep track of how many trace elements we've loaded so that we can make sure
                        // that our element trace list is complete
                        int x = xact_ctr.incrementAndGet();
                        if (debug.val && x % 10000 == 0) LOG.debug("Processed " + xact_ctr + " transactions...");
                        query_ctr.addAndGet(xact.getQueryCount());
                        element_ctr.addAndGet(1 + xact.getQueries().size());
                        weightedTxn_ctr.addAndGet(xact.weight);
                        for (QueryTrace q : xact.getQueries()) {
                            weightedQuery_ctr.addAndGet(q.weight);
                        } // FOR
                        
                        // This call just updates the various other index structures 
                        this.workload.addTransaction(xact.getCatalogItem(catalog_db), xact, true);
                        
                    // Unknown!
                    } else {
                        throw new Exception("Unexpected serialization line in workload trace file '" + input_path.getAbsolutePath() + "'");
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Error on line " + (line_ctr+1) + " of workload trace file '" + input_path.getAbsolutePath() + "'", ex);
                }
            } // WHILE
        }
        
        public void stop() {
            if (debug.val) LOG.debug("Told to stop [queue_size=" + this.reader.lines.size() + "]");
            this.stop = true;
        }
    } // END CLASS

    
    /**
     * WRITE THREAD
     */
    public static final class WriteThread implements Runnable {
        final Database catalog_db;
        final OutputStream output;
        final LinkedBlockingDeque<TransactionTrace> traces = new LinkedBlockingDeque<TransactionTrace>();
        static final byte[] newline = "\n".getBytes();
        
        public WriteThread(Database catalog_db, OutputStream output) {
            this.catalog_db = catalog_db;
            this.output = output;
            assert(this.output != null);
        }
        
        @Override
        public void run() {
            TransactionTrace xact = null;
            while (true) {
                try {
                    xact = this.traces.take();
                    assert(xact != null);
                    write(this.catalog_db, xact, this.output);
                } catch (InterruptedException ex) {
                    // IGNORE
                    break;
                }
            } // WHILE
        }
        
        public static void write(Database catalog_db, TransactionTrace xact, OutputStream output) {
            try {
                output.write(xact.toJSONString(catalog_db).getBytes());
                output.write(newline);
                output.flush();
                if (debug.val) LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
            } catch (IOException ex) {
                LOG.fatal("Failed to write " + xact + " out to file", ex);
                System.exit(1);
            }
        }
    } // END CLASS

    /**
     * Read a Workload file and generate a Histogram for how often each procedure 
     * is executed in the trace. This is a faster method than having to deserialize the entire
     * workload trace into memory.
     * @param workload_path
     * @return
     * @throws Exception
     */
    public static ObjectHistogram<String> getProcedureHistogram(File workload_path) throws Exception {
        final ObjectHistogram<String> h = new ObjectHistogram<String>();
        final String regex = "^\\{[\\s]*" +
                             // Old Format: Start with an ID#
                             "(\"ID\":[\\d]+,)?" +
                             "\"" +
                             AbstractTraceElement.Members.NAME.name() +
                             "\":[\\s]*\"([\\w\\d]+)\"[\\s]*,[\\s]*.*";
        final Pattern p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        if (debug.val) LOG.debug("Fast generation of Procedure Histogram from Workload '" + workload_path.getAbsolutePath() + "'");
        BufferedReader reader = FileUtil.getReader(workload_path.getAbsolutePath());
        int line_ctr = 0;
        while (reader.ready()) {
            String line = reader.readLine();
            Matcher m = p.matcher(line);
            assert(m != null) : "Invalid Line #" + line_ctr + " [" + workload_path + "]\n" + line;
            assert(m.matches()) : "Invalid Line #" + line_ctr + " [" + workload_path + "]\n" + line;
            if (m.groupCount() > 0) {
                try {
                    h.put(m.group(2));
                } catch (IllegalStateException ex) {
                    LOG.error("Invalud Workload Line: " + line, ex);
                    System.exit(1);
                }
            } else {
                LOG.error("Invalid Workload Line: " + line);
                assert(m.groupCount() == 0);
            }
            line_ctr++;
        } // WHILE
        reader.close();
        
        if (debug.val) LOG.debug("Processed " + line_ctr + " workload trace records for histogram\n" + h);
        return (h);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        assert(args != null);
        System.out.println(getProcedureHistogram(new File("/home/pavlo/Documents/H-Store/SVN-Brown/trunk/files/workloads/tpce.trace.gz")));
        
    }
}
