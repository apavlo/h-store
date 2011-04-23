/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.workload;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.WorkloadTrace;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.brown.workload.Workload.Filter.FilterResult;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class Workload implements WorkloadTrace, Iterable<AbstractTraceElement<? extends CatalogType>> {
    private static final Logger LOG = Logger.getLogger(Workload.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static boolean ENABLE_SHUTDOWN_HOOKS = true;
    
    /** The output stream that we're going to write our traces to **/
    private FileOutputStream out;

    /** The last file that we loaded from **/
    private File input_path;
    private File output_path;
    
    /** Stats Path **/
    protected String stats_output;
    protected boolean saved_stats = false;
     
    /** Basic data members **/
    private Catalog catalog;
    private Database catalog_db;

    // Ordered list of element ids that are used
    private final transient List<Long> element_ids = new ArrayList<Long>();
    
    // Mapping from element ids to actual objects
    private final transient Map<Long, AbstractTraceElement<? extends CatalogType>> element_id_xref = new HashMap<Long, AbstractTraceElement<? extends CatalogType>>();
    
    // The following data structures are specific to Transactions
    private final transient List<TransactionTrace> xact_trace = new ArrayList<TransactionTrace>();
    private final transient Map<Long, TransactionTrace> xact_trace_xref = new HashMap<Long, TransactionTrace>();
    private final transient Map<TransactionTrace, Integer> trace_bach_id = new HashMap<TransactionTrace, Integer>();

    // Reverse mapping from QueryTraces to TxnIds
    private final transient Map<QueryTrace, Long> query_txn_xref = new ConcurrentHashMap<QueryTrace, Long>();
   
    // List of running queries in each batch per transaction
    // TXN_ID -> <BATCHID, COUNTER>
    private final transient Map<Long, Map<Integer, AtomicInteger>> xact_open_queries = new ConcurrentHashMap<Long, Map<Integer,AtomicInteger>>();
    
    // A mapping from a particular procedure to a list
    // Procedure Catalog Key -> List of Txns
    protected final transient Map<String, List<TransactionTrace>> proc_xact_xref = new HashMap<String, List<TransactionTrace>>();
    
    // We can have a list of Procedure names that we want to ignore, which we make transparent
    // to whomever is calling us. But this means that we need to keep track of transaction ids
    // that are being ignored. 
    protected Set<String> ignored_procedures = new HashSet<String>();
    protected Set<Long> ignored_xact_ids = new HashSet<Long>();
    
    // We also have a list of procedures that are used for bulk loading so that we can determine
    // the number of tuples and other various information about the tables.
    protected Set<String> bulkload_procedures = new HashSet<String>();
    
    // Table Statistics (generated from bulk loading)
    protected WorkloadStatistics stats;
    protected Database stats_catalog_db;
    protected final Map<String, Statement> stats_load_stmts = new HashMap<String, Statement>();
    
    // Histogram for the distribution of procedures
    protected final Histogram<String> proc_histogram = new Histogram<String>();
    
    // Transaction Start Timestamp Ranges
    protected transient Long min_start_timestamp;
    protected transient Long max_start_timestamp;

    // ----------------------------------------------------------
    // THREADS
    // ----------------------------------------------------------
    
    private static final class WriteThread implements Runnable {
        final Database catalog_db;
        final OutputStream output;
        final LinkedBlockingDeque<TransactionTrace> traces = new LinkedBlockingDeque<TransactionTrace>();
        
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
                output.write("\n".getBytes());
                output.flush();
                if (debug.get()) LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
            } catch (IOException ex) {
                LOG.fatal("Failed to write " + xact + " out to file", ex);
                System.exit(1);
            }
        }
    } // END CLASS
    
    private static WriteThread writerThread = null; 
    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output) {
        writeTransactionToStream(catalog_db, xact, output, false);
    }
    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output, boolean multithreaded) {
        if (multithreaded) {
            if (writerThread == null) {
                synchronized (Workload.class) {
                    if (writerThread == null) {
                        writerThread = new WriteThread(catalog_db, output);
                        Thread t = new Thread(writerThread);
                        t.setDaemon(true);
                        t.start();
                    }
                } // SYNCH
            }
            writerThread.traces.offer(xact);
        } else {
            WriteThread.write(catalog_db, xact, output);
        }
    }
    
    /**
     * READ THREAD
     */
    private final class ReadThread implements Runnable {
        final File input_path;
        final List<LoadThread> load_threads = new ArrayList<LoadThread>();
        final LinkedBlockingDeque<Pair<Integer, String>> lines;
        final Pattern pattern;
        boolean stop = false;
        Thread self;
        
        public ReadThread(File input_path, Pattern pattern, int num_threads) {
            this.input_path = input_path;
            this.pattern = pattern;
            this.lines = new LinkedBlockingDeque<Pair<Integer, String>>(num_threads * 2);
        }
        
        @Override
        public void run() {
            self = Thread.currentThread();
            
            BufferedReader in = null;
            try {
                in = FileUtil.getReader(this.input_path);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            
            int line_ctr = 0;
            int fast_ctr = 0;
            try {
                while (in.ready() && this.stop == false) {
                    String line = in.readLine().trim();
                    if (line.isEmpty()) continue;
                    if (this.pattern != null && this.pattern.matcher(line).find() == false) {
                        fast_ctr++;
                        continue;
                    }
                    this.lines.offerLast(Pair.of(line_ctr, line), 100, TimeUnit.SECONDS);
                    line_ctr++;
                } // WHILE
                in.close();
                if (debug.get()) LOG.debug("Finished reading file. Telling all LoadThreads to stop when their queue is empty");
            } catch (InterruptedException ex) {
                if (this.stop == false) throw new RuntimeException(ex);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                // Tell all the load threads to stop before we finish
                for (LoadThread lt : this.load_threads) {
                    lt.stop();
                } // FOR
            }
            if (debug.get()) LOG.debug(String.format("Read %d lines [fast_filter=%d]", line_ctr, fast_ctr));
        }
        public synchronized void stop() {
            if (this.stop == false) {
                if (debug.get()) LOG.debug("ReadThread told to stop by LoadThread [queue_size=" + this.lines.size() + "]");
                this.stop = true;
                this.lines.clear();
                this.self.interrupt();
            }
        }
    } // END CLASS
    
    private class LoadThread implements Runnable {
        final ReadThread reader;
        final Database catalog_db;
        final Filter filter;
        final AtomicInteger counters[];
         
        boolean stop = false;
        
        public LoadThread(ReadThread reader, Database catalog_db, Filter filter, AtomicInteger counters[]) {
            this.reader = reader;
            this.catalog_db = catalog_db;
            this.filter = filter;
            this.counters = counters;
        }
        
        @Override
        public void run() {
            final boolean trace = LOG.isTraceEnabled();

            AtomicInteger xact_ctr = this.counters[0];
            AtomicInteger query_ctr = this.counters[1];
            AtomicInteger element_ctr = this.counters[2];
            
            while (true) {
                String line = null;
                Integer line_ctr = null;
                JSONObject jsonObject = null;
                Pair<Integer, String> p = null;
                
                try {
                    p = this.reader.lines.poll(100, TimeUnit.MILLISECONDS);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }

                if (p == null) {
                    if (this.stop) {
                        if (trace) LOG.trace("Queue is empty and we were told to stop!");
                        break;
                    }
                    if (trace) LOG.trace("Queue is empty but we haven't been told to stop yet");
                    continue;
                }
                
                line_ctr = p.getFirst();
                line = p.getSecond();
                try {
                    jsonObject = new JSONObject(line);    
                
                    // TransactionTrace
                    if (jsonObject.has(TransactionTrace.Members.TXN_ID.name())) {
                        
                        // If we have already loaded in up to our limit, then we don't need to
                        // do anything else. But we still have to keep reading because we need
                        // be able to load in our index structures that are at the bottom of the file
                        //
                        // NOTE: If we ever load something else but the straight trace dumps, then the following
                        // line should be a continue and not a break.
                        
                        // Load the xact from the jsonObject
                        TransactionTrace xact = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
                        if (xact == null) {
                            throw new Exception("Failed to deserialize transaction trace on line " + xact_ctr);
                        } else if (filter != null) {
                            FilterResult result = filter.apply(xact);
                            if (result == FilterResult.HALT) {
                                // We have to tell the ReadThread to stop too!
                                if (trace) LOG.trace("Got HALT response from filter! Telling ReadThread to stop!");
                                this.reader.stop();
                                break;
                            }
                            else if (result == FilterResult.SKIP) continue;
                            if (trace) LOG.trace(result + ": " + xact);
                        }
    
                        // Keep track of how many trace elements we've loaded so that we can make sure
                        // that our element trace list is complete
                        int x = xact_ctr.incrementAndGet();
                        if (trace && x % 10000 == 0) LOG.trace("Read in " + xact_ctr + " transactions...");
                        query_ctr.addAndGet(xact.getQueryCount());
                        element_ctr.addAndGet(1 + xact.getQueries().size());
                        
                        // This call just updates the various other index structures 
                        Workload.this.addTransaction(xact.getCatalogItem(catalog_db), xact, true);
                        
                    // Unknown!
                    } else {
                        throw new Exception("Unexpected serialization line in workload trace");
                    }
                } catch (Exception ex) {
                    throw new RuntimeException("Error on line " + (line_ctr+1) + " of workload trace file", ex);
                }
            } // WHILE
        }
        
        public void stop() {
            LOG.trace("Told to stop [queue_size=" + this.reader.lines.size() + "]");
            this.stop = true;
        }
    }
    
    // ----------------------------------------------------------
    // ITERATOR
    // ----------------------------------------------------------
    
    /**
     * WorkloadIterator Filter
     */
    public abstract static class Filter {
        private Filter next;
        
        public enum FilterResult {
            ALLOW,
            SKIP,
            HALT,
        };
        
        public Filter(Filter next) {
            this.next = next;
        }
        
        public Filter() {
            this(null);
        }
        
        /**
         * Returns an ordered list of the filters within the chain that are the same type as the
         * given search class (inclusive).
         * @param <T>
         * @param search
         * @return
         */
        public final <T extends Filter> List<T> getFilters(Class<? extends T> search) {
            return (this.getFilters(search, new ArrayList<T>()));
        }
        
        @SuppressWarnings("unchecked")
        private final <T extends Filter> List<T> getFilters(Class<? extends T> search, List<T> found) {
            if (ClassUtil.getSuperClasses(this.getClass()).contains(search)) {
                found.add((T)this);
            }
            if (this.next != null) this.next.getFilters(search, found);
            return (found);
        }
        
        public final Filter attach(Filter next) {
            if (this.next != null) this.next.attach(next);
            else this.next = next;
            assert(this.next != null);
            return (this);
        }
        
        public FilterResult apply(AbstractTraceElement<? extends CatalogType> element) {
            assert(element != null);
            FilterResult result = this.filter(element); 
            if (result == FilterResult.ALLOW) {
                return (this.next != null ? this.next.apply(element) : FilterResult.ALLOW);
            }
            return (result);
        }
        
        public void reset() {
            this.resetImpl();
            if (this.next != null) this.next.reset();
            return;
        }
        
        protected abstract FilterResult filter(AbstractTraceElement<? extends CatalogType> element); 
        
        protected abstract void resetImpl();
        
        protected abstract String debug();
        
        public final String toString() {
            return (this.debug() + (this.next != null ? "\n" + this.next.toString() : ""));
        }
    }
    
    /**
     * WorkloadIterator
     */
    public class WorkloadIterator implements Iterator<AbstractTraceElement<? extends CatalogType>> {
        private int idx = 0;
        private boolean is_init = false;
        private AbstractTraceElement<? extends CatalogType> peek;
        private final Workload.Filter filter;
        
        public WorkloadIterator(Workload.Filter filter) {
            this.filter = filter;
        }
        
        public WorkloadIterator() {
            this(null);
        }
        
        private void init() {
            assert(!this.is_init);
            this.next();
            this.is_init = true;
        }
        
        @Override
        public boolean hasNext() {
            // Important! If this is the first time, then we need to get the next element
            // using next() so that we know we can even begin grabbing stuff
            if (!this.is_init) this.init();
            return (this.peek != null);
        }
        
        @Override
        public AbstractTraceElement<? extends CatalogType> next() {
            // Always set the current one what we peeked ahead and saw earlier
            AbstractTraceElement<? extends CatalogType> current = this.peek;
            this.peek = null;
            
            // Now figure out what the next element should be the next time next() is called
            int size = Workload.this.element_ids.size();
            while (this.peek == null && this.idx++ < size) {
                Long next_id = Workload.this.element_ids.get(this.idx - 1);
                AbstractTraceElement<? extends CatalogType> element = Workload.this.element_id_xref.get(next_id);
                if (element == null) {
                    LOG.warn("idx: " + this.idx);
                    LOG.warn("next_id: " + next_id);
                    // System.err.println("elements: " + Workload.this.element_id_xref);
                    LOG.warn("size: " + Workload.this.element_id_xref.size());
                }
                if (this.filter == null || (this.filter != null && this.filter.apply(element) == Filter.FilterResult.ALLOW)) {
                    this.peek = element;
                }
            } // WHILE
            return (current);
        }
        
        @Override
        public void remove() {
            // Not implemented...
        }
    };
    
    /**
     * Default Constructor
     */
    public Workload() {
        // Create a shutdown hook to make sure that always call finalize()
        if (ENABLE_SHUTDOWN_HOOKS) {
            if (LOG.isDebugEnabled()) if (debug.get()) LOG.debug("Created shutdown hook for " + Workload.class.getName());
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        Workload.this.finalize();
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                    return;
                }
            });
        }
    }
    
    /**
     * The real constructor
     * @param catalog
     */
    public Workload(Catalog catalog) {
        this();
        this.catalog = catalog;
    }
    
    /**
     * Copy Constructor with a Filter!
     * @param workload
     * @param filter
     */
    public Workload(Workload workload, Filter filter) {
        this(workload.catalog);
        
        Iterator<AbstractTraceElement<? extends CatalogType>> it = workload.iterator(filter);
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> trace = it.next();
            if (trace instanceof TransactionTrace) {
                TransactionTrace txn = (TransactionTrace)trace;
                this.addTransaction(txn.getCatalogItem(CatalogUtil.getDatabase(this.catalog)), txn);
            }
        } // WHILE
    }
    
    /**
     * We want to dump out index structures and other things to the output file
     * when we are going down.
     */
    @Override
    protected void finalize() throws Throwable {
        if (this.out != null) {
            System.err.println("Flushing workload trace output and closing files...");
            
            //
            // Workload Statistics
            //
            if (this.stats != null) this.saveStats();
            //
            // This was here in case I wanted to dump out auxillary data structures
            // As of right now, I don't need to do that, so we'll just flush and close the file
            //
            this.out.flush();
            this.out.close();
        }
        super.finalize();
    }

    /**
     * 
     */
    protected void validate() {
        if (debug.get()) LOG.debug("Checking to make sure there are no duplicate trace objects in workload");
        Set<Long> trace_ids = new HashSet<Long>();
        for (AbstractTraceElement<?> element : this) {
            long trace_id = element.getId();
            assert(!trace_ids.contains(trace_id)) : "Duplicate Trace Element: " + element;
            trace_ids.add(trace_id);
        } // FOR
    }

    /**
     * 
     */
    @Override
    public void setOutputPath(String path) {
        this.output_path = new File(path);
        try {
            this.out = new FileOutputStream(path);
            if (debug.get()) LOG.debug("Opened file '" + path + "' for logging workload trace");
        } catch (Exception ex) {
            LOG.fatal("Failed to open trace output file: " + path);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public void setStatsOutputPath(String path) {
        this.stats_output = path;
    }
    
    /**
     * 
     * @throws Exception
     */
    private void saveStats() throws Exception {
        for (TableStatistics table_stats : this.stats.getTableStatistics()) {
            table_stats.postprocess(this.stats_catalog_db);
        } // FOR
        if (this.stats_output != null) {
            this.stats.save(this.stats_output);
        }
    }

    /**
     * 
     * @param input_path
     * @param catalog_db
     * @throws Exception
     */
    public void load(String input_path, Database catalog_db) throws Exception {
        this.load(input_path, catalog_db, null);
    }
    
    /**
     * 
     * @param input_path
     * @param catalog_db
     * @param limit
     * @throws Exception
     */
    public void load(String input_path, Database catalog_db, Filter filter) throws Exception {
        if (debug.get()) LOG.debug("Reading workload trace from file '" + input_path + "'");
        this.input_path = new File(input_path);

        
        // HACK: Throw out traces unless they have the procedures that we're looking for
        Pattern temp_pattern = null;
        if (filter != null) {
            List<ProcedureNameFilter> procname_filters = filter.getFilters(ProcedureNameFilter.class);
            if (procname_filters.isEmpty() == false) {
                Set<String> names = new HashSet<String>();
                for (ProcedureNameFilter f : procname_filters) {
                    for (String name : f.getProcedureNames()) {
                        names.add(Pattern.quote(name));
                    } // FOR
                } // FOR
                if (names.isEmpty() == false) {
                    temp_pattern = Pattern.compile(String.format("\"NAME\":[\\s]*\"(%s)\"", StringUtil.join("|", names)), Pattern.CASE_INSENSITIVE);
                    if (debug.get()) {
                        LOG.debug(String.format("Fast filter for %d procedure names", names.size()));
                        LOG.debug("PATTERN: " + temp_pattern.pattern());
                    }
                } 
            }
        }
        final Pattern pattern = temp_pattern;
        
        final AtomicInteger counters[] = new AtomicInteger[] {
            new AtomicInteger(0), // TXN COUNTER
            new AtomicInteger(0), // QUERY COUNTER
            new AtomicInteger(0)  // ELEMENT COUNTER
        };
        
        List<Runnable> all_runnables = new ArrayList<Runnable>();
        int num_threads = ThreadUtil.getMaxGlobalThreads() - 1;
        
        // Create the reader thread first
        ReadThread rt = new ReadThread(this.input_path, pattern, num_threads);
        all_runnables.add(rt);
        
        // Then create all of our load threads
        for (int i = 0; i < num_threads; i++) {
            LoadThread lt = new LoadThread(rt, catalog_db, filter, counters);
            rt.load_threads.add(lt);
            all_runnables.add(lt);
        } // FOR
        
        if (debug.get()) LOG.debug(String.format("Loading workload trace using %d LoadThreads", rt.load_threads.size())); 
        ThreadUtil.runGlobalPool(all_runnables);
        this.validate();
        LOG.info("Loaded in " + this.xact_trace.size() + " txns with " + counters[1].get() + " queries from '" + this.input_path.getName() + "'");
        return;
    }
    

    
    // ----------------------------------------------------------
    // ITERATORS METHODS
    // ----------------------------------------------------------
    
    /**
     * Creates a new iterator that allows you walk through the trace elements
     * one by one in the order that they were created
     */

    /**
     * Creates a new iterator using a Filter that allows you to walk through trace elements
     * and get back just the elements that you want based on some criteria
     * @param filter
     * @return
     */
    @Override
    public Iterator<AbstractTraceElement<? extends CatalogType>> iterator() {
        return (new Workload.WorkloadIterator());
    }

    public Iterator<AbstractTraceElement<? extends CatalogType>> iterator(Workload.Filter filter) {
        return (new Workload.WorkloadIterator(filter));
    }
    
    // ----------------------------------------------------------
    // BASIC METHODS
    // ----------------------------------------------------------
    
    /**
     * Adds a stored procedure to a list of procedures to ignore.
     * If a procedure is ignored, then startTransaction() will return a null handle
     * 
     * @param name - the name of the stored procedure to ignore
     */
    @Override
    public void addIgnoredProcedure(String name) {
        this.ignored_procedures.add(name.toUpperCase());
    }
    
    /**
     * Adds a stored procedure to a list of procedures that are used for bulk-loading data
     * If a procedure is ignored, then startTransaction() will return a null handle, but
     * the trace manager could be recording additional profile statistics
     * 
     * @param name - the name of the stored procedure used for bulk loading
     */
    public void addBulkLoadProcedure(String name) {
        this.bulkload_procedures.add(name.toUpperCase());
    }

    /**
     * For the given transaction handle, get the next query batch id (starting at zero)
     * 
     * @param xact_handle - the transaction handle created by startTransaction()
     * @return the next query batch id for a transaction
     */
    @Override
    public int getNextBatchId(Object xact_handle) {
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            int batch_id = -1;
            if (this.trace_bach_id.containsKey(xact)) {
                batch_id = this.trace_bach_id.get(xact);
            }
            this.trace_bach_id.put(xact, ++batch_id);
            return (batch_id);
        }
        return -1;
    }
    
    /**
     * Returns the number of transactions loaded into this AbstractWorkload object
     * @return
     */
    public int getTransactionCount() {
        return (this.xact_trace.size());
    }
    
    /**
     * Get the set of procedures that were invoked in this workload
     * @param catalog_db
     * @return
     */
    public Set<Procedure> getProcedures(final Database catalog_db) {
        Set<String> proc_keys = this.proc_histogram.values();
        Set<Procedure> procedures = new HashSet<Procedure>();
        for (String proc_key : proc_keys) {
            procedures.add(CatalogKey.getFromKey(catalog_db, proc_key, Procedure.class));
        } // FOR
        return (procedures);
    }
    
    /**
     * Return the procedure execution histogram
     * The keys of the Histogram will be CatalogKeys
     * @return
     */
    public Histogram<String> getProcedureHistogram() {
        // Install debug label mapping
        if (!this.proc_histogram.hasDebugLabels()) {
            Map<Object, String> labels = new HashMap<Object, String>();
            for (Object o : this.proc_histogram.values()) {
                labels.put(o, CatalogKey.getNameFromKey(o.toString()));
            } // FOR
            this.proc_histogram.setDebugLabels(labels);
        }
        return (this.proc_histogram);
    }
    
    /**
     * Return the max id
     * @return
     */
    public long getMaxTraceId() {
        return (Collections.max(this.element_ids));
    }
    
    /**
     * 
     * @return
     */
    public Long getMinStartTimestamp() {
        return (this.min_start_timestamp);
    }
    /**
     * 
     * @return
     */
    public Long getMaxStartTimestamp() {
        return (this.max_start_timestamp);
    }

    /**
     * Return the time interval this txn started execution in
     * @param xact
     * @param num_intervals
     * @return
     */
    public int getTimeInterval(TransactionTrace xact, int num_intervals) {
        long timestamp = xact.getStartTimestamp();
        if (timestamp == this.max_start_timestamp) timestamp--;
        double ratio = (timestamp - this.min_start_timestamp) / (double)(this.max_start_timestamp - this.min_start_timestamp);
        int interval = (int)(num_intervals  * ratio);
        
//        System.err.println("min_timestamp=" + this.min_start_timestamp);
//        System.err.println("max_timestamp=" + this.max_start_timestamp);
//        System.err.println("timestamp=" + timestamp);
//        System.err.println("ratio= " + ratio);
//        System.err.println("interval=" + interval);
//        System.err.println("-------");
        
        return interval;
    }
    
    public Histogram<Integer> getTimeIntervalProcedureHistogram(int num_intervals) {
        final Histogram<Integer> h = new Histogram<Integer>();
        for (TransactionTrace xact : this.getTransactions()) {
            h.put(this.getTimeInterval(xact, num_intervals));
        }
        return (h);
    }
    
    public Histogram<Integer> getTimeIntervalQueryHistogram(int num_intervals) {
        final Histogram<Integer> h = new Histogram<Integer>();
        for (TransactionTrace xact : this.getTransactions()) {
            int interval = this.getTimeInterval(xact, num_intervals);
            int num_queries = xact.getQueryCount();
            h.put(interval, num_queries);
        } // FOR
        return (h);
    }
    
    /**
     * Set the catalog for monitoring
     * This must be set before starting any trace operations
     * 
     * @param catalog - the catalog that the trace manager will be working with
     */
    @Override
    public void setCatalog(Catalog catalog) {
        this.catalog = catalog;
    }
    
    /**
     * Utility method to remove a transaction from the various data structures that
     * we have to keep track of it.
     * @param xact the transaction handle to remove
     */
    protected void removeTransaction(TransactionTrace xact) {
        this.xact_trace.remove(xact);
        this.xact_trace_xref.remove(xact.getTransactionId());
        this.xact_open_queries.remove(xact.getTransactionId());
        this.trace_bach_id.remove(xact);
    }
    
    /**
     * Add a transaction to the helper data structures that we have
     * @param catalog_proc
     * @param xact
     */
    public void addTransaction(Procedure catalog_proc, TransactionTrace xact) {
        this.addTransaction(catalog_proc, xact, false);
    }
    
    /**
     * 
     * @param catalog_proc
     * @param xact
     * @param force_index_update
     */
    protected synchronized void addTransaction(Procedure catalog_proc, TransactionTrace xact, boolean force_index_update) {
        long txn_id = xact.getTransactionId();
        this.xact_trace.add(xact);
        this.xact_trace_xref.put(txn_id, xact);
        this.xact_open_queries.put(txn_id, new HashMap<Integer, AtomicInteger>());
        
        // Check whether we need to add the element id of the txn and all of its queries.
        // This happens when if we are trying to insert the txn from another one
        // It's kind of a hack, but what are you going to do when times are tough..
        if (force_index_update || this.element_ids.contains(xact.getId()) == false) {
            this.element_ids.add(xact.getId());
            this.element_id_xref.put(xact.getId(), xact);
            for (QueryTrace query : xact.getQueries()) {
                this.element_ids.add(query.getId());
                this.element_id_xref.put(query.getId(), query);
            } // FOR
        }
        
        String proc_key = CatalogKey.createKey(catalog_proc);
        if (!this.proc_xact_xref.containsKey(proc_key)) {
            this.proc_xact_xref.put(proc_key, new ArrayList<TransactionTrace>());
        }
        this.proc_xact_xref.get(proc_key).add(xact);
        this.proc_histogram.put(proc_key);
        
        if (this.min_start_timestamp == null || this.min_start_timestamp > xact.getStartTimestamp()) {
            this.min_start_timestamp = xact.getStartTimestamp();
        }
        if (this.max_start_timestamp == null || this.max_start_timestamp < xact.getStartTimestamp()) {
            this.max_start_timestamp = xact.getStartTimestamp();
        }
    }
    
    /**
     * Returns an ordered collection of all the transactions
     * @return
     */
    public List<TransactionTrace> getTransactions() {
        return (this.xact_trace);
    }
    
    /**
     * Return the proper TransactionTrace object for the given txn_id
     * @param txn_id
     * @return
     */
    public TransactionTrace getTransaction(long txn_id) { 
        return this.xact_trace_xref.get(txn_id);
    }
    
    /**
     * For a given Procedure catalog object, return all the transaction traces for it
     * @param catalog_proc
     * @return
     */
    public List<TransactionTrace> getTraces(Procedure catalog_proc) {
        String proc_key = CatalogKey.createKey(catalog_proc);
        if (this.proc_xact_xref.containsKey(proc_key)) {
            return (this.proc_xact_xref.get(proc_key));
        }
        return (new ArrayList<TransactionTrace>());
    }
    
    /**
     * Return the AbstractTraceElement object for the given id 
     * @param id
     * @return
     */
    @SuppressWarnings("unchecked")
    public <T extends AbstractTraceElement<? extends CatalogType>> T getTraceObject(long id) {
        return ((T)this.element_id_xref.get(id));
    }
    
    /**
     * 
     * @param xact_id
     * @param catalog_proc
     * @param args
     * @return
     */
    private void processBulkLoader(long xact_id, Procedure catalog_proc, Object args[]) {
        // Process the arguments and analyze the tuples being inserted
        for (int ctr = 0; ctr < args.length; ctr++) {
            // We only want VoltTables...
            if (!(args[ctr] instanceof VoltTable)) continue;
            VoltTable voltTable = (VoltTable)args[ctr];
            
            // Workload Initialization
            if (this.stats_catalog_db == null) {
                this.stats_catalog_db = (Database)catalog_proc.getParent();
                this.stats = new WorkloadStatistics(this.stats_catalog_db);
                for (Table catalog_tbl : this.stats_catalog_db.getTables()) {
                    this.stats.getTableStatistics(catalog_tbl).preprocess(this.stats_catalog_db);
                } // FOR
            }
            final Table catalog_tbl = CatalogUtil.getCatalogTable(stats_catalog_db, voltTable);
            if (catalog_tbl == null) {
                LOG.fatal("Failed to find corresponding table catalog object for parameter " + ctr + " in " + catalog_proc);
                String msg = "Table Columns: ";
                for (int i = 0, cnt = voltTable.getColumnCount(); i < cnt; i++) {
                    msg += voltTable.getColumnName(i) + " ";
                }
                LOG.fatal(msg);
                return;
            }
            final String table_key = CatalogKey.createKey(catalog_tbl);
            TableStatistics table_stats = this.stats.getTableStatistics(table_key);
            
            // Temporary Loader Procedure
            TransactionTrace loader_xact = new TransactionTrace(xact_id, catalog_proc, new Object[0]) {
                /**
                 * We need this wrapper so that when CatalogUtil tries to figure out what
                 * the index of partitioning parameter we can just use the column index of partitioning
                 * attribute of the table that we are inserting into
                 */
                private final Procedure proc = new Procedure() {
                    @Override
                    public int getPartitionparameter() {
                        return (catalog_tbl.getPartitioncolumn().getIndex());
                    }
                    @Override
                    public Catalog getCatalog() {
                        return (catalog);
                    }
                };
                @Override
                public Procedure getCatalogItem(Database catalog_db) {
                    return (proc);
                }
            }; // Nasty...
            
            // Use a temporary query trace to wrap the "insert" of each tuple
            Statement catalog_stmt = this.stats_load_stmts.get(table_key);
            if (catalog_stmt == null) {
                catalog_stmt = catalog_proc.getStatements().add("INSERT_" + catalog_tbl.getName());
                catalog_stmt.setQuerytype(QueryType.INSERT.getValue());
                this.stats_load_stmts.put(table_key, catalog_stmt);

                // TERRIBLE HACK!
                // 2011-01-25 :: Why do I need to do this??
//                String stmt_key = CatalogKey.createKey(catalog_stmt);
//                CatalogUtil.CACHE_STATEMENT_COLUMNS_KEYS.put(stmt_key, new java.util.HashSet<String>());
//                CatalogUtil.CACHE_STATEMENT_COLUMNS_KEYS.get(stmt_key).add(table_key);
            }
            QueryTrace loader_query = new QueryTrace(catalog_stmt, new Object[0], 0);
            loader_xact.addQuery(loader_query);
            
            // Gather column types
            int num_columns = voltTable.getColumnCount();
            VoltType col_types[] = new VoltType[num_columns];
            for (int i = 0; i < num_columns; i++) {
                Column catalog_col = catalog_tbl.getColumns().get(i);
                col_types[i] = VoltType.get((byte)catalog_col.getType());
            } // FOR

            loader_query.params = new Object[num_columns];
            int num_tuples = voltTable.getRowCount();
            try {
                LOG.info("Processing " + num_tuples + " (sample=10) tuples for statistics on " + catalog_tbl.getName());
                boolean show_progress = (num_tuples > 25000);
                for (int i = 0; i < num_tuples; i += 10) {
                    if (i >= num_tuples) break;
                    VoltTableRow tuple = voltTable.fetchRow(i);
                    for (int j = 0; j < num_columns; j++) {
                        loader_query.params[j] = tuple.get(j, col_types[j]);
                    } // FOR
                    table_stats.process(stats_catalog_db, loader_xact);
                    if (show_progress && i > 0 && i % 10000 == 0) LOG.info(i + " tuples");
//                        if (i > 25000) break;
                } // FOR
                LOG.info("Processing finished for " + catalog_tbl.getName());
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        this.ignored_xact_ids.add(xact_id);
    }
    
    /**
     * 
     * @param txn_id
     * @param catalog_proc
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public TransactionTrace startTransaction(long xact_id, Procedure catalog_proc, Object args[]) {
        String proc_name = catalog_proc.getName().toUpperCase();
        TransactionTrace xact_handle = null;

        // Bulk-loader Procedure
        if (this.bulkload_procedures.contains(proc_name)) {
            this.processBulkLoader(xact_id, catalog_proc, args);
            
        // Ignored/Sysproc Procedures
        } else if (this.ignored_procedures.contains(proc_name) || catalog_proc.getSystemproc()) {
            if (debug.get()) LOG.debug("Ignoring start transaction call for procedure '" + proc_name + "'");
            this.ignored_xact_ids.add(xact_id);
            
        // Procedures we want to trace
        } else {
            xact_handle = new TransactionTrace(xact_id, catalog_proc, args);
            this.addTransaction(catalog_proc, xact_handle);
            this.element_ids.add(xact_handle.getId());
            this.element_id_xref.put(xact_handle.getId(), xact_handle);
            if (debug.get()) LOG.debug(String.format("Created %s TransactionTrace with %d parameters", proc_name, args.length));
            
            // HACK
            if (this.catalog_db == null) this.catalog_db = CatalogUtil.getDatabase(catalog_proc);

            // If this is the first non bulk-loader proc that we have seen, then
            // go ahead and save the stats out to a file in case we crash later on
            if (!this.saved_stats && this.stats != null) {
                try {
                    this.saveStats();
                    this.saved_stats = true;
                } catch (Exception ex) {
                    LOG.fatal(String.format("Failed to save stats for txn #%d", xact_id), ex);
                    throw new RuntimeException(ex);
                }
            }
        }
        return (xact_handle);
    }
    
    /**
     * 
     * @param txn_id
     */
    @Override
    public void stopTransaction(Object xact_handle) {
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace txn_trace = (TransactionTrace)xact_handle;
            
            // Make sure we have stopped all of our queries
            boolean unclean = false;
            for (QueryTrace query : txn_trace.getQueries()) {
                if (!query.isStopped()) {
                    if (debug.get()) LOG.warn("Trace for '" + query + "' was not stopped before the transaction. Assuming it was aborted");
                    query.aborted = true;
                    unclean = true;
                }
            } // FOR
            if (unclean) LOG.warn("The entries in " + txn_trace + " were not stopped cleanly before the transaction was stopped");
            
            // Mark the txn as stopped
            txn_trace.stop();
            
            // Remove from internal cache data structures
            this.removeTransaction(txn_trace);
            
            if (debug.get()) LOG.debug("Stopping trace for transaction " + txn_trace);
        } else {
            LOG.fatal("Unable to stop transaction trace: Invalid transaction handle");
        }

        // Write the trace object out to our file if it is not null
        if (xact_handle != null && xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            if (this.catalog_db == null) {
                LOG.warn("The database catalog handle is null: " + xact);
            } else {
                if (this.out == null) {
                    if (debug.get()) LOG.warn("No output path is set. Unable to log trace information to file");
                } else {
                    writeTransactionToStream(this.catalog_db, xact, this.out);
                }
            }
        }
        return;
    }
    
    @Override
    public void abortTransaction(Object xact_handle) {
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace txn_trace = (TransactionTrace)xact_handle;
            
            // Abort any open queries
            for (QueryTrace query : txn_trace.getQueries()) {
                if (query.isStopped() == false) {
                    query.abort();
                    this.query_txn_xref.remove(query);
                }
            } // FOR
            txn_trace.abort();
            if (debug.get()) LOG.debug("Aborted trace for transaction " + txn_trace);
            
            // Write the trace object out to our file if it is not null
            if (this.catalog_db == null) {
                LOG.warn("The database catalog handle is null: " + txn_trace);
            } else {
                if (this.out == null) {
                    if (debug.get()) LOG.warn("No output path is set. Unable to log trace information to file");
                } else {
                    writeTransactionToStream(this.catalog_db, txn_trace, this.out);
                }
            }
        } else {
            LOG.fatal("Unable to abort transaction trace: Invalid transaction handle");
        }
    }
    
    /**
     * 
     * @param txn_id
     * @param catalog_stmt
     * @param catalog_statement
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public Object startQuery(Object xact_handle, Statement catalog_statement, Object args[], int batch_id) {
        QueryTrace query_handle = null;
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            long txn_id = xact.getTransactionId();
            
            if (this.ignored_xact_ids.contains(txn_id)) return (null);
            
            Map<Integer, AtomicInteger> open_queries = this.xact_open_queries.get(txn_id);
            // HACK
            if (open_queries == null) {
                open_queries = new HashMap<Integer, AtomicInteger>();
                this.xact_open_queries.put(txn_id, open_queries);
            }
            
            assert(open_queries != null) : "Starting a query before starting the txn?? [" + txn_id + "]";
            
            query_handle = new QueryTrace(catalog_statement, args, batch_id);
            xact.addQuery(query_handle);
            this.element_ids.add(query_handle.getId());
            this.element_id_xref.put(query_handle.getId(), query_handle);
            this.query_txn_xref.put(query_handle, txn_id);
            
            // Make sure that there aren't still running queries in the previous batch
            if (batch_id > 0) {
                AtomicInteger last_batch_ctr = open_queries.get(batch_id - 1);
                if (last_batch_ctr != null && last_batch_ctr.intValue() != 0) {
                    String msg = "Txn #" + txn_id + " is trying to start a new query in batch #" + batch_id + 
                                 " but there are still " + last_batch_ctr.intValue() + " queries running in batch #" + (batch_id-1);
                    throw new IllegalStateException(msg);
                }
            }
            
            AtomicInteger batch_ctr = open_queries.get(batch_id);
            if (batch_ctr == null) {
                synchronized (open_queries) {
                    batch_ctr = new AtomicInteger(0);
                    open_queries.put(batch_id, batch_ctr);
                } // SYNCHRONIZED
            }
            batch_ctr.incrementAndGet();
            
            if (debug.get()) LOG.debug("Created '" + catalog_statement.getName() + "' query trace record for xact '" + txn_id + "'");
        } else {
            LOG.fatal("Unable to create new query trace: Invalid transaction handle");
        }
        return (query_handle);
    }
    
    @Override
    public void stopQuery(Object query_handle) {
        if (query_handle instanceof QueryTrace) {
            QueryTrace query = (QueryTrace)query_handle;
            query.stop();
            
            // Decrement open query counter for this batch
            Long txn_id = this.query_txn_xref.remove(query);
            assert(txn_id != null) : "Unexpected QueryTrace handle that doesn't have a txn id!";
            Map<Integer, AtomicInteger> m = this.xact_open_queries.get(txn_id);
            if (m != null) {
                AtomicInteger batch_ctr = m.get(query.getBatchId());
                int count = batch_ctr.decrementAndGet();
                assert(count >= 0) : "Invalid open query counter for batch #" + query.getBatchId() + " in Txn #" + txn_id;
                if (debug.get()) LOG.debug("Stopping trace for query " + query);
            } else {
                LOG.warn(String.format("No open query counters for txn #%d???", txn_id)); 
            }
        } else {
            LOG.fatal("Unable to stop query trace: Invalid query handle");
        }
        return;
    }

    public void save(String path, Database catalog_db) {
        this.setOutputPath(path);
        this.save(catalog_db);
    }

    public void save(Database catalog_db) {
        LOG.info("Writing out workload trace to '" + this.output_path + "'");
        for (AbstractTraceElement<?> element : this) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                try {
                    //String json = xact.toJSONString(catalog_db);
                    //JSONObject jsonObject = new JSONObject(json);
                    //this.out.write(jsonObject.toString(2).getBytes());
                    
                    this.out.write(xact.toJSONString(catalog_db).getBytes());
                    this.out.write("\n".getBytes());
                    this.out.flush();
                    if (debug.get()) LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
                } catch (Exception ex) {
                    LOG.fatal(ex.getMessage());
                    ex.printStackTrace();
                }
            }
        } // FOR
    }    
    
    /**
     * 
     * @param catalog_db
     * @return
     */
    public String debug(Database catalog_db) {
        StringBuilder builder = new StringBuilder();
        for (TransactionTrace xact : this.xact_trace) {
            builder.append(xact.toJSONString(catalog_db));
        } // FOR
        JSONObject jsonObject = null;
        String ret = null;
        try {
            jsonObject = new JSONObject(builder.toString());
            ret = jsonObject.toString(2);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return (ret);
    }
}