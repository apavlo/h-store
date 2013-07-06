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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.WorkloadTrace;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 */
public class Workload implements WorkloadTrace, Iterable<TransactionTrace> {
    private static final Logger LOG = Logger.getLogger(Workload.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
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
    protected File stats_output;
    protected boolean saved_stats = false;
     
    /** Basic data members **/
    private Catalog catalog;
    private Database catalog_db;

    // The following data structures are specific to Transactions
    private final transient ListOrderedMap<Long, TransactionTrace> txn_traces = new ListOrderedMap<Long, TransactionTrace>();

    // Reverse mapping from QueryTraces to TxnIds
    private final transient Map<QueryTrace, Long> query_txn_xref = new ConcurrentHashMap<QueryTrace, Long>();
    private transient int query_ctr = 0;
   
    // List of running queries in each batch per transaction
    // TXN_ID -> <BATCHID, COUNTER>
    private final transient Map<Long, Map<Integer, AtomicInteger>> xact_open_queries = new ConcurrentHashMap<Long, Map<Integer,AtomicInteger>>();
    
    // A mapping from a particular procedure to a list
    // Procedure Catalog Key -> List of Txns
    protected final transient Map<String, List<TransactionTrace>> proc_xact_xref = new HashMap<String, List<TransactionTrace>>();
    
    // We can have a list of Procedure names that we want to ignore, which we make transparent
    // to whomever is calling us. But this means that we need to keep track of transaction ids
    // that are being ignored. 
    protected Set<String> ignored_procedures = null;
    protected final Set<Long> ignored_xact_ids = new HashSet<Long>();
    
    // We also have a list of procedures that are used for bulk loading so that we can determine
    // the number of tuples and other various information about the tables.
    protected Set<String> bulkload_procedures = new HashSet<String>();
    
    // Table Statistics (generated from bulk loading)
    protected WorkloadStatistics stats;
    protected Database stats_catalog_db;
    protected final Map<String, Statement> stats_load_stmts = new HashMap<String, Statement>();
    
    // Histogram for the distribution of procedures
    protected final ObjectHistogram<String> proc_histogram = new ObjectHistogram<String>();
    
    // Transaction Start Timestamp Ranges
    protected transient Long min_start_timestamp;
    protected transient Long max_start_timestamp;

    // ----------------------------------------------------------
    // THREADS
    // ----------------------------------------------------------
    
    private static WorkloadUtil.WriteThread writerThread = null; 
    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output) {
        writeTransactionToStream(catalog_db, xact, output, false);
    }
    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output, boolean multithreaded) {
        if (multithreaded) {
            if (writerThread == null) {
                synchronized (Workload.class) {
                    if (writerThread == null) {
                        writerThread = new WorkloadUtil.WriteThread(catalog_db, output);
                        Thread t = new Thread(writerThread);
                        t.setDaemon(true);
                        t.start();
                    }
                } // SYNCH
            }
            writerThread.traces.offer(xact);
        } else {
            synchronized (Workload.class) {
                WorkloadUtil.WriteThread.write(catalog_db, xact, output);
            } // SYNCH
        }
    }
    
    // ----------------------------------------------------------
    // FILTERS
    // ----------------------------------------------------------
    
    /**
     * WorkloadIterator
     */
    public class WorkloadIterator implements Iterator<TransactionTrace> {
        private int idx = 0;
        private boolean is_init = false;
        private TransactionTrace peek;
        private final Filter filter;
        
        public WorkloadIterator(Filter filter) {
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
        public TransactionTrace next() {
            // Always set the current one what we peeked ahead and saw earlier
            TransactionTrace current = this.peek;
            this.peek = null;
            
            // Now figure out what the next element should be the next time next() is called
            int size = Workload.this.txn_traces.size();
            while (this.peek == null && this.idx++ < size) {
                Long next_id = Workload.this.txn_traces.get(this.idx - 1);
                TransactionTrace element = Workload.this.txn_traces.get(next_id);
                if (element == null) {
                    LOG.warn("idx: " + this.idx);
                    LOG.warn("next_id: " + next_id);
                    // System.err.println("elements: " + Workload.this.element_id_xref);
                    LOG.warn("size: " + Workload.this.txn_traces.size());
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
            if (debug.val) LOG.debug("Created shutdown hook for " + Workload.class.getName());
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
     * Copy Constructor!
     * @param catalog
     * @param workload
     */
    public Workload(Catalog catalog, Workload...workloads) {
        this(catalog, null, workloads);
    }
    
    /**
     * Copy Constructor with a Filter!
     * @param catalog
     * @param filter
     * @param workload
     */
    public Workload(Catalog catalog, Filter filter, Workload...workloads) {
        this(catalog);
        Database catalog_db = CatalogUtil.getDatabase(this.catalog);
        for (Workload w : workloads) {
            assert(w != null);
            for (TransactionTrace txn : CollectionUtil.iterable(w.iterator(filter))) {
                this.addTransaction(txn.getCatalogItem(catalog_db), txn, false);
            } // WHILE
        } // FOR
    }
    
    /**
     * We want to dump out index structures and other things to the output file
     * when we are going down.
     */
    @Override
    protected void finalize() throws Throwable {
        if (this.out != null) {
            System.err.println("Flushing workload trace output and closing files...");
            
            // Workload Statistics
            if (this.stats != null) this.saveStats();
            // This was here in case I wanted to dump out auxillary data structures
            // As of right now, I don't need to do that, so we'll just flush and close the file
            this.out.flush();
            this.out.close();
        }
        super.finalize();
    }
    
    public void flush() throws IOException {
        if (this.out != null) {
            this.out.flush();
        }
    }

    /**
     * 
     */
    @Override
    public void setOutputPath(File path) {
        this.output_path = path;
        try {
            this.out = new FileOutputStream(path);
            if (debug.val) LOG.debug("Opened file '" + path + "' for logging workload trace");
        } catch (Exception ex) {
            LOG.fatal("Failed to open trace output file: " + path);
            ex.printStackTrace();
            System.exit(1);
        }
    }

    public void setStatsOutputPath(File path) {
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
    public Workload load(File input_path, Database catalog_db) throws Exception {
        return this.load(input_path, catalog_db, null);
    }
    
    /**
     * 
     * @param input_path
     * @param catalog_db
     * @param limit
     * @throws Exception
     */
    public Workload load(File input_path, Database catalog_db, Filter filter) throws Exception {
        if (debug.val)
            LOG.debug("Reading workload trace from file '" + input_path + "'");
        this.input_path = input_path;
        long start = System.currentTimeMillis();
        
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
                    if (debug.val) {
                        LOG.debug(String.format("Fast filter for %d procedure names", names.size()));
                        LOG.debug("PATTERN: " + temp_pattern.pattern());
                    }
                } 
            }
        }
        final Pattern pattern = temp_pattern;
        
        final AtomicInteger counters[] = new AtomicInteger[] {
            new AtomicInteger(0), // ELEMENT COUNTER
            new AtomicInteger(0), // TXN COUNTER
            new AtomicInteger(0), // QUERY COUNTER
            new AtomicInteger(0), // WEIGHTED TXN COUNTER
            new AtomicInteger(0), // WEIGHTED QUERY COUNTER
        };
        
        List<Runnable> all_runnables = new ArrayList<Runnable>();
        int num_threads = ThreadUtil.getMaxGlobalThreads();
        
        // Create the reader thread first
        WorkloadUtil.ReadThread rt = new WorkloadUtil.ReadThread(this.input_path, pattern, num_threads);
        all_runnables.add(rt);
        
        // Then create all of our processing threads
        for (int i = 0; i < num_threads; i++) {
            WorkloadUtil.ProcessingThread lt = new WorkloadUtil.ProcessingThread(this,
                                                                                 this.input_path,
                                                                                 rt,
                                                                                 catalog_db,
                                                                                 filter,
                                                                                 counters);
            rt.processingThreads.add(lt);
            all_runnables.add(lt);
        } // FOR
        
        if (debug.val)
            LOG.debug(String.format("Loading workload trace using %d ProcessThreads",
                      rt.processingThreads.size())); 
        ThreadUtil.runNewPool(all_runnables, all_runnables.size());
        VerifyWorkload.verify(catalog_db, this);
        
        long stop = System.currentTimeMillis();
        LOG.info(String.format("Loaded %d txns / %d queries from '%s' in %.1f seconds using %d threads",
                 this.txn_traces.size(), counters[2].get(), this.input_path.getName(), (stop - start) / 1000d, num_threads));
        if (counters[1].get() != counters[3].get() || counters[2].get() != counters[4].get()) {
            LOG.info(String.format("Weighted Workload: %d txns / %d queries",
                     counters[3].get(), counters[4].get()));
        }
        return (this);
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
    public Iterator<TransactionTrace> iterator() {
        return (new Workload.WorkloadIterator());
    }

    public Iterator<TransactionTrace> iterator(Filter filter) {
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
        if (this.ignored_procedures == null) {
            synchronized (this) {
                if (this.ignored_procedures == null) {
                    this.ignored_procedures = new HashSet<String>();
                }
            } // SYNCH
        }
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
     * Returns the number of transactions loaded into this Workload object
     * @return
     */
    public int getTransactionCount() {
        return (this.txn_traces.size());
    }
    
    /**
     * Returns the number of queries loaded into this Workload object
     * @return
     */
    public int getQueryCount() {
        return (this.query_ctr);
    }
    
    /**
     * Get the set of procedures that were invoked in this workload
     * @param catalog_db
     * @return
     */
    public Set<Procedure> getProcedures(final Database catalog_db) {
        Set<Procedure> procedures = new HashSet<Procedure>();
        for (String proc_key : this.proc_histogram.values()) {
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
        double ratio = (timestamp - this.min_start_timestamp) /
                       (double)(this.max_start_timestamp - this.min_start_timestamp);
        int interval = (int)(num_intervals  * ratio);
        
//        System.err.println("min_timestamp=" + this.min_start_timestamp);
//        System.err.println("max_timestamp=" + this.max_start_timestamp);
//        System.err.println("timestamp=" + timestamp);
//        System.err.println("ratio= " + ratio);
//        System.err.println("interval=" + interval);
//        System.err.println("-------");
        
        return interval;
    }
    
    public ObjectHistogram<Integer> getTimeIntervalProcedureHistogram(int num_intervals) {
        final ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
        for (TransactionTrace txn_trace : this.getTransactions()) {
            h.put(this.getTimeInterval(txn_trace, num_intervals));
        }
        return (h);
    }
    
    public ObjectHistogram<Integer> getTimeIntervalQueryHistogram(int num_intervals) {
        final ObjectHistogram<Integer> h = new ObjectHistogram<Integer>();
        for (TransactionTrace txn_trace : this.getTransactions()) {
            int interval = this.getTimeInterval(txn_trace, num_intervals);
            int num_queries = txn_trace.getQueryCount();
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
     * @param txn_trace the transaction handle to remove
     */
    protected void removeTransaction(TransactionTrace txn_trace) {
        this.txn_traces.remove(txn_trace.getTransactionId());
        this.xact_open_queries.remove(txn_trace.getTransactionId());
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
     * @param txn_trace
     * @param force_index_update
     */
    protected synchronized void addTransaction(Procedure catalog_proc, TransactionTrace txn_trace, boolean force_index_update) {
        if (debug.val)
            LOG.debug(String.format("Adding new %s [numTraces=%d]", txn_trace, this.txn_traces.size()));
        
        long txn_id = txn_trace.getTransactionId();
        this.txn_traces.put(txn_id, txn_trace);
        this.xact_open_queries.put(txn_id, new HashMap<Integer, AtomicInteger>());
        
        String proc_key = CatalogKey.createKey(catalog_proc);
        if (!this.proc_xact_xref.containsKey(proc_key)) {
            this.proc_xact_xref.put(proc_key, new ArrayList<TransactionTrace>());
        }
        this.proc_xact_xref.get(proc_key).add(txn_trace);
        this.proc_histogram.put(proc_key);
        
        if (this.min_start_timestamp == null || this.min_start_timestamp > txn_trace.getStartTimestamp()) {
            this.min_start_timestamp = txn_trace.getStartTimestamp();
        }
        if (this.max_start_timestamp == null || this.max_start_timestamp < txn_trace.getStartTimestamp()) {
            this.max_start_timestamp = txn_trace.getStartTimestamp();
        }
        this.query_ctr += txn_trace.getQueryCount();
    }
    
    /**
     * Returns an ordered collection of all the transactions
     * @return
     */
    public Collection<TransactionTrace> getTransactions() {
        return (this.txn_traces.values());
    }
    
    /**
     * Return the proper TransactionTrace object for the given txn_id
     * @param txn_id
     * @return
     */
    public TransactionTrace getTransaction(long txn_id) { 
        return this.txn_traces.get(txn_id);
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
        }
        // Ignored/Sysproc Procedures
        else if (catalog_proc.getSystemproc() ||
                (this.ignored_procedures != null && this.ignored_procedures.contains(proc_name))) {
            if (debug.val) LOG.debug("Ignoring start transaction call for procedure '" + proc_name + "'");
            this.ignored_xact_ids.add(xact_id);
        }
        // Procedures we want to trace
        else {
            xact_handle = new TransactionTrace(xact_id, catalog_proc, args);
            this.addTransaction(catalog_proc, xact_handle, false);
            if (debug.val) LOG.debug(String.format("Created %s TransactionTrace with %d parameters", proc_name, args.length));
            
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
    public void stopTransaction(Object xact_handle, VoltTable...result) {
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace txn_trace = (TransactionTrace)xact_handle;
            
            // Make sure we have stopped all of our queries
            boolean unclean = false;
            for (QueryTrace query : txn_trace.getQueries()) {
                if (!query.isStopped()) {
                    if (debug.val)
                        LOG.warn("Trace for '" + query + "' was not stopped before the transaction. Assuming it was aborted");
                    query.aborted = true;
                    unclean = true;
                }
            } // FOR
            if (unclean)
                LOG.warn("The entries in " + txn_trace + " were not stopped cleanly before the transaction was stopped");
            
            // Mark the txn as stopped
            txn_trace.stop();
            if (result != null) txn_trace.setOutput(result);
            
            // Remove from internal cache data structures
            this.removeTransaction(txn_trace);
            
            if (debug.val) LOG.debug("Stopping trace for transaction " + txn_trace);
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
                    if (debug.val) LOG.warn("No output path is set. Unable to log trace information to file");
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
            if (debug.val) LOG.debug("Aborted trace for transaction " + txn_trace);
            
            // Write the trace object out to our file if it is not null
            if (this.catalog_db == null) {
                LOG.warn("The database catalog handle is null: " + txn_trace);
            } else {
                if (this.out == null) {
                    if (debug.val) LOG.warn("No output path is set. Unable to log trace information to file");
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
            
            if (debug.val) LOG.debug("Created '" + catalog_statement.getName() + "' query trace record for xact '" + txn_id + "'");
        } else {
            LOG.fatal("Unable to create new query trace: Invalid transaction handle");
        }
        return (query_handle);
    }
    
    @Override
    public void stopQuery(Object query_handle, VoltTable result) {
        if (query_handle instanceof QueryTrace) {
            QueryTrace query = (QueryTrace)query_handle;
            query.stop();
            if (result != null) query.setOutput(result);
            
            // Decrement open query counter for this batch
            Long txn_id = this.query_txn_xref.remove(query);
            assert(txn_id != null) : "Unexpected QueryTrace handle that doesn't have a txn id!";
            Map<Integer, AtomicInteger> m = this.xact_open_queries.get(txn_id);
            if (m != null) {
                AtomicInteger batch_ctr = m.get(query.getBatchId());
                int count = batch_ctr.decrementAndGet();
                assert(count >= 0) : "Invalid open query counter for batch #" + query.getBatchId() + " in Txn #" + txn_id;
                if (debug.val) LOG.debug("Stopping trace for query " + query);
            } else {
                LOG.warn(String.format("No open query counters for txn #%d???", txn_id)); 
            }
        } else {
            LOG.fatal("Unable to stop query trace: Invalid query handle");
        }
        return;
    }

    public void save(File path, Database catalog_db) {
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
                    if (debug.val) LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
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
        for (TransactionTrace xact : this.txn_traces.values()) {
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