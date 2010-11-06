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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.json.JSONObject;
import org.voltdb.*;
import org.voltdb.catalog.*;
import org.voltdb.types.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.*;
import edu.brown.workload.Workload.Filter.FilterResult;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class Workload implements WorkloadTrace, Iterable<AbstractTraceElement<? extends CatalogType>> {
    private static final Logger LOG = Logger.getLogger(Workload.class.getName());

    // The output stream that we're going to write our traces to
    private FileOutputStream out;

    // Handle -> Database
    private final Map<TransactionTrace, Database> xact_db_xref = new HashMap<TransactionTrace, Database>();
    
    //
    // The last file that we loaded from
    //
    private File input_path;
    private File output_path;
    
    //
    // Stats Path
    //
    protected String stats_output;
    protected boolean saved_stats = false;
    
    
    public static boolean ENABLE_SHUTDOWN_HOOKS = true; 
    
    //
    // Basic data members
    //
    protected Catalog catalog;

    //
    // Ordered list of element ids that are used
    //
    protected final transient List<Long> element_ids = new ArrayList<Long>();
    
    //
    // Mapping from element ids to actual objects
    //
    protected final transient Map<Long, AbstractTraceElement<? extends CatalogType>> element_id_xref = new HashMap<Long, AbstractTraceElement<? extends CatalogType>>();
    
    //
    // The following data structures are specific to Transactions
    //
    protected final transient ListOrderedSet<TransactionTrace> xact_trace = new ListOrderedSet<TransactionTrace>();
    protected final transient Map<String, TransactionTrace> xact_trace_xref = new HashMap<String, TransactionTrace>();
    protected final transient Map<TransactionTrace, Integer> trace_bach_id = new HashMap<TransactionTrace, Integer>();
    
    //
    // A mapping from a particular procedure to a list
    // Procedure Catalog Key -> List of Txns
    //
    protected final transient Map<String, List<TransactionTrace>> proc_xact_xref = new HashMap<String, List<TransactionTrace>>();
    
    //
    // We can have a list of Procedure names that we want to ignore, which we make transparent
    // to whomever is calling us. But this means that we need to keep track of transaction ids
    // that are being ignored. 
    //
    protected Set<String> ignored_procedures = new HashSet<String>();
    protected Set<String> ignored_xact_ids = new HashSet<String>();
    
    //
    // We also have a list of procedures that are used for bulk loading so that we can determine
    // the number of tuples and other various information about the tables.
    //
    protected Set<String> bulkload_procedures = new HashSet<String>();
    
    //
    // Table Statistics (generated from bulk loading)
    //
    protected WorkloadStatistics stats;
    protected Database stats_catalog_db;
    protected final Map<String, Statement> stats_load_stmts = new HashMap<String, Statement>();
    
    //
    // Histogram for the distribution of procedures
    protected final Histogram proc_histogram = new Histogram();
    
    //
    // Transaction Start Timestamp Ranges
    //
    protected transient Long min_start_timestamp;
    protected transient Long max_start_timestamp;
    
    /**
     * WorkloadIterator Filter
     *
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
            //
            // Important! If this is the first time, then we need to get the next element
            // using next() so that we know we can even begin grabbing stuff
            //
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
                    System.err.println("idx: " + this.idx);
                    System.err.println("next_id: " + next_id);
                    System.err.println("elements: " + Workload.this.element_id_xref);
                    System.err.println("size: " + Workload.this.element_id_xref.size());
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
            if (LOG.isDebugEnabled()) LOG.debug("Created shutdown hook for " + Workload.class.getName());
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
        LOG.debug("Checking to make sure there are no duplicate trace objects in workload");
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
            LOG.debug("Opened file '" + path + "' for logging workload trace");
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
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        if (debug) LOG.debug("Reading workload trace from file '" + input_path + "'");
        File file = new File(input_path);

        // Check whether it's gzipped. Yeah that's right, we support that!
        BufferedReader in = null;
        if (file.getPath().endsWith(".gz")) {
            FileInputStream fin = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fin);
            in = new BufferedReader(new InputStreamReader(gzis));
        } else {
            in = new BufferedReader(new FileReader(file));   
        }
        
        long xact_ctr = 0;
        long query_ctr = 0;
        long line_ctr = 0;
        long element_ctr = 0;
        
        while (in.ready()) {
//            StringBuilder buffer = new StringBuilder();
//            do {
//                String line = in.readLine();
//                buffer.append(line);
//                if (line.equals("}")) break;
//            } while (in.ready());
//            JSONObject jsonObject = new JSONObject(buffer.toString()); //in.readLine());
            String line = in.readLine().trim();
            if (line.isEmpty()) continue;
            JSONObject jsonObject = null; 
            try {
                jsonObject = new JSONObject(line);
            } catch (Exception ex) {
                throw new Exception("Error on line " + (line_ctr+1) + " of workload trace file '" + file.getName() + "'", ex);
            }
            
            //
            // TransactionTrace
            //
            if (jsonObject.has(TransactionTrace.Members.XACT_ID.name())) {
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
                } catch (Exception ex) {
                    LOG.error("Failed JSONObject:\n" + jsonObject.toString(2));
                    throw ex;
                }
                if (xact == null) {
                    throw new Exception("Failed to deserialize transaction trace on line " + xact_ctr);
                } else if (filter != null) {
                    FilterResult result = filter.apply(xact);
                    if (result == FilterResult.HALT) break;
                    else if (result == FilterResult.SKIP) continue;
                    if (trace) LOG.trace(result + ": " + xact);
                }

                // Keep track of how many trace elements we've loaded so that we can make sure
                // that our element trace list is complete
                xact_ctr++;
                query_ctr += xact.getQueryCount();
                if (trace && xact_ctr % 10000 == 0) LOG.trace("Read in " + xact_ctr + " transactions...");
                element_ctr += 1 + xact.getQueries().size();
                
                // This call just updates the various other index structures 
                this.addTransaction(xact.getCatalogItem(catalog_db), xact, true);
                
            // Unknown!
            } else {
                LOG.fatal("Unexpected serialization line in workload trace '" + input_path + "' on line " + line_ctr);
                System.exit(1);
            }
            line_ctr++;
        } // WHILE
        in.close();
        this.validate();
        LOG.info("Loaded in " + this.xact_trace.size() + " transactions with a total of " + query_ctr + " queries from workload trace '" + file.getName() + "'");
        this.input_path = new File(input_path);
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
     * Return the procedure execution histogram
     * The keys of the Histogram will be CatalogKeys
     * @return
     */
    public Histogram getProcedureHistogram() {
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
        double ratio = (timestamp - this.min_start_timestamp) / (double)(this.max_start_timestamp - this.min_start_timestamp);
        int interval = (int)((num_intervals - 1) * ratio);
//        System.err.println("min_timestamp=" + this.min_start_timestamp);
//        System.err.println("max_timestamp=" + this.max_start_timestamp);
//        System.err.println("timestamp=" + timestamp);
//        System.err.println("ratio= " + ratio);
//        System.err.println("interval=" + interval);
//        System.err.println("-------");
        return interval;
    }
    
    public Histogram getTimeIntervalProcedureHistogram(int num_intervals) {
        final Histogram h = new Histogram();
        for (TransactionTrace xact : this.getTransactions()) {
            h.put(this.getTimeInterval(xact, num_intervals));
        }
        return (h);
    }
    
    public Histogram getTimeIntervalQueryHistogram(int num_intervals) {
        final Histogram h = new Histogram();
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
     * Creates a locally-unique transaction id
     * @param caller a reference to the object calling for a new xact id
     * @return
     */
    private final String createTransactionId(Object caller) {
        return (Integer.toString(caller.hashCode()) + "-" + Long.toString(System.currentTimeMillis()));
    }
    
    /**
     * Utility method to remove a transaction from the various data structures that
     * we have to keep track of it.
     * @param xact the transaction handle to remove
     */
    protected void removeTransaction(TransactionTrace xact) {
        this.xact_trace.remove(xact);
        this.xact_trace_xref.remove(xact.getTransactionId());
        this.trace_bach_id.remove(xact);
    }
    
    /**
     * Add a transaction to the helper data structures that we have
     * @param catalog_proc
     * @param xact
     */
    protected void addTransaction(Procedure catalog_proc, TransactionTrace xact) {
        this.addTransaction(catalog_proc, xact, false);
    }
    
    protected void addTransaction(Procedure catalog_proc, TransactionTrace xact, boolean force_index_update) {
        this.xact_trace.add(xact);
        this.xact_trace_xref.put(xact.xact_id, xact);
        
        // Check whether we need to add the element id of the txn and all of its queries.
        // This happens when if we are trying to insert the txn from another one
        // It's kind of a hack, but what times are tough..
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
    public ListOrderedSet<TransactionTrace> getTransactions() {
        return (this.xact_trace);
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
     * @param txn_id
     * @param catalog_proc
     * @param args
     * @return
     * @throws Exception
     */
    @Override
    public TransactionTrace startTransaction(Object caller, Procedure catalog_proc, Object args[]) {
        String proc_name = catalog_proc.getName().toUpperCase();
        String xact_id = this.createTransactionId(caller);
        TransactionTrace xact_handle = null;
        
        //
        // Bulk-loader Procedure
        //
        if (this.bulkload_procedures.contains(proc_name)) {
            //
            // Process the arguments and analyze the tuples being inserted
            //
            for (int ctr = 0; ctr < args.length; ctr++) {
                // We only want VoltTables...
                if (!(args[ctr] instanceof VoltTable)) continue;
                VoltTable voltTable = (VoltTable)args[ctr];
                
                //
                // Workload Initialization
                //
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
                    return (null);
                }
                final String table_key = CatalogKey.createKey(catalog_tbl);
                TableStatistics table_stats = this.stats.getTableStatistics(table_key);
                
                //
                // Temporary Loader Procedure
                //
                TransactionTrace loader_xact = new TransactionTrace(caller.toString(), catalog_proc, new Object[0]) {
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
                
                //
                // Use a temporary query trace to wrap the "insert" of each tuple
                //
                Statement catalog_stmt = this.stats_load_stmts.get(table_key);
                if (catalog_stmt == null) {
                    catalog_stmt = catalog_proc.getStatements().add("INSERT_" + catalog_tbl.getName());
                    catalog_stmt.setQuerytype(QueryType.INSERT.getValue());
                    this.stats_load_stmts.put(table_key, catalog_stmt);

                    // TERRIBLE HACK!
                    String stmt_key = CatalogKey.createKey(catalog_stmt);
                    CatalogUtil.CACHE_STATEMENT_COLUMNS_KEYS.put(stmt_key, new java.util.HashSet<String>());
                    CatalogUtil.CACHE_STATEMENT_COLUMNS_KEYS.get(stmt_key).add(table_key);
                }
                QueryTrace loader_query = new QueryTrace(caller.toString(), catalog_stmt, new Object[0], 0);
                loader_xact.addQuery(loader_query);
                
                //
                // Gather column types
                //
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
//                            if (i > 25000) break;
                    } // FOR
                    LOG.info("Processing finished for " + catalog_tbl.getName());
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            } // FOR
            this.ignored_xact_ids.add(xact_id);
        //
        // Ignored/Sysproc Procedures
        //
        } else if (this.ignored_procedures.contains(proc_name) || catalog_proc.getSystemproc()) {
            LOG.debug("Ignoring start transaction call for procedure '" + proc_name + "'");
            this.ignored_xact_ids.add(xact_id);
        //
        // Procedures we want to trace
        //
        } else {
            xact_handle = new TransactionTrace(xact_id, catalog_proc, args);
            this.addTransaction(catalog_proc, xact_handle);
            this.element_ids.add(xact_handle.getId());
            this.element_id_xref.put(xact_handle.getId(), xact_handle);
            LOG.debug("Created '" + catalog_proc.getName() + "' transaction trace record with " + xact_handle.getParams().length + " parameters");
        }

        if (xact_handle != null) {
            this.xact_db_xref.put(xact_handle, (Database)catalog_proc.getParent());
            // If this is the first non bulk-loader proc that we have seen, then
            // go ahead and save the stats out to a file in case we crash later on
            if (!this.saved_stats && this.stats != null) {
                try {
                    this.saveStats();
                    this.saved_stats = true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.exit(1);
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
            TransactionTrace xact = (TransactionTrace)xact_handle;
            //
            // Make sure we have stopped all of our queries
            //
            boolean unclean = false;
            for (QueryTrace query : xact.getQueries()) {
                if (!query.isStopped()) {
                    LOG.warn("Trace for '" + query + "' was not stopped before the transaction. Assuming it was aborted");
                    query.aborted = true;
                    unclean = true;
                }
            } // FOR
            if (unclean) LOG.warn("The entries in " + xact + " were not stopped cleanly before the transaction was stopped");
            
            xact.stop();
            LOG.debug("Stopping trace for transaction " + xact);
        } else {
            LOG.fatal("Unable to stop transaction trace: Invalid transaction handle");
        }

        //
        // Write the trace object out to our file if it is not null
        //
        if (xact_handle != null && xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            Database catalog_db = this.xact_db_xref.remove(xact);
            
            if (catalog_db == null) {
                LOG.warn("The database catalog handle is null: " + xact);
            } else {
                if (this.out == null) {
                    LOG.warn("No output path is set. Unable to log trace information to file");
                } else {
                    writeTransactionToStream(catalog_db, xact, this.out);
                }
            }
            // Remove from cache
            this.removeTransaction(xact);
        }
        
        return;
    }

    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output) {
        try {
            output.write(xact.toJSONString(catalog_db).getBytes());
            output.write("\n".getBytes());
            output.flush();
            LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
        } catch (Exception ex) {
            LOG.fatal(ex.getMessage());
            ex.printStackTrace();
        }
    }  
    
    @Override
    public void abortTransaction(Object xact_handle) {
        if (xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            
            // Abort any open queries
            for (QueryTrace query : xact.getQueries()) {
                if (query.isStopped() == false) {
                    query.abort();
                }
            } // FOR
            xact.abort();
            LOG.debug("Aborted trace for transaction " + xact);
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
            if (this.ignored_xact_ids.contains(xact.xact_id)) return (null);
            query_handle = new QueryTrace(xact.xact_id, catalog_statement, args, batch_id);
            xact.addQuery(query_handle);
            this.element_ids.add(query_handle.getId());
            this.element_id_xref.put(query_handle.getId(), query_handle);
            LOG.debug("Created '" + catalog_statement.getName() + "' query trace record for xact '" + xact.xact_id + "'");
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
            LOG.debug("Stopping trace for query " + query);
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
                    LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
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