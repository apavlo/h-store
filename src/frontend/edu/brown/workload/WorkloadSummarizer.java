package edu.brown.workload;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.Consumer;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.Producer;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class WorkloadSummarizer {
    private static final Logger LOG = Logger.getLogger(WorkloadSummarizer.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final Database catalog_db;
    private final PartitionEstimator p_estimator;
    private final ParameterMappingsSet mappings;
    private final Collection<Procedure> target_procedures;
    private final Collection<Column> candidate_columns;
    private final Map<Statement, List<StmtParameter>> target_stmt_params = new HashMap<Statement, List<StmtParameter>>();
    private final Map<Procedure, List<ProcParameter>> target_proc_params = new HashMap<Procedure, List<ProcParameter>>();
    private Integer num_intervals;
    
    private class DuplicateTraceElements<CT extends CatalogType, T extends AbstractTraceElement<CT>> extends HashMap<CT, Map<String, Set<T>>> {
        private static final long serialVersionUID = 1L;
        private boolean has_duplicates = false;
        
        synchronized void add(CT catalog_item, String hash_str, T trace) {
            Map<String, Set<T>> m = this.get(catalog_item);
            if (m == null) {
                m = new ConcurrentHashMap<String, Set<T>>();
                this.put(catalog_item, m);
            }
            Set<T> s = m.get(hash_str);
            if (s == null) {
                s = new ListOrderedSet<T>();
                m.put(hash_str, s);
            }
            s.add(trace);
            this.has_duplicates = this.has_duplicates || s.size() > 1;
        }
        
        public Collection<T> getWeightedTraceElements() {
            List<T> new_elements = new ArrayList<T>();
            for (CT catalog_item : this.keySet()) {
                for (Set<T> s : this.get(catalog_item).values()) {
                    int weight = 0;
                    for (T t : s) {
                        weight += t.getWeight();
                    }
                    if (weight == 0) continue;
                    T t = CollectionUtil.first(s);
                    t.setWeight(weight);
                    new_elements.add(t);
                } // FOR
            } // FOR
            return (new_elements);
        }
        
        @Override
        public void clear() {
            for (Map<String, Set<T>> m : this.values()) {
                for (Set<T> s : m.values()) {
                    s.clear();
                } // FOR
            } // FOR
            this.has_duplicates = false;
        }
        
        public boolean hasDuplicates() {
            return (this.has_duplicates);
        }
    }
    
    /**
     * Constructor
     * @param catalog_db
     * @param p_estimator
     * @param mappings
     * @param procedures
     * @param candidate_columns
     */
    public WorkloadSummarizer(Database catalog_db, PartitionEstimator p_estimator, ParameterMappingsSet mappings, Collection<Procedure> procedures, Collection<Column> candidate_columns) {
        assert(procedures != null);
        assert(candidate_columns != null);
        
        this.catalog_db = catalog_db;
        this.p_estimator = p_estimator;
        this.mappings = mappings;
        this.target_procedures = procedures;
        this.candidate_columns = candidate_columns;
        this.buildTargetParameters();
    }
    
    protected WorkloadSummarizer(Database catalog_db, PartitionEstimator p_estimator, ParameterMappingsSet mappings) {
        this(catalog_db, p_estimator, mappings,
             CollectionUtil.addAll(new HashSet<Procedure>(), catalog_db.getProcedures()),
             CatalogUtil.getAllColumns(catalog_db));
    }
    
    public void setIntervals(Integer intervals) {
        if (debug.val) LOG.debug("Compression Intervals: " + intervals);
        this.num_intervals = intervals;
    }
    
    /**
     * Main entry point
     * @param workload
     * @return
     */
    public Workload process(Workload workload) {
        return (this.removeDuplicateTransactions(this.removeDuplicateQueries(workload)));
    }
    
    protected List<StmtParameter> getTargetParameters(Statement catalog_stmt) {
        return (this.target_stmt_params.get(catalog_stmt));
    }
    
    protected List<ProcParameter> getTargetParameters(Procedure catalog_proc) {
        return (this.target_proc_params.get(catalog_proc));
    }
    
    /**
     * Construct the internal lists that identify which StmtParameters we actually care about
     * when pruning duplicate queries based on the parameter hashes 
     */
    private void buildTargetParameters() {
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;

            // For each StmtParameter, look to see whether the column that it references is in our 
            // list of candidate columns. If it is, then that means we will want to include it the value's hash 
            // when determining whether a QueryTrace is unique
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                List<StmtParameter> stmt_params = new ArrayList<StmtParameter>();
                for (StmtParameter catalog_param : catalog_stmt.getParameters()) {
                    Column catalog_col = PlanNodeUtil.getColumnForStmtParameter(catalog_param);
                    assert(catalog_col != null);
                    if (this.candidate_columns.contains(catalog_col)) {
                        stmt_params.add(catalog_param);
                    }
                } // FOR (parameter)
                this.target_stmt_params.put(catalog_stmt, stmt_params);
                if (debug.val)
                    LOG.debug(String.format("%s - Relevant Parameters: %s", catalog_stmt.fullName(), stmt_params)); 
            } // FOR (statement)
            
            // For each ProcParameter, get the mappings to all of the StmtParameters
            // We can then check whether those StmtParameters are used against a column that we care about
            // If it is, then can put it in our list of relevant ProcParameters for this Procedure
            List<ProcParameter> proc_params = new ArrayList<ProcParameter>();
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                boolean matched = false;
                for (ParameterMapping c : mappings.get(catalog_param)) {
                    assert(c.getStatementColumn() != null);
                    if (this.candidate_columns.contains(c.getStatementColumn())) {
                        matched = true;
                        break;
                    }
                } // FOR
                if (matched) proc_params.add(catalog_param);
            } // FOR (parameter)
            this.target_proc_params.put(catalog_proc, proc_params);
            if (debug.val)
                LOG.debug(String.format("%s - Relevant Parameters: %s", catalog_proc.fullName(), proc_params)); 
        } // FOR (procedure)
    }

    protected String getTransactionTraceSignature(Procedure catalog_proc, TransactionTrace txn_trace, Integer interval) {
        SortedSet<String> queries = new TreeSet<String>();
        for (QueryTrace query_trace : txn_trace.getQueries()) {
            Statement catalog_stmt = query_trace.getCatalogItem(catalog_db);
            queries.add(this.getQueryTraceSignature(catalog_stmt, query_trace));
        } // FOR
        
        String signature = catalog_proc.getName() + "->";
        if (interval != null) signature += "INT[" + interval + "]";
        signature += this.getParamSignature(txn_trace, this.target_proc_params.get(catalog_proc));
        
        for (String q : queries) {
            signature += "\n" + q;
        } // FOR
        
        if (trace.val) LOG.trace(txn_trace + " ==> " + signature);
        
        return (signature);
    }
    
    protected String getQueryTraceSignature(Statement catalog_stmt, QueryTrace query_trace) {
//        int weight = (query_trace.hasWeight() ? query_trace.getWeight() : 1);
        String param_signature = this.getParamSignature(query_trace, this.target_stmt_params.get(catalog_stmt));
//        return String.format("%s[%.2f]%s", catalog_stmt.getName(), weight, param_signature);
        return String.format("%s->%s", catalog_stmt.getName(), param_signature);
    }
        
    protected String getParamSignature(AbstractTraceElement<? extends CatalogType> element, List<? extends CatalogType> target_params) {
        Object params[] = element.getParams();
        
        String sig = (element.aborted ? "ABRT-" : "");
        if (target_params != null) {
            AbstractHasher hasher = p_estimator.getHasher();
            boolean first = true;
            for (CatalogType catalog_param : target_params) {
                // Skip types that are always unique (and not useful for partitioning)
                VoltType vtype = VoltType.get((catalog_param instanceof StmtParameter ? ((StmtParameter)catalog_param).getJavatype() :
                                                                                        ((ProcParameter)catalog_param).getType()));
                if (vtype == VoltType.STRING || vtype == VoltType.TIMESTAMP) continue;
                
                // Add a prefix to separate parameters
                if (first == false) sig += "|";
                
                // StmtParameter
                if (catalog_param instanceof StmtParameter) {
                    int idx = ((StmtParameter)catalog_param).getIndex();
                    sig += hasher.hash(params[idx]);
                    
                // ProcParameter
                } else if (catalog_param instanceof ProcParameter) {
                    ProcParameter catalog_procparam = (ProcParameter)catalog_param;
                    int idx = catalog_procparam.getIndex();
                    
                    // ARRAY
                    if (catalog_procparam.getIsarray()) {
                        Set<Integer> hashes = new TreeSet<Integer>();
                        for (Object o : (Object[])params[idx]) {
                            hashes.add(hasher.hash(o));
                        } // FOR
                        boolean first_hash = true;
                        for (Integer hash : hashes) {
                            if (first_hash == false) sig += ","; 
                            sig += hash;
                            first_hash = false;
                        } // FOR
                    // SCALAR
                    } else {
                        sig += hasher.hash(params[idx]);        
                    }
                } else {
                    assert(false) : "Unexpected: " + catalog_param;
                }
                first = false;
            } // FOR
        }
        return (sig);
    }
    
    /**
     * Remove duplicate transaction invocations and populate a new Workload
     * @param workload
     * @return
     */
    protected Workload removeDuplicateTransactions(final Workload workload) {
        final DuplicateTraceElements<Procedure, TransactionTrace> duplicates = new DuplicateTraceElements<Procedure, TransactionTrace>();
        
        // PRODUCER
        Producer<TransactionTrace, TransactionTrace> producer = new Producer<TransactionTrace, TransactionTrace>(workload) {
            @Override
            public Pair<Consumer<TransactionTrace>, TransactionTrace> transform(TransactionTrace t) {
                return this.defaultTransform(t);
            }
        };
        
        // CONSUMERS
        for (int i = 0, cnt = ThreadUtil.getMaxGlobalThreads(); i < cnt; i++) {
            Consumer<TransactionTrace> c = new Consumer<TransactionTrace>() {
                @Override
                public void process(TransactionTrace txn_trace) {
                    Procedure catalog_proc = txn_trace.getCatalogItem(catalog_db);
                    if (target_procedures.contains(catalog_proc) == false) return;
                    Integer interval = (num_intervals != null ? workload.getTimeInterval(txn_trace, num_intervals) : null); 
                    String signature = getTransactionTraceSignature(catalog_proc, txn_trace, interval);
                    assert(signature != null);
                    assert(signature.isEmpty() == false);
                    duplicates.add(catalog_proc, signature, txn_trace);
                }
            };
            producer.addConsumer(c);
        } // FOR
        
        ThreadUtil.runGlobalPool(producer.getRunnablesList()); // BLOCKING
        
        if (duplicates.hasDuplicates() == false) return (workload);
        Workload new_workload = new Workload(this.catalog_db.getCatalog());
        for (TransactionTrace txn_trace : duplicates.getWeightedTraceElements()) {
            new_workload.addTransaction(txn_trace.getCatalogItem(catalog_db), txn_trace);
        } // FOR
        
        LOG.info(String.format("Reduced Workload from (%d txns / %d queries) to (%d txns / %d queries)",
                               workload.getTransactionCount(), workload.getQueryCount(),
                               new_workload.getTransactionCount(), new_workload.getQueryCount()));
        return (new_workload);
    }
    
    /**
     * Removed duplicate query invocations within a single TransactionTrace. Duplicate QueryTraces will
     * be replaced with a single QueryTrace that is weighted. This will remove batch boundaries.
     * Returns a new workload that contains the new TransactionTraces instances with the 
     * pruned list of QueryTraces.
     * @param workload
     * @return
     */
    protected Workload removeDuplicateQueries(Workload workload) {
        final Workload new_workload = new Workload(this.catalog_db.getCatalog());
        final AtomicInteger trimmed_ctr = new AtomicInteger(0);
        
        // PRODUCER
        Producer<TransactionTrace, TransactionTrace> producer = Producer.defaultProducer(workload);
        
        // CONSUMERS
        for (int i = 0, cnt = ThreadUtil.getMaxGlobalThreads(); i < cnt; i++) {
            final DuplicateTraceElements<Statement, QueryTrace> duplicates = new DuplicateTraceElements<Statement, QueryTrace>();
            Consumer<TransactionTrace> c = new Consumer<TransactionTrace>() {
                @Override
                public void process(TransactionTrace txn_trace) {
                    Procedure catalog_proc = txn_trace.getCatalogItem(catalog_db);
                    if (target_procedures.contains(catalog_proc) == false) return;
                    duplicates.clear();            
                    for (QueryTrace query_trace : txn_trace.getQueries()) {
                        Statement catalog_stmt = query_trace.getCatalogItem(catalog_db);
                        String param_hashes = getQueryTraceSignature(catalog_stmt, query_trace);
                        duplicates.add(catalog_stmt, param_hashes, query_trace);
                    } // FOR (query)
                    
                    // If this TransactionTrace has duplicate queries, then we will want to consturct
                    // a new TransactionTrace that has the weighted queries. Note that will cause us 
                    // to have to remove any batches
                    if (duplicates.hasDuplicates()) {
                        TransactionTrace new_txn_trace = (TransactionTrace)txn_trace.clone();
                        new_txn_trace.setQueries(duplicates.getWeightedTraceElements());
                        new_workload.addTransaction(new_txn_trace.getCatalogItem(catalog_db), new_txn_trace);
                        trimmed_ctr.incrementAndGet();
                    } else {
                        new_workload.addTransaction(txn_trace.getCatalogItem(catalog_db), txn_trace);
                    }
                }
            };
            producer.addConsumer(c);
        } // FOR
        
        ThreadUtil.runGlobalPool(producer.getRunnablesList()); // BLOCKING
        
        if (debug.val)
            LOG.debug(String.format("Reduced Workload %d -> %d txns [%.2f]  / %d -> %d queries [%.2f]",
                                    workload.getTransactionCount(), new_workload.getTransactionCount(),
                                    (workload.getTransactionCount() - new_workload.getTransactionCount()) / (double)new_workload.getTransactionCount(),
                                    workload.getQueryCount(), new_workload.getQueryCount(),
                                    (workload.getQueryCount() - new_workload.getQueryCount()) / (double)new_workload.getQueryCount()
            ));
        return (new_workload);
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_WORKLOAD_OUTPUT,
            ArgumentsParser.PARAM_MAPPINGS
        );
        Integer intervals = args.getIntParam(ArgumentsParser.PARAM_DESIGNER_INTERVALS);
        
        LOG.info(String.format("Compressing workload based on %d partitions%s",
                 args.catalogContext.numberOfPartitions,
                 (intervals != null ? " over " + intervals + " intervals" : "")));
        LOG.info("BEFORE:\n" + args.workload.getProcedureHistogram());
        
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalogContext);
        WorkloadSummarizer ws = new WorkloadSummarizer(args.catalog_db, p_estimator, args.param_mappings);
        if (intervals != null) ws.setIntervals(intervals);
        Workload new_workload = ws.process(args.workload);
        assert(new_workload != null);
        LOG.info("AFTER:\n" + new_workload.getProcedureHistogram());
        
        File output_path = args.getFileParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT);
        LOG.info("Saving compressed workload '" + output_path + "'");
        new_workload.save(output_path, args.catalog_db);
    }
    
}
