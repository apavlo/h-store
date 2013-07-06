package edu.brown.mappings;

import java.io.File;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.StringUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

public class MappingCalculator {
    private static final Logger LOG = Logger.getLogger(MappingCalculator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static final int QUERY_INSTANCE_MAX = 100;
    
    /**
     * Represents the correlation calculations for a single stored procedure
     */
    protected class ProcedureMappings {
        private final Procedure catalog_proc;
        private final Map<Statement, AtomicInteger> query_counters = new TreeMap<Statement, AtomicInteger>();
        private final Map<Statement, Map<Integer, QueryInstance>> query_instances = new TreeMap<Statement, Map<Integer,QueryInstance>>();
        private final AtomicInteger xact_counter = new AtomicInteger(0);
        private transient boolean started = false;
        
        /**
         * Constructor
         * @param catalog_proc
         */
        public ProcedureMappings(Procedure catalog_proc) {
            super();
            
            this.catalog_proc = catalog_proc;
            for (Statement catalog_stmt : this.catalog_proc.getStatements()) {
                this.query_counters.put(catalog_stmt, new AtomicInteger(0));
                this.query_instances.put(catalog_stmt, new HashMap<Integer, QueryInstance>());
            } // FOR
        }
        
        /**
         * Marking the start of a new transaction
         * All query instance counters are reset to zero
         */
        public void start() {
            assert(!this.started);
            if (trace.val) LOG.trace("Starting new transaction for " + this.catalog_proc + " and reseting query instance counters");
            this.started = true;
            for (Statement catalog_stmt : this.query_counters.keySet()) {
                this.query_counters.get(catalog_stmt).set(0);
            } // FOR
            this.xact_counter.getAndIncrement();
        }
        
        /**
         * Marking the end of a transaction (just for sanity)
         */
        public void finish() {
            assert(this.started);
            if (trace.val) LOG.trace("Finished transaction for " + this.catalog_proc);
            this.started = false;
        }
        
        /**
         * Calculate the correlations for all underlying counters
         */
        public void calculate() {
            if (trace.val)
                LOG.trace("Calculating correlation coefficient for " + this.query_instances.size() +
                          " query instances in " + this.catalog_proc);
            for (Entry<Statement, Map<Integer, QueryInstance>> e : this.query_instances.entrySet()) {
                if (trace.val)
                    LOG.trace(CatalogUtil.getDisplayName(e.getKey()) + ": " + e.getValue().size() + " query instances"); 
                for (QueryInstance query_instance : e.getValue().values()) {
                    query_instance.calculate();
                }
            } // FOR
            if (trace.val) LOG.trace("Completed Calculations for " + this.catalog_proc);
        }
        
        /**
         * For the current transaction, return the QueryInstance object for this Statement object
         * @param catalog_stmt
         * @return
         */
        public QueryInstance getQueryInstance(Statement catalog_stmt) {
            assert(this.started) : "Must call start() before grabbing query instances";
            
            // Don't allow the counter to go beyond the limit.
            // We should probably do something more intelligent like merging...
            int current = this.query_counters.get(catalog_stmt).getAndIncrement();
            if (current > QUERY_INSTANCE_MAX) {
                this.query_counters.get(catalog_stmt).set(QUERY_INSTANCE_MAX);
                current = QUERY_INSTANCE_MAX;
            }
            
            Map<Integer, QueryInstance> instances = this.query_instances.get(catalog_stmt);
            QueryInstance ret = instances.get(current);
            if (ret == null) {
                ret = new QueryInstance(catalog_stmt, current);
                instances.put(current, ret);
                if (trace.val) LOG.trace("Created new QueryInstance for record " + ret);
            }
            assert(ret != null);
            return (ret);
        }
        
        /**
         * Returns a mapping from QueryInstance objects to another map of StmtParameters to Correlation objects
         * for those ProcParameters that have a correlation greater than the given threshold.
         * The set of Correlation objects are sorted in descending order by their coefficients.
         * @param threshold
         * @return
         */
        public ParameterMappingsSet getCorrelations(double threshold) {
            if (trace.val)
                LOG.trace("Extracting correlations above " + threshold + " for " + this.catalog_proc);
            ParameterMappingsSet results = new ParameterMappingsSet();
            for (Entry<Statement, Map<Integer, QueryInstance>> e : this.query_instances.entrySet()) {
                if (trace.val)
                    LOG.trace(String.format("%s: %d query instance",
                              CatalogUtil.getDisplayName(e.getKey()), e.getValue().size())); 
                for (QueryInstance query_instance : e.getValue().values()) {
                    results.addAll(query_instance.getParameterMappingsSet(threshold));
                } // FOR
            } // FOR
            return (results);
        }
        
        @Override
        public String toString() {
            String ret = this.catalog_proc.getName() + " Correlations:\n";
            
            String format = "%-22s %d\n";
            ret += String.format(format, "# of Statements:", this.query_counters.size());
            ret += String.format(format, "# of Transactions:", this.xact_counter.get());
            
            ret += "Query Instance Counters:\n";
            for (Statement catalog_stmt : this.query_instances.keySet()) {
                ret += StringUtil.SPACER + String.format(format, catalog_stmt.getName() + ":", this.query_instances.get(catalog_stmt).size());
            } // FOR
            
            ret += StringUtil.DOUBLE_LINE + StringUtil.DOUBLE_LINE;
            
            for (Statement catalog_stmt : this.query_instances.keySet()) {
                if (this.query_instances.get(catalog_stmt).isEmpty()) continue;
                ret += this.debug(catalog_stmt);
                ret += StringUtil.DOUBLE_LINE;
            } // FOR
            ret += StringUtil.DOUBLE_LINE + StringUtil.DOUBLE_LINE;
            
            return (ret);
        }
        
        public String debug(Statement catalog_stmt) {
            String ret = catalog_stmt.getName() + "\n";
            String inner_spacer = "|" + StringUtil.SPACER;
            for (QueryInstance query_instance : this.query_instances.get(catalog_stmt).values()) {
                ret += query_instance.toString(inner_spacer);
            }
            return (ret);
        }
        
    } // END CLASS
    
    /**
     * Represents an instance of a query being executed in a transaction
     * The index value indicates the number of times this query was executed before in the same transaction
     */
    protected class QueryInstance extends Pair<Statement, Integer> {
        private final Map<StmtParameter, Map<ProcParameter, ProcParameterCorrelation>> correlations = new ListOrderedMap<StmtParameter, Map<ProcParameter,ProcParameterCorrelation>>();
        
        public QueryInstance(Statement catalog_stmt, Integer index) {
            super(catalog_stmt, index);
            
            Procedure catalog_proc = (Procedure)catalog_stmt.getParent();
            for (StmtParameter catalog_stmt_param : CatalogUtil.getSortedCatalogItems(catalog_stmt.getParameters(), "index")) {
                Map<ProcParameter, ProcParameterCorrelation> c = new ListOrderedMap<ProcParameter, ProcParameterCorrelation>();
                for (ProcParameter catalog_proc_param : CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index")) {
                    c.put(catalog_proc_param, new ProcParameterCorrelation(catalog_proc_param));
                } // FOR
                this.correlations.put(catalog_stmt_param, c);
            } // FOR
        }
        
        public ProcParameterCorrelation getProcParameterCorrelation(StmtParameter catalog_stmt_param, ProcParameter catalog_proc_param) {
            return (this.correlations.get(catalog_stmt_param).get(catalog_proc_param));
        }
        
        public void calculate() {
            if (trace.val) LOG.trace("Calculating correlation coefficients for " + this.correlations.size() + " StmtParameters in " + this.getFirst());
            for (Entry<StmtParameter, Map<ProcParameter, ProcParameterCorrelation>> e : this.correlations.entrySet()) {
                if (trace.val) LOG.trace(CatalogUtil.getDisplayName(e.getKey()) + ": " + e.getValue().size() + " ProcParameterMappings"); 
                for (ProcParameterCorrelation ppc : e.getValue().values()) {
                    ppc.calculate();
                } // FOR
            } // FOR
        }
        
        /**
         * Returns a mapping from StmtParameters to ProcParameters that have a correlation greater
         * than the given threshold. The set of Correlation objects are sorted in descending order
         * by their coefficients.
         * @param threshold
         * @return
         */
        public ParameterMappingsSet getParameterMappingsSet(double threshold) {
            ParameterMappingsSet results = new ParameterMappingsSet();
            if (trace.val)
                LOG.trace(String.format("Extracting correlations for %d StmtParameters in %s",
                          this.correlations.size(), this.getFirst().fullName()));
            
            for (Entry<StmtParameter, Map<ProcParameter, ProcParameterCorrelation>> e : this.correlations.entrySet()) {
                if (trace.val)
                    LOG.trace(String.format("%s: %d %s",
                              CatalogUtil.getDisplayName(e.getKey()),
                              e.getValue().size(),
                              ProcParameterCorrelation.class.getSimpleName())); 
                // Loop through all of the ProcParameter correlation objects and create new 
                // Correlation objects for any results that we get back from each of them 
                for (ProcParameterCorrelation ppc : e.getValue().values()) {
                    for (Pair<Integer, Double> pair : ppc.getMappings(threshold)) {
                        ParameterMapping c = new ParameterMapping(
                                this.getFirst(),
                                this.getSecond(),
                                e.getKey(),
                                ppc.getProcParameter(),
                                pair.getFirst(),
                                pair.getSecond()
                        );
                        results.add(c);
                        if (trace.val) LOG.trace("New Correlation: " + c);
                    } // FOR (results)
                }
            } // FOR
            return (results);
        }
        
        @Override
        public String toString() {
            return (this.getClass().getSimpleName() + "[" + this.getFirst().getName() + "::#" + this.getSecond() + "]");
        }
        
        public String toString(String spacer) {
            String ret = spacer + "+ " + this.toString() + " - [# of Parameters=" + this.correlations.size() + "]\n";
            String inner_spacer = spacer + "|" + StringUtil.SPACER;
            String inner_inner_spacer = inner_spacer + "|" + StringUtil.SPACER;
            for (StmtParameter catalog_stmt_param : this.correlations.keySet()) {
                ret += inner_spacer + "+ StmtParameter[Index=" + catalog_stmt_param.getIndex() + "]\n";
                for (ProcParameter catalog_proc_param : this.correlations.get(catalog_stmt_param).keySet()) {
                    ret += this.correlations.get(catalog_stmt_param).get(catalog_proc_param).toString(inner_inner_spacer);
                } // FOR
                // ret += spacer + StringUtil.SINGLE_LINE;
            } // FOR
            return (ret);
        }
    } // END CLASS
        
    /**
     * For the given ProcParameter, this class maintains a AbstractCorrelation calculation for a QueryInstance 
     * Provides a wrapper to handle array values
     */
    protected class ProcParameterCorrelation extends TreeMap<Integer, AbstractMapping> {
        private static final long serialVersionUID = 1L;
        private final ProcParameter catalog_proc_param;
        private final boolean is_array;
        
        public ProcParameterCorrelation(ProcParameter catalog_param) {
            super();
            this.catalog_proc_param = catalog_param;
            this.is_array = catalog_param.getIsarray(); 
        }
        
        public AbstractMapping getAbstractCorrelation() {
            return (this.get(0));
        }
        
        public AbstractMapping getAbstractCorrelation(int index) {
            assert(index == 0 || (this.is_array && index > 0));
            AbstractMapping p = this.get(index);
            if (p == null) {
                p = new RatioMapping();
                this.put(index, p);
            }
            return (p);
        }
        
        public ProcParameter getProcParameter() {
            return this.catalog_proc_param;
        }
        
        public boolean getIsArray() {
            return (this.is_array);
        }
        
        public void calculate() {
            if (trace.val) LOG.trace("Calculating correlation coefficients for " + this.size() + " ProcParameters instances");
            for (AbstractMapping p : this.values()) {
                p.calculate();
            } // FOR
            if (trace.val) LOG.trace(this.toString());
        }

        /**
         * Returns all of the ProcParameters that have a correlation greater than the given threshold
         * @param threshold
         * @return
         */
        public Set<Pair<Integer, Double>> getMappings(double threshold) {
            Set<Pair<Integer, Double>> ret = new HashSet<Pair<Integer,Double>>();
            for (Integer index : this.keySet()) {
                AbstractMapping p = this.get(index);
                Double result = p.calculate();
                if (result != null && result >= threshold) {
                    ret.add(Pair.of(index, result));
                }
            } // FOR
            return (ret);
        }
        
        @Override
        public String toString() {
            return this.toString("");
        }
        
        public String toString(String spacer) {
            StringBuilder sb = new StringBuilder();
            sb.append(spacer)
              .append(this.getClass().getSimpleName())
              .append(" [")
              .append("Index=" + this.catalog_proc_param.getIndex() + ", ")
              .append("# of Entries=" + this.size())
              .append("]");
            return (sb.toString());
        }
        
        public String debug() {
            String spacer = "";
            StringBuilder sb = new StringBuilder(this.toString());
            sb.append("\n");
            for (Integer index : this.keySet()) {
                sb.append(spacer).append(StringUtil.SPACER)
                  .append("[" + index + "] " + this.get(index) + "\n");
            } // FOR
            return (sb.toString());
        }
    } // END CLASS

    private final Database catalog_db;
    private final Map<Procedure, ProcedureMappings> mappings = new HashMap<Procedure, ProcedureMappings>();
   
    /**
     * Constructor
     * @param catalog_db
     */
    public MappingCalculator(Database catalog_db) {
        this.catalog_db = catalog_db;
        
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            this.mappings.put(catalog_proc, new ProcedureMappings(catalog_proc));
        } // FOR
    }
    
    public ProcedureMappings getProcedureCorrelations(Procedure catalog_proc) {
        return (this.mappings.get(catalog_proc));
    }
    
    /**
     * Recursively invoke the calculate method for all underlying ProcedureCorrelation objects
     */
    public void calculate() {
        if (debug.val)
            LOG.debug(String.format("Calculating correlations for %d %s",
                      this.mappings.size(), ProcedureMappings.class.getSimpleName()));
        for (ProcedureMappings pm : this.mappings.values()) {
            if (pm.catalog_proc.getSystemproc()) continue;
            pm.calculate();
        } // FOR
        if (debug.val)
            LOG.debug(String.format("Completed calculations for %s",
                      ProcedureMappings.class.getSimpleName()));
    }
    
    /**
     * Process all transaction records in a WorkloadIterator
     * @param it
     * @throws Exception
     */
    public void process(Iterator<AbstractTraceElement<? extends CatalogType>> it) throws Exception {
        long xact_ctr = 0;
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (xact_ctr++ % 100 == 0) LOG.info("Processing xact #" + xact_ctr);
                this.processTransaction(xact);
            }
            
        } // WHILE
    }
    
    /**
     * Process a single transaction trace
     * @param xact_trace
     * @throws Exception
     */
    public void processTransaction(TransactionTrace xact_trace) throws Exception {
        if (trace.val) LOG.trace("Processing correlations for " + xact_trace);
        Procedure catalog_proc = xact_trace.getCatalogItem(this.catalog_db);
        assert(catalog_proc != null);
        ProcedureMappings correlation = this.mappings.get(catalog_proc);
        correlation.start();
        
        // Cast all the ProcParameters once in the beginning
        Number xact_params[][] = new Number[xact_trace.getParams().length][];
        for (int i = 0; i < xact_params.length; i++) {
            ProcParameter catalog_proc_param = catalog_proc.getParameters().get("index", i);
            assert(catalog_proc_param != null);
            VoltType proc_param_type = VoltType.get(catalog_proc_param.getType());
            
            try {
                // Arrays
                if (catalog_proc_param.getIsarray()) {
                    Object param_arr[] = xact_trace.getParam(i);
                    xact_params[i] = new Number[param_arr.length];
                    for (int ii = 0; ii < param_arr.length; ii++) {
                        xact_params[i][ii] = this.getParamAsNumber(proc_param_type, param_arr[ii]);
                    } // FOR
                    
                // Scalars (just store in the first element of the array
                } else {
                    xact_params[i] = new Number[] {
                        this.getParamAsNumber(proc_param_type, xact_trace.getParam(i))
                    };
                }
            } catch (Exception ex) {
                LOG.error("Failed to process " + CatalogUtil.getDisplayName(catalog_proc_param));
                throw ex;
            }
        } // FOR
        
        // Now run through all of the queries and calculate the correlation between StmtParameters and ProcParameters
        for (QueryTrace query_trace : xact_trace.getQueries()) {
            Statement catalog_stmt = query_trace.getCatalogItem(this.catalog_db);
            QueryInstance query_instance = correlation.getQueryInstance(catalog_stmt);
            Object query_params[] = query_trace.getParams();
            
            // For each of the StmtParameter, update the correlation information for each of the ProcParameters
            for (int i = 0; i < query_params.length; i++) {
                StmtParameter catalog_stmt_param = catalog_stmt.getParameters().get(i);
                assert(catalog_stmt_param != null);
                VoltType stmt_param_type = VoltType.get(catalog_stmt_param.getJavatype());
                assert(stmt_param_type != VoltType.INVALID);
                Number stmt_param_val = this.getParamAsNumber(stmt_param_type, query_params[i]);

                for (int ii = 0; ii < xact_params.length; ii++) {
                    ProcParameter catalog_proc_param = catalog_proc.getParameters().get(ii);
                    assert(catalog_proc_param != null) : "Missing ProcParameter in " + catalog_proc + " at index " + ii;
                    VoltType proc_param_type = VoltType.get(catalog_proc_param.getType());
                    assert(proc_param_type != VoltType.INVALID);
                    
                    ProcParameterCorrelation ppc = query_instance.getProcParameterCorrelation(catalog_stmt_param, catalog_proc_param);
                    for (int iii = 0; iii < xact_params[ii].length; iii++) {
                        ppc.getAbstractCorrelation(iii).addOccurrence(stmt_param_val, xact_params[ii][iii]);
                    } // FOR
                } // FOR (xact_params)
            } // FOR (query_params)
        } // FOR (query_trace)
        correlation.finish();
    }
    
    /**
     * Get a ParameterMappings object
     * @param threshold
     * @return
     */
    public ParameterMappingsSet getParameterMappings(double threshold) {
        ParameterMappingsSet ret = new ParameterMappingsSet();
        LOG.debug("Extracting ParameterMappings above threshold " + threshold + " [# of correlations=" + this.mappings.size() + "]");
        for (ProcedureMappings pc : this.mappings.values()) {
            if (pc.catalog_proc.getSystemproc()) continue;
            ret.addAll(pc.getCorrelations(threshold));
        } // FOR
        return (ret);
    }
    
    /**
     * Helper function to cast the raw Object of a parameter into the proper Number
     * This will convert Dates to longs representing their time in milliseconds
     * @param type
     * @param raw_value
     * @return
     * @throws ParseException
     */
    protected Number getParamAsNumber(VoltType type, Object raw_value) throws ParseException {
        if (raw_value == null) return (null);
        assert(type != VoltType.INVALID);
        
        Number ret = null;
        
        switch (type) {
            case TIMESTAMP: {
                Object param_obj = VoltTypeUtil.getObjectFromString(type, raw_value.toString());
                ret = ((TimestampType)param_obj).getTime();
                break;
            }
            case STRING:
                ret = raw_value.hashCode();
                break;
            case BOOLEAN:
                ret = ((Boolean)raw_value ? 1 : 0);
                break;
            default: {
                Object param_obj = VoltTypeUtil.getObjectFromString(type, raw_value.toString());
                ret = (Number)param_obj;
                break;
            }
        } // SWITCH
        return (ret);
    }
    
    /**
     * Tester
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_MAPPINGS_OUTPUT
        );
        LOG.info("Starting " + MappingCalculator.class.getSimpleName());
        if (debug.val)
            LOG.debug("Workload Procedures Distribution:\n" + args.workload.getProcedureHistogram());
        
        MappingCalculator cc = new MappingCalculator(args.catalog_db);
        int ctr = 0;
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                try {
                    cc.processTransaction((TransactionTrace)element);
                } catch (Exception ex) {
                    throw new Exception("Failed to process " + element, ex);
                }
                ctr++;
            }
        } // FOR
        LOG.info("Finished processing " + ctr + " TransactionTraces. Now calculating correlation coeffcients...");
        cc.calculate();
        
//        System.err.println("Dumping out correlations...");
//        
//        for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
//            if (!catalog_proc.getName().equals("neworder")) continue;
//            System.err.println(cc.getProcedureCorrelations(catalog_proc));
//        } // FOR
        
        double threshold = 1.0d;
        if (args.hasDoubleParam(ArgumentsParser.PARAM_MAPPINGS_THRESHOLD)) {
            threshold = args.getDoubleParam(ArgumentsParser.PARAM_MAPPINGS_THRESHOLD);
        }
        ParameterMappingsSet pc = cc.getParameterMappings(threshold);
        File output_path = args.getFileParam(ArgumentsParser.PARAM_MAPPINGS_OUTPUT);
        assert(!pc.isEmpty());
        if (debug.val) LOG.debug("DEBUG DUMP:\n" + pc.debug());
        pc.save(output_path);
        LOG.info(String.format("Wrote %s to '%s'", pc.getClass().getSimpleName(), output_path));
    }
}