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
package edu.brown.utils;

import java.io.*;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.*;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import org.voltdb.CatalogContext;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.costmodel.AbstractCostModel;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.*;
import edu.brown.designer.indexselectors.*;
import edu.brown.designer.mappers.*;
import edu.brown.designer.partitioners.*;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hashing.*;
import edu.brown.logging.LoggerUtil;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.EstimationThresholds;
import edu.brown.statistics.*;
import edu.brown.workload.*;
import edu.brown.workload.filters.*;
import edu.brown.hstore.conf.HStoreConf;

/**
 * @author pavlo
 */
public class ArgumentsParser {
    protected static final Logger LOG = Logger.getLogger(ArgumentsParser.class);
    static {
        LoggerUtil.setupLogging();
    }
    
    // HACK
    public static boolean DISABLE_UPDATE_CATALOG = false;

    // --------------------------------------------------------------
    // INPUT PARAMETERS
    // --------------------------------------------------------------

    public static final String PARAM_CATALOG = "catalog";
    public static final String PARAM_CATALOG_JAR = PARAM_CATALOG + ".jar";
    public static final String PARAM_CATALOG_OUTPUT = PARAM_CATALOG + ".output";
    public static final String PARAM_CATALOG_TYPE = PARAM_CATALOG + ".type";
    public static final String PARAM_CATALOG_SCHEMA = PARAM_CATALOG + ".schema";
    public static final String PARAM_CATALOG_LABELS = PARAM_CATALOG + ".labels";
    public static final String PARAM_CATALOG_HOSTS = PARAM_CATALOG + ".hosts";
    public static final String PARAM_CATALOG_NUM_HOSTS = PARAM_CATALOG + ".numhosts";
    public static final String PARAM_CATALOG_HOST_CORES = PARAM_CATALOG + ".hosts.cores";
    public static final String PARAM_CATALOG_HOST_THREADS = PARAM_CATALOG + ".hosts.threads";
    public static final String PARAM_CATALOG_HOST_MEMORY = PARAM_CATALOG + ".hosts.memory";
    public static final String PARAM_CATALOG_PORT = PARAM_CATALOG + ".port";
    public static final String PARAM_CATALOG_PARTITION = PARAM_CATALOG + ".partition";
    public static final String PARAM_CATALOG_SITES_PER_HOST = PARAM_CATALOG + ".hosts.numsites";
    public static final String PARAM_CATALOG_PARTITIONS_PER_SITE = PARAM_CATALOG + ".site.numpartitions";

    public static final String PARAM_CONF = "conf";
    public static final String PARAM_CONF_OUTPUT = PARAM_CONF + ".output";

    public static final String PARAM_WORKLOAD = "workload";
    public static final String PARAM_WORKLOAD_XACT_LIMIT = PARAM_WORKLOAD + ".xactlimit";
    public static final String PARAM_WORKLOAD_XACT_WEIGHTS = PARAM_WORKLOAD + ".xactweights";
    public static final String PARAM_WORKLOAD_XACT_OFFSET = PARAM_WORKLOAD + ".xactoffset";
    public static final String PARAM_WORKLOAD_QUERY_LIMIT = PARAM_WORKLOAD + ".querylimit";
    public static final String PARAM_WORKLOAD_REMOVE_DUPES = PARAM_WORKLOAD + ".removedupes";
    public static final String PARAM_WORKLOAD_PROC_EXCLUDE = PARAM_WORKLOAD + ".procexclude";
    public static final String PARAM_WORKLOAD_PROC_INCLUDE = PARAM_WORKLOAD + ".procinclude";
    public static final String PARAM_WORKLOAD_PROC_SAMPLE = PARAM_WORKLOAD + ".sampling";
    public static final String PARAM_WORKLOAD_PROC_INCLUDE_MULTIPLIER = PARAM_WORKLOAD_PROC_INCLUDE + ".multiplier";
    public static final String PARAM_WORKLOAD_RANDOM_PARTITIONS = PARAM_WORKLOAD + ".randompartitions";
    public static final String PARAM_WORKLOAD_BASE_PARTITIONS = PARAM_WORKLOAD + ".basepartitions";
    public static final String PARAM_WORKLOAD_OUTPUT = PARAM_WORKLOAD + ".output";

    public static final String PARAM_STATS = "stats";
    public static final String PARAM_STATS_OUTPUT = PARAM_STATS + ".output";
    public static final String PARAM_STATS_SCALE_FACTOR = PARAM_STATS + ".scalefactor";

    public static final String PARAM_MAPPINGS = "mappings";
    public static final String PARAM_MAPPINGS_OUTPUT = PARAM_MAPPINGS + ".output";
    public static final String PARAM_MAPPINGS_THRESHOLD = PARAM_MAPPINGS + ".threshold";

    public static final String PARAM_MARKOV = "markov";
    public static final String PARAM_MARKOV_OUTPUT = PARAM_MARKOV + ".output";
    public static final String PARAM_MARKOV_THRESHOLDS = PARAM_MARKOV + ".thresholds";
    public static final String PARAM_MARKOV_THRESHOLDS_VALUE = PARAM_MARKOV + ".thresholds.value";
    public static final String PARAM_MARKOV_THRESHOLDS_OUTPUT = PARAM_MARKOV_THRESHOLDS + ".output";
    public static final String PARAM_MARKOV_PARTITIONS = PARAM_MARKOV + ".partitions";
    public static final String PARAM_MARKOV_TOPK = PARAM_MARKOV + ".topk";
    public static final String PARAM_MARKOV_ROUNDS = PARAM_MARKOV + ".rounds";
    public static final String PARAM_MARKOV_THREADS = PARAM_MARKOV + ".threads";
    public static final String PARAM_MARKOV_SPLIT = PARAM_MARKOV + ".split";
    public static final String PARAM_MARKOV_GLOBAL = PARAM_MARKOV + ".global";
    public static final String PARAM_MARKOV_RECOMPUTE_END = PARAM_MARKOV + ".recompute_end";
    public static final String PARAM_MARKOV_RECOMPUTE_WARMUP = PARAM_MARKOV + ".recompute_warmup";
    public static final String PARAM_MARKOV_SPLIT_TRAINING = PARAM_MARKOV_SPLIT + ".training";
    public static final String PARAM_MARKOV_SPLIT_VALIDATION = PARAM_MARKOV_SPLIT + ".validation";
    public static final String PARAM_MARKOV_SPLIT_TESTING = PARAM_MARKOV_SPLIT + ".testing";
    
    private static final String PARAM_CONFLICTS = "conflicts";
    public static final String PARAM_CONFLICTS_EXCLUDE_PROCEDURES = PARAM_CONFLICTS + ".exclude_procedures";
    public static final String PARAM_CONFLICTS_EXCLUDE_STATEMENTS = PARAM_CONFLICTS + ".exclude_statements";
    public static final String PARAM_CONFLICTS_FOCUS_PROCEDURE = PARAM_CONFLICTS + ".focus";

    public static final String PARAM_DESIGNER = "designer";
    public static final String PARAM_DESIGNER_PARTITIONER = PARAM_DESIGNER + ".partitioner";
    public static final String PARAM_DESIGNER_MAPPER = PARAM_DESIGNER + ".mapper";
    public static final String PARAM_DESIGNER_INDEXER = PARAM_DESIGNER + ".indexer";
    public static final String PARAM_DESIGNER_THREADS = PARAM_DESIGNER + ".threads";
    public static final String PARAM_DESIGNER_INTERVALS = PARAM_DESIGNER + ".intervals";
    public static final String PARAM_DESIGNER_COSTMODEL = PARAM_DESIGNER + ".costmodel";
    public static final String PARAM_DESIGNER_HINTS = PARAM_DESIGNER + ".hints";
    public static final String PARAM_DESIGNER_HINTS_PREFIX = PARAM_DESIGNER_HINTS + ".";
    public static final String PARAM_DESIGNER_CHECKPOINT = PARAM_DESIGNER + ".checkpoint";

    public static final String PARAM_PARTITION_PLAN = "partitionplan";
    public static final String PARAM_PARTITION_PLAN_OUTPUT = PARAM_PARTITION_PLAN + ".output";
    public static final String PARAM_PARTITION_PLAN_APPLY = PARAM_PARTITION_PLAN + ".apply";
    public static final String PARAM_PARTITION_PLAN_REMOVE_PROCS = PARAM_PARTITION_PLAN + ".removeprocs";
    public static final String PARAM_PARTITION_PLAN_RANDOM_PROCS = PARAM_PARTITION_PLAN + ".randomprocs";
    public static final String PARAM_PARTITION_PLAN_NO_SECONDARY = PARAM_PARTITION_PLAN + ".nosecondary";
    public static final String PARAM_PARTITION_PLAN_IGNORE_MISSING = PARAM_PARTITION_PLAN + ".ignore_missing";

    public static final String PARAM_PARTITION_MAP = "partitionmap";
    public static final String PARAM_PARTITION_MAP_OUTPUT = PARAM_PARTITION_MAP + ".output";

    private static final String PARAM_HASHER = "hasher";
    public static final String PARAM_HASHER_CLASS = PARAM_HASHER + ".class";
    public static final String PARAM_HASHER_PROFILE = PARAM_HASHER + ".profile";
    public static final String PARAM_HASHER_OUTPUT = PARAM_HASHER + ".output";
    
    private static final String PARAM_TERMINAL = "terminal";
    public static final String PARAM_TERMINAL_CSV = PARAM_TERMINAL + ".csv";
    public static final String PARAM_TERMINAL_HOST = PARAM_TERMINAL + ".host";
    public static final String PARAM_TERMINAL_PORT = PARAM_TERMINAL + ".port";

    private static final String PARAM_SITE = "site";
    public static final String PARAM_SITE_HOST = PARAM_SITE + ".host";
    public static final String PARAM_SITE_PORT = PARAM_SITE + ".port";
    public static final String PARAM_SITE_PARTITION = PARAM_SITE + ".partition";
    public static final String PARAM_SITE_ID = PARAM_SITE + ".id";
    public static final String PARAM_SITE_IGNORE_DTXN = PARAM_SITE + ".ignore_dtxn";
    public static final String PARAM_SITE_STATUS_INTERVAL = PARAM_SITE + ".statusinterval";
    public static final String PARAM_SITE_STATUS_INTERVAL_KILL = PARAM_SITE + ".statusinterval_kill";
    public static final String PARAM_SITE_CLEANUP_INTERVAL = PARAM_SITE + ".cleanup_interval";
    public static final String PARAM_SITE_CLEANUP_TXN_EXPIRE = PARAM_SITE + ".cleanup_txn_expire";
    public static final String PARAM_SITE_ENABLE_PROFILING = PARAM_SITE + ".enable_profiling";
    public static final String PARAM_SITE_MISPREDICT_CRASH = PARAM_SITE + ".mispredict_crash";

    public static final List<String> PARAMS = new ArrayList<String>();
    static {
        for (Field field : ArgumentsParser.class.getDeclaredFields()) {
            try {
                if (field.getName().startsWith("PARAM_")) {
                    ArgumentsParser.PARAMS.add(field.get(null).toString());
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }; // STATIC

    /**
     * Parameter Key -> Value
     */
    private final Map<String, String> params = new LinkedHashMap<String, String>();

    private final Map<String, String> conf_params = new LinkedHashMap<String, String>();

    /**
     * "Leftover" Parameters (getopt style)
     */
    private final List<String> opt_params = new ArrayList<String>();

    /**
     * Special Case: Designer Hints Override
     */
    private final Map<String, String> hints_params = new ListOrderedMap<String, String>();

    /**
     * Catalog Attributes
     */
    public CatalogContext catalogContext = null;
    public Catalog catalog = null;
    public Database catalog_db = null;
    public File catalog_path = null;
    public ProjectType catalog_type = null;

    /**
     * Workload Trace Attributes
     */
    public Workload workload = null;
    public File workload_path = null;
    public Long workload_xact_limit = null;
    public Long workload_xact_offset = 0l;
    public Long workload_query_limit = null;
    public final Set<Integer> workload_base_partitions = new HashSet<Integer>();
    public Filter workload_filter = null;

    /**
     * Workload Statistics Attributes
     */
    public WorkloadStatistics stats = null;
    public File stats_path = null;

    /**
     * Transaction Estimation Stuff
     */
    public final ParameterMappingsSet param_mappings = new ParameterMappingsSet();
    public EstimationThresholds thresholds;

    /**
     * Designer Components
     */
    public int max_concurrent = 1;
    public int num_intervals = 100;
    public final DesignerHints designer_hints = new DesignerHints();
    public File designer_checkpoint;
    public Class<? extends AbstractPartitioner> partitioner_class = BranchAndBoundPartitioner.class;
    public Class<? extends AbstractMapper> mapper_class = AffinityMapper.class;
    public Class<? extends AbstractIndexSelector> indexer_class = SimpleIndexSelector.class;
    public PartitionPlan pplan;
    public PartitionMapping pmap;
    public IndexPlan iplan;
    public Class<? extends AbstractCostModel> costmodel_class = SingleSitedCostModel.class;
    public AbstractCostModel costmodel;

    /**
     * Hasher
     */
    public Class<? extends AbstractHasher> hasher_class = DefaultHasher.class;
    public AbstractHasher hasher;

    /**
     * Empty Constructor
     */
    public ArgumentsParser() {
        // Nothing to do
    }

    public List<String> getOptParams() {
        return (this.opt_params);
    }

    public int getOptParamCount() {
        return (this.opt_params.size());
    }

    public String getOptParam(int idx) {
        return (this.opt_params.get(idx));
    }

    @SuppressWarnings("unchecked")
    public <T> T getOptParam(int idx, VoltType vt) {
        assert (idx >= 0);
        String val = (idx < this.opt_params.size() ? this.opt_params.get(idx) : null);
        if (val != null) {
            try {
                return ((T) VoltTypeUtil.getObjectFromString(vt, val));
            } catch (ParseException ex) {
                throw new RuntimeException("Failed to cast optional parameter " + idx + " [value=" + val + "]", ex);
            }
        }
        return (null);
    }

    public Boolean getBooleanOptParam(int idx) {
        return (this.getOptParam(idx, VoltType.BOOLEAN));
    }

    public Byte getByteOptParam(int idx) {
        Object obj = this.getOptParam(idx, VoltType.TINYINT);
        if (obj != null)
            obj = ((Number) obj).byteValue();
        return ((Byte) obj);
    }

    public Short getShortOptParam(int idx) {
        Object obj = this.getOptParam(idx, VoltType.SMALLINT);
        if (obj != null)
            obj = ((Number) obj).shortValue();
        return ((Short) obj);
    }

    public Integer getIntOptParam(int idx) {
        Object obj = this.getOptParam(idx, VoltType.INTEGER);
        if (obj != null)
            obj = ((Number) obj).intValue();
        return ((Integer) obj);
    }

    public Long getLongOptParam(int idx) {
        return (this.getOptParam(idx, VoltType.BIGINT));
    }

    public Double getDoubleOptParam(int idx) {
        return (this.getOptParam(idx, VoltType.DECIMAL));
    }

    public Map<String, String> getParams() {
        return (this.params);
    }

    public String getParam(String key) {
        return (this.params.get(key));
    }

    public Integer getIntParam(String key) {
        String val = this.params.get(key);
        Integer ret = null;
        if (val != null)
            ret = Integer.valueOf(val);
        return (ret);
    }

    public Long getLongParam(String key) {
        String val = this.params.get(key);
        Long ret = null;
        if (val != null)
            ret = Long.valueOf(val);
        return (ret);
    }

    public Double getDoubleParam(String key) {
        String val = this.params.get(key);
        Double ret = null;
        if (val != null)
            ret = Double.valueOf(val);
        return (ret);
    }

    public Boolean getBooleanParam(String key, Boolean defaultValue) {
        String val = this.params.get(key);
        Boolean ret = defaultValue;
        if (val != null)
            ret = Boolean.valueOf(val);
        return (ret);
    }

    public Boolean getBooleanParam(String key) {
        return (this.getBooleanParam(key, null));
    }

    public File getFileParam(String key) {
        String val = this.params.get(key);
        File ret = null;
        if (val != null)
            ret = new File(val);
        return (ret);
    }

    public boolean hasParam(String key) {
        return (this.params.get(key) != null);
    }

    public boolean hasIntParam(String key) {
        if (this.hasParam(key)) {
            try {
                Long val = Long.valueOf(this.params.get(key));
                if (val != null)
                    return (true);
            } catch (NumberFormatException ex) {
                // Nothing...
            }
        }
        return (false);
    }

    public boolean hasDoubleParam(String key) {
        if (this.hasParam(key)) {
            try {
                Double val = Double.valueOf(this.params.get(key));
                if (val != null)
                    return (true);
            } catch (NumberFormatException ex) {
                // Nothing...
            }
        }
        return (false);
    }

    public boolean hasBooleanParam(String key) {
        if (this.hasParam(key)) {
            Boolean val = Boolean.valueOf(this.params.get(key));
            if (val != null)
                return (true);
        }
        return (false);
    }

    public void setDatabase(Database catalog_db) {
        this.catalog_db = catalog_db;
    }

    /**
     * Check whether they have all the parameters they need
     * 
     * @param params
     * @throws IllegalArgumentException
     */
    public void require(String... params) throws IllegalArgumentException {
        for (String param : params) {
            if (!this.hasParam(param)) {
                throw new IllegalArgumentException("Missing parameter '" + param + "'. Required Parameters = " + Arrays.asList(params));
            }
        } // FOR
        return;
    }

    public Map<String, String> getHStoreConfParameters() {
        return (this.conf_params);
    }

    /**
     * Return an object with the proper objects loaded
     * 
     * @param args
     * @return
     * @throws Exception
     */
    public static ArgumentsParser load(String args[], String... required) throws Exception {
        ArgumentsParser au = new ArgumentsParser();
        au.process(args, required);

        // System.out.println("catalog: " + au.catalog);
        // System.out.println("catalog_db: " + au.catalog_db);
        // System.out.println("workload: " + au.workload);
        // System.out.println("workload.limit: " + au.workload_limit);
        // System.out.println("stats: " + au.stats);

        return (au);
    }

    /**
     * @throws Exception
     */
    private void loadWorkload() throws Exception {
        final boolean debug = LOG.isDebugEnabled();
        // Workload Trace
        if (this.params.containsKey(PARAM_WORKLOAD)) {
            assert (this.catalog_db != null) : "Missing catalog!";
            File path = new File(this.params.get(PARAM_WORKLOAD));

            boolean weightedTxns = this.getBooleanParam(PARAM_WORKLOAD_XACT_WEIGHTS, false);
            if (debug)
                LOG.debug("Use Transaction Weights in Limits: " + weightedTxns);

            // This will prune out duplicate trace records...
            if (params.containsKey(PARAM_WORKLOAD_REMOVE_DUPES)) {
                DuplicateTraceFilter filter = new DuplicateTraceFilter();
                this.workload_filter = (this.workload_filter != null ? filter.attach(this.workload_filter) : filter);
                if (debug)
                    LOG.debug("Attached " + filter.debugImpl());
            }

            // TRANSACTION OFFSET
            if (params.containsKey(PARAM_WORKLOAD_XACT_OFFSET)) {
                this.workload_xact_offset = Long.parseLong(params.get(PARAM_WORKLOAD_XACT_OFFSET));
                ProcedureLimitFilter filter = new ProcedureLimitFilter(-1l, this.workload_xact_offset, weightedTxns);
                // Important! The offset should go in the front!
                this.workload_filter = (this.workload_filter != null ? filter.attach(this.workload_filter) : filter);
                if (debug)
                    LOG.debug("Attached " + filter.debugImpl());
            }

            // BASE PARTITIONS
            if (params.containsKey(PARAM_WORKLOAD_RANDOM_PARTITIONS) || params.containsKey(PARAM_WORKLOAD_BASE_PARTITIONS)) {
                BasePartitionTxnFilter filter = new BasePartitionTxnFilter(new PartitionEstimator(this.catalogContext));

                // FIXED LIST
                if (params.containsKey(PARAM_WORKLOAD_BASE_PARTITIONS)) {
                    for (String p_str : this.getParam(PARAM_WORKLOAD_BASE_PARTITIONS).split(",")) {
                        workload_base_partitions.add(Integer.valueOf(p_str));
                    } // FOR
                    // RANDOM
                } else {
                    double factor = this.getDoubleParam(PARAM_WORKLOAD_RANDOM_PARTITIONS);
                    List<Integer> all_partitions = new ArrayList<Integer>(catalogContext.getAllPartitionIds());
                    Collections.shuffle(all_partitions, new Random());
                    workload_base_partitions.addAll(all_partitions.subList(0, (int) (all_partitions.size() * factor)));
                }
                filter.addPartitions(workload_base_partitions);
                this.workload_filter = (this.workload_filter != null ? this.workload_filter.attach(filter) : filter);
                if (debug)
                    LOG.debug("Attached " + filter.debugImpl());
            }

            // Txn Limit
            this.workload_xact_limit = this.getLongParam(PARAM_WORKLOAD_XACT_LIMIT);
            ObjectHistogram<String> proc_histogram = null;

            // Include/exclude procedures from the traces
            if (params.containsKey(PARAM_WORKLOAD_PROC_INCLUDE) || params.containsKey(PARAM_WORKLOAD_PROC_EXCLUDE)) {
                Filter filter = new ProcedureNameFilter(weightedTxns);

                // INCLUDE
                String temp = params.get(PARAM_WORKLOAD_PROC_INCLUDE);
                if (temp != null && !temp.equals(ProcedureNameFilter.INCLUDE_ALL)) {

                    // We can take the counts for PROC_INCLUDE and scale them
                    // with the multiplier
                    double multiplier = 1.0d;
                    if (this.hasDoubleParam(PARAM_WORKLOAD_PROC_INCLUDE_MULTIPLIER)) {
                        multiplier = this.getDoubleParam(PARAM_WORKLOAD_PROC_INCLUDE_MULTIPLIER);
                        if (debug)
                            LOG.debug("Workload Procedure Multiplier: " + multiplier);
                    }

                    // Default Txn Frequencies
                    String procinclude = params.get(PARAM_WORKLOAD_PROC_INCLUDE);
                    if (procinclude.equalsIgnoreCase("default")) {
                        procinclude = AbstractProjectBuilder.getProjectBuilder(catalog_type).getTransactionFrequencyString();
                    }

                    Map<String, Integer> limits = new HashMap<String, Integer>();
                    int total_unlimited = 0;
                    int total = 0;
                    for (String proc_name : procinclude.split(",")) {
                        int limit = -1;
                        // Check if there is a limit for this procedure
                        if (proc_name.contains(":")) {
                            String pieces[] = proc_name.split(":");
                            proc_name = pieces[0];
                            limit = (int) Math.round(Integer.parseInt(pieces[1]) * multiplier);
                        }

                        if (limit < 0) {
                            if (proc_histogram == null) {
                                if (debug)
                                    LOG.debug("Generating procedure histogram from workload file");
                                proc_histogram = WorkloadUtil.getProcedureHistogram(path);
                            }
                            limit = (int) proc_histogram.get(proc_name, 0);
                            total_unlimited += limit;
                        } else {
                            total += limit;
                        }
                        limits.put(proc_name, limit);
                    } // FOR
                      // If we have a workload limit and some txns that we want
                      // to get unlimited
                      // records from, then we want to modify the other txns so
                      // that we fill in the "gap"
                    if (this.workload_xact_limit != null && total_unlimited > 0) {
                        int remaining = this.workload_xact_limit.intValue() - total - total_unlimited;
                        if (remaining > 0) {
                            for (Entry<String, Integer> e : limits.entrySet()) {
                                double ratio = e.getValue() / (double) total;
                                e.setValue((int) Math.ceil(e.getValue() + (ratio * remaining)));
                            } // FOR
                        }
                    }

                    ObjectHistogram<String> proc_multiplier_histogram = null;
                    if (debug) {
                        if (proc_histogram != null)
                            LOG.debug("Full Workload Histogram:\n" + proc_histogram);
                        proc_multiplier_histogram = new ObjectHistogram<String>();
                    }
                    total = 0;
                    for (Entry<String, Integer> e : limits.entrySet()) {
                        if (debug)
                            proc_multiplier_histogram.put(e.getKey(), e.getValue());
                        ((ProcedureNameFilter) filter).include(e.getKey(), e.getValue());
                        total += e.getValue();
                    } // FOR
                    if (debug)
                        LOG.debug("Multiplier Histogram [total=" + total + "]:\n" + proc_multiplier_histogram);
                }

                // EXCLUDE
                temp = params.get(PARAM_WORKLOAD_PROC_EXCLUDE);
                if (temp != null) {
                    for (String proc_name : params.get(PARAM_WORKLOAD_PROC_EXCLUDE).split(",")) {
                        ((ProcedureNameFilter) filter).exclude(proc_name);
                    } // FOR
                }

                // Sampling!!
                if (this.getBooleanParam(PARAM_WORKLOAD_PROC_SAMPLE, false)) {
                    if (debug)
                        LOG.debug("Attaching sampling filter");
                    if (proc_histogram == null)
                        proc_histogram = WorkloadUtil.getProcedureHistogram(path);
                    Map<String, Integer> proc_includes = ((ProcedureNameFilter) filter).getProcIncludes();
                    SamplingFilter sampling_filter = new SamplingFilter(proc_includes, proc_histogram);
                    filter = sampling_filter;
                    if (debug)
                        LOG.debug("Workload Procedure Histogram:\n" + proc_histogram);
                }

                // Attach our new filter to the chain (or make it the head if
                // it's the first one)
                this.workload_filter = (this.workload_filter != null ? this.workload_filter.attach(filter) : filter);
                if (debug)
                    LOG.debug("Attached " + filter.debugImpl());
            }

            // TRANSACTION LIMIT
            if (this.workload_xact_limit != null) {
                ProcedureLimitFilter filter = new ProcedureLimitFilter(this.workload_xact_limit, weightedTxns);
                this.workload_filter = (this.workload_filter != null ? this.workload_filter.attach(filter) : filter);
                if (debug)
                    LOG.debug("Attached " + filter.debugImpl());
            }

            // QUERY LIMIT
            if (params.containsKey(PARAM_WORKLOAD_QUERY_LIMIT)) {
                this.workload_query_limit = Long.parseLong(params.get(PARAM_WORKLOAD_QUERY_LIMIT));
                QueryLimitFilter filter = new QueryLimitFilter(this.workload_query_limit);
                this.workload_filter = (this.workload_filter != null ? this.workload_filter.attach(filter) : filter);
            }

            if (this.workload_filter != null && debug)
                LOG.debug("Workload Filters: " + this.workload_filter.toString());
            this.workload = new Workload(this.catalog);
            this.workload.load(path, this.catalog_db, this.workload_filter);
            this.workload_path = path;
            if (this.workload_filter != null)
                this.workload_filter.reset();
        }

        // Workload Statistics
        if (this.catalog_db != null) {
            if (this.params.containsKey(PARAM_STATS)) {
                this.stats = new WorkloadStatistics(this.catalog_db);
                String path = this.params.get(PARAM_STATS);
                if (debug)
                    LOG.debug("Loading in workload statistics from '" + path + "'");
                this.stats_path = new File(path);
                try {
                    this.stats.load(this.stats_path, this.catalog_db);
                } catch (Throwable ex) {
                    throw new RuntimeException("Failed to load stats file '" + this.stats_path + "'", ex);
                }
            }

            // Scaling
            if (this.params.containsKey(PARAM_STATS_SCALE_FACTOR) && this.stats != null) {
                double scale_factor = this.getDoubleParam(PARAM_STATS_SCALE_FACTOR);
                LOG.info("Scaling TableStatistics: " + scale_factor);
                AbstractTableStatisticsGenerator generator = AbstractTableStatisticsGenerator.factory(this.catalog_db, this.catalog_type, scale_factor);
                generator.apply(this.stats);
            }
        }
    }

    public void updateCatalog(Catalog catalog, File catalog_path) {
        this.catalog = catalog;
        this.catalogContext = new CatalogContext(catalog, catalog_path);
        this.catalog_db = CatalogUtil.getDatabase(catalog);
        if (catalog_path != null)
            this.catalog_path = catalog_path;
    }

    /**
     * @param args
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    public void process(String[] args, String... required) throws Exception {
        final boolean debug = LOG.isDebugEnabled();

        if (debug)
            LOG.debug("Processing " + args.length + " parameters...");
        final Pattern p = Pattern.compile("=");
        for (int i = 0, cnt = args.length; i < cnt; i++) {
            final String arg = args[i];
            final String[] parts = p.split(arg, 2);
            if (parts[0].startsWith("-"))
                parts[0] = parts[0].substring(1);

            if (parts.length == 1) {
                if (parts[0].startsWith("${") == false)
                    this.opt_params.add(parts[0]);
                continue;
            } else if (parts[0].equalsIgnoreCase("tag")) {
                continue;
            } else if (parts[1].startsWith("${") || parts[0].startsWith("#")) {
                continue;
            }
            if (debug)
                LOG.debug(String.format("%-35s = %s", parts[0], parts[1]));

            // DesignerHints Override
            if (parts[0].startsWith(PARAM_DESIGNER_HINTS_PREFIX)) {
                String param = parts[0].replace(PARAM_DESIGNER_HINTS_PREFIX, "").toLowerCase();
                try {
                    Field f = DesignerHints.class.getField(param);
                    this.hints_params.put(f.getName(), parts[1]);
                    if (debug)
                        LOG.debug(String.format("DesignerHints.%s = %s", param, parts[1]));
                } catch (NoSuchFieldException ex) {
                    throw new Exception("Unknown DesignerHints parameter: " + param, ex);
                }

            }
            // HStoreConf Parameter
            else if (HStoreConf.isConfParameter(parts[0])) {
                this.conf_params.put(parts[0].toLowerCase(), parts[1]);
            }
            // ArgumentsParser Parameter
            else if (PARAMS.contains(parts[0].toLowerCase())) {
                this.params.put(parts[0].toLowerCase(), parts[1]);
            }
            // Invalid!
            else {
                String suggestions = "";
                i = 0;
                String end = CollectionUtil.last(parts[0].split("\\."));
                for (String param : PARAMS) {
                    String param_end = CollectionUtil.last(param.split("\\."));
                    if (param.startsWith(parts[0]) || (end != null && param.endsWith(end)) || (end != null && param_end != null && param_end.startsWith(end))) {
                        if (suggestions.isEmpty())
                            suggestions = ". Possible Matches:";
                        suggestions += String.format("\n [%02d] %s", ++i, param);
                    }
                } // FOR
                throw new Exception("Unknown parameter '" + parts[0] + "'" + suggestions);
            }
        } // FOR

        // -------------------------------------------------------
        // CATALOGS
        // -------------------------------------------------------

        // Text File
        if (this.params.containsKey(PARAM_CATALOG)) {
            String path = this.params.get(PARAM_CATALOG);
            if (debug)
                LOG.debug("Loading catalog from file '" + path + "'");
            Catalog catalog = CatalogUtil.loadCatalog(path);
            if (catalog == null)
                throw new Exception("Failed to load catalog object from file '" + path + "'");
            this.updateCatalog(catalog, new File(path));
        }
        // Jar File
        else if (this.params.containsKey(PARAM_CATALOG_JAR)) {
            String path = this.params.get(PARAM_CATALOG_JAR);
            this.params.put(PARAM_CATALOG, path);
            File jar_file = new File(path);
            Catalog catalog = CatalogUtil.loadCatalogFromJar(path);
            if (catalog == null)
                throw new Exception("Failed to load catalog object from jar file '" + path + "'");
            if (debug)
                LOG.debug("Loaded catalog from jar file '" + path + "'");
            this.updateCatalog(catalog, jar_file);

            if (!this.params.containsKey(PARAM_CATALOG_TYPE)) {
                String jar_name = jar_file.getName();
                int jar_idx = jar_name.lastIndexOf(".jar");
                if (jar_idx != -1) {
                    ProjectType type = ProjectType.get(jar_name.substring(0, jar_idx));
                    if (type != null) {
                        if (debug)
                            LOG.debug("Set catalog type '" + type + "' from catalog jar file name");
                        this.catalog_type = type;
                        this.params.put(PARAM_CATALOG_TYPE, this.catalog_type.toString());
                    }
                }
            }
            
            // HACK: Extract the ParameterMappings embedded in jar and write them to a temp file
            // This is terrible, confusing, and a total mess...
            // I have no one to blame but myself...
            JarReader reader = new JarReader(jar_file.getAbsolutePath());
            for (String file : reader.getContentsFromJarfile()) {
                if (file.endsWith(".mappings")) {
                    String contents = new String(JarReader.readFileFromJarAtURL(jar_file.getAbsolutePath(), file));
                    File copy = FileUtil.writeStringToTempFile(contents, "mappings", true);
                    this.params.put(PARAM_MAPPINGS, copy.toString());
                    break;
                }
            } // FOR
        }
        // Schema File
        else if (this.params.containsKey(PARAM_CATALOG_SCHEMA)) {
            String path = this.params.get(PARAM_CATALOG_SCHEMA);
            Catalog catalog = CompilerUtil.compileCatalog(path);
            if (catalog == null)
                throw new Exception("Failed to load schema from '" + path + "'");
            if (debug)
                LOG.debug("Loaded catalog from schema file '" + path + "'");
            this.updateCatalog(catalog, new File(path));
        }
        
        // Catalog Type
        if (this.params.containsKey(PARAM_CATALOG_TYPE)) {
            String catalog_type = this.params.get(PARAM_CATALOG_TYPE);
            ProjectType type = ProjectType.get(catalog_type);
            if (type == null) {
                throw new Exception("Unknown catalog type '" + catalog_type + "'");
            }
            this.catalog_type = type;
        }

        // Update Cluster Configuration
        if (this.params.containsKey(ArgumentsParser.PARAM_CATALOG_HOSTS) && DISABLE_UPDATE_CATALOG == false) {
            ClusterConfiguration cc = new ClusterConfiguration(this.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS));
            this.updateCatalog(FixCatalog.cloneCatalog(this.catalog, cc), null);
        }

        // Check the requirements after loading the catalog, because some of the
        // above parameters will set the catalog one
        if (required != null && required.length > 0)
            this.require(required);

        // -------------------------------------------------------
        // PHYSICAL DESIGN COMPONENTS
        // -------------------------------------------------------
        if (this.params.containsKey(PARAM_PARTITION_PLAN)) {
            assert (this.catalog_db != null);
            File path = new File(this.params.get(PARAM_PARTITION_PLAN));
            boolean ignoreMissing = this.getBooleanParam(ArgumentsParser.PARAM_PARTITION_PLAN_IGNORE_MISSING, false);
            if (path.exists() || (path.exists() == false && ignoreMissing == false)) {
                if (debug)
                    LOG.debug("Loading in partition plan from '" + path + "'");
                this.pplan = new PartitionPlan();
                this.pplan.load(path, this.catalog_db);

                // Apply!
                if (this.params.containsKey(PARAM_PARTITION_PLAN_APPLY) && this.getBooleanParam(PARAM_PARTITION_PLAN_APPLY)) {
                    boolean secondaryIndexes = this.getBooleanParam(PARAM_PARTITION_PLAN_NO_SECONDARY, false) == false;
                    LOG.info(String.format("Applying PartitionPlan '%s' to catalog [enableSecondaryIndexes=%s]",
                             path.getName(), secondaryIndexes));
                    this.pplan.apply(this.catalog_db, secondaryIndexes);
                }
            }
        }

        // -------------------------------------------------------
        // DESIGNER COMPONENTS
        // -------------------------------------------------------

        if (this.params.containsKey(PARAM_DESIGNER_THREADS)) {
            this.max_concurrent = Integer.valueOf(this.params.get(PARAM_DESIGNER_THREADS));
        }
        if (this.params.containsKey(PARAM_DESIGNER_INTERVALS)) {
            this.num_intervals = Integer.valueOf(this.params.get(PARAM_DESIGNER_INTERVALS));
        }
        if (this.params.containsKey(PARAM_DESIGNER_HINTS)) {
            File path = this.getFileParam(PARAM_DESIGNER_HINTS);
            if (debug)
                LOG.debug("Loading in designer hints from '" + path + "'.\nForced Values:\n" + StringUtil.formatMaps(this.hints_params));
            this.designer_hints.load(path, catalog_db, this.hints_params);
        }
        if (this.params.containsKey(PARAM_DESIGNER_CHECKPOINT)) {
            this.designer_checkpoint = new File(this.params.get(PARAM_DESIGNER_CHECKPOINT));
        }

        String designer_attributes[] = { PARAM_DESIGNER_PARTITIONER, PARAM_DESIGNER_MAPPER, PARAM_DESIGNER_INDEXER, PARAM_DESIGNER_COSTMODEL };
        ClassLoader loader = ClassLoader.getSystemClassLoader();
        for (String key : designer_attributes) {
            if (this.params.containsKey(key)) {
                String target_name = this.params.get(key);
                Class<?> target_class = loader.loadClass(target_name);
                assert (target_class != null);
                if (debug)
                    LOG.debug("Set " + key + " class to " + target_class.getName());

                if (key.equals(PARAM_DESIGNER_PARTITIONER)) {
                    this.partitioner_class = (Class<? extends AbstractPartitioner>) target_class;
                } else if (key.equals(PARAM_DESIGNER_MAPPER)) {
                    this.mapper_class = (Class<? extends AbstractMapper>) target_class;
                } else if (key.equals(PARAM_DESIGNER_INDEXER)) {
                    this.indexer_class = (Class<? extends AbstractIndexSelector>) target_class;
                } else if (key.equals(PARAM_DESIGNER_COSTMODEL)) {
                    this.costmodel_class = (Class<? extends AbstractCostModel>) target_class;

                    // Special Case: TimeIntervalCostModel
                    if (target_name.endsWith(TimeIntervalCostModel.class.getSimpleName())) {
                        this.costmodel = new TimeIntervalCostModel<SingleSitedCostModel>(this.catalogContext, SingleSitedCostModel.class, this.num_intervals);
                    } else {
                        this.costmodel = ClassUtil.newInstance(this.costmodel_class, new Object[] { this.catalogContext }, new Class[] { Database.class });
                    }
                } else {
                    assert (false) : "Invalid key '" + key + "'";
                }
            }
        } // FOR

        // -------------------------------------------------------
        // TRANSACTION ESTIMATION COMPONENTS
        // -------------------------------------------------------
        if (this.params.containsKey(PARAM_MAPPINGS) && DISABLE_UPDATE_CATALOG == false) {
            assert (this.catalog_db != null);
            File path = new File(this.params.get(PARAM_MAPPINGS));
            if (path.exists()) {
                this.param_mappings.load(path, this.catalog_db);
            } else {
                LOG.warn("The ParameterMappings file '" + path + "' does not exist");
            }
        }
        if (this.params.containsKey(PARAM_MARKOV_THRESHOLDS_VALUE)) {
            assert (this.catalog_db != null);
            float defaultValue = this.getDoubleParam(PARAM_MARKOV_THRESHOLDS_VALUE).floatValue();
            this.thresholds = new EstimationThresholds(defaultValue);
            this.params.put(PARAM_MARKOV_THRESHOLDS, this.thresholds.toString());
            LOG.debug("CREATED THRESHOLDS: " + this.thresholds);

        } else if (this.params.containsKey(PARAM_MARKOV_THRESHOLDS)) {
            assert (this.catalog_db != null);
            this.thresholds = new EstimationThresholds();
            File path = new File(this.params.get(PARAM_MARKOV_THRESHOLDS));
            if (path.exists()) {
                this.thresholds.load(path, this.catalog_db);
            } else {
                LOG.warn("The estimation thresholds file '" + path + "' does not exist");
            }
            LOG.debug("LOADED THRESHOLDS: " + this.thresholds);
        }

        // -------------------------------------------------------
        // HASHER
        // -------------------------------------------------------
        if (this.catalog != null) {
            if (this.params.containsKey(PARAM_HASHER_CLASS)) {
                String hasherClassName = this.params.get(PARAM_HASHER_CLASS);
                this.hasher_class = (Class<? extends AbstractHasher>) loader.loadClass(hasherClassName);
            }
            Class<?> paramClasses[] = new Class[] { CatalogContext.class, int.class };
            Object paramValues[] = new Object[] { this.catalogContext, this.catalogContext.numberOfPartitions };
            
            Constructor<? extends AbstractHasher> constructor = this.hasher_class.getConstructor(paramClasses);
            this.hasher = constructor.newInstance(paramValues);
            
            if (!(this.hasher instanceof DefaultHasher))
                LOG.debug("Loaded hasher " + this.hasher.getClass());

            if (this.params.containsKey(PARAM_HASHER_PROFILE)) {
                this.hasher.load(this.getFileParam(PARAM_HASHER_PROFILE), null);
            }
        }

        // -------------------------------------------------------
        // SAMPLE WORKLOAD TRACE
        // -------------------------------------------------------
        this.loadWorkload();
    }

    @Override
    public String toString() {
        Map<String, Object> m0 = new ListOrderedMap<String, Object>();
        m0.putAll(this.params);

        Map<String, Object> m1 = null;
        int opt_cnt = this.opt_params.size();
        if (opt_cnt > 0) {
            Map<String, Object> opt_inner = new ListOrderedMap<String, Object>();
            for (int i = 0; i < opt_cnt; i++) {
                opt_inner.put(String.format("#%02d", i), this.opt_params.get(i));
            }
            m1 = new ListOrderedMap<String, Object>();
            m1.put(String.format("Optional Parameters [%d]", opt_cnt), StringUtil.formatMaps(opt_inner));
        }

        return StringUtil.formatMaps(m0, m1);
    }
}
