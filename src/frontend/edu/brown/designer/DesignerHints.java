package edu.brown.designer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.FileUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.StringUtil;

public class DesignerHints implements Cloneable, JSONSerializable {
    private static final Logger LOG = Logger.getLogger(DesignerHints.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static final Field[] MEMBERS;
    static {
        List<Field> fields = new ArrayList<Field>();
        Class<?> clazz = DesignerHints.class;
        for (Field f : clazz.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) == false && Modifier.isPublic(modifiers) == true && Modifier.isStatic(modifiers) == false) {
                fields.add(f);
            }
        } // FOR
        MEMBERS = fields.toArray(new Field[0]);
    } // STATIC

    // ----------------------------------------------------------------------------
    // INTERNAL DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The location of the file that we loaded for this DesignerHints
     */
    private transient File source_file;

    /**
     * When we started keeping track of time
     */
    private transient TimestampType start_time = null;

    /**
     * The FileWriter handle where we dump out new solutions
     */
    private transient FileWriter log_solutions_costs_writer = null;

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * Whether the catalog should be forced into a random partitioning
     * configuration before we start working on it.
     */
    public boolean start_random = false;

    /**
     * Whether to exhaustively search all possible designs
     */
    public boolean exhaustive_search = false;

    /**
     * Whether to greedily search for a design
     */
    public boolean greedy_search = false;

    /**
     * Search time limits (seconds)
     */
    public Integer limit_total_time = null;
    public Integer limit_local_time = null;
    public double local_time_multiplier = 1.01;

    /**
     * Limit the # of back tracks
     */
    public Integer limit_back_tracks = null;
    public double back_tracks_multiplier = 1.01;

    /**
     * The amount of memory available to each partition
     */
    public long max_memory_per_partition = 0;

    /**
     * The list of procedures we should only consider
     */
    public final Set<String> proc_include = new HashSet<String>();
    public final Set<String> proc_exclude = new HashSet<String>();

    /**
     * Replication Candidate Control
     */
    public boolean enable_replication_readonly = true;
    public boolean enable_replication_readmostly = true;

    /**
     * Allow array ProcParameters to be used as partitioning candidates
     */
    public boolean enable_array_procparameter_candidates = false;

    /** Mark tables as read-only */
    public final Set<String> readonly_tables = new HashSet<String>();
    /** Mark tables as read-mostly */
    public final Set<String> readmostly_tables = new HashSet<String>();

    /** Whether we can have multi-attribute partitioning attributes */
    public boolean enable_multi_partitioning = false;

    /** Enable vertical partitioning search */
    public boolean enable_vertical_partitioning = false;

    /** Enable caching in cost models */
    public boolean enable_costmodel_caching = false;

    /** Enable skew calculations in cost models */
    public boolean enable_costmodel_skew = true;

    /** Enable execution calculations in cost models */
    public boolean enable_costmodel_execution = true;

    /** Enable the inclusion of Java execution partitions in cost models */
    public boolean enable_costmodel_java_execution = false;

    /** Enable Multipartition Penalty factoring */
    public boolean enable_costmodel_multipartition_penalty = true;

    /** Enable Idle Partition Penalty factoring */
    public boolean enable_costmodel_idlepartition_penalty = true;

    /** Enable searching for the partitioning ProcParameter */
    public boolean enable_procparameter_search = true;

    /** Enable increasing local search parameters after a restart */
    public boolean enable_local_search_increase = true;

    /** Enable partitioner checkpoints */
    public boolean enable_checkpoints = true;

    /**
     * Force a table to be replicated Set<TableKey>
     */
    public final Set<String> force_replication = new HashSet<String>();

    public Double force_replication_size_limit = null;

    /**
     * Force a table to be partitioned on a particular column TableKey ->
     * ColumnKey
     */
    public final Map<String, Set<String>> force_table_partition = new HashMap<String, Set<String>>();

    /**
     * Enable debugging on certain columns Set<ColumnKey>
     */
    public final Set<String> force_debugging = new HashSet<String>();

    /**
     * Force one column to be mapped to another column Map<ColumnKey, ColumnKey>
     */
    public final Map<String, String> force_dependency = new HashMap<String, String>();

    /**
     * Cost Model Weights
     */
    public double weight_costmodel_execution = 1.0;
    public double weight_costmodel_skew = 1.0;
    public double weight_costmodel_multipartition_penalty = 1.0;
    public int weight_costmodel_java_execution = 1;

    /**
     * Keep track of the cost of the solutions as we find them
     */
    public String log_solutions_costs = null;

    /**
     * A list of procedure names we should ignore when doing any calculations
     */
    public final Set<String> ignore_procedures = new HashSet<String>();
    
    /**
     * A list of table names we should ignore when doing any calculations
     */
    public final Set<String> ignore_tables = new HashSet<String>();

    /**
     * Relaxation Factors
     */
    public double relaxation_factor_min = 0.25;
    public double relaxation_factor_max = 0.5;
    public int relaxation_min_size = 5;

    /**
     * If we were given a target PartitionPlan, then we will check whether every
     * new solution equals this plan. If it does, then we will halt. This is
     * used to measure how long it takes us to find the optimal solution.
     */
    public File target_plan_path = null;
    public transient PartitionPlan target_plan = null;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Empty Constructor
     */
    public DesignerHints() {

    }

    /**
     * Copy Constructor
     * 
     * @param orig
     */
    public DesignerHints(DesignerHints orig) {
        this.start_time = orig.start_time;
        this.source_file = orig.source_file;
    }

    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Clone
     */
    public DesignerHints clone() {
        DesignerHints clone = new DesignerHints(this);
        try {
            clone.fromJSON(new JSONObject(this.toJSONString()), null);
        } catch (Exception ex) {
            LOG.fatal("Failed to clone DesignerHints", ex);
            System.exit(1);
        }
        return (clone);
    }

    @Override
    public int hashCode() {
        return this.toString().hashCode();
    }

    @Override
    public String toString() {
        Class<?> hints_class = this.getClass();
        SortedMap<String, Object> m = new TreeMap<String, Object>();
        for (Field f : hints_class.getFields()) {
            String key = f.getName().toUpperCase();
            Object val = null;
            try {
                val = f.get(this);
            } catch (IllegalAccessException ex) {
                val = ex.getMessage();
            }
            m.put(key, val);
        } // FOR
        return (StringUtil.formatMaps(m));
    }

    public File getSourceFile() {
        return (this.source_file);
    }

    // --------------------------------------------------------------------------------------------
    // COST LOGGING
    // --------------------------------------------------------------------------------------------

    public boolean shouldLogSolutionCosts() {
        return (this.log_solutions_costs != null);
    }

    /**
     * Write a solution cost out to a file. Each entry will also include the
     * time at which the new cost was discovered
     * 
     * @param cost
     */
    public void logSolutionCost(double cost, double singlep_txns) {
        assert (this.log_solutions_costs != null);
        try {
            if (this.log_solutions_costs_writer == null) {
                File file = new File(this.log_solutions_costs);
                FileUtil.makeDirIfNotExists(file.getParent());
                this.log_solutions_costs_writer = new FileWriter(file, true);
                this.log_solutions_costs_writer.write("-- " + (new Date().toString()) + "\n");
                LOG.info("Creating solution costs log file: " + file.getAbsolutePath());
            }
            long offset = System.currentTimeMillis() - this.startGlobalSearchTimer().getMSTime();
            this.log_solutions_costs_writer.write(String.format("%d\t%.05f\t%.05f\n", offset, cost, singlep_txns));
            this.log_solutions_costs_writer.flush();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to log solution cost to '" + this.log_solutions_costs + "'", ex);
        }
    }

    // --------------------------------------------------------------------------------------------
    // GLOBAL & LOCAL SEARCH TIMERS
    // --------------------------------------------------------------------------------------------

    /**
     * We're coming in after a checkpoint restart, so we need to offset the
     * total time by the time that already elapsed
     * 
     * @param orig_start_time
     * @param last_checkpoint
     */
    public void offsetCheckpointTime(TimestampType orig_start_time, TimestampType last_checkpoint) {
        if (this.limit_total_time != null) {
            assert (last_checkpoint.getTime() > orig_start_time.getTime());
            int delta = (int) (last_checkpoint.getMSTime() - orig_start_time.getMSTime()) / 1000;
            this.limit_total_time -= delta;
        }
    }

    /**
     * Start the timer used to keep track of how long we are searching for
     * solutions
     */
    public TimestampType startGlobalSearchTimer() {
        if (this.start_time == null) {
            this.start_time = new TimestampType();
        }
        return (this.start_time);
    }

    public TimestampType getStartTime() {
        return (this.start_time);
    }

    public TimestampType getGlobalStopTime() {
        long stop = 9999999;
        if (this.limit_total_time != null && this.limit_total_time >= 0) {
            stop = (this.limit_total_time * 1000);
        }
        return new TimestampType((this.startGlobalSearchTimer().getMSTime() + stop) * 1000);
    }

    /**
     * Return the amount of time remaining (in ms) for the global search process
     * 
     * @return
     */
    public long getRemainingGlobalTime() {
        return (this.getGlobalStopTime().getMSTime() - System.currentTimeMillis());
    }

    public TimestampType getNextLocalStopTime() {
        long now = System.currentTimeMillis();
        long stop = 9999999;
        if (this.limit_local_time != null && this.limit_local_time >= 0) {
            stop = (this.limit_local_time * 1000);
        }
        return new TimestampType((now + stop) * 1000);
    }

    /**
     * Returns the next stop time for the current time. This will the global
     * stop time if that comes before the next local stop time
     * 
     * @return Next stop timestamp, True if it was the local time
     */
    public Pair<TimestampType, Boolean> getNextStopTime() {
        TimestampType next = null;
        Boolean is_local = null;
        TimestampType stop_local = null;
        TimestampType stop_total = null;

        if (this.limit_local_time != null && this.limit_local_time >= 0) {
            stop_local = this.getNextLocalStopTime();
        }
        if (this.limit_total_time != null && this.limit_total_time >= 0) {
            stop_total = this.getGlobalStopTime();
        }
        if (stop_local != null && stop_total != null) {
            if (stop_local.compareTo(stop_total) < 0) {
                next = stop_local;
                is_local = true;
            } else {
                next = stop_total;
                is_local = false;
            }
        } else if (stop_local != null) {
            next = stop_local;
            is_local = true;
        } else if (stop_total != null) {
            next = stop_total;
            is_local = false;
        }
        return (next != null ? Pair.of(next, is_local) : null);
    }

    /**
     * Return the ratio of the amount of time that remains for the global search
     * process 0.0 means that the search just started 1.0 means that the search
     * is out of time
     * 
     * @return
     */
    public double getElapsedGlobalPercent() {
        long delta = (this.getGlobalStopTime().getMSTime() - this.getStartTime().getMSTime());
        long now = System.currentTimeMillis();
        double ratio = Math.abs((now - this.getStartTime().getMSTime()) / (double) delta);
        assert (ratio <= 1.0) : String.format("Invalid Elapsed Ratio: %f [delta=%d, now=%d, start=%d, stop=%d]", ratio, delta, now, getStartTime().getMSTime(), getGlobalStopTime().getMSTime());
        return ratio;
    }

    // --------------------------------------------------------------------------------------------
    // PARTITION INFO
    // --------------------------------------------------------------------------------------------

    public void addTablePartitionCandidate(Database catalog_db, String table_name, String column_name) {
        Table catalog_tbl = catalog_db.getTables().get(table_name);
        assert (catalog_tbl != null) : "Invalid table name '" + table_name + "'";

        Column catalog_col = null;
        if (column_name.equals(ReplicatedColumn.COLUMN_NAME)) {
            catalog_col = ReplicatedColumn.get(catalog_tbl);
        } else {
            catalog_col = catalog_tbl.getColumns().get(column_name);
        }
        assert (catalog_col != null) : "Invalid column name '" + table_name + "." + column_name + "'";
        this.addTablePartitionCandidate(catalog_tbl, catalog_col);
    }

    public void addTablePartitionCandidate(Table catalog_tbl, Column catalog_col) {
        final String table_key = CatalogKey.createKey(catalog_tbl);
        final String column_key = CatalogKey.createKey(catalog_col);

        if (!this.force_table_partition.containsKey(table_key)) {
            this.force_table_partition.put(table_key, new ListOrderedSet<String>());
        }
        this.force_table_partition.get(table_key).add(column_key);
    }

    public Collection<Column> getForcedTablePartitionCandidates(Table catalog_tbl) {
        final Database catalog_db = CatalogUtil.getDatabase(catalog_tbl);
        final String table_key = CatalogKey.createKey(catalog_tbl);
        ListOrderedSet<Column> ret = new ListOrderedSet<Column>();
        if (this.force_table_partition.containsKey(table_key)) {
            for (String column_key : this.force_table_partition.get(table_key)) {
                ret.add(CatalogKey.getFromKey(catalog_db, column_key, Column.class));
            } // FOR
        }
        return (ret);
    }

    public void enablePartitionCandidateDebugging(CatalogType catalog_item) {
        final String catalog_key = CatalogKey.createKey(catalog_item);
        this.force_debugging.add(catalog_key);
    }

    public boolean isDebuggingEnabled(String catalog_key) {
        return (this.force_debugging.contains(catalog_key));
    }

    public boolean isDebuggingEnabled(CatalogType catalog_item) {
        final String catalog_key = CatalogKey.createKey(catalog_item);
        return (this.isDebuggingEnabled(catalog_key));
    }

    /**
     * Returns true if this procedure should be ignored
     * 
     * @param catalog_proc
     * @return
     */
    public boolean shouldIgnoreProcedure(Procedure catalog_proc) {
        return (this.ignore_procedures.contains(catalog_proc.getName()));
    }

    // --------------------------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // --------------------------------------------------------------------------------------------

    /**
     * Load with the ability to override values
     */
    public void load(File input_path, Database catalog_db, Map<String, String> override) throws IOException {
        // First call the regular load() method to bring all of our options
        this.load(input_path, catalog_db);

        // Then construct a JSONObject from the map to override the parameters
        if (override.isEmpty() == false) {
            JSONStringer stringer = new JSONStringer();
            try {
                stringer.object();
                for (Entry<String, String> e : override.entrySet()) {
                    stringer.key(e.getKey().toUpperCase()).value(e.getValue());
                } // FOR
                stringer.endObject();
                this.fromJSON(new JSONObject(stringer.toString()), catalog_db);
            } catch (JSONException ex) {
                throw new IOException("Failed to load override parameters: " + override, ex);
            }
        }
    }

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
        this.source_file = input_path;
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, DesignerHints.class, DesignerHints.MEMBERS);
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, DesignerHints.class, true, DesignerHints.MEMBERS);

        // HACK: Convert negatives to nulls
        if (this.limit_back_tracks != null && this.limit_back_tracks < 0)
            this.limit_back_tracks = null;
        if (this.limit_local_time != null && this.limit_local_time < 0)
            this.limit_local_time = null;
        if (this.limit_total_time != null && this.limit_total_time < 0)
            this.limit_total_time = null;

        // HACK: Process wildcards
        if (this.ignore_procedures.size() > 0) {
            Set<String> to_add = new HashSet<String>();
            for (String proc_name : this.ignore_procedures) {
                if (proc_name.endsWith("*")) {
                    proc_name = proc_name.substring(0, proc_name.length() - 1);
                    for (Procedure catalog_proc : catalog_db.getProcedures()) {
                        if (catalog_proc.getName().startsWith(proc_name))
                            to_add.add(catalog_proc.getName());
                    } // FOR
                } // FOR
            } // FOR
            if (to_add.size() > 0) {
                if (debug.val)
                    LOG.debug("Added ignore procedures: " + to_add);
                this.ignore_procedures.addAll(to_add);
            }
        }
        // HACK: Process wildcards
        if (this.ignore_tables.size() > 0) {
            Set<String> to_add = new HashSet<String>();
            for (String table_name : this.ignore_tables) {
                if (table_name.endsWith("*")) {
                    table_name = table_name.substring(0, table_name.length() - 1);
                    for (Table catalog_tbl : catalog_db.getTables()) {
                        if (catalog_tbl.getName().startsWith(table_name))
                            to_add.add(catalog_tbl.getName());
                    } // FOR
                } // FOR
            } // FOR
            if (to_add.size() > 0) {
                if (debug.val)
                    LOG.debug("Added ignore tables: " + to_add);
                this.ignore_tables.addAll(to_add);
            }
        }

        // Target PartitionPlan
        if (this.target_plan_path != null && this.target_plan_path != null) {
            if (debug.val)
                LOG.debug("Loading in target PartitionPlan from '" + this.target_plan_path + "'");
            this.target_plan = new PartitionPlan();
            try {
                this.target_plan.load(this.target_plan_path, catalog_db);
            } catch (IOException ex) {
                throw new RuntimeException("Failed to load target PartitionPlan '" + this.target_plan_path + "'", ex);
            }
        }
    }

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        LoggerUtil.setupLogging();
        // Make an empty DesignerHints and print it out
        DesignerHints hints = new DesignerHints();
        System.out.println(JSONUtil.format(hints.toJSONString()));
    }

}
