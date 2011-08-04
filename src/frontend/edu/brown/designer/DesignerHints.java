package edu.brown.designer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.StringUtil;

public class DesignerHints implements Cloneable, JSONSerializable {
    private static final Logger LOG = Logger.getLogger(DesignerHints.class);

    public static final Field[] MEMBERS;
    static {
        List<Field> fields = new ArrayList<Field>();
        Class<?> clazz = DesignerHints.class;
        for (Field f : clazz.getDeclaredFields()) {
            int modifiers = f.getModifiers();
            if (Modifier.isTransient(modifiers) == false &&
                Modifier.isPublic(modifiers) == true &&
                Modifier.isStatic(modifiers) == false) {
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
    private transient String source_file;
    
    /**
     * When we started keeping track of time
     */
    private transient Long start_time = null;
    
    /**
     * The FileWriter handle where we dump out new solutions
     */
    private transient FileWriter log_solutions_costs_writer = null;
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * Whether the catalog should be forced into a random partitioning configuration before
     * we start working on it.
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
    public boolean enable_costmodel_caching = true;
    
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
     * Force a table to be replicated
     * Set<TableKey>
     */
    public final Set<String> force_replication = new HashSet<String>(); 
   
    public Double force_replication_size_limit = null;
    
    /**
     * Force a table to be partitioned on a particular column
     * TableKey -> ColumnKey
     */
    public final Map<String, Set<String>> force_table_partition = new HashMap<String, Set<String>>();
    
    /**
     * Enable debugging on certain columns
     * Set<ColumnKey>
     */
    public final Set<String> force_debugging = new HashSet<String>();
    
    /**
     * Force one column to be mapped to another column
     * Map<ColumnKey, ColumnKey> 
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
     * Relaxation Factors
     */
    public double relaxation_factor_min = 0.25;
    public double relaxation_factor_max = 0.5;
    public int relaxation_min_size = 5;
    
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
     * @param orig
     */
    private DesignerHints(DesignerHints orig) {
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

    
    public String getSourceFile() {
        return (this.source_file);
    }

    public boolean shouldLogSolutionCosts() {
        return (this.log_solutions_costs != null);
    }
    
    /**
     * Write a solution cost out to a file. Each entry will also include the time at which
     * the new cost was discovered 
     * @param cost
     */
    public void logSolutionCost(double cost) {
        assert(this.log_solutions_costs != null);
        try {
            if (this.log_solutions_costs_writer == null) {
                File file = new File(this.log_solutions_costs);
                this.log_solutions_costs_writer = new FileWriter(file, true);
                this.log_solutions_costs_writer.write("-- " + (new Date().toString()) + "\n");
                LOG.info("Creating solution costs log file: " + file.getAbsolutePath());
            }
            long offset = System.currentTimeMillis() - this.start_time;
            this.log_solutions_costs_writer.write(String.format("%d\t%.05f\n", offset, cost));
            this.log_solutions_costs_writer.flush();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * We're coming in after a checkpoint restart, so we need to offset
     * the total time by the time that already elapsed
     * @param orig_start_time
     * @param last_checkpoint
     */
    public void offsetCheckpointTime(long orig_start_time, long last_checkpoint) {
        assert(last_checkpoint > orig_start_time);
        int delta = (int)(last_checkpoint - orig_start_time) / 1000;
        this.limit_total_time -= delta;
    }
    
    
    /**
     * Start the timer used to keep track of how long we are searching for solutions
     */
    public void startTimer() {
        if (this.start_time == null) {
            this.start_time = System.currentTimeMillis();
        }
    }
    
    public Long getStartTime() {
        return (this.start_time);
    }
    
    public long getGlobalStopTime() {
        long stop = Long.MAX_VALUE;
        if (this.limit_total_time != null) {
            stop = (this.limit_total_time * 1000);
        }
        return (this.start_time + stop);
    }
    
    public long getRemainingGlobalTime() {
        return (this.getGlobalStopTime() - System.currentTimeMillis());
    }
    
    public double getElapsedGlobalPercent() {
        long now = System.currentTimeMillis();
        return Math.abs((now - this.getStartTime()) / (double)(this.getGlobalStopTime() - this.getStartTime()));
    }
    
    public long getNextLocalStopTime() {
        long now = System.currentTimeMillis();
        long stop = Long.MAX_VALUE;
        if (this.limit_local_time != null) {
            stop = (this.limit_local_time * 1000);
        }
        return (now + stop);
    }

    
    public void addTablePartitionCandidate(Database catalog_db, String table_name, String column_name) {
        Table catalog_tbl = catalog_db.getTables().get(table_name);
        assert(catalog_tbl != null) : "Invalid table name '" + table_name + "'";
        
        Column catalog_col = null;
        if (column_name.equals(ReplicatedColumn.COLUMN_NAME)) {
            catalog_col = ReplicatedColumn.get(catalog_tbl);
        } else {
            catalog_col = catalog_tbl.getColumns().get(column_name);
        }
        assert(catalog_col != null) : "Invalid column name '" + table_name + "." + column_name + "'";
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
    
    public Set<Column> getTablePartitionCandidates(Table catalog_tbl) {
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
     * @param catalog_proc
     * @return
     */
    public boolean shouldIgnoreProcedure(Procedure catalog_proc) {
        return (this.ignore_procedures.contains(catalog_proc.getName()));
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Load with the ability to override values
     */
    public void load(String input_path, Database catalog_db, Map<String, String> override) throws IOException {
        // First call the regular load() method to bring all of our options
        this.load(input_path, catalog_db);
        
        // Then construct a JSONObject from the map to override the parameters
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            for (Entry<String, String> e : override.entrySet()) {
                stringer.key(e.getKey()).value(e.getValue());
            } // FOR
            stringer.endObject();
            this.fromJSON(new JSONObject(stringer.toString()), catalog_db);
        } catch (JSONException ex) {
            throw new IOException("Failed to load override parameters: " + override, ex);
        }
    }
    
    @Override
    public void load(String input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
        this.source_file = input_path;
    }
    
    @Override
    public void save(String output_path) throws IOException {
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
        
        // HACK: Process wildcards
        if (this.ignore_procedures.size() > 0) {
            Set<String> to_add = new HashSet<String>();
            for (String proc_name : this.ignore_procedures) {
                if (proc_name.endsWith("*")) {
                    proc_name = proc_name.substring(0, proc_name.length()-1);
                    for (Procedure catalog_proc : catalog_db.getProcedures()) {
                        if (catalog_proc.getName().startsWith(proc_name)) to_add.add(catalog_proc.getName());
                    } // FOR
                } // FOR
            } // FOR
            if (to_add.size() > 0) {
                if (LOG.isDebugEnabled()) LOG.debug("Added ignore procedures: " + to_add);
                this.ignore_procedures.addAll(to_add);
            }
        }
    }
    
    /**
     * 
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        LoggerUtil.setupLogging();
        // Make an empty DesignerHints and print it out
        DesignerHints hints = new DesignerHints();
        System.out.println(JSONUtil.format(hints.toJSONString()));
    }

}
