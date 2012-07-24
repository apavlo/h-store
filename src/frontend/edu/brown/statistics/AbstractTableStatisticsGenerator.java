package edu.brown.statistics;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;

import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.MemoryEstimator;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.graphs.VertexTreeWalker.TraverseOrder;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * @author pavlo
 */
public abstract class AbstractTableStatisticsGenerator {
    protected static final Logger LOG = Logger.getLogger(AbstractTableStatisticsGenerator.class);

    protected final Database catalog_db;
    protected final ProjectType project_type;
    protected final double scale_factor;

    private final Map<Table, TableProfile> table_profiles = new ListOrderedMap<Table, TableProfile>();

    /**
     * DependencyOperation
     */
    private static class DependencyOperation {
        private final Table catalog_tbl;
        private final ExpressionType type;
        private final double scale_factor;

        public DependencyOperation(Table catalog_tbl, ExpressionType type, double scale_factor) {
            this.catalog_tbl = catalog_tbl;
            this.type = type;
            this.scale_factor = scale_factor;
        }

        @Override
        public String toString() {
            return this.type + " (" + this.catalog_tbl.getName() + " * " + this.scale_factor + ")";
        }
    } // END CLASS

    /**
     * TableProfile
     */
    protected static class TableProfile {

        private Table catalog_tbl = null;
        private Long tuple_count = null;
        private boolean is_fixed = false;
        private final List<DependencyOperation> dependencies = new ArrayList<DependencyOperation>();

        /**
         * Default Constructor
         * 
         * @param catalog_tbl
         * @param tuple_size
         */
        public TableProfile(Table catalog_tbl, boolean is_fixed, Long tuple_count) {
            this.catalog_tbl = catalog_tbl;
            this.is_fixed = is_fixed;
            this.tuple_count = tuple_count;
        }

        /**
         * Convenience Constructor
         * 
         * @param catalog_db
         * @param table_name
         */
        public TableProfile(Database catalog_db, String table_name, boolean is_fixed) {
            this(catalog_db.getTables().get(table_name), is_fixed, 1l);
        }

        /**
         * Convenience Constructor
         * 
         * @param catalog_db
         * @param table_name
         * @param size
         */
        public TableProfile(Database catalog_db, String table_name, boolean is_fixed, long tuple_count) {
            this(catalog_db.getTables().get(table_name), is_fixed, tuple_count);
        }

        /**
         * Adds a dependency between this table and another table where
         * TABLE_COUNT = TABLE_COUNT + (PARENT_TABLE_COUNT * SCALE_FACTOR)
         * 
         * @param catalog_db
         * @param parent_table_name
         * @param scale_factor
         */
        public void addAdditionDependency(Database catalog_db, String parent_table_name, double scale_factor) {
            Table parent_tbl = catalog_db.getTables().get(parent_table_name);
            assert (!this.catalog_tbl.equals(parent_tbl)) : "Trying to make table " + this.catalog_tbl + " depend on itself";
            this.dependencies.add(new DependencyOperation(parent_tbl, ExpressionType.OPERATOR_PLUS, scale_factor));
        }

        /**
         * Adds a dependency between this table and another table where
         * TABLE_COUNT = TABLE_COUNT * (PARENT_TABLE_COUNT * SCALE_FACTOR)
         * 
         * @param catalog_db
         * @param parent_table_name
         * @param scale_factor
         */
        public void addMultiplicativeDependency(Database catalog_db, String parent_table_name, double scale_factor) {
            Table parent_tbl = catalog_db.getTables().get(parent_table_name);
            assert (!this.catalog_tbl.equals(parent_tbl)) : "Trying to make table " + this.catalog_tbl + " depend on itself";
            this.dependencies.add(new DependencyOperation(parent_tbl, ExpressionType.OPERATOR_MULTIPLY, scale_factor));
        }

        public boolean hasDependencies() {
            return (!this.dependencies.isEmpty());
        }

        /**
         * Return the set of tables this TableProfile is dependent on
         * 
         * @return
         */
        public Set<Table> getDependentTables() {
            Set<Table> tables = new HashSet<Table>();
            for (DependencyOperation d : this.dependencies) {
                tables.add(d.catalog_tbl);
            } // FOR
            return (Collections.unmodifiableSet(tables));
        }

    }

    /**
     * Constructor
     * 
     * @param catalog_db
     * @param project_type
     * @param scale_factor
     */
    public AbstractTableStatisticsGenerator(Database catalog_db, ProjectType project_type, double scale_factor) {
        this.catalog_db = catalog_db;
        this.project_type = project_type;
        this.scale_factor = scale_factor;

        assert (this.scale_factor > 0);
        this.createProfiles();
    }

    /**
     * All child clases must implement this method that will populate the
     * generator with TableProfiles
     */
    public abstract void createProfiles();

    /**
     * @param profile
     */
    public void addTableProfile(TableProfile profile) {
        Table catalog_tbl = profile.catalog_tbl;
        assert (!this.table_profiles.containsKey(catalog_tbl)) : "Duplicate TableProfile for " + catalog_tbl;

        LOG.debug("Adding table profile for " + catalog_tbl);
        this.table_profiles.put(catalog_tbl, profile);
    }

    /**
     * Generate a DependencyGraph using the TableProfile records A table will
     * have an edge coming into it from another table if the number of tuples
     * for it is dependent on the number of tuples of the other table
     * 
     * @return
     */
    private DependencyGraph generateDependencyGraph() {
        DependencyGraph dgraph = new DependencyGraph(this.catalog_db);

        for (Table catalog_tbl : this.table_profiles.keySet()) {
            dgraph.addVertex(new DesignerVertex(catalog_tbl));
        } // FOR

        for (Entry<Table, TableProfile> e : this.table_profiles.entrySet()) {
            Table catalog_tbl = e.getKey();
            TableProfile profile = e.getValue();
            DesignerVertex v = dgraph.getVertex(catalog_tbl);

            for (Table other_tbl : profile.getDependentTables()) {
                boolean ret = dgraph.addEdge(new DesignerEdge(dgraph), dgraph.getVertex(other_tbl), v, EdgeType.DIRECTED);
                assert (ret) : "Failed to add edge from " + other_tbl + " to " + catalog_tbl;
            } // FOR
        } // FOR
        return (dgraph);
    }

    /**
     * @return
     * @throws Exception
     */
    public Map<Table, TableStatistics> generate() throws Exception {
        LOG.info("Generating TableStatistics for " + this.table_profiles.size() + " tables with scale factor " + this.scale_factor);
        final String f = "%-30s %-15d [%.2fGB]"; // TableName -> TupleCount
                                                 // TableSize
        final double gb = 1073741824d;

        // First we need to generate a DependencyGraph
        final DependencyGraph dgraph = this.generateDependencyGraph();
        assert (dgraph.getVertexCount() == this.table_profiles.size());
        // GraphVisualizationPanel.createFrame(dgraph).setVisible(true);

        // Now loop through and generate our TableStatistics
        final Map<Table, TableStatistics> stats = new HashMap<Table, TableStatistics>();

        // First generate all the TableStatistics for tables without any
        // dependencies
        for (Entry<Table, TableProfile> e : this.table_profiles.entrySet()) {
            Table catalog_tbl = e.getKey();
            TableProfile profile = e.getValue();

            if (profile.hasDependencies())
                continue;
            LOG.debug("Generating FIXED TableStatistics for " + e.getKey());

            // There's not much we can do here other than this...
            // If the table is not fixed, then modify the number of tuples by
            // the scale factor
            TableStatistics ts = new TableStatistics(catalog_tbl);
            ts.tuple_count_total = Math.round(profile.tuple_count / (profile.is_fixed ? 1.0 : this.scale_factor));
            ts.tuple_size_max = ts.tuple_size_min = ts.tuple_size_avg = MemoryEstimator.estimateTupleSize(catalog_tbl);
            ts.tuple_size_total = ts.tuple_size_avg * ts.tuple_count_total;
            stats.put(catalog_tbl, ts);
            LOG.info(String.format(f, catalog_tbl.getName(), ts.tuple_count_total, ts.tuple_size_total / gb));
        } // FOR

        // Now traverse the DependencyGraph and generate the rest of the tables
        for (DesignerVertex root : dgraph.getRoots()) {
            new VertexTreeWalker<DesignerVertex, DesignerEdge>(dgraph, TraverseOrder.LONGEST_PATH) {
                protected boolean hasVisited(DesignerVertex element) {
                    return (super.hasVisited(element) || stats.containsKey(element.getCatalogItem()));
                };

                protected void callback(DesignerVertex element) {
                    if (stats.containsKey(element.getCatalogItem()))
                        return;
                    Table catalog_tbl = element.getCatalogItem();
                    TableProfile profile = table_profiles.get(catalog_tbl);

                    TableStatistics ts = new TableStatistics(catalog_tbl);
                    ts.tuple_count_total = profile.tuple_count;

                    // Dependencies
                    if (profile.hasDependencies()) {
                        LOG.debug("Calculating tuple count for " + catalog_tbl.getName() + " using " + profile.dependencies.size() + " dependencies");
                        for (DependencyOperation d : profile.dependencies) {
                            LOG.debug(catalog_tbl.getName() + " => " + ts.tuple_count_total + " " + d);

                            TableStatistics parent_ts = stats.get(d.catalog_tbl);
                            assert (parent_ts != null) : "Missing parent stats '" + d.catalog_tbl + "' for '" + catalog_tbl + "'";
                            long parent_tuples = Math.round(parent_ts.tuple_count_total * d.scale_factor);
                            switch (d.type) {
                                case OPERATOR_MULTIPLY:
                                    ts.tuple_count_total *= parent_tuples;
                                    break;
                                case OPERATOR_PLUS:
                                    ts.tuple_count_total += parent_tuples;
                                    break;
                                default:
                                    assert (false) : "Unexpected DependencyOperation type " + d.type;
                            } // SWITCH
                        } // FOR
                    }

                    // Final calculations
                    ts.tuple_count_total = Math.round(ts.tuple_count_total / (profile.is_fixed ? 1.0 : scale_factor));
                    ts.tuple_size_max = ts.tuple_size_min = ts.tuple_size_avg = MemoryEstimator.estimateTupleSize(catalog_tbl);
                    ts.tuple_size_total = ts.tuple_size_avg * ts.tuple_count_total;
                    stats.put(catalog_tbl, ts);
                    LOG.info(String.format(f, catalog_tbl.getName(), ts.tuple_count_total, ts.tuple_size_total / gb));
                };
            }.traverse(root);
        } // FOR

        // Validate
        long total_tuples = 0;
        long total_size = 0;
        for (Table catalog_tbl : this.table_profiles.keySet()) {
            TableStatistics ts = stats.get(catalog_tbl);
            assert (ts != null) : "Failed to create TableStatistics for " + catalog_tbl;
            total_tuples += ts.tuple_count_total;
            total_size += ts.tuple_size_total;
        } // FOR
        LOG.info(StringUtil.repeat("-", 60));
        LOG.info(String.format(f, "TOTAL SIZE", total_tuples, total_size / gb));

        return (stats);
    }

    /**
     * Generate table stats and apply them to
     * 
     * @param stats
     * @throws Exception
     */
    public void apply(WorkloadStatistics stats) throws Exception {
        Map<Table, TableStatistics> table_stats = this.generate();
        assert (table_stats != null);
        stats.apply(table_stats);
    }

    /**
     * Create a new instance of a TableStatisticsGenerator for the given
     * ProjectType
     * 
     * @param catalog_db
     * @param ptype
     * @param scale_factor
     * @return
     */
    public static AbstractTableStatisticsGenerator factory(Database catalog_db, ProjectType ptype, double scale_factor) {
        String generator_className = String.format("%s.%sTableStatisticsGenerator", ptype.getPackageName(), ptype.getBenchmarkPrefix());
        AbstractTableStatisticsGenerator generator = (AbstractTableStatisticsGenerator) ClassUtil.newInstance(generator_className, new Object[] { catalog_db, scale_factor }, new Class<?>[] {
                Database.class, double.class });
        assert (generator != null);
        return (generator);
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG_TYPE, ArgumentsParser.PARAM_STATS_SCALE_FACTOR, ArgumentsParser.PARAM_STATS_OUTPUT);

        double scale_factor = args.getDoubleParam(ArgumentsParser.PARAM_STATS_SCALE_FACTOR);
        File output = args.getFileParam(ArgumentsParser.PARAM_STATS_OUTPUT);

        AbstractTableStatisticsGenerator generator = factory(args.catalog_db, args.catalog_type, scale_factor);
        Map<Table, TableStatistics> table_stats = generator.generate();
        assert (table_stats != null);
        WorkloadStatistics stats = new WorkloadStatistics(args.catalog_db);
        stats.apply(table_stats);
        stats.save(output);
    }
}