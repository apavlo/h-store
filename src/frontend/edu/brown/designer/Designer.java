package edu.brown.designer;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.generators.PartitionPlanTreeGenerator;
import edu.brown.designer.indexselectors.AbstractIndexSelector;
import edu.brown.designer.mappers.AbstractMapper;
import edu.brown.designer.mappers.PartitionMapping;
import edu.brown.designer.partitioners.AbstractPartitioner;
import edu.brown.designer.partitioners.RandomPartitioner;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.graphs.IGraph;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ArgumentsParser;
import edu.brown.workload.Workload;

public class Designer {
    private static final Logger LOG = Logger.getLogger(Designer.class.getName());

    // ----------------------------------------------------
    // DESIGNER INFORMATION
    // ----------------------------------------------------
    protected final DesignerInfo info;
    protected final DesignerHints hints;
    protected final ArgumentsParser args;

    protected PhysicalDesign design;
    protected PartitionTree final_graph;

    protected final Map<Procedure, AccessGraph> proc_access_graphs = new HashMap<Procedure, AccessGraph>();
    protected final Map<Procedure, Set<IGraph<DesignerVertex, DesignerEdge>>> proc_graphs = new HashMap<Procedure, Set<IGraph<DesignerVertex, DesignerEdge>>>();

    // ----------------------------------------------------
    // PARTITIONING
    // ----------------------------------------------------
    protected final AbstractPartitioner partitioner;
    protected PartitionPlan pplan;

    // ----------------------------------------------------
    // MAPPING
    // ----------------------------------------------------
    protected final AbstractMapper mapper;
    protected PartitionMapping pmap;

    // ----------------------------------------------------
    // INDEX SELECTION
    // ----------------------------------------------------
    protected final AbstractIndexSelector indexer;
    protected IndexPlan iplan;

    /**
     * Constructor
     */
    public Designer(DesignerInfo info, DesignerHints hints, ArgumentsParser args) throws Exception {
        this.info = info;
        this.hints = hints;
        this.args = args;

        // Partitioner
        assert (info.partitioner_class != null);
        Constructor<? extends AbstractPartitioner> partitionerConstructor = info.partitioner_class.getConstructor(new Class[] { Designer.class, DesignerInfo.class });
        assert (partitionerConstructor != null);
        this.partitioner = partitionerConstructor.newInstance(new Object[] { this, this.info });
        if (this.args.pplan != null)
            this.pplan = this.args.pplan;

        // Mapper
        assert (info.mapper_class != null);
        Constructor<? extends AbstractMapper> mapperConstructor = info.mapper_class.getConstructor(new Class[] { Designer.class, DesignerInfo.class });
        assert (mapperConstructor != null);
        this.mapper = mapperConstructor.newInstance(new Object[] { this, this.info });
        if (this.args.pmap != null)
            this.pmap = this.args.pmap;

        // Indexer
        assert (info.indexer_class != null);
        Constructor<? extends AbstractIndexSelector> indexerConstructor = info.indexer_class.getConstructor(new Class[] { Designer.class, DesignerInfo.class });
        assert (indexerConstructor != null);
        this.indexer = indexerConstructor.newInstance(new Object[] { this, this.info });
        if (this.args.iplan != null)
            this.iplan = this.args.iplan;

        // Initialize stuff!
        this.init();
    }

    /**
     * 
     */
    private void init() throws Exception {
        //
        // Step 1: Workload Analysis
        //
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            if ((!this.hints.proc_include.isEmpty() && !this.hints.proc_include.contains(catalog_proc.getName())) || this.hints.proc_exclude.contains(catalog_proc.getName())) {
                LOG.debug("Skipping " + catalog_proc);
                continue;
            }

            // Generate a new AccessGraph for this Procedure
            LOG.debug("Generating AccessGraph for " + catalog_proc);
            AccessGraph agraph = new AccessGraph(info.catalogContext.database);
            new AccessGraphGenerator(info, catalog_proc).generate(agraph);
            if (agraph.getVertexCount() == 0) {
                if (LOG.isDebugEnabled())
                    LOG.warn("The workload analyzer created an AccessGraph for " + catalog_proc + " with no vertices. Skipping...");
                continue;
            }
            this.proc_access_graphs.put(catalog_proc, agraph);
            this.proc_graphs.put(catalog_proc, new LinkedHashSet<IGraph<DesignerVertex, DesignerEdge>>());
            this.proc_graphs.get(catalog_proc).add(agraph);
        } // FOR
    }

    public AbstractPartitioner getPartitioner() {
        return this.partitioner;
    }

    public AbstractMapper getMapper() {
        return this.mapper;
    }

    public AbstractIndexSelector getIndexer() {
        return this.indexer;
    }

    public DesignerInfo getDesignerInfo() {
        return (this.info);
    }

    public Set<Procedure> getProcedures() {
        return (this.proc_access_graphs.keySet());
    }

    public DependencyGraph getDependencyGraph() {
        return (this.info.dgraph);
    }

    // public PartitionTree getPartitionTree() {
    // return (this.pplan.getPartitionTree());
    // }

    public AccessGraph getAccessGraph(Procedure catalog_proc) {
        return (this.proc_access_graphs.get(catalog_proc));
    }

    public Set<IGraph<DesignerVertex, DesignerEdge>> getGraphs(Procedure catalog_proc) {
        return (this.proc_graphs.get(catalog_proc));
    }

    public IGraph<DesignerVertex, DesignerEdge> getFinalGraph() {
        return (this.final_graph);
    }

    public Database getDatabase() {
        return this.info.catalogContext.database;
    }

    public Workload getWorkload() {
        return this.info.workload;
    }

    public PartitionPlan getPartitionPlan() {
        return (this.pplan);
    }

    public void apply() throws Exception {
        //
        // Apply the mapping and check whether we are single-sited
        //
        /*
         * Catalog catalog = this.design.createCatalog();
         * System.out.println("============================================");
         * Database catalog_db = CatalogUtil.getDatabase(catalog); for (Table
         * catalog_tbl : catalog_db.getTables()) {
         * System.out.println(catalog_tbl); Map<String, Object> fields = new
         * HashMap<String, Object>(); fields.put("isreplicated",
         * catalog_tbl.getIsreplicated()); fields.put("partitioncolumn",
         * catalog_tbl.getPartitioncolumn()); for (String field :
         * fields.keySet()) { System.out.println("   " + field + ": " +
         * fields.get(field)); } System.out.println(); }
         */
    }

    /**
     * 
     */
    public PhysicalDesign process() throws Exception {
        //
        // Step 2: Table Partitioner
        //
        if (this.pplan == null) {
            if (this.hints.start_random) {
                LOG.debug("Randomizing the catalog partitioning attributes");
                PartitionPlan random_pplan = new RandomPartitioner(this, this.info, false).generate(this.hints);
                random_pplan.apply(this.info.catalogContext.database);
                LOG.debug("Randomized Catalog:\n" + random_pplan);
            }

            LOG.debug("Creating partition plan using " + this.partitioner.getClass().getSimpleName());
            this.pplan = this.partitioner.generate(this.hints);
            if (this.args.hasParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT)) {
                File path = this.args.getFileParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT);
                this.pplan.save(path);
                LOG.info("Saved generated PartitionPlan to '" + path.getName() + "'");
            }
        }
        LOG.info("Partition Plan:\n" + this.pplan.toString());

        this.design = new PhysicalDesign(info.catalogContext.database);
        this.design.plan = this.pplan;

        this.final_graph = new PartitionTree(this.info.catalogContext.database);
        new PartitionPlanTreeGenerator(this.info, this.pplan).generate(this.final_graph);
        // System.out.println(this.plan);
        // System.exit(1);

        //
        // Step 3: Partition Placement/Mapper
        //
        if (false && this.pmap == null) {
            LOG.info("Creating partition mapping using " + this.mapper.getClass().getSimpleName());
            this.pmap = this.mapper.generate(this.hints, this.pplan);
            System.out.println(this.pmap);
        }

        //
        // Step 4: Index Selector
        //

        return (this.design);
    }

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG,
                     ArgumentsParser.PARAM_WORKLOAD,
                     ArgumentsParser.PARAM_STATS,
                     ArgumentsParser.PARAM_MAPPINGS
        );
        HStoreConf.initArgumentsParser(args);
        System.err.println("TEMP DIR: " + HStoreConf.singleton().global.temp_dir);

        if (args.hasParam(ArgumentsParser.PARAM_CATALOG_HOSTS)) {
            ClusterConfiguration cc = new ClusterConfiguration(args.getParam(ArgumentsParser.PARAM_CATALOG_HOSTS));
            args.updateCatalog(FixCatalog.cloneCatalog(args.catalog, cc), null);
        }

        // Create the container object that will hold all the information that
        // the designer will need to use
        DesignerInfo info = new DesignerInfo(args);
        DesignerHints hints = args.designer_hints;

        long start = System.currentTimeMillis();
        // System.out.println("WHITELIST: " + hints.proc_include);
        Designer designer = new Designer(info, hints, args);
        PhysicalDesign design = designer.process();
        LOG.info("STOP: " + (System.currentTimeMillis() - start) / 1000d);

        // if (args.hasParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT)) {
        // String output =
        // args.getParam(ArgumentsParser.PARAM_PARTITION_PLAN_OUTPUT);
        // LOG.info("Saving PartitionPlan to '" + output + "'");
        // PartitionPlan pplan = design.plan;
        // pplan.save(output);
        // }

        // int total = 0;
        // int singlesited = 0;
        // SingleSitedCostModel cmodel = new
        // SingleSitedCostModel(design.catalog_db);
        // for (AbstractTraceElement element : info.workload) {
        // if (element instanceof TransactionTrace) {
        // TransactionTrace xact = (TransactionTrace)element;
        // total++;
        // SingleSitedCostModel.CostEstimate estimate =
        // cmodel.isSingleSited(design.catalog_db, xact, null);
        // if (estimate.singlesited_xact) singlesited++;
        // }
        // } // FOR
        // System.out.println("SINGLE-SITED: " + singlesited + "/" + total);

        // design.apply(catalog);

        // Let's try to recompile this mofo!
        // VoltCompiler compiler = new VoltCompiler(null,
        // BackendTarget.HSQLDB_BACKEND, 1, 1, "localhost", System.out);
        // compiler.recompile(jar_path, catalog);
        // System.out.println(catalog.serialize());

        // System.out.println(graph.toString(true));
        // System.out.println(graph.toString(true));
    }
}
