package edu.brown.designer.partitioners;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.ProcedureEntry;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.CollectionUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.filters.Filter;

public abstract class AbstractPartitioner {
    private static final Logger LOG = Logger.getLogger(AbstractPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Why the partitioner halted its last search
     */
    enum HaltReason {
        NULL, LOCAL_TIME_LIMIT, GLOBAL_TIME_LIMIT, BACKTRACK_LIMIT, EXHAUSTED_SEARCH, FOUND_TARGET,
    }

    protected final Random rng = new Random();
    protected final Designer designer;
    protected final DesignerInfo info;
    protected final int num_partitions;
    protected final File checkpoint;
    protected HaltReason halt_reason = HaltReason.NULL;

    public AbstractPartitioner(Designer designer, DesignerInfo info) {
        this.designer = designer;
        this.info = info;
        this.num_partitions = info.catalogContext.numberOfPartitions;
        this.checkpoint = info.getCheckpointFile();
        if (this.checkpoint != null)
            LOG.debug("Checkpoint File: " + this.checkpoint.getAbsolutePath());
    }

    /**
     * Generate a new PartitionPlan for the database using the given
     * DesignerHints
     * 
     * @param hints
     * @return
     * @throws Exception
     */
    public abstract PartitionPlan generate(DesignerHints hints) throws Exception;

    /**
     * Get the HaltReason for the last call to generate()
     * 
     * @return
     */
    public HaltReason getHaltReason() {
        return (this.halt_reason);
    }

    /**
     * Apply the PartitionPlan to the catalog and then run through the entire
     * workload to determine which procedures are single-partition all of the
     * time
     * 
     * @param pplan
     * @param hints
     * @throws Exception
     */
    protected void setProcedureSinglePartitionFlags(final PartitionPlan pplan, final DesignerHints hints) {
        pplan.apply(info.catalogContext.database);
        if (debug.val)
            LOG.debug("Processing workload and checking which procedures are single-partitioned");
        if (info.getCostModel() != null) {
            try {
                info.getCostModel().estimateWorkloadCost(info.catalogContext, info.workload);
            } catch (Throwable ex) {
                LOG.warn("Failed to estimate workload cost", ex);
                return;
            }
            for (Entry<Procedure, ProcedureEntry> e : pplan.getProcedureEntries().entrySet()) {
                try {
                    Boolean singlepartitioned = info.getCostModel().isAlwaysSinglePartition(e.getKey());
                    if (singlepartitioned != null) {
                        if (trace.val)
                            LOG.trace("Setting single-partition flag for " + e.getKey() + ":  " + singlepartitioned);
                        e.getValue().setSinglePartition(singlepartitioned);
                    }
                } catch (Throwable ex) {
                    LOG.warn("Failed to determine whether " + e.getKey() + " is always single-partitioned", ex);
                    continue;
                }
            } // FOR
        } else {
            LOG.warn("CostModel is null! Unable to set single-partition flags");
        }
    }

    /**
     * Generates an AccessGraph for the entire database
     * 
     * @return
     * @throws Exception
     */
    protected AccessGraph generateAccessGraph() throws Exception {
        if (debug.val)
            LOG.debug("Generating AccessGraph for entire catalog");
        assert (info.workload != null);

        AccessGraph agraph = new AccessGraph(info.catalogContext.database);
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            // Skip if there are no transactions in the workload for this
            // procedure
            if (info.workload.getTraces(catalog_proc).isEmpty()) {
                if (debug.val)
                    LOG.debug("No " + catalog_proc + " transactions in workload. Skipping...");
            } else if (this.designer.getGraphs(catalog_proc) != null) {
                this.designer.getGraphs(catalog_proc).add(agraph);
                new AccessGraphGenerator(info, catalog_proc).generate(agraph);
            }
        } // FOR
        // GraphVisualizationPanel.createFrame(agraph).setVisible(true);
        return (agraph);
    }

    /**
     * 
     */
    protected class WorkloadFilter extends Filter {
        private final Database catalog_db;
        private final Set<String> stmt_cache = new HashSet<String>();

        public WorkloadFilter(Database catalog_db) {
            this.catalog_db = catalog_db;
        }

        public WorkloadFilter(Database catalog_db, Procedure catalog_proc) throws Exception {
            this(catalog_db);
            Set<Procedure> set = new HashSet<Procedure>();
            set.add(catalog_proc);
            this.addProcedures(set);
        }

        public WorkloadFilter(Database catalog_db, Collection<Table> tables) throws Exception {
            this(catalog_db);
            assert (tables.size() > 0);
            this.addTables(tables);
        }

        @Override
        public String debugImpl() {
            return (AbstractPartitioner.class.getSimpleName() + "." + this.getClass().getSimpleName() + "[cache=" + this.stmt_cache + "]");
        }

        /**
         * Creates the list of Statement catalog keys that we want to let
         * through our filter
         * 
         * @param tables
         * @throws Exception
         */
        protected void addTables(Collection<Table> tables) throws Exception {
            // Iterate through all of the procedures/queries and figure out
            // which
            // ones we'll actually want to look at
            Database catalog_db = (Database) CollectionUtil.first(tables).getParent();
            for (Procedure catalog_proc : catalog_db.getProcedures()) {
                for (Statement catalog_stmt : catalog_proc.getStatements()) {
                    Collection<Table> stmt_tables = CatalogUtil.getReferencedTables(catalog_stmt);
                    if (tables.containsAll(stmt_tables)) {
                        this.stmt_cache.add(CatalogKey.createKey(catalog_stmt));
                    }
                } // FOR
            } // FOR
            return;
        }

        /**
         * Creates the list of Statement catalog keys that we want to let
         * through our filter
         * 
         * @param tables
         * @throws Exception
         */
        protected void addProcedures(Collection<Procedure> procedures) throws Exception {
            // Iterate through all of the procedures/queries and figure out
            // which
            // ones we'll actually want to look at
            for (Procedure catalog_proc : procedures) {
                for (Statement catalog_stmt : catalog_proc.getStatements()) {
                    this.stmt_cache.add(CatalogKey.createKey(catalog_stmt));
                } // FOR
            } // FOR
            return;
        }

        @Override
        public FilterResult filter(AbstractTraceElement<? extends CatalogType> element) {
            // We want to examine all transactions
            if (element instanceof TransactionTrace) {
                // return (element.getCatalogItemName().equals("neworder") ?
                // FilterResult.PASS : FilterResult.SKIP);
                return (FilterResult.ALLOW);
                // But filter queries
            } else if (element instanceof QueryTrace) {
                QueryTrace query = (QueryTrace) element;
                return (this.stmt_cache.contains(CatalogKey.createKey(query.getCatalogItem(this.catalog_db))) ? FilterResult.ALLOW : FilterResult.SKIP);
            }
            return (FilterResult.HALT);
        }

        @Override
        protected void resetImpl() {
            // Nothing...
        }
    } // END CLASS
}