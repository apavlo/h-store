package edu.brown.hstore.util;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 * @author cjl6
 */
public class PrefetchQueryPlanner implements Loggable {
    private static final Logger LOG = Logger.getLogger(PrefetchQueryPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // private final Database catalog_db;
    private final Map<Procedure, BatchPlanner> planners = new HashMap<Procedure, BatchPlanner>();
    private final int[] partitionSiteXref;
    private final int num_sites;
    private final BitSet touched_sites;
    private final FastSerializer fs = new FastSerializer(); // TODO: Use pooled memory

    /**
     * Constructor
     * @param catalog_db
     * @param p_estimator
     */
    public PrefetchQueryPlanner(Database catalog_db, PartitionEstimator p_estimator) {
        // this.catalog_db = catalog_db;
        this.num_sites = CatalogUtil.getNumberOfSites(catalog_db);
        this.touched_sites = new BitSet(this.num_sites);

        // Initialize a BatchPlanner for each Procedure if it has the
        // prefetch flag set to true. We generate an array of the SQLStmt
        // handles that we will want to prefetch for each Procedure
        List<SQLStmt> prefetchStmts = new ArrayList<SQLStmt>();
        for (Procedure catalog_proc : catalog_db.getProcedures().values()) {
            if (catalog_proc.getPrefetchable() == false) continue;
            
            prefetchStmts.clear();
            for (Statement catalog_stmt : catalog_proc.getStatements().values()) {
                if (catalog_stmt.getPrefetchable() == false) continue;
                // Make sure that all of this Statement's input parameters
                // are mapped to one of the Procedure's ProcParameter
                boolean valid = true;
                for (StmtParameter catalog_param : catalog_stmt.getParameters().values()) {
                    if (catalog_param.getProcparameter() == null) {
                        LOG.warn(String.format("Unable to mark %s as prefetchable because %s is not mapped to a ProcParameter",
                                               catalog_stmt.fullName(), catalog_param.fullName()));
                        valid = false;
                    }
                } // FOR
                if (valid) prefetchStmts.add(new SQLStmt(catalog_stmt));
            } // FOR
            if (prefetchStmts.isEmpty() == false) {
                BatchPlanner planner = new BatchPlanner(prefetchStmts.toArray(new SQLStmt[0]),
                                                        prefetchStmts.size(),
                                                        catalog_proc,
                                                        p_estimator);
                planner.setPrefetchFlag(true);
                this.planners.put(catalog_proc, planner);
                if (debug.get()) LOG.debug(String.format("%s Prefetch Statements: %s",
                                                         catalog_proc.getName(), prefetchStmts));
            } else {
                LOG.warn("There are no prefetchable Statements available for " + catalog_proc);
                catalog_proc.setPrefetchable(false);
            }
        } // FOR (procedure)

        this.partitionSiteXref = CatalogUtil.getPartitionSiteXrefArray(catalog_db);
        if (debug.get()) LOG.debug(String.format("Initialized QueryPrefetchPlanner for %d " +
        		                                 "Procedures with prefetchable Statements",
        		                                 this.planners.size()));
    }

    /**
     * @param ts
     * @return
     */
    public TransactionInitRequest[] generateWorkFragments(LocalTransaction ts) {
        if (debug.get()) LOG.debug(ts + " - Generating prefetch WorkFragments");
        
        Procedure catalog_proc = ts.getProcedure();
        assert (ts.getProcedureParameters() != null) : 
            "Unexpected null ParameterSet for " + ts;
        Object proc_params[] = ts.getProcedureParameters().toArray();

        // Use the StmtParameter mappings for the queries we
        // want to prefetch and extract the ProcParameters
        // to populate an array of ParameterSets to use as the batchArgs
        BatchPlanner planner = this.planners.get(catalog_proc);
        assert (planner != null) : "Missing BatchPlanner for " + catalog_proc;
        ParameterSet prefetchParams[] = new ParameterSet[planner.getBatchSize()];
        ByteString prefetchParamsSerialized[] = new ByteString[prefetchParams.length];

        // Makes a list of ByteStrings containing the ParameterSets that we need
        // to send over to the remote sites so that they can execute our
        // prefetchable queries
        for (int i = 0; i < prefetchParams.length; i++) {
            Statement catalog_stmt = planner.getStatement(i);
            if (debug.get()) LOG.debug(String.format("%s - Building ParameterSet for prefetchable query %s",
                                                     ts, catalog_stmt.fullName()));
            Object stmt_params[] = new Object[catalog_stmt.getParameters().size()];

            // Generates a new object array using a mapping from the
            // ProcParameter to the StmtParameter. This relies on a
            // ParameterMapping already being installed in the catalog
            // TODO: Precompute this as arrays (it will be much faster)
            for (StmtParameter catalog_param : catalog_stmt.getParameters().values()) {
                ProcParameter catalog_proc_param = catalog_param.getProcparameter();
                assert(catalog_proc_param != null) : "Missing mapping from " + catalog_param.fullName() + " to ProcParameter";
                stmt_params[catalog_param.getIndex()] = proc_params[catalog_proc_param.getIndex()];
            } // FOR (StmtParameter)
            prefetchParams[i] = new ParameterSet(stmt_params);

            if (debug.get()) LOG.debug(String.format("%s - [%02d] Prefetch %s -> %s",
                                                     ts, i, catalog_stmt.getName(), prefetchParams[i]));

            // Serialize this ParameterSet for the TransactionInitRequests
            try {
                if (i > 0) this.fs.clear();
                prefetchParams[i].writeExternal(this.fs);
                prefetchParamsSerialized[i] = ByteString.copyFrom(this.fs.getBBContainer().b);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
            }
        } // FOR (Statement)

        // Generate the WorkFragments that we will need to send in our
        // TransactionInitRequest
        BatchPlan plan = planner.plan(ts.getTransactionId(),
                                      ts.getClientHandle(),
                                      ts.getBasePartition(),
                                      ts.getPredictTouchedPartitions(),
                                      ts.isPredictSinglePartition(),
                                      ts.getTouchedPartitions(),
                                      prefetchParams);
        List<WorkFragment> fragments = new ArrayList<WorkFragment>();
        plan.getWorkFragments(ts.getTransactionId(), fragments);

        // Loop through the fragments and check whether at least one of
        // them needs to be executed at the base (local) partition. If so, we need a
        // separate TransactionInitRequest per site. Group the WorkFragments by siteID.
        // If we have a prefetchable query for the base partition, it means that
        // we will try to execute it before we actually need it whenever the
        // PartitionExecutor is idle That means, we don't want to serialize all this
        // if it's only going to the base partition.
        TransactionInitRequest.Builder[] builders = new TransactionInitRequest.Builder[this.num_sites];
        for (WorkFragment frag : fragments) {
            int site_id = this.partitionSiteXref[frag.getPartitionId()];
            if (builders[site_id] == null) {
                builders[site_id] = TransactionInitRequest.newBuilder()
                                            .setTransactionId(ts.getTransactionId().longValue())
                                            .setProcedureId(ts.getProcedure().getId())
                                            .setBasePartition(ts.getBasePartition())
                                            .addAllPartitions(ts.getPredictTouchedPartitions());
                for (ByteString bs : prefetchParamsSerialized) {
                    builders[site_id].addPrefetchParams(bs);
                } // FOR
            }
            builders[site_id].addPrefetchFragments(frag);
        } // FOR (WorkFragment)

        Collection<Integer> touched_partitions = ts.getPredictTouchedPartitions();
        this.touched_sites.clear();
        for (int partition : touched_partitions) {
            this.touched_sites.set(this.partitionSiteXref[partition]);
        } // FOR
        TransactionInitRequest[] init_requests = new TransactionInitRequest[this.num_sites];
        TransactionInitRequest default_request = null;
        for (int site_id = 0; site_id < this.num_sites; ++site_id) {
            // If this site has no prefetched fragments ...
            if (builders[site_id] == null) {
                // but it has other non-prefetched WorkFragments, create a
                // default TransactionInitRequest.
                if (this.touched_sites.get(site_id)) {
                    if (default_request == null) {
                        default_request = TransactionInitRequest.newBuilder()
                                                .setTransactionId(ts.getTransactionId())
                                                .setProcedureId(ts.getProcedure().getId())
                                                .setBasePartition(ts.getBasePartition())
                                                .addAllPartitions(ts.getPredictTouchedPartitions()).build();
                    }
                    init_requests[site_id] = default_request;
                    if (debug.get()) LOG.debug(ts + " - Sending default TransactionInitRequest to site " + site_id);
                }
                // and no other WorkFragments, set the TransactionInitRequest to
                // null.
                else {
                    init_requests[site_id] = null;
                }
            }
            // Otherwise, just build it.
            else {
                init_requests[site_id] = builders[site_id].build();
                if (debug.get()) LOG.debug(ts + " - Sending prefetch WorkFragments to site " + site_id);
            }
        } // FOR (Site)

        if (trace.get()) LOG.trace(ts + " - TransactionInitRequests\n" + StringUtil.join("\n", init_requests));
        return (init_requests);
    }

    @Override
    public void updateLogging() {
        // TODO Auto-generated method stub

    }

}
