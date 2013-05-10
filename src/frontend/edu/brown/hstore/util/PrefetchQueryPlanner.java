package edu.brown.hstore.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Special planner for prefetching queries for distributed txns.
 * @author pavlo
 * @author cjl6
 */
public class PrefetchQueryPlanner {
    private static final Logger LOG = Logger.getLogger(PrefetchQueryPlanner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final Map<Integer, BatchPlanner> planners = new HashMap<Integer, BatchPlanner>();
    private final Map<Integer, ParameterMapping[][]> mappingsCache = new HashMap<Integer, ParameterMapping[][]>();
    
    private final PartitionEstimator p_estimator;
    private final int[] partitionSiteXref;
    private final CatalogContext catalogContext;
    private final FastSerializer fs = new FastSerializer(); // TODO: Use pooled memory

    /**
     * Constructor
     * @param catalogContext
     * @param p_estimator
     */
    public PrefetchQueryPlanner(CatalogContext catalogContext, PartitionEstimator p_estimator) {
        this.catalogContext = catalogContext;
        this.p_estimator = p_estimator;

        // Initialize a BatchPlanner for each Procedure if it has the
        // prefetch flag set to true. We generate an array of the SQLStmt
        // handles that we will want to prefetch for each Procedure
        List<SQLStmt> prefetchStmts = new ArrayList<SQLStmt>();
        for (Procedure catalog_proc : this.catalogContext.procedures.values()) {
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
//                addPlanner(prefetchStmts.toArray(new SQLStmt[0]), catalog_proc, p_estimator, true);
            } else {
                LOG.warn("There are no prefetchable Statements available for " + catalog_proc);
                catalog_proc.setPrefetchable(false);
            }
        } // FOR (procedure)

        this.partitionSiteXref = CatalogUtil.getPartitionSiteXrefArray(catalogContext.database);
        if (debug.val)
            LOG.debug(String.format("Initialized QueryPrefetchPlanner for %d " +
                      "Procedures with prefetchable Statements",
                      this.planners.size()));
        if (this.catalogContext.paramMappings == null) {
            LOG.warn("Unable to generate prefetachable query plans without a ParameterMappingSet");
        }
    }

    /**
     * Initialize a new cached BatchPlanner that is specific to the prefetch batch. 
     * @param catalog_proc
     * @param prefetchable
     * @param prefetchStmts
     * @return
     */
    protected BatchPlanner addPlanner(Procedure catalog_proc,
                                      List<CountedStatement> prefetchable,
                                      SQLStmt[] prefetchStmts) {
        BatchPlanner planner = new BatchPlanner(prefetchStmts, prefetchStmts.length, catalog_proc, this.p_estimator);
        planner.setPrefetchFlag(true);
        
        // For each Statement in this batch, cache its ParameterMappings objects
        ParameterMapping mappings[][] = new ParameterMapping[prefetchStmts.length][]; 
        for (int i = 0; i < prefetchStmts.length; i++) {
            CountedStatement counted_stmt = prefetchable.get(i);
            mappings[i] = new ParameterMapping[counted_stmt.statement.getParameters().size()];
            for (StmtParameter catalog_param : counted_stmt.statement.getParameters().values()) {
                Collection<ParameterMapping> pmSets = this.catalogContext.paramMappings.get(
                                                                counted_stmt.statement,
                                                                counted_stmt.counter,
                                                                catalog_param);
                assert(pmSets != null) : String.format("Unexpected %s for %s", counted_stmt, catalog_param);
                mappings[i][catalog_param.getIndex()] = CollectionUtil.first(pmSets);
            } // FOR (StmtParameter)
        } // FOR (CountedStatement)
        
        int batchId = VoltProcedure.getBatchHashCode(prefetchStmts, prefetchStmts.length);
        this.planners.put(batchId, planner);
        this.mappingsCache.put(batchId, mappings);
        
        if (debug.val)
            LOG.debug(String.format("%s Prefetch Statements: %s",
                      catalog_proc.getName(), Arrays.toString(prefetchStmts)));
        return planner;
    }
    
    /**
     * @param ts
     * @return
     */
    public TransactionInitRequest[] generateWorkFragments(LocalTransaction ts) {
        // We can't do this without a ParameterMappingSet
        if (this.catalogContext.paramMappings == null) {
            return (null);
        }
        // Or without queries that can be prefetched.
        List<CountedStatement> prefetchable = ts.getEstimatorState().getPrefetchableStatements(); 
        if (prefetchable.isEmpty()) {
            return (null);
        }
        
        Procedure catalog_proc = ts.getProcedure();
        assert (ts.getProcedureParameters() != null) : 
            "Unexpected null ParameterSet for " + ts;
        Object proc_params[] = ts.getProcedureParameters().toArray();
        
        if (debug.val)
            LOG.debug(String.format("%s - Generating prefetch WorkFragments using %s",
                      ts, ts.getProcedureParameters()));
        
        SQLStmt[] prefetchStmts = new SQLStmt[prefetchable.size()];
        for (int i = 0; i < prefetchStmts.length; ++i) {
            prefetchStmts[i] = new SQLStmt(prefetchable.get(i).statement);
        } // FOR
        
        // Use the StmtParameter mappings for the queries we
        // want to prefetch and extract the ProcParameters
        // to populate an array of ParameterSets to use as the batchArgs
        int hashcode = VoltProcedure.getBatchHashCode(prefetchStmts, prefetchStmts.length);
        
        // Check if we've used this planner in the past. If not, then create it.
        BatchPlanner planner = this.planners.get(hashcode);
        if (planner == null) {
            planner = this.addPlanner(catalog_proc, prefetchable, prefetchStmts);
        }
        assert(planner != null) : "Missing BatchPlanner for " + catalog_proc;
        ParameterSet prefetchParams[] = new ParameterSet[planner.getBatchSize()];
        ByteString prefetchParamsSerialized[] = new ByteString[prefetchParams.length];
        ParameterMapping mappings[][] = this.mappingsCache.get(hashcode);
        assert(mappings != null) : "Missing cached ParameterMappings for " + catalog_proc;
        
        // Makes a list of ByteStrings containing the ParameterSets that we need
        // to send over to the remote sites so that they can execute our
        // prefetchable queries
        for (int i = 0; i < prefetchParams.length; i++) {
            CountedStatement counted_stmt = prefetchable.get(i);
            if (debug.val)
                LOG.debug(String.format("%s - Building ParameterSet for prefetchable query %s",
                          ts, counted_stmt));
            Object stmt_params[] = new Object[counted_stmt.statement.getParameters().size()];

            // Generates a new object array using a mapping from the
            // ProcParameter to the StmtParameter. This relies on a
            // ParameterMapping already being installed in the catalog
            for (StmtParameter catalog_param : counted_stmt.statement.getParameters().values()) {
                ParameterMapping pm = mappings[i][catalog_param.getIndex()];
                assert(pm != null) :
                    String.format("Unexpected null %s for %s [%s]",
                                  ParameterMapping.class.getSimpleName(), catalog_param.fullName(), counted_stmt);
                
                if (pm.procedure_parameter.getIsarray()) {
                    stmt_params[catalog_param.getIndex()] = ParametersUtil.getValue(ts.getProcedureParameters(), pm);
                }
                else {
                    ProcParameter catalog_proc_param = pm.procedure_parameter;
                    assert(catalog_proc_param != null) :
                        "Missing mapping from " + catalog_param.fullName() + " to ProcParameter";
                    stmt_params[catalog_param.getIndex()] = proc_params[catalog_proc_param.getIndex()];
                }
            } // FOR (StmtParameter)
            prefetchParams[i] = new ParameterSet(stmt_params);

            if (debug.val)
                LOG.debug(String.format("%s - [Prefetch %02d] %s -> %s",
                          ts, i, counted_stmt, prefetchParams[i]));

            // Serialize this ParameterSet for the TransactionInitRequests
            try {
                if (i > 0) this.fs.clear();
                prefetchParams[i].writeExternal(this.fs);
                prefetchParamsSerialized[i] = ByteString.copyFrom(this.fs.getBBContainer().b);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
            }
        } // FOR (Statement)

        // Generate the WorkFragments that we will need to send in our TransactionInitRequest
        BatchPlan plan = planner.plan(ts.getTransactionId(),
                                      ts.getBasePartition(),
                                      ts.getPredictTouchedPartitions(),
                                      ts.isPredictSinglePartition(),
                                      ts.getTouchedPartitions(),
                                      prefetchParams);
        List<WorkFragment.Builder> fragmentBuilders = new ArrayList<WorkFragment.Builder>();
        plan.getWorkFragmentsBuilders(ts.getTransactionId(), fragmentBuilders);

        // Loop through the fragments and check whether at least one of
        // them needs to be executed at the base (local) partition. If so, we need a
        // separate TransactionInitRequest per site. Group the WorkFragments by siteID.
        // If we have a prefetchable query for the base partition, it means that
        // we will try to execute it before we actually need it whenever the
        // PartitionExecutor is idle. That means, we don't want to serialize all this
        // if it's only going to the base partition.
        TransactionInitRequest.Builder[] builders = new TransactionInitRequest.Builder[this.catalogContext.numberOfSites];
        for (WorkFragment.Builder fragmentBuilder : fragmentBuilders) {
            int site_id = this.partitionSiteXref[fragmentBuilder.getPartitionId()];
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
            builders[site_id].addPrefetchFragments(fragmentBuilder);
        } // FOR (WorkFragment)

        PartitionSet touched_partitions = ts.getPredictTouchedPartitions();
        boolean touched_sites[] = new boolean[this.catalogContext.numberOfSites];
        Arrays.fill(touched_sites, false);
        for (int partition : touched_partitions) {
            touched_sites[this.partitionSiteXref[partition]] = true;
        } // FOR
        TransactionInitRequest[] init_requests = new TransactionInitRequest[this.catalogContext.numberOfSites];
        TransactionInitRequest default_request = null;
        for (int site_id = 0; site_id < this.catalogContext.numberOfSites; ++site_id) {
            // If this site has no prefetched fragments ...
            if (builders[site_id] == null) {
                // but it has other non-prefetched WorkFragments, create a
                // default TransactionInitRequest.
                if (touched_sites[site_id]) {
                    if (default_request == null) {
                        default_request = TransactionInitRequest.newBuilder()
                                                .setTransactionId(ts.getTransactionId())
                                                .setProcedureId(ts.getProcedure().getId())
                                                .setBasePartition(ts.getBasePartition())
                                                .addAllPartitions(ts.getPredictTouchedPartitions()).build();
                    }
                    init_requests[site_id] = default_request;
                    if (debug.val) LOG.debug(ts + " - Sending default TransactionInitRequest to site " + site_id);
                }
                // And no other WorkFragments, set the TransactionInitRequest to null.
                else {
                    init_requests[site_id] = null;
                }
            }
            // Otherwise, just build it.
            else {
                init_requests[site_id] = builders[site_id].build();
                if (debug.val) LOG.debug(ts + " - Sending prefetch WorkFragments to site " + site_id);
            }
        } // FOR (Site)

        if (debug.val) LOG.debug(ts + " - TransactionInitRequests\n" + StringUtil.join("\n", init_requests));
        return (init_requests);
    }
}
