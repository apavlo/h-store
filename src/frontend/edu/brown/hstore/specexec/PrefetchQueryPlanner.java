package edu.brown.hstore.specexec;

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
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.BatchPlanner;
import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.estimators.markov.MarkovEstimatorState;
import edu.brown.hstore.txns.DependencyTracker;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.TransactionUtil;
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
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private final PartitionEstimator p_estimator;
    private final int[] partitionSiteXref;
    private final CatalogContext catalogContext;
    
    // ThreadLocal Stuff
    private final ThreadLocal<Map<Integer, BatchPlanner>> planners = new ThreadLocal<Map<Integer,BatchPlanner>>() {
        protected java.util.Map<Integer,BatchPlanner> initialValue() {
            return (new HashMap<Integer, BatchPlanner>());
        }
    };


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
        int stmt_ctr = 0;
        int proc_ctr = 0;
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
                        LOG.warn(String.format("Unable to mark %s as prefetchable because %s is not " +
                        		 "mapped to a ProcParameter",
                                 catalog_stmt.fullName(), catalog_param.fullName()));
                        valid = false;
                    }
                } // FOR
                if (valid) prefetchStmts.add(new SQLStmt(catalog_stmt));
            } // FOR
            if (prefetchStmts.isEmpty() == false) {
                stmt_ctr += prefetchStmts.size();
                proc_ctr++;
            } else {
                LOG.warn("There are no prefetchable Statements available for " + catalog_proc);
                catalog_proc.setPrefetchable(false);
            }
        } // FOR (procedure)

        this.partitionSiteXref = CatalogUtil.getPartitionSiteXrefArray(catalogContext.database);
        if (debug.val)
            LOG.debug(String.format("Initialized %s for %d Procedures " +
            		  "with a total of %d prefetchable Statements",
            		  this.getClass().getSimpleName(), proc_ctr, stmt_ctr));
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
        
        int batchId = VoltProcedure.getBatchHashCode(prefetchStmts, prefetchStmts.length);
        this.planners.get().put(batchId, planner);
        
        if (debug.val)
            LOG.debug(String.format("%s Prefetch Statements: %s",
                      catalog_proc.getName(), Arrays.toString(prefetchStmts)));
        return planner;
    }

    /**
     * Generate a list of TransactionInitRequest builders that contain WorkFragments
     * for queries that can be prefetched.
     * @param ts The txn handle to generate plans for.
     * @param procParams The txn's procedure input parameters.
     * @param depTracker The DependencyTracker for this txn's base partition.
     * @param fs The FastSerializer handle to use to serialize various data items.
     * @return
     */
    public TransactionInitRequest.Builder[] plan(LocalTransaction ts,
                                                 ParameterSet procParams,
                                                 DependencyTracker depTracker,
                                                 FastSerializer fs) {
        // We can't do this without a ParameterMappingSet
        if (this.catalogContext.paramMappings == null) {
            if (debug.val)
                LOG.warn(ts + " - No parameter mappings available. Unable to schedule prefetch queries");
            return (null);
        }
        // Or without queries that can be prefetched.
        List<CountedStatement> prefetchable = ts.getEstimatorState().getPrefetchableStatements(); 
        if (prefetchable.isEmpty()) {
            if (debug.val)
                LOG.warn(ts + " - No prefetchable queries were found in the transaction's initial path estimate. " +
                		 "Unable to schedule prefetch queries.");
            return (null);
        }
        
        if (debug.val)
            LOG.debug(String.format("%s - Generating prefetch WorkFragments using %s",
                      ts, procParams));
        
        // Create a SQLStmt batch as if it was created during the normal txn execution process
        SQLStmt[] prefetchStmts = new SQLStmt[prefetchable.size()];
        for (int i = 0; i < prefetchStmts.length; ++i) {
            prefetchStmts[i] = new SQLStmt(prefetchable.get(i).statement);
        } // FOR
        
        // Use the StmtParameter mappings for the queries we
        // want to prefetch and extract the ProcParameters
        // to populate an array of ParameterSets to use as the batchArgs
        int hashcode = VoltProcedure.getBatchHashCode(prefetchStmts, prefetchStmts.length);
        
        // Check if we've used this planner in the past. If not, then create it.
        BatchPlanner planner = this.planners.get().get(hashcode);
        if (planner == null) {
            planner = this.addPlanner(ts.getProcedure(), prefetchable, prefetchStmts);
        }
        assert(planner != null) : "Missing BatchPlanner for " + ts.getProcedure();
        final ParameterSet prefetchParams[] = new ParameterSet[planner.getBatchSize()];
        final int prefetchCounters[] = new int[planner.getBatchSize()]; 
        final ByteString prefetchParamsSerialized[] = new ByteString[prefetchParams.length];
        final int basePartition = ts.getBasePartition();
        
        // Makes a list of ByteStrings containing the ParameterSets that we need
        // to send over to the remote sites so that they can execute our
        // prefetchable queries
        Object proc_params[] = procParams.toArray();
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
                Collection<ParameterMapping> pmSets = this.catalogContext.paramMappings.get(
                                                                counted_stmt.statement,
                                                                counted_stmt.counter,
                                                                catalog_param);
                assert(pmSets != null) : 
                    String.format("Unexpected null %s %s set for %s",
                                  counted_stmt, ParameterMapping.class.getSimpleName(), catalog_param);
                ParameterMapping pm = CollectionUtil.first(pmSets);
                assert(pm != null) :
                    String.format("Unexpected null %s for %s [%s]",
                                  ParameterMapping.class.getSimpleName(),
                                  catalog_param.fullName(), counted_stmt);
                assert(pm.statement_index == counted_stmt.counter) :
                    String.format("Mismatch StmtCounter for %s - Expected[%d] != Actual[%d]\n%s",
                                  counted_stmt, counted_stmt.counter, pm.statement_index, pm);
                
                if (pm.procedure_parameter.getIsarray()) {
                    try {
                        stmt_params[catalog_param.getIndex()] = ParametersUtil.getValue(procParams, pm);
                    } catch (Throwable ex) {
                        String msg = String.format("Unable to get %s value for %s in %s\n" +
                        		                   "ProcParams: %s\nParameterMapping: %s",
                                                   catalog_param.fullName(), ts, counted_stmt,
                                                   procParams, pm);
                        LOG.error(msg, ex);
                        throw new ServerFaultException(msg, ex, ts.getTransactionId());
                    }
                }
                else {
                    ProcParameter catalog_proc_param = pm.procedure_parameter;
                    assert(catalog_proc_param != null) :
                        "Missing mapping from " + catalog_param.fullName() + " to ProcParameter";
                    stmt_params[catalog_param.getIndex()] = proc_params[catalog_proc_param.getIndex()];
                }
            } // FOR (StmtParameter)
            prefetchParams[i] = new ParameterSet(stmt_params);
            prefetchCounters[i] = counted_stmt.counter;

            if (debug.val)
                LOG.debug(String.format("%s - [Prefetch %02d] %s -> %s",
                          ts, i, counted_stmt, prefetchParams[i]));

            // Serialize this ParameterSet for the TransactionInitRequests
            try {
                fs.clear();
                prefetchParams[i].writeExternal(fs);
                prefetchParamsSerialized[i] = ByteString.copyFrom(fs.getBBContainer().b);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
            }
        } // FOR (Statement)

        // Generate the WorkFragments that we will need to send in our TransactionInitRequest
        BatchPlan plan = planner.plan(ts.getTransactionId(),
                                      basePartition,
                                      ts.getPredictTouchedPartitions(),
                                      ts.getTouchedPartitions(),
                                      prefetchParams);
        List<WorkFragment.Builder> fragmentBuilders = new ArrayList<WorkFragment.Builder>();
        plan.getWorkFragmentsBuilders(ts.getTransactionId(), prefetchCounters, fragmentBuilders);
        
        // Loop through the fragments and check whether at least one of
        // them needs to be executed at the base (local) partition. If so, we need a
        // separate TransactionInitRequest per site. Group the WorkFragments by siteID.
        // If we have a prefetchable query for the base partition, it means that
        // we will try to execute it before we actually need it whenever the
        // PartitionExecutor is idle. That means, we don't want to serialize all this
        // if it's only going to the base partition.
        TransactionInitRequest.Builder[] builders = new TransactionInitRequest.Builder[this.catalogContext.numberOfSites];
        boolean first = true;
        int local_site_id = this.partitionSiteXref[ts.getBasePartition()];
        for (WorkFragment.Builder fragment : fragmentBuilders) {
            // IMPORTANT: We need to check whether our estimator goofed and is trying to have us
            // prefetch a query at our base partition. This is bad for all sorts of reasons...
            if (basePartition == fragment.getPartitionId()) {
                if (debug.val)
                    LOG.warn(String.format("%s - Trying to schedule prefetch %s at base partition %d. Skipping...\n" +
                		 "ProcParameters: %s\n",
                         ts, WorkFragment.class.getSimpleName(), basePartition, procParams));
                
                // If we got a busted estimate, then we definitely want to be able to
                // follow the txn as it runs and correct ourselves!
                if (ts.getEstimatorState() instanceof MarkovEstimatorState) {
                    ts.getEstimatorState().enableUpdates();
                }
                continue;
            }
            
            // Update DependencyTracker
            // This has to be done *before* you add it to the TransactionInitRequest
            if (first) {
                // Make sure that we initialize our internal PrefetchState for this txn
                ts.initializePrefetch();
                depTracker.addTransaction(ts); 
                if (ts.profiler != null) ts.profiler.addPrefetchQuery(prefetchStmts.length);
                first = false;
            }
            // HACK: Attach the prefetch params in the transaction handle in case we need to use it locally
            int site_id = this.partitionSiteXref[fragment.getPartitionId()];
            if (site_id == local_site_id && ts.hasPrefetchParameters() == false) {
                ts.attachPrefetchParameters(prefetchParams);
            }
            
            if (builders[site_id] == null) {
                builders[site_id] = TransactionUtil.createTransactionInitBuilder(ts, fs);
                for (ByteString bs : prefetchParamsSerialized) {
                    builders[site_id].addPrefetchParams(bs);
                } // FOR
            }
            
            depTracker.addPrefetchWorkFragment(ts, fragment, prefetchParams);
            builders[site_id].addPrefetchFragments(fragment);
        } // FOR (WorkFragment)
        if (first == true) {
            if (debug.val)
                LOG.warn(ts + " - No remote partition prefetchable queries were found in the transaction's " +
                		 "initial path estimate. Unable to schedule prefetch queries.");
            return (null);
        }

        PartitionSet touched_partitions = ts.getPredictTouchedPartitions();
        boolean touched_sites[] = new boolean[this.catalogContext.numberOfSites];
        Arrays.fill(touched_sites, false);
        for (int partition : touched_partitions.values()) {
            touched_sites[this.partitionSiteXref[partition]] = true;
        } // FOR

        TransactionInitRequest.Builder default_request = null;
        for (int site_id = 0; site_id < this.catalogContext.numberOfSites; ++site_id) {
            // If this site has no prefetched fragments ...
            // but it has other non-prefetched WorkFragments, create a
            // default TransactionInitRequest.
            if (builders[site_id] == null && touched_sites[site_id]) {
                if (default_request == null) {
                    default_request = TransactionUtil.createTransactionInitBuilder(ts, fs);
                }
                builders[site_id] = default_request;
                if (debug.val)
                    LOG.debug(String.format("%s - Sending default %s to site %s",
                              ts, TransactionInitRequest.class.getSimpleName(),
                              HStoreThreadManager.formatSiteName(site_id)));
            }
        } // FOR (Site)

        if (trace.val)
            LOG.trace(ts + " - TransactionInitRequests\n" + StringUtil.join("\n", builders));
        return (builders);
    }
}
