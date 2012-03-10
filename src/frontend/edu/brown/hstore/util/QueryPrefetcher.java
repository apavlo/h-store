package edu.brown.hstore.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
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
import edu.brown.utils.PartitionEstimator;

/**
 * 
 * @author pavlo
 * @author cjl6
 */
public class QueryPrefetcher implements Loggable {

//    private final Database catalog_db;
    private final Map<Procedure, BatchPlanner> planners = new HashMap<Procedure, BatchPlanner>();
    private final int[] partition_site_xref;
    private final int num_sites;
    
    public QueryPrefetcher(Database catalog_db, PartitionEstimator p_estimator) {
//        this.catalog_db = catalog_db;
        this.num_sites = CatalogUtil.getNumberOfSites(catalog_db);

        // Initialize a BatchPlanner for each Procedure if it has the
        // prefetch flag set to true. We generate an array of the SQLStmt handles
        // that we will want to prefetch for each Procedure
        for (Procedure proc : catalog_db.getProcedures()) {
            if (proc.getPrefetch()) {
                List<SQLStmt> prefetachable = new ArrayList<SQLStmt>();
                for (Statement catalog_stmt : proc.getStatements()) {
                    if (catalog_stmt.getPrefetch()) {
                        prefetachable.add(new SQLStmt(catalog_stmt));
                    }
                } // FOR
                if (prefetachable.isEmpty() == false) {
                    BatchPlanner planner = new BatchPlanner(prefetachable.toArray(new SQLStmt[0]),
                                                            prefetachable.size(),
                                                            proc,
                                                            p_estimator);
                    this.planners.put(proc, planner);
                }
            }
        } // FOR (procedure)

        this.partition_site_xref = new int[CatalogUtil.getNumberOfPartitions(catalog_db)];
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_db)) {
            this.partition_site_xref[catalog_part.getId()] = ((Site) catalog_part.getParent()).getId();
        } // FOR
    }

    /**
     * 
     * @param ts
     * @return
     */
    public TransactionInitRequest[] generateWorkFragments(LocalTransaction ts) {
        Procedure catalog_proc = ts.getProcedure();
        assert (ts.getProcedureParameters() != null) : "Unexpected null ParameterSet for " + ts;
        Object proc_params[] = ts.getProcedureParameters().toArray();

        // TODO: Use the StmtParameter mappings for the queries we
        // want to prefetch and extract the ProcParameters
        // to populate an array of ParameterSets to use as the batchArgs
        BatchPlanner planner = this.planners.get(catalog_proc);
        ParameterSet prefetch_params[] = new ParameterSet[planner.getBatchSize()];
        ByteString prefetch_params_serialized[] = new ByteString[prefetch_params.length];

        // Makes a list of ByteStrings containing the ParameterSets that we need
        // to send over to the remote sites so that they can execute our pre-fetchable queries 
        FastSerializer fs = new FastSerializer(); // TODO: Use pooled memory
        for (int i = 0; i < prefetch_params.length; i++) {
            Statement catalog_stmt = planner.getStatements()[i];
            Object stmt_params[] = new Object[catalog_stmt.getParameters().size()];
            
            // Generates a new object array using a mapping from the ProcParameter to 
            // the StmtParameter. This relies on a ParameterMapping already being installed
            // in the catalog
            for (StmtParameter catalog_param : catalog_stmt.getParameters()) {
                ProcParameter catalog_proc_param = catalog_param.getProcparameter();
                assert(catalog_proc_param != null) :
                    "Missing mapping from " + catalog_param.fullName() + " to ProcParameter";
                stmt_params[catalog_param.getIndex()] = proc_params[catalog_proc_param.getIndex()];
            } // FOR
            prefetch_params[i] = new ParameterSet(stmt_params);

            // Serialize this ParameterSet for the TransactionInitRequests
            try {
                if (i > 0) fs.clear();
                prefetch_params[i].writeExternal(fs);
                prefetch_params_serialized[i] = ByteString.copyFrom(fs.getBBContainer().b);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
            }
        } // FOR (Prefetchable Statement)

        // Generate the WorkFragments that we will need to send in our TransactionInitRequest
        BatchPlan plan = planner.plan(ts.getTransactionId(),
                                      ts.getClientHandle(),
                                      ts.getBasePartition(),
                                      ts.getPredictTouchedPartitions(),
                                      ts.isPredictSinglePartition(),
                                      ts.getTouchedPartitions(),
                                      prefetch_params);
        List<WorkFragment> fragments = new ArrayList<WorkFragment>();
        plan.getWorkFragments(ts.getTransactionId(), fragments);

        // TODO: Loop through the fragments and check whether at least one of them
        // needs to be executed at the base (local) partition. If so, we need a separate
        // TransactionInitRequest per site. Group the WorkFragments by siteID.
        // If we have a prefetchable query for the base partition, it means that we will try 
        // to execute it before we actually need it whenever the PartitionExecutor is idle
        // That means, we don't want to serialize all this if it's only going to the base partition.
        TransactionInitRequest.Builder[] builders = new TransactionInitRequest.Builder[this.num_sites];
        for (WorkFragment frag : fragments) {
            int site_id = this.partition_site_xref[frag.getPartitionId()];
            TransactionInitRequest.Builder builder = builders[site_id];
            if (builder == null) {
                builders[site_id] = TransactionInitRequest.newBuilder()
                                          .setTransactionId(ts.getTransactionId().longValue())
                                          .setProcedureId(ts.getProcedure().getId())
                                          .addAllPartitions(ts.getPredictTouchedPartitions());
                builder = builders[site_id];
                for (ByteString bs : prefetch_params_serialized) {
                    builder.addPrefetchParameterSets(bs);
                } // FOR
            }
            builder.addPrefetchFragments(frag);
        } // FOR (WorkFragment)

        TransactionInitRequest[] init_requests = new TransactionInitRequest[this.num_sites];
        for (int i = 0; i < num_sites; ++i) {
            init_requests[i] = builders[i].build();
        }

        return init_requests;
    }

    @Override
    public void updateLogging() {
        // TODO Auto-generated method stub

    }

}
