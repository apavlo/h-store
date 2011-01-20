package edu.brown.costmodel;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import weka.core.Instances;

import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.Vertex;
import edu.brown.markov.TransactionEstimator.Estimate;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.markov.Vertex.Type;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.Workload.Filter;

public class MarkovCostModel extends AbstractCostModel {
    private static final Logger LOG = Logger.getLogger(MarkovCostModel.class);
    
    private final Map<Integer, TransactionEstimator> t_estimators = new HashMap<Integer, TransactionEstimator>();

    // If this is set to true, then we will use the txnid_cluster_xref to figure out
    // what cluster each TransactionTrace belongs to
    private boolean txnid_cluster = false;

    
    // Hackish cross-reference table to go from the TransactionId to Cluster#
    private final Map<Long, Integer> txnid_cluster_xref = new HashMap<Long, Integer>();
    
    private transient List<Vertex> last_actual;
    private transient List<Vertex> last_estimated;
    
    /**
     * Constructor
     * @param catalog_db
     * @param p_estimator
     */
    public MarkovCostModel(Database catalog_db, PartitionEstimator p_estimator) {
        super(MarkovCostModel.class, catalog_db, p_estimator);
    }

    public void setTransactionClusterMapping(boolean flag) {
        this.txnid_cluster = flag;
    }
    
    /**
     * Add a TransactionEstimator that is mapped to the given cluster_id
     * @param cluster_id
     * @param t_estimator
     */
    public synchronized void addTransactionEstimator(int cluster_id, TransactionEstimator t_estimator) {
        this.t_estimators.put(cluster_id, t_estimator);
    }
    
    public TransactionEstimator getTransactionEstimator(int cluster_id) {
        return (this.t_estimators.get(cluster_id));
    }
    
    /**
     * Map a TransactionId to a ClusterId
     * @param txn_id
     * @param cluster_id
     */
    public void addTransactionClusterXref(long txn_id, int cluster_id) {
        this.txnid_cluster_xref.put(txn_id, cluster_id);
    }
    

    @Override
    public AbstractCostModel clone(Database catalogDb) throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }
    
    public List<Vertex> getLastActualPath() {
        return (this.last_actual);
    }
    
    public List<Vertex> getLastEstimatedPath() {
        return (this.last_estimated);
    }

    @Override
    public double estimateTransactionCost(Database catalog_db, Workload workload, Filter filter, TransactionTrace txn_trace) throws Exception {
        long txn_id = txn_trace.getTransactionId();
        Procedure catalog_proc = txn_trace.getCatalogItem(catalog_db);
        
        // This will allow us to select the right TransactionEstimator
        Integer cluster = null;
        if (this.txnid_cluster) {
            // Look-up what cluster our TransactionTrace belongs to
            cluster = this.txnid_cluster_xref.get(txn_id);
        } else {
            // Otherwise just use the base partition
            cluster = this.p_estimator.getBasePartition(txn_trace);
        }
        assert(cluster != null) : "Unexpected Txn# " + txn_id;

        TransactionEstimator t_estimator = this.t_estimators.get(cluster);
        if (t_estimator == null) {
            LOG.warn("Unexpected Cluster# " + cluster);
            return (1.0);
        }
        assert(t_estimator != null) : "Unexpected Cluster# " + cluster;
        
        // Throw the txn at the estimator and let it come up with the initial path estimation.
        Estimate est = t_estimator.startTransaction(txn_id, catalog_proc, txn_trace.getParams());
        assert(est != null);
        
        // Now execute the queries and see what path the txn actually takes
        // I don't think it matters whether we do this in batches, but it probably doesn't hurt
        // to do it right in case we need it later
        // At this point we know what the transaction actually would do using the TransactionEstimator's
        // internal Markov models. So let's compare the initial path estimate with the actual and
        // come up with some sort of cost for how off we were
        State s = t_estimator.processTransactionTrace(txn_trace);
        assert(s != null);
        
        this.last_estimated = s.getEstimatedPath();
        this.last_actual = s.getActualPath();
        
        Vertex actual_last = this.last_actual.get(this.last_actual.size() - 1);
        if (txn_trace.isAborted()) {
            assert(actual_last.isAbortVertex());
        } else {
            assert(actual_last.isCommitVertex());
        }
        
        return this.comparePaths(this.last_estimated, this.last_actual);
    }
       
    /**
     * Calculate relative cost difference the estimated and actual execution paths 
     * @param estimated
     * @param actual
     * @return
     */
    protected double comparePaths(List<Vertex> estimated, List<Vertex> actual) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        double cost = 0.0d;
        
        if (trace) LOG.trace(String.format("Estimating Cost Different: Estimated [size=%d] vs. Actual Cost [size=%d]", estimated.size(), actual.size()));
        
        // There's two things we care about here:
        //  (1) That the partitions that we predicted that the txn would read/write are the same
        //  (2) That the predicted execution path was complete
        
        
        Pair<Set<Integer>, Set<Integer>> e_partitions = MarkovUtil.getReadWritePartitions(estimated);
        Pair<Set<Integer>, Set<Integer>> a_partitions = MarkovUtil.getReadWritePartitions(actual);
        
        if (e_partitions.getFirst().equals(a_partitions.getFirst()) == false ||
            e_partitions.getSecond().equals(a_partitions.getSecond()) == false) {
            cost += 1.0d;
        }
        
        
        return (cost);
        
//        if (trace) {
//            LOG.trace("Estimated: " + estimated);
//            LOG.trace("Actual:    " + actual);
//        }
        
//        Set<Integer> e_all_partitions = new HashSet<Integer>();
//        Set<Integer> a_all_partitions = new HashSet<Integer>();
//        
//        int e_cnt = estimated.size();
//        int a_cnt = actual.size();
//        for (int e_i = 1, a_i = 1; a_i < a_cnt-1; a_i++) {
//            Vertex a = actual.get(a_i);
//            Vertex e = null;
//            try {
//                e = estimated.get(e_i);
//            } catch (IndexOutOfBoundsException ex) {
//                // IGNORE
//            }
//        
//            if (trace) LOG.trace(String.format("Estimated[%02d]%s <==> Actual[%02d]%s", e_i, e, a_i, a));
//            
//            if (e != null) {
//                Statement e_stmt = e.getCatalogItem();
//                Set<Integer> e_partitions = e.getPartitions();
//                e_all_partitions.addAll(e_partitions);
//                
//                Statement a_stmt = a.getCatalogItem();
//                Set<Integer> a_partitions = a.getPartitions();
//                a_all_partitions.addAll(a_partitions);
//                
//                // Check whether they're executing the same queries
//                if (a_stmt.equals(e_stmt) == false) {
//                    if (trace) LOG.trace("STMT MISMATCH: " + e_stmt + " != " + a_stmt);
//                    cost += 1;
//                // Great, we're getting there. Check whether these queries are
//                // going to touch the same partitions
//                } else if ((a_partitions.size() != e_partitions.size()) || 
//                           (a_partitions.equals(e_partitions) == false)) {
//                    if (trace) LOG.trace("PARTITION MISMATCH: " + e_partitions + " != " + a_partitions);
//                    
//                    // Do we want to penalize them for every partition they get wrong?
//                    for (Integer p : a_partitions) {
//                        if (e_partitions.contains(p) == false) {
//                            cost += 1.0d;
//                        }
//                    }
//                    
//                // Ok they have the same query and they're going to the same partitions,
//                // So now check whether that this is the same number of times that they've
//                // executed this query before 
//                } else if (a.getQueryInstanceIndex() != e.getQueryInstanceIndex()) {
//                    int a_idx = a.getQueryInstanceIndex();
//                    int e_idx = e.getQueryInstanceIndex();
//                    if (trace) LOG.trace("QUERY INDEX MISMATCH: " + e_idx + " != " + a_idx);
//                    // Do we actually to penalize them for this??
//                }
//                
//                e_i++;
//                if (trace) LOG.trace("");
//                
//            // If our estimated path is too short, then yeah that's going to cost ya'!
//            } else {
//                cost += 1.0d;
//            }
//        } // FOR
//        
//        // Penalize them for invalid partitions
//        // One point for every missing one and one point for every one too many
//        int p_missing = 0;
//        int p_incorrect = 0;
//        for (Integer p : a_all_partitions) {
//            if (e_all_partitions.contains(p) == false) p_missing++;
//        }
//        for (Integer p : e_all_partitions) {
//            if (a_all_partitions.contains(p) == false) p_incorrect++;
//        }
//        cost += p_missing + p_incorrect;
//        
//        // Look at the last vertices to check the COMMIT/ABORT status
//        Vertex e_last = estimated.get(e_cnt-1);
//        Type e_last_type = e_last.getType(); 
//        assert(e_last_type != Type.START);
//        Vertex a_last = actual.get(a_cnt-1);
//        Type a_last_type = a_last.getType();
//        assert(a_last_type != Type.START);
//        
//        // Basically we want to bang them hard if the estimation is wrong
//        if (e_last_type != Type.QUERY && e_last_type != a_last_type) {
//            assert(e_last_type != Type.START);
//            if (trace) LOG.debug("COMMIT/ABORT MISMATCH: " + e_last_type + " != " + a_last_type);
//        }
//        
//        return (cost);
    }
    

    
    @Override
    public void invalidateCache(String catalogKey) {
        // Nothing...
    }

    @Override
    public void prepareImpl(Database catalog_db) {
        // This is the start of a new run through the workload, so we need to re-init
        // our PartitionEstimator so that we are getting the proper catalog objects back
        this.p_estimator.initCatalog(catalog_db);
        this.txnid_cluster_xref.clear();
        this.t_estimators.clear();
    }

}