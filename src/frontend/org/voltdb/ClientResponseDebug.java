package org.voltdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.voltdb.catalog.Statement;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.Pair;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * Special wrapper object that contains extra information about a transaction
 */
public class ClientResponseDebug implements FastSerializable {
    
    /**
     * Inner class that converts CountedStatements into ids+counter, since
     * we won't have the catalog when we deserialize it on the client side
     */
    private class QueryEstimate implements FastSerializable {
        /**
         * Pair -> StatementId : StatementCounter
         */
        private final List<Pair<Integer, Integer>> statements = new ArrayList<Pair<Integer, Integer>>();
        private int partition;
        
        public QueryEstimate() {
            // Nothing to do
        }
        
        public QueryEstimate(int partition, Collection<CountedStatement> statements) {
            this.partition = partition;
            for (CountedStatement cntStmt : statements) {
                this.statements.add(Pair.of(cntStmt.statement.getId(), cntStmt.counter));    
            } // FOR
        }
        
        @Override
        public void readExternal(FastDeserializer in) throws IOException {
            this.partition = in.readInt();
            int num_statements = in.readShort();
            this.statements.clear();
            for (int i = 0; i < num_statements; i++) {
                int stmtId = in.readInt();
                int stmtCtr = in.readShort();
                this.statements.add(Pair.of(stmtId, stmtCtr));
            } // FOR
            assert(num_statements == this.statements.size());
        }
        @Override
        public void writeExternal(FastSerializer out) throws IOException {
            out.writeInt(this.partition);
            out.writeShort(this.statements.size());
            for (Pair<Integer, Integer> stmt : this.statements) {
                out.writeInt(stmt.getFirst());
                out.writeShort(stmt.getSecond());
            } // FOR
        }
        @Override
        public String toString() {
            return String.format("{Partition #%02d - %s}", this.partition, this.statements);
        }
    } // CLASS
    
    private boolean predict_singlePartition;
    private boolean predict_abortable;
    private boolean predict_readOnly;
    private final PartitionSet predict_touchedPartitions = new PartitionSet();
    
    private String estimateType;
    private int estimateUpdateCount = 0;

    /**
     * Partition -> List<QueryEstimate>
     */
    private final Map<Integer, List<QueryEstimate>> estimateRemoteQueryUpdates = new TreeMap<Integer, List<QueryEstimate>>();

    
    /**
     * Partitions that were marked for early 2PC
     */
    private final PartitionSet early_preparePartitions = new PartitionSet();
    
    private boolean prefetched = false;
    private final PartitionSet exec_touchedPartitions = new PartitionSet();
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public ClientResponseDebug() {
        // Needed for serialization
    }
    
    public ClientResponseDebug(LocalTransaction ts) {
        this.predict_singlePartition = ts.isPredictSinglePartition();
        this.predict_abortable = ts.isPredictAbortable();
        this.predict_readOnly = ts.isPredictReadOnly();
        this.predict_touchedPartitions.addAll(ts.getPredictTouchedPartitions());
        this.prefetched = ts.hasPrefetchQueries();
        this.exec_touchedPartitions.addAll(ts.getTouchedPartitions().values());
        
        if (this.predict_singlePartition == false) {
            this.early_preparePartitions.addAll(ts.getDonePartitions());
        }
        
        EstimatorState t_state = ts.getEstimatorState();
        if (t_state != null) {
            this.estimateType = t_state.getClass().getSimpleName();
            for (Estimate est : t_state.getEstimates()) {
                this.estimateUpdateCount++;
                for (int partition : this.exec_touchedPartitions) {
                    if (est.hasQueryEstimate(partition)) {
                        Collection<CountedStatement> stmts = est.getQueryEstimate(partition);
                        this.addQueryEstimate(new QueryEstimate(partition, stmts));
                    }
                } // FOR (partition)
            } // FOR (estimate)
        } 
    }
    
    protected void addQueryEstimate(QueryEstimate query_estimate) {
        List<QueryEstimate> estimates = this.estimateRemoteQueryUpdates.get(query_estimate.partition);
        if (estimates == null) {
            estimates = new ArrayList<QueryEstimate>();
            this.estimateRemoteQueryUpdates.put(query_estimate.partition, estimates);
        }
        estimates.add(query_estimate);
    }
    
    // ----------------------------------------------------------------------------
    // DATA METHODS
    // ----------------------------------------------------------------------------

    public boolean isPredictSinglePartition() {
        return this.predict_singlePartition;
    }

    public boolean isPredictAbortable() {
        return this.predict_abortable;
    }

    public boolean isPredictReadOnly() {
        return this.predict_readOnly;
    }

    public PartitionSet getPredictTouchedPartitions() {
        return this.predict_touchedPartitions;
    }

    public PartitionSet getExecTouchedPartitions() {
        return this.exec_touchedPartitions;
    }
    
    /**
     * Get the set of partitions that were marked for early 2PC prepare
     * @return
     */
    public PartitionSet getEarlyPreparePartitions() {
        return this.early_preparePartitions;
    }

    /**
     * Returns true if this transaction was executed with prefetched queries.
     * @return
     */
    public boolean hadPrefetchedQueries() {
        return this.prefetched;
    }
    
    public String getEstimatorType() {
        return (this.estimateType);
    }
    public int getEstimatorUpdateCount() {
        return (this.estimateUpdateCount);
    }
    
    public List<CountedStatement>[] getRemoteQueryEstimates(CatalogContext catalogContext, int partition) {
        List<QueryEstimate> estimates = this.estimateRemoteQueryUpdates.get(partition);
        int num_estimates = (estimates != null ? estimates.size() : 0);
        @SuppressWarnings("unchecked")
        List<CountedStatement> result[] = (List<CountedStatement>[])new List<?>[num_estimates];
        for (int i = 0; i < result.length; i++) {
            result[i] = new ArrayList<CountedStatement>();
            QueryEstimate query_est = estimates.get(i);
            for (Pair<Integer, Integer> p : query_est.statements) {
                Statement stmt =  catalogContext.getStatementById(p.getFirst());
                assert(stmt != null) : "Invalid Statement id '" + p.getFirst() + "'";
                result[i].add(new CountedStatement(stmt, p.getSecond()));
            } // FOR
        } // FOR
        return (result);
    }
    
    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------
    
    
    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        this.predict_singlePartition = in.readBoolean();
        this.predict_abortable = in.readBoolean();
        this.predict_readOnly = in.readBoolean();
        this.predict_touchedPartitions.readExternal(in);
        this.early_preparePartitions.readExternal(in);
        this.prefetched = in.readBoolean();
        this.exec_touchedPartitions.readExternal(in);
        
        // TXN ESTIMATE INFO
        this.estimateType = in.readString();
        this.estimateUpdateCount = in.readInt();
        
        // QUERY ESTIMATES
        int num_partitions = in.readShort();
        for (int i = 0; i < num_partitions; i++) {
            int partition = in.readInt();
            int num_estimates = in.readShort();
            for (int j = 0; j < num_estimates; j++) {
                QueryEstimate query_est = new QueryEstimate();
                query_est.readExternal(in);
                assert(partition == query_est.partition);
                this.addQueryEstimate(query_est);    
            } // FOR
        } // FOR
    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        out.writeBoolean(this.predict_singlePartition);
        out.writeBoolean(this.predict_abortable);
        out.writeBoolean(this.predict_readOnly);
        this.predict_touchedPartitions.writeExternal(out);
        this.early_preparePartitions.writeExternal(out);
        out.writeBoolean(this.prefetched);
        this.exec_touchedPartitions.writeExternal(out);
        
        // TXN ESTIMATE INFO
        out.writeString(this.estimateType);
        out.writeInt(this.estimateUpdateCount);
        
        // QUERY ESTIMATES
        out.writeShort(this.estimateRemoteQueryUpdates.size());
        for (Entry<Integer, List<QueryEstimate>> e : this.estimateRemoteQueryUpdates.entrySet()) {
            out.writeInt(e.getKey());
            out.writeShort(e.getValue().size());
            for (QueryEstimate query_est : e.getValue()) {
                query_est.writeExternal(out);    
            } // FOR
        } // FOR
    }
    
    @Override
    public String toString() {
        List<Map<String, Object>> maps = new ArrayList<Map<String,Object>>();
        Map<String, Object> m;
        
        m = new LinkedHashMap<String, Object>();
        m.put("Predict Single-Partitioned", this.predict_singlePartition);
        m.put("Predict Touched Partitions", this.predict_touchedPartitions);
        m.put("Predict Read Only", this.predict_readOnly);
        m.put("Predict Abortable", this.predict_abortable);
        m.put("Had Prefetched Queries", this.prefetched);
        maps.add(m);
        
        m = new LinkedHashMap<String, Object>();
        m.put("Estimator Type", this.estimateType);
        m.put("Estimate Updates", this.estimateUpdateCount);
        m.put("Remote Query Estimates", this.estimateRemoteQueryUpdates);
        maps.add(m);
        
        m = new LinkedHashMap<String, Object>();
        m.put("Exec Touched Partitions", this.exec_touchedPartitions);
        m.put("Early 2PC Partitions", this.early_preparePartitions);
        maps.add(m);
        
        return StringUtil.formatMaps(maps.toArray(new Map<?, ?>[maps.size()]));
    }

}
