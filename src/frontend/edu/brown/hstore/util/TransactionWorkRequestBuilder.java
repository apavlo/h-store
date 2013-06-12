package edu.brown.hstore.util;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.ByteString;

import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionSet;

public class TransactionWorkRequestBuilder {

    /**
     * Set of ParameterSet indexes that this TransactionWorkRequest needs
     */
    private final BitSet param_indexes = new BitSet();
    
    /**
     * Set of input DependencyIds needed by this TransactionWorkRequest
     */
    private final Set<Integer> inputs = new HashSet<Integer>();

    private TransactionWorkRequest.Builder builder;

    /**
     * Get the TransactionWorkRequest.Builder
     * If the reset flag is set to true, then a new Builder will be initialized
     * @param ts
     * @return
     */
    public TransactionWorkRequest.Builder getBuilder(LocalTransaction ts, PartitionSet doneNotifications) {
        if (this.builder == null) {
            this.builder = TransactionWorkRequest.newBuilder()
                                        .setTransactionId(ts.getTransactionId().longValue())
                                        .setSourcePartition(ts.getBasePartition())
                                        .setProcedureId(ts.getProcedure().getId());
            if (doneNotifications != null) {
                this.builder.addAllDonePartition(doneNotifications);
            }
            this.param_indexes.clear();
            this.inputs.clear();
        }
        return (this.builder);
    }
    
    /**
     * Build the TransactionWorkRequest and mark the object to be reset
     * the next time that it is used.
     * @return
     */
    public TransactionWorkRequest build() {
        TransactionWorkRequest request = this.builder.build();
        this.builder = null;
        return (request);
    }
    
    public void addParamIndexes(Collection<Integer> param_indexes) {
        for (Integer idx : param_indexes) {
            this.param_indexes.set(idx.intValue());
        } // FOR
    }
    
    public void addInputDependencyId(Integer dep_id) {
        this.inputs.add(dep_id);
    }
    
    public boolean hasInputDependencyId(Integer dep_id) {
        return (this.inputs.contains(dep_id));
    }
    
    public void addParameterSets(List<ByteString> params) {
        for (int i = 0, cnt = params.size(); i < cnt; i++) {
            ByteString bs = (this.param_indexes.get(i) ? params.get(i) : ByteString.EMPTY);
            this.builder.addParams(bs); 
        } // FOR
    }
    
    /**
     * Returns true if there is a TransactionWorkRequest builder that
     * needs to be sent out
     * @return
     */
    public boolean isDirty() {
        return (this.builder != null);
    }
    
}
