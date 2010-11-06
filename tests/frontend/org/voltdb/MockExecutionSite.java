package org.voltdb;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.messaging.FragmentTaskMessage;

import edu.brown.utils.PartitionEstimator;

/**
 * 
 * @author pavlo
 */
public class MockExecutionSite extends ExecutionSite {
    
    private static final BackendTarget BACKEND_TARGET = BackendTarget.HSQLDB_BACKEND;
    
    private final Map<Long, VoltTable> dependencies = new HashMap<Long, VoltTable>();
    
    public MockExecutionSite(int partition_id, Catalog catalog, PartitionEstimator p_estimator) {
        super(partition_id, catalog, BACKEND_TARGET, p_estimator, null);
    }
    
    @Override
    protected void requestWork(TransactionState ts, List<FragmentTaskMessage> tasks) {
        // Nothing!
    }
    
    @Override
    public void sendClientResponse(ClientResponseImpl cresponse) {
        // Nothing!
    }
    
    @Override
    public VoltTable[] waitForResponses(long txn_id, List<FragmentTaskMessage> tasks) {
        return (new VoltTable[]{ });
    }
    
    @Override
    public void storeDependency(long txnId, int senderPartitionId, int dependencyId, VoltTable data) {
        this.dependencies.put(txnId, data);
    }
    
    public VoltTable getDependency(long txnId) {
        return this.dependencies.get(txnId);
    }
    
    @Override
    public String getThreadName() {
        return "MOCK";
    }
}
