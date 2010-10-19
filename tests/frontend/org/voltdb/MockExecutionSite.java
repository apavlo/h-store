package org.voltdb;

import java.util.List;

import org.voltdb.catalog.Catalog;
import org.voltdb.messaging.FragmentTaskMessage;

import edu.brown.utils.PartitionEstimator;

/**
 * 
 * @author pavlo
 */
public class MockExecutionSite extends ExecutionSite {
    
    private static final BackendTarget BACKEND_TARGET = BackendTarget.HSQLDB_BACKEND;
    
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
    public String getThreadName() {
        return "MOCK";
    }
}
