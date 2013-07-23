package edu.brown.hstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltTable;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.utils.PartitionEstimator;
import edu.brown.hstore.txns.LocalTransaction;

/**
 * 
 * @author pavlo
 */
public class MockPartitionExecutor extends PartitionExecutor {
    
    private static final BackendTarget BACKEND_TARGET = BackendTarget.HSQLDB_BACKEND;
    
    private final Map<Long, VoltTable> dependencies = new HashMap<Long, VoltTable>();
    private final Map<Long, CountDownLatch> latches = new HashMap<Long, CountDownLatch>();
    
    public MockPartitionExecutor(int partition_id, CatalogContext catalogContext, PartitionEstimator p_estimator) {
        super(partition_id, catalogContext, BACKEND_TARGET, p_estimator, null);
        this.initializeVoltProcedures();
    }

    @Override
    public void processClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        // Nothing!
    }
    
    @Override
    public VoltTable[] dispatchWorkFragments(LocalTransaction ts, int batchSize, ParameterSet[] parameters, Collection<WorkFragment.Builder> fragments, boolean finalTask) {
        return (new VoltTable[]{ });
    }
    
    public synchronized void storeDependency(long txnId, int senderPartitionId, int dependencyId, VoltTable data) {
        System.err.println("STORING TXN #" + txnId);
        this.dependencies.put(txnId, data);
        CountDownLatch latch = this.latches.get(txnId);
        if (latch != null) {
            System.err.println("UNBLOCKING TXN #" + txnId);
            latch.countDown();
        }
    }
    
    public synchronized VoltTable getDependency(long txnId) {
        return this.dependencies.get(txnId);
    }
    
    public synchronized VoltTable waitForDependency(long txnId) {
        VoltTable vt = this.dependencies.get(txnId);
        if (vt == null) {
            CountDownLatch latch = this.latches.get(txnId);
            if (latch == null) {
                latch = new CountDownLatch(1);
                this.latches.put(txnId, latch);
            }
            try {
                System.err.println("WAITING FOR TXN #" + txnId);
                latch.await(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
            vt = this.dependencies.get(txnId);
        }
        return (vt);
    }
}
