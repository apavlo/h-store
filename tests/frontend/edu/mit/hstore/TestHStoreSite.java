package edu.mit.hstore;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.mit.dtxn.Dtxn;
import edu.mit.dtxn.Dtxn.CoordinatorFragment;
import edu.mit.dtxn.Dtxn.CoordinatorResponse;
import edu.mit.dtxn.Dtxn.FinishRequest;
import edu.mit.dtxn.Dtxn.FinishResponse;
import edu.mit.hstore.HStoreSite;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.ExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Site;
import org.voltdb.messaging.*;

import ca.evanjones.protorpc.StoreResultCallback;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class TestHStoreSite extends BaseTestCase {
    private static final String TARGET_PROCEDURE = "GetNewDestination";
    private static final long CLIENT_HANDLE = 1l;
    
    private MockDtxnCoordinator dtxnCoordinator;
    private HStoreSite coordinator;
    private PartitionEstimator p_estimator;
    private StoredProcedureInvocation invocation;
    private byte[] invocation_bytes;

    private static final Object PARAMS[] = {
        new Long(0), // S_ID
        new Long(1), // SF_TYPE
        new Long(2), // START_TIME
        new Long(3), // END_TIME
    };

    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        dtxnCoordinator = new MockDtxnCoordinator();
        p_estimator = new PartitionEstimator(catalog_db);
        invocation = new StoredProcedureInvocation(CLIENT_HANDLE, TARGET_PROCEDURE, PARAMS);
        invocation_bytes = FastSerializer.serialize(invocation);
        
        Site catalog_site = CollectionUtil.getFirst(CatalogUtil.getCluster(catalog).getSites());
        Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
        coordinator = new HStoreSite(catalog_site, executors, p_estimator);
        coordinator.setDtxnCoordinator(dtxnCoordinator);
    }
    
    @Test
    public void testSinglePartitionPassThrough() {
        // FIXME This won't work because the HStoreCoordinatorNode is now the thing that
        // actually fires off the txn in the ExecutionSite
        
//        StoreResultCallback<byte[]> done = new StoreResultCallback<byte[]>();
//        coordinator.procedureInvocation(invocation_bytes, done);
//
//        // Passed through to the mock coordinator
//        assertTrue(dtxnCoordinator.request.getLastFragment());
//        assertEquals(1, dtxnCoordinator.request.getTransactionId());
//        assertEquals(1, dtxnCoordinator.request.getFragmentCount());
//        assertEquals(0, dtxnCoordinator.request.getFragment(0).getPartitionId());
//        InitiateTaskMessage task = (InitiateTaskMessage) VoltMessage.createMessageFromBuffer(
//                dtxnCoordinator.request.getFragment(0).getWork().asReadOnlyByteBuffer(), true);
//        assertEquals(TARGET_PROCEDURE, task.getStoredProcedureName());
//        assertArrayEquals(PARAMS, task.getParameters());
//        assertEquals(null, done.getResult());
//
//        // Return results
//        Dtxn.CoordinatorResponse.Builder response = CoordinatorResponse.newBuilder();
//        response.setTransactionId(0);
//        response.setStatus(Dtxn.FragmentResponse.Status.OK);
//        byte[] output = { 0x3, 0x2, 0x1 };
//        response.addResponse(CoordinatorResponse.PartitionResponse.newBuilder()
//                .setPartitionId(0).setOutput(ByteString.copyFrom(output)));
//        dtxnCoordinator.done.run(response.build());
//        assertArrayEquals(output, done.getResult());
    }
}
