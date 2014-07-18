package edu.brown;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.voltdb.VoltProcedure;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.PartitionLockQueue;
import edu.brown.utils.ThreadUtil;

public abstract class HStoreSiteTestUtil extends TestCase {
    
    public static final int NOTIFY_TIMEOUT = 1000; // ms

    public static class LatchableProcedureCallback implements ProcedureCallback {
        public final List<ClientResponse> responses = new ArrayList<ClientResponse>();
        public final CountDownLatch latch;
        private boolean debug = false;
        public LatchableProcedureCallback(int expected) {
            this.latch = new CountDownLatch(expected);
        }
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            if (this.debug) {
                System.err.printf("Response #%02d:\n%s\n",
                                  this.responses.size(), clientResponse);
            }
            this.responses.add(clientResponse);
            this.latch.countDown();
        }
        public LatchableProcedureCallback setDebug(boolean val) {
            this.debug = val;
            return (this);
        }
        @Override
        public String toString() {
            String ret = super.toString();
            ret += " " + this.latch + "\n";
            ret += this.responses;
            return (ret);
        }
    }
    
    public static class WrapperProcedureCallback extends LatchableProcedureCallback {
        final ProcedureCallback orig;
        
        public WrapperProcedureCallback(int expected, ProcedureCallback orig) {
            super(expected);
            this.orig = orig;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            this.orig.clientCallback(clientResponse);
            super.clientCallback(clientResponse);
        }
    }


//    /**
//     * This checks to make sure that there aren't any active objects in the
//     * the various object pools
//     */
//    public static void checkObjectPools(HStoreSite hstore_site) throws Exception {
//        // Sleep just a little bit to give the HStoreSite time to clean things up
//        ThreadUtil.sleep(1000);
//        
//        Map<String, TypedObjectPool<?>[]> allPools = hstore_site.getObjectPools().getPartitionedPools(); 
//        assertNotNull(allPools);
//        assertFalse(allPools.isEmpty());
//        for (String name : allPools.keySet()) {
//            TypedObjectPool<?> pools[] = allPools.get(name);
//            TypedPoolableObjectFactory<?> factory = null;
//            assertNotNull(name, pools);
//            assertNotSame(0, pools.length);
//            for (int i = 0; i < pools.length; i++) {
//                if (pools[i] == null) continue;
//                String poolName = String.format("%s-%02d", name, i);  
//                factory = (TypedPoolableObjectFactory<?>)pools[i].getFactory();
//                assertTrue(poolName, factory.isCountingEnabled());
//              
//                System.err.println(poolName + ": " + pools[i].toString());
//                assertEquals(poolName, 0, pools[i].getNumActive());
//            } // FOR
//        } // FOR
//    }
    
    @SuppressWarnings("unchecked")
    public static <T extends VoltProcedure> T getCurrentVoltProcedure(PartitionExecutor executor, Class<T> expectedType) {
        int tries = 3;
        VoltProcedure voltProc = null;
        while (tries-- > 0) {
            voltProc = executor.getDebugContext().getCurrentVoltProcedure();
            if (voltProc != null) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertNotNull(String.format("Failed to get %s from %s", expectedType, executor), voltProc);
        assertEquals(expectedType, voltProc.getClass());
        return ((T)voltProc);
    }

    public static void checkQueuedTxns(PartitionExecutor executor, int expected) {
        // Wait until they have all been executed but make sure that nobody actually returned yet
        int tries = 3;
        int blocked = -1;
        PartitionLockQueue queue = executor.getHStoreSite()
                                           .getTransactionQueueManager()
                                           .getLockQueue(executor.getPartitionId()); 
        while (tries-- > 0) {
            blocked = queue.size();
            if (blocked == expected) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertEquals(executor.toString(), expected, blocked);
    }

    public static void checkBlockedSpeculativeTxns(PartitionExecutor executor, int expected) {
        // Wait until they have all been executed but make sure that nobody actually returned yet
        int tries = 3;
        while (tries-- > 0) {
            int blocked = executor.getDebugContext().getBlockedSpecExecCount();
            if (blocked == expected) break;
            ThreadUtil.sleep(NOTIFY_TIMEOUT);    
        } // WHILE
        assertEquals(expected, executor.getDebugContext().getBlockedSpecExecCount());
    }
    

}
