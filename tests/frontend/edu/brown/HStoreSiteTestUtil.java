package edu.brown;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import junit.framework.TestCase;

import edu.brown.hstore.HStoreSite;
import edu.brown.pools.TypedObjectPool;
import edu.brown.pools.TypedPoolableObjectFactory;

public abstract class HStoreSiteTestUtil extends TestCase {

    public static class LatchableProcedureCallback implements ProcedureCallback {
        public final List<ClientResponse> responses = new ArrayList<ClientResponse>();
        public final CountDownLatch latch;
        protected boolean debug = false;
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
    }

    /**
     * This checks to make sure that there aren't any active objects in the
     * the various object pools
     */
    public static void checkObjectPools(HStoreSite hstore_site) throws Exception {
        Map<String, TypedObjectPool<?>[]> allPools = hstore_site.getObjectPools().getPartitionedPools(); 
        assertNotNull(allPools);
        assertFalse(allPools.isEmpty());
        for (String name : allPools.keySet()) {
            TypedObjectPool<?> pools[] = allPools.get(name);
            TypedPoolableObjectFactory<?> factory = null;
            assertNotNull(name, pools);
            assertNotSame(0, pools.length);
            for (int i = 0; i < pools.length; i++) {
                if (pools[i] == null) continue;
                String poolName = String.format("%s-%02d", name, i);  
                factory = (TypedPoolableObjectFactory<?>)pools[i].getFactory();
                assertTrue(poolName, factory.isCountingEnabled());
              
                System.err.println(poolName + ": " + pools[i].toString());
                assertEquals(poolName, 0, pools[i].getNumActive());
            } // FOR
        } // FOR
    }

}
