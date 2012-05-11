/**
 * Test Count query---fast aggregate
 */

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.orderbyprocs.InsertO1;

import edu.brown.hstore.Hstoreservice.Status;


/**
 * @author mimosally
 *
 */
public class TestFastAggregateSuite extends RegressionSuite {

	/**
	 * @param name
	 */
	public TestFastAggregateSuite(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	static final Class<?>[] PROCEDURES = { InsertO1.class};
	ArrayList<Integer> a_int = new ArrayList<Integer>();
	ArrayList<String> a_inline_str = new ArrayList<String>();
	ArrayList<String> a_pool_str = new ArrayList<String>();
	public final static String bigString = "ABCDEFGHIJ";
			
	
	 /** add 20 shuffled rows
     * @throws InterruptedException */
    private void load(Client client) throws NoConnectionsException, ProcCallException, IOException, InterruptedException {
        int pkey = 0;
        a_int.clear();
        a_inline_str.clear();
        a_pool_str.clear();

        // if you want to test synchronous latency, this
        //  is a good variable to change
        boolean async = true;

        for (int i=0; i < 20; i++) {
            a_int.add(i);
            a_inline_str.add("a_" + i);
            a_pool_str.add(bigString + i);
        }

        Collections.shuffle(a_int);
        Collections.shuffle(a_inline_str);
        Collections.shuffle(a_pool_str);

        for (int i=0; i < 20; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb,
                    "InsertO1",
                    pkey++,
                    a_int.get(i),
                    a_inline_str.get(i),
                    a_pool_str.get(i));

            if (!async) {
                cb.waitForResponse();
                VoltTable vt = cb.getResponse().getResults()[0];
                assertTrue(vt.getRowCount() == 1);
            }
        }

        client.drain();
    }
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = 
                new MultiConfigSuiteBuilder(TestFastAggregateSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder("fastagg");
        VoltServerConfig config = null;
 
        // Schema + Table Partitions
        project.addSchema(TestFastAggregateSuite.class.getResource("testorderby-ddl.sql"));
        project.addTablePartitionInfo("O1", "PKEY");
        project.addProcedures(PROCEDURES);
        // Single Statement Procedures
        
        project.addStmtProcedure("TestCount", "SELECT COUNT(*) FROM O1");
 
        // CLUSTER CONFIG #1
        // One site with two partitions running in this JVM
        config = new LocalSingleProcessServer("fastagg-twoPart.jar", 2,
                                              BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        // CLUSTER CONFIG #2
        // Two sites, each with two partitions running in separate JVMs
        config = new LocalCluster("fastagg-twoSiteTwoPart.jar", 2, 2, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        return builder;
    }
    
    @Test
    public void testCount() throws IOException, ProcCallException, InterruptedException {
    	Client client = this.getClient();
    	load(client);
    	ClientResponse cr = client.callProcedure("TestCount");
    	assertEquals(Status.OK, cr.getStatus());
    	VoltTable result = cr.getResults()[0];
    	assertTrue(result.advanceRow());
    	assertEquals(20, result.get(0, VoltType.INTEGER));
    	
    }
	
}
