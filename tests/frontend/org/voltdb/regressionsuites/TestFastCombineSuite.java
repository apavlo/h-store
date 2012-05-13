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

public class TestFastCombineSuite extends RegressionSuite {

	public TestFastCombineSuite(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	static final Class<?>[] PROCEDURES = { InsertO1.class };
	ArrayList<Integer> a_int = new ArrayList<Integer>();
	ArrayList<String> a_inline_str = new ArrayList<String>();
	ArrayList<String> a_pool_str = new ArrayList<String>();
	public final static String simpleString = "ABCDEFGHIJ";

	/**
	 * add 20 shuffled rows
	 * 
	 * @throws InterruptedException
	 */
	private void load(Client client) throws NoConnectionsException,
			ProcCallException, IOException, InterruptedException {
		int pkey = 0;
		a_int.clear();
		a_inline_str.clear();
		a_pool_str.clear();

		boolean async = true;

		for (int i = 0; i < 20; i++) {
			a_int.add(i);
			a_inline_str.add("a_" + i);
			a_pool_str.add(simpleString + i);
		}

		Collections.shuffle(a_int);
		Collections.shuffle(a_inline_str);
		Collections.shuffle(a_pool_str);

		for (int i = 0; i < 20; i++) {
			SyncCallback cb = new SyncCallback();
			client.callProcedure(cb, "InsertO1", pkey, a_int.get(i),
					a_inline_str.get(i), a_pool_str.get(i));

			if (!async) {
				cb.waitForResponse();
				VoltTable vt = cb.getResponse().getResults()[0];
				assertTrue(vt.getRowCount() == 1);
			}
			pkey = pkey + 1;
		}

		client.drain();
	}

	// duplicate values in A_INT
	private void loadWithDupes(Client client) throws NoConnectionsException,
			ProcCallException, IOException {
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(1),
				new Long(1), "Jane", "AAAAAA");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(2),
				new Long(1), "Lily", "BBBBBB");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(3),
				new Long(1), "Tommy", "CCCCCC");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(4),
				new Long(2), "Alice", "DDDDDD");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(5),
				new Long(2), "Mike", "EEEEEE");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(6),
				new Long(2), "Joe", "FFFFFF");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(7),
				new Long(2), "Jane", "GGGGGGG");
		client.callProcedure(new SyncCallback(), "InsertO1", new Long(8),
				new Long(2), "Lily", "HHHHHH");
		client.drain();
	}

	static public junit.framework.Test suite() {
		MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
				TestFastCombineSuite.class);
		VoltProjectBuilder project = new VoltProjectBuilder("fastcomb");
		VoltServerConfig config = null;

		// Schema + Table Partitions
		project.addSchema(TestFastAggregateSuite.class
				.getResource("testorderby-ddl.sql"));
		project.addTablePartitionInfo("O1", "PKEY");
		project.addProcedures(PROCEDURES);
		// Single Statement Procedures

		project.addStmtProcedure("TestSelect",
				"SELECT PKEY FROM O1 WHERE A_INT=?");

		// CLUSTER CONFIG #1
		// One site with two partitions running in this JVM
		config = new LocalSingleProcessServer("fastcomb-twoPart.jar", 2,
				BackendTarget.NATIVE_EE_JNI);
		config.compile(project);
		builder.addServerConfig(config);

		/**
		 * // CLUSTER CONFIG #2 // Two sites, each with two partitions running
		 * in separate JVMs config = new
		 * LocalCluster("fastcomb-twoSiteTwoPart.jar", 2, 2, 1,
		 * BackendTarget.NATIVE_EE_JNI); config.compile(project);
		 * builder.addServerConfig(config);
		 */

		return builder;
	}

	@Test
	public void testCount() throws IOException, ProcCallException,
			InterruptedException {
		int a = 3;
		Client client = this.getClient();
		load(client);
		ClientResponse cr = client.callProcedure("TestSelect", a);
		assertEquals(Status.OK, cr.getStatus());
		VoltTable result = cr.getResults()[0];
		// make sure select just one tuple
		assertEquals(1, result.getRowCount());
		// make sure the row contents
		System.out.println("Start to compare the results...");
		// result.advanceRow();
		// check content of result
		// assertEquals(a,result.get(0, VoltType.INTEGER));

		System.out.println("Finish the comparing testCount() successfully");

	}

	@Test
	public void testCountwithduplicates() throws IOException,
			ProcCallException, InterruptedException {
		Long a = new Long(2);
		Client client = this.getClient();
		loadWithDupes(client);
		ClientResponse cr = client.callProcedure("TestSelect", a);
		assertEquals(Status.OK, cr.getStatus());
		VoltTable result = cr.getResults()[0];
		// make sure select all 5 tuples which match the query
		assertEquals(5, result.getRowCount());
		// make sure the row contents
		System.out
				.println("Start to compare testCountwithduplicates() the results...");
		// result.advanceRow();
		// check content of result
		// assertEquals(a,result.get(0, VoltType.INTEGER));

		System.out
				.println("Finish the comparing testCountwithduplicates() successfully");

	}

}
