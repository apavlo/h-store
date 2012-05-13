package org.voltdb.regressionsuites;

import java.io.IOException;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.client.ProcCallException;

public class TestLiveMigrationSuite extends RegressionSuite{

	public TestLiveMigrationSuite(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}

	@Test
	public void test() {
		System.out.println("here");
		assertEquals(1, 1);
	}
//	static public junit.framework.Test suite() {
//        MultiConfigSuiteBuilder builder = 
//                new MultiConfigSuiteBuilder(TestLiveMigrationSuite.class);
//        
//        VoltServerConfig config = null;
//        
//        TPCCProjectBuilder project = new TPCCProjectBuilder();
//        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
//        project.addDefaultSchema();
//        project.addDefaultProcedures();
//        project.addDefaultPartitioning();
//        
//        // CLUSTER CONFIG #1
//        // One sites two partitions running in one JVM
//        config = new LocalCluster("onesitetwopart.jar", 1, 2, 1,
//                                  BackendTarget.NATIVE_EE_JNI);
//        config.compile(project);
//        builder.addServerConfig(config);
// 
//        return builder;
//    }
	
	@Test
    public void testLiveMigrationMessageAndCallBack() throws IOException, ProcCallException {
        ;
    }
}
