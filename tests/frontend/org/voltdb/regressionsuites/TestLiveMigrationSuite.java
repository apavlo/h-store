package org.voltdb.regressionsuites;

import java.io.IOException;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.client.ProcCallException;

import edu.brown.catalog.CatalogUtil;

public class TestLiveMigrationSuite extends RegressionSuite{

	public TestLiveMigrationSuite(String name) {
		super(name);
		// TODO Auto-generated constructor stub
	}
	static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = 
                new MultiConfigSuiteBuilder(TestLiveMigrationSuite.class);
        
        VoltServerConfig config = null;
        
        TPCCProjectBuilder project = new TPCCProjectBuilder();
        //project.setBackendTarget(BackendTarget.NATIVE_EE_IPC);
        project.addDefaultSchema();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        
        // CLUSTER CONFIG #1
        // One sites two partitions running in one JVM
        config = new LocalCluster("onesitetwopart.jar", 2, 2, 1,
                                  BackendTarget.NATIVE_EE_JNI);
        config.compile(project);
        builder.addServerConfig(config);
 
        return builder;
    }
	
	@Test
    public void testLiveMigrationMessageAndCallBack() throws IOException, ProcCallException, InterruptedException {
	    Runtime.getRuntime().exec("ant hstore-site -Dproject=onesitetwopart -Dsite.id=1 " +
	    		                    "-Dconf=properties/default.properties -Dsite.newsiteinfo=localhost:1:2-3:8899:9988");
	    Thread.sleep(5000);
	    Catalog cl = getCatalog();
	    Cluster catalog_clus = CatalogUtil.getCluster(cl);
        CatalogMap<Host> hosts = catalog_clus.getHosts();
        CatalogMap<Site> sites = catalog_clus.getSites();
        Site[] sites_value = sites.values();
        Host[] hosts_value = hosts.values();
        
        String lastest_host_info;
        String host_name = hosts_value[hosts_value.length - 1].getIpaddr();
        int site_id = sites_value[sites_value.length - 1].getId();
        
        CatalogMap<Partition> partitions = sites_value[sites_value.length -1].getPartitions();
        Partition[] partitions_value = partitions.values();
        int first_partition = partitions_value[0].getId();
        int last_partition = partitions_value[partitions_value.length - 1].getId();
        
        if(first_partition != last_partition){
            lastest_host_info = host_name + ":" +site_id +":"+first_partition+"-"+(last_partition);
        }else{
            lastest_host_info = host_name + ":" +site_id +":"+first_partition;
        }
        System.out.println(lastest_host_info.toString());
        assert(lastest_host_info.compareTo("localhost:1:2-3") == 0);
    }
}
