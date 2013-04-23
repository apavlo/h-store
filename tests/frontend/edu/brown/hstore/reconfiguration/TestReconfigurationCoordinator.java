/**
 * 
 */
package edu.brown.hstore.reconfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

import org.junit.Before;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.MockPartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.utils.ProjectType;

/**
 * @author aelmore
 *
 */
public class TestReconfigurationCoordinator extends BaseTestCase {
    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 4;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
   
    private final HStoreSite hstore_sites[] = new HStoreSite[NUM_SITES_PER_HOST];
    private final HStoreCoordinator coordinators[] = new HStoreCoordinator[NUM_SITES_PER_HOST];
    
    public void testDummy() {
      assertTrue(true);
    }
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        HStoreConf.singleton().site.coordinator_sync_time = false;
        
        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCatalog(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            this.hstore_sites[i] = new MockHStoreSite(i, catalogContext, HStoreConf.singleton());
            //TODO this.coordinators[i] = this.hstore_sites[i].initHStoreCoordinator();
            
            // We have to make our fake ExecutionSites for each Partition at this site
            for (Partition catalog_part : catalog_site.getPartitions()) {
                MockPartitionExecutor es = new MockPartitionExecutor(catalog_part.getId(), catalog, p_estimator);
                this.hstore_sites[i].addPartitionExecutor(catalog_part.getId(), es);
                es.initHStoreSite(this.hstore_sites[i]);
            } // FOR
        } // FOR

        //TODO this.startMessengers();
        System.err.println("All HStoreCoordinators started!");
    }
    
    @Override
    protected void tearDown() throws Exception {
        System.err.println("TEAR DOWN!");        
        super.tearDown();
        /* TODO
        this.stopMessengers();
        
        // Check to make sure all of the ports are free for each messenger
        for (HStoreCoordinator m : this.coordinators) {
            // assert(m.isStopped()) : "Site #" + m.getLocalSiteId() + " wasn't stopped";
            int port = m.getLocalMessengerPort();
            ServerSocketChannel channel = ServerSocketChannel.open();
            try {
                channel.socket().bind(new InetSocketAddress(port));
            } catch (IOException ex) {
                ex.printStackTrace();
                assert(false) : "Messenger port #" + port + " for Site #" + m.getLocalSiteId() + " isn't open: " + ex.getLocalizedMessage();
            } finally {
                channel.close();
                Thread.yield();
            }
        } // FOR
        */
    }

}
