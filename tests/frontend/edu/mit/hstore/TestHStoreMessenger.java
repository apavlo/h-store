package edu.mit.hstore;

import java.io.IOException;
import java.lang.Thread.State;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.ExecutionSite;
import org.voltdb.MockExecutionSite;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.MessageAcknowledgement;
import edu.brown.hstore.Hstore.MessageType;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

/**
 * 
 * @author pavlo
 */
public class TestHStoreMessenger extends BaseTestCase {

    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 4;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
    
    private final HStoreSite sites[] = new HStoreSite[NUM_SITES_PER_HOST];
    private final HStoreMessenger messengers[] = new HStoreMessenger[NUM_SITES_PER_HOST];
    
    private final VoltTable.ColumnInfo columns[] = {
        new VoltTable.ColumnInfo("key", VoltType.STRING),
        new VoltTable.ColumnInfo("value", VoltType.BIGINT),
    };
    private final VoltTable fragment = new VoltTable(columns);
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCluster(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            Site catalog_site = this.getSite(i);
            
            // We have to make our fake ExecutionSites for each Partition at this site
            Map<Integer, ExecutionSite> executors = new HashMap<Integer, ExecutionSite>();
            for (Partition catalog_part : catalog_site.getPartitions()) {
                executors.put(catalog_part.getId(), new MockExecutionSite(catalog_part.getId(), catalog, p_estimator));
            } // FOR
            
            this.sites[i] = new HStoreSite(catalog_site, executors, p_estimator);
            this.messengers[i] = this.sites[i].getMessenger();
        } // FOR

        this.startMessengers();
    }
    
    @Override
    protected void tearDown() throws Exception {
        System.err.println("TEAR DOWN!");
        super.tearDown();
        this.stopMessengers();
        
        // Check to make sure all of the ports are free for each messenger
        for (HStoreMessenger m : this.messengers) {
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
    }
    
    /**
     * To keep track out how many threads fail
     */
    private class AssertThreadGroup extends ThreadGroup {
        private List<Throwable> exceptions = new ArrayList<Throwable>();
        
        public AssertThreadGroup() {
            super("Assert");
        }
        public void uncaughtException(Thread t, Throwable e) {
            e.printStackTrace();
            this.exceptions.add(e);
        }
    }
    
    /**
     * Start all of the HStoreMessengers
     * @throws Exception
     */
    private void startMessengers() throws Exception {
        // We have to fire of threads in parallel because HStoreMessenger.start() blocks!
        List<Thread> threads = new ArrayList<Thread>();
        AssertThreadGroup group = new AssertThreadGroup();
        for (final HStoreMessenger m : this.messengers) {
            threads.add(new Thread(group, "Site#" + m.getLocalSiteId()) {
                @Override
                public void run() {
                    m.start();
                    System.err.println("START: " + m);
                } 
            });
        } // FOR
        ThreadUtil.runNewPool(threads);
        if (group.exceptions.isEmpty() == false) stopMessengers(); 
        assert(group.exceptions.isEmpty()) : group.exceptions;
    }
    
    /**
     * Stop all of the HStoreMessengers
     * @throws Exception
     */
    private void stopMessengers() throws Exception {
        // Tell everyone to prepare to stop
        for (final HStoreMessenger m : this.messengers) {
            if (m.isStarted()) m.prepareShutdown();
        } // FOR
        // Now stop everyone for real!
        for (final HStoreMessenger m : this.messengers) {
            if (m.isStarted() && m.isShuttingDown() == false) {
                System.err.println("STOP: " + m);
                m.shutdown();
            }
        } // FOR
    }
    
    /**
	 * testStartConnection
	 */
	@Test
	public void testStartConnection() throws Exception {
		for (final HStoreMessenger m : this.messengers) {
			// Check that the messenger state is correct
			assert (m.isStarted());

			// Check that the messenger's listener thread is running
			assert (m.getListenerThread().isAlive());

			// Check that we can connect to the messenger's listening port
			int port = m.getLocalMessengerPort();
			SocketChannel channel = SocketChannel.open();
			channel.connect(new InetSocketAddress(port));
			assert (channel.isConnected());
		} // FOR
	}

    /**
     * testStopConnection
     */
    @Test
    public void testStopConnection() throws Exception {
        // TODO: Stop one of the messengers and check
        //          (1) Whether the listener thread has stopped
        //          (2) Whether the socket has been closed and the port freed
        //              You can get the HStoreMessenger port # from the site catalog object
        //              Example: this.sites[i].getSite().getMessenger_port()
        //              
        //      Note that I don't know what happens to the other messengers when you kill one of them
        //      so you test whether they are alive or not. It's not a big deal either way, since 
        //      HStore isn't a production database, but it would be nice to know.
		for (int i = 0; i < NUM_SITES_PER_HOST; i++) {
			HStoreMessenger m = this.messengers[i];
			// stop messenger
			m.shutdown();
			assertEquals(m.getListenerThread().getState(), State.TERMINATED);
			// check that the socket is closed
			int port = this.sites[i].getSite().getMessenger_port();
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
//			break;
		}
    	
    }

    /**
     * testSendFragmentLocal
     */
    @Test
    public void testSendFragmentLocal() throws Exception {
        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
        //       one partition to another that is on the same site using HStoreMessenger.sendFragment().
        //       You can cast the destination ExecutionSite to a MockExecutionSite and use getDependency()
        //       to get back the VoltTable stored by HStoreMessenger. You can check that there is no
        //       HStoreService for target partition and thus it couldn't possibly use the network to send the message.
    	Integer sender_partition_id;
    	Integer dest_partition_id;
    	for (int i = 0; i < NUM_SITES_PER_HOST; i++)
    	{
    		Object[] testarray = new Object[]{"test", 10};
    		fragment.addRow(testarray);
    		Set<Integer> sets = sites[i].getMessenger().getLocalPartitionIds();
    		sender_partition_id = Integer.parseInt(sets.toArray()[0].toString());
    		dest_partition_id = Integer.parseInt(sets.toArray()[1].toString());
    		messengers[i].sendDependency(0, sender_partition_id, dest_partition_id, 0, fragment);
    		MockExecutionSite executor = (MockExecutionSite)sites[i].getExecutionSite(dest_partition_id);
    		VoltTable vt = executor.getDependency(0);
    		// Verify row expected
    		for (int j = 0; j < vt.getRowCount(); j++)
    		{
    			assertEquals(vt.fetchRow(j).get(0, VoltType.STRING), "test");
				assertEquals(vt.fetchRow(j).get(1, VoltType.BIGINT), new Long("10"));
				break;
    		}
//    		break;
    	}
    }

    /**
     * testSendFragmentRemote
     */
    @Test
    public void testSendFragmentRemote() throws Exception {
        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
        //       one partition to another that is on a remote site. This will check whether the
        //       HStoreMessenger.sendFragment() can properly discern that a partition is remote and should use
        //       a network message to send the data. You can stuff some data into the fragment
        //       and make sure that it arrives on the other side with the proper values.
    	Integer sender_partition_id;
    	Integer dest_partition_id;
    	int txn_id = 0;
    	for (int i = 0; i < NUM_SITES_PER_HOST; i++)
    	{
    		Object[] testarray = new Object[]{"test", 10};
    		fragment.addRow(testarray);
    		Set<Integer> sets = sites[i].getMessenger().getLocalPartitionIds();
    		sender_partition_id = CollectionUtil.getFirst(sets);
    		for (int j = i+1; j < NUM_SITES_PER_HOST; j++)
    		{
        		Set<Integer> sets2 = sites[j].getMessenger().getLocalPartitionIds();
        		dest_partition_id = CollectionUtil.getFirst(sets2);
        		messengers[i].sendDependency(txn_id, sender_partition_id, dest_partition_id, 0, fragment);
        		System.err.println("SITE #" + j + ": " + sites[j].getLocalPartitionIds());
        		MockExecutionSite executor = (MockExecutionSite)sites[j].getExecutionSite(dest_partition_id);
        		assertNotNull(executor);
        		
        		VoltTable vt = null; // executor.waitForDependency(txn_id);
        		while (vt == null) {
        			vt = executor.getDependency(txn_id);
        		}
        		assertNotNull(String.format("Site #%d -> Site #%d", i, j), vt);
        		
        		// Verify row expected
        		for (int k = 0; k < vt.getRowCount(); k++)
        		{
        			assertEquals(vt.fetchRow(k).get(0, VoltType.STRING), "test");
    				assertEquals(vt.fetchRow(k).get(1, VoltType.BIGINT), new Long("10"));
    				break;
        		}
        		txn_id++;
//        		break;
    		}
    	}
    	
    }
    
    /**
     * testSendMessage
     */
    @Test
    public void testSendMessage() throws Exception {
        // Send a StatusRequest message to each of our remote sites
        final Map<Integer, String> responses = new HashMap<Integer, String>();
        final Set<Integer> waiting = new HashSet<Integer>();
        
        // We will block on this until we get responses from all of the remote sites
        final CountDownLatch latch = new CountDownLatch(NUM_SITES);
        
        final RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
            @Override
            public void run(MessageAcknowledgement parameter) {
                int sender_site_id = parameter.getSenderSiteId();
                String status = new String(parameter.getData().toByteArray());
                responses.put(sender_site_id, status);
                waiting.remove(sender_site_id);
                latch.countDown();
                
                if (waiting.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("TestConnection Responses:\n");
                    for (Entry<Integer, String> e : responses.entrySet()) {
                        sb.append(String.format("  Partition %03d: %s\n", e.getKey(), e.getValue()));
                    } // FOR
                    System.err.println(sb.toString());
                }
            }
        };
        
        // Send a StatusRequest message to all of our sites. They will respond with some sort of message
        // We have to wrap the sendMessage() calls in threads so that we can fire them off in parallel
        // The sender partition can just be our first partition that we have
        final int sender_id = 0;
        List<Thread> threads = new ArrayList<Thread>();
        AssertThreadGroup group = new AssertThreadGroup();
        for (int i = 0; i < NUM_SITES; i++) {
            final int dest_id = i;
            // Local site (i.e., ourself!)
            if (sender_id == dest_id) {
                responses.put(dest_id, "LOCAL");
                latch.countDown();
            // Remote site
            } else {
                final Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
                                                                .setSenderSiteId(sender_id)
                                                                .setDestSiteId(dest_id)
                                                                .setType(MessageType.STATUS)
                                                                .build();
                threads.add(new Thread(group, (sender_id + "->" + dest_id)) {
                    public void run() {
                        messengers[sender_id].getSiteChannel(dest_id).sendMessage(new ProtoRpcController(), sm, callback);        
                    };
                });
                waiting.add(i);
            }
        } // FOR

        // BOMBS AWAY!
        ThreadUtil.runNewPool(threads);
        
        // BLOCK!
        latch.await();
        assert(group.exceptions.isEmpty()) : group.exceptions;
    }
}