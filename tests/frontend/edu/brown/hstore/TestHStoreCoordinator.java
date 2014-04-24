package edu.brown.hstore;

import java.io.IOException;
import java.lang.Thread.State;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Test;
import org.voltdb.exceptions.SerializableException;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.BaseTestCase;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareRequest;
import edu.brown.hstore.Hstoreservice.UnevictDataRequest;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.protorpc.ProtoRpcController;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

/**
 * HStoreCoordinator Tests
 * @author pavlo
 */
public class TestHStoreCoordinator extends BaseTestCase {

    private final int NUM_HOSTS               = 1;
    private final int NUM_SITES_PER_HOST      = 4;
    private final int NUM_PARTITIONS_PER_SITE = 2;
    private final int NUM_SITES               = (NUM_HOSTS * NUM_SITES_PER_HOST);
    
    private MockHStoreSite hstore_sites[] = new MockHStoreSite[NUM_SITES];
    private HStoreCoordinator coordinators[] = new HStoreCoordinator[NUM_SITES];
    private HStoreConf hstore_conf;    
//    private final VoltTable.ColumnInfo columns[] = {
//        new VoltTable.ColumnInfo("key", VoltType.STRING),
//        new VoltTable.ColumnInfo("value", VoltType.BIGINT),
//    };
//    private final VoltTable fragment = new VoltTable(columns);
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.hstore_conf = HStoreConf.singleton(); 
        hstore_conf.site.coordinator_sync_time = false;
        hstore_conf.site.status_enable = false;
        
        // Create a fake cluster of two HStoreSites, each with two partitions
        // This will allow us to test same site communication as well as cross-site communication
        this.initializeCatalog(NUM_HOSTS, NUM_SITES_PER_HOST, NUM_PARTITIONS_PER_SITE);
        for (int i = 0; i < NUM_SITES; i++) {
            this.hstore_sites[i] = new MockHStoreSite(i, catalogContext, hstore_conf);
            this.hstore_sites[i].setCoordinator();
            this.coordinators[i] = this.hstore_sites[i].getCoordinator();
            
            // We have to make our fake ExecutionSites for each Partition at this site
            for (int id : this.hstore_sites[i].getLocalPartitionIds().values()) {
                MockPartitionExecutor es = new MockPartitionExecutor(id, catalogContext, p_estimator);
                this.hstore_sites[i].addPartitionExecutor(id, es);
            } // FOR
        } // FOR

        this.startMessengers();
        System.err.println("All HStoreCoordinators started!");
    }
    
    @Override
    protected void tearDown() throws Exception {
        System.err.println("TEAR DOWN!");
        super.tearDown();
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
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    /**
     * To keep track out how many threads fail
     */
    public class AssertThreadGroup extends ThreadGroup {
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
        for (final HStoreCoordinator m : this.coordinators) {
            threads.add(new Thread(group, "Site#" + m.getLocalSiteId()) {
                @Override
                public void run() {
                    System.err.println("START: " + m);
                    m.start();
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
        for (final HStoreCoordinator m : this.coordinators) {
            if (m.isStarted()) {
                System.err.println("PREPARE: " + m);
                m.prepareShutdown(false);
            }
        } // FOR
        // Now stop everyone for real!
        for (final HStoreCoordinator m : this.coordinators) {
            if (m.isShuttingDown()) {
                System.err.println("STOP: " + m);
                m.shutdown();
            }
        } // FOR
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testPrepareShutdownSerializeError
     */
    @Test
    public void testPrepareShutdownSerializeError() throws Exception {
        String errorMsg = "XXXXXXXXX";
        Throwable error = null;
        try {
            throw new RuntimeException(errorMsg);
        } catch (Throwable ex) {
            error = ex;
        }
        assertNotNull(error);
        SerializableException sError = new SerializableException(error);
        ByteBuffer buffer = sError.serializeToBuffer();
        buffer.rewind();
        
        ShutdownPrepareRequest.Builder builder = ShutdownPrepareRequest.newBuilder()
                                                        .setSenderSite(0)
                                                        .setError(ByteString.copyFrom(buffer));
        ShutdownPrepareRequest request = builder.build();
        
        assertTrue(request.hasError());
        buffer = request.getError().asReadOnlyByteBuffer();
        SerializableException clone = SerializableException.deserializeFromBuffer(buffer);
        assertTrue(clone.getMessage(), clone.getMessage().contains(errorMsg));
    }
    
//    /**
//     * testPrepareShutdown
//     */
//    @Test
//    public void testPrepareShutdown() throws Exception {

//        
//        // Attach an EventObserver to each HStoreSite
//        @SuppressWarnings("unchecked")
//        final EventObserver<Object> observers[] = new EventObserver[this.hstore_sites.length];
//        final Object observerValues[] = new Object[observers.length];
//        final CountDownLatch observerLatches[] = new CountDownLatch[observers.length];
//        for (int i = 0; i < hstore_sites.length; i++) {
//            final int offset = i;
//            observerLatches[i] = new CountDownLatch(1);
//            observers[i] = new EventObserver<Object>() {
//                @Override
//                public void update(EventObservable<Object> o, Object arg) {
//                    observerValues[offset] = arg;
//                    observerLatches[offset].countDown();
//                }
//            };
//            this.hstore_sites[i].getPrepareShutdownObservable().addObserver(observers[i]);
//        } // FOR
//        
//        // Now tell the first HStoreCoordinator that we want to prepare
//        // to shutdown the cluster. Make sure that everybody got the 
//        // proper error message
//        this.coordinators[0].prepareShutdownCluster(error);
//        for (int i = 1; i < hstore_sites.length; i++) {
//            boolean ret = observerLatches[i].await(1000, TimeUnit.MILLISECONDS);
//            assertTrue("Latch " + i, ret);
//        } // FOR
//    }
    
    /**
     * testStartConnection
     */
    @Test
    public void testStartConnection() throws Exception {
        System.err.println("testStartConnection()");
        for (final HStoreCoordinator m : this.coordinators) {
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
        System.err.println("testStopConnection()");
        
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
            HStoreCoordinator m = this.coordinators[i];
            // stop messenger
            m.shutdown();
            assertEquals(m.getListenerThread().getState(), State.TERMINATED);
            // check that the socket is closed
            int port = this.hstore_sites[i].getSite().getMessenger_port();
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
//          break;
        }
        
    }

//    /**
//     * testSendFragmentLocal
//     */
//    @Test
//    public void testSendFragmentLocal() throws Exception {
//        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
//        //       one partition to another that is on the same site using HStoreMessenger.sendFragment().
//        //       You can cast the destination ExecutionSite to a MockExecutionSite and use getDependency()
//        //       to get back the VoltTable stored by HStoreMessenger. You can check that there is no
//        //       HStoreService for target partition and thus it couldn't possibly use the network to send the message.
//      Integer sender_partition_id;
//      Integer dest_partition_id;
//      for (int i = 0; i < NUM_SITES_PER_HOST; i++)
//      {
//          Object[] testarray = new Object[]{"test", 10};
//          fragment.addRow(testarray);
//          Set<Integer> sets = sites[i].getMessenger().getLocalPartitionIds();
//          sender_partition_id = Integer.parseInt(sets.toArray()[0].toString());
//          dest_partition_id = Integer.parseInt(sets.toArray()[1].toString());
//          messengers[i].sendDependency(0, sender_partition_id, dest_partition_id, 0, fragment);
//          MockExecutionSite executor = (MockExecutionSite)sites[i].getExecutionSite(dest_partition_id);
//          VoltTable vt = executor.getDependency(0);
//          // Verify row expected
//          for (int j = 0; j < vt.getRowCount(); j++)
//          {
//              assertEquals(vt.fetchRow(j).get(0, VoltType.STRING), "test");
//              assertEquals(vt.fetchRow(j).get(1, VoltType.BIGINT), new Long("10"));
//              break;
//          }
////            break;
//      }
//    }

//    /**
//     * testSendFragmentRemote
//     */
//    @Test
//    public void testSendFragmentRemote() throws Exception {
//        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
//        //       one partition to another that is on a remote site. This will check whether the
//        //       HStoreMessenger.sendFragment() can properly discern that a partition is remote and should use
//        //       a network message to send the data. You can stuff some data into the fragment
//        //       and make sure that it arrives on the other side with the proper values.
//      Integer sender_partition_id;
//      Integer dest_partition_id;
//      int txn_id = 0;
//      for (int i = 0; i < NUM_SITES_PER_HOST; i++)
//      {
//          Object[] testarray = new Object[]{"test", 10};
//          fragment.addRow(testarray);
//          Set<Integer> sets = sites[i].getMessenger().getLocalPartitionIds();
//          sender_partition_id = CollectionUtil.first(sets);
//          for (int j = i+1; j < NUM_SITES_PER_HOST; j++)
//          {
//              Set<Integer> sets2 = sites[j].getMessenger().getLocalPartitionIds();
//              dest_partition_id = CollectionUtil.first(sets2);
//              messengers[i].sendDependency(txn_id, sender_partition_id, dest_partition_id, 0, fragment);
//              System.err.println("SITE #" + j + ": " + sites[j].getLocalPartitionIds());
//              MockExecutionSite executor = (MockExecutionSite)sites[j].getExecutionSite(dest_partition_id);
//              assertNotNull(executor);
//              
//              VoltTable vt = null; // executor.waitForDependency(txn_id);
//              while (vt == null) {
//                  vt = executor.getDependency(txn_id);
//              }
//              assertNotNull(String.format("Site #%d -> Site #%d", i, j), vt);
//              
//              // Verify row expected
//              for (int k = 0; k < vt.getRowCount(); k++)
//              {
//                  assertEquals(vt.fetchRow(k).get(0, VoltType.STRING), "test");
//                  assertEquals(vt.fetchRow(k).get(1, VoltType.BIGINT), new Long("10"));
//                  break;
//              }
//              txn_id++;
////                break;
//          }
//      }
//    }
    
    /**
     * testSendMessage
     */
//    @Test
//    public void testSendMessage() throws Exception {
//        // Send a StatusRequest message to each of our remote sites
//        final Map<Integer, String> responses = new HashMap<Integer, String>();
//        final Set<Integer> waiting = new HashSet<Integer>();
//        
//        // We will block on this until we get responses from all of the remote sites
//        final CountDownLatch latch = new CountDownLatch(NUM_SITES);
//        
//        final RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
//            @Override
//            public void run(MessageAcknowledgement parameter) {
//                int sender_site_id = parameter.getSenderSiteId();
//                String status = new String(parameter.getData().toByteArray());
//                responses.put(sender_site_id, status);
//                waiting.remove(sender_site_id);
//                latch.countDown();
//                
//                if (waiting.isEmpty()) {
//                    StringBuilder sb = new StringBuilder();
//                    sb.append("TestConnection Responses:\n");
//                    for (Entry<Integer, String> e : responses.entrySet()) {
//                        sb.append(String.format("  Partition %03d: %s\n", e.getKey(), e.getValue()));
//                    } // FOR
//                    System.err.println(sb.toString());
//                }
//            }
//        };
//        
//        // Send a StatusRequest message to all of our sites. They will respond with some sort of message
//        // We have to wrap the sendMessage() calls in threads so that we can fire them off in parallel
//        // The sender partition can just be our first partition that we have
//        final int sender_id = 0;
//        List<Thread> threads = new ArrayList<Thread>();
//        AssertThreadGroup group = new AssertThreadGroup();
//        for (int i = 0; i < NUM_SITES; i++) {
//            final int dest_id = i;
//            // Local site (i.e., ourself!)
//            if (sender_id == dest_id) {
//                responses.put(dest_id, "LOCAL");
//                latch.countDown();
//            // Remote site
//            } else {
//                final Hstore.MessageRequest sm = Hstore.MessageRequest.newBuilder()
//                                                                .setSenderSiteId(sender_id)
//                                                                .setDestSiteId(dest_id)
//                                                                .setType(MessageType.STATUS)
//                                                                .build();
//                threads.add(new Thread(group, (sender_id + "->" + dest_id)) {
//                    public void run() {
//                        messengers[sender_id].getSiteChannel(dest_id).sendMessage(new ProtoRpcController(), sm, callback);        
//                    };
//                });
//                waiting.add(i);
//            }
//        } // FOR
//
//        // BOMBS AWAY!
//        ThreadUtil.runNewPool(threads);
//        
//        // BLOCK!
//        latch.await();
//        assert(group.exceptions.isEmpty()) : group.exceptions;
//    }
    /**
     * testUnevictData
     */
    @Test
    public void testUnevictDataShouldInvokeSpecifiedCallback() throws Exception {
        final Map<Integer, String> responses = new HashMap<Integer, String>();
        
        // We will block on this until we get responses from the remote site
        final CountDownLatch latch = new CountDownLatch(1);
        
        final RpcCallback<UnevictDataResponse> callback = new RpcCallback<UnevictDataResponse>() {
            @Override
            public void run(UnevictDataResponse parameter) {
                int sender_site_id = parameter.getSenderSite();
                String status = parameter.getStatus().name();
                assertEquals("OK", status);
				responses.put(sender_site_id, status );
                StringBuilder sb = new StringBuilder();
                sb.append("TestConnection Responses:\n");
                for (java.util.Map.Entry<Integer, String> e : responses.entrySet()) {
                    sb.append(String.format("  Partition %03d: %s\n", e.getKey(), e.getValue()));
                } // FOR
                System.err.println(sb.toString());
                latch.countDown();
            }
        };
        
        // The sender partition can just be our first partition that we have
        final int sender_id = 0;
        // Remote site        
        final int dest_id = 1;
        final UnevictDataRequest request = UnevictDataRequest.newBuilder()
                        .setSenderSite(sender_id)
                        .setPartitionId(0)
                        .setTableId(99)
                        .setTransactionId(-1)
			.setNewTransactionId(12)
                        .build();      
            	
        hstore_sites[sender_id].getCoordinator().getChannel(dest_id).unevictData(new ProtoRpcController(), request, callback);
        // BLOCK!
        latch.await();
        assertEquals(1, responses.size());
    }    


}
