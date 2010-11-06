package edu.mit.hstore;

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
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestHStoreMessenger extends BaseTestCase {

    /**
     * REMOVE THIS VARIABLE ONCE YOU GET STOP CONNECTIONS WORKING!!!
     */
    private final boolean DYLAN_REMOVE_FLAG = false;
    
    
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
        
        // XXX
        if (DYLAN_REMOVE_FLAG) this.startMessengers();
    }
    
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        this.stopMessengers();
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
            threads.add(new Thread(group, Integer.toString(m.getLocalSiteId())) {
                @Override
                public void run() {
                    m.start();
                } 
            });
        } // FOR
        ThreadUtil.run(threads);
        assert(group.exceptions.isEmpty()) : group.exceptions;
    }
    
    /**
     * Stop all of the HStoreMessengers
     * @throws Exception
     */
    private void stopMessengers() throws Exception {
        for (final HStoreMessenger m : this.messengers) {
            if (m.isStarted() && m.isStopped() == false) {
                System.err.println("STOP: " + m);
                m.stop();
            }
        } // FOR
    }
    
    /**
     * testStartConnection
     */
    @Test
    public void testStartConnection() throws Exception {
        // TODO: Check that each HStoreMessenger has a running listener thread! 
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
    }

    /**
     * testSendFragmentLocal
     */
    @Test
    public void testSendFragmentLocal() throws Exception {
        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
        //       one partition to another that is on the same site. You can cast the destination ExecutionSite 
        //       to a MockExecutionSite and use getDependency() to get back the VoltTable stored by HStoreMessenger
        //       You can check that there is no HStoreService for target partition and thus it couldn't possibly
        //       use the network to send the message.
    }

    /**
     * testSendFragmentRemote
     */
    @Test
    public void testSendFragmentRemote() throws Exception {
        // TODO: Write a test case that tests that we can send a data fragment (i.e., VoltTable) from
        //       one partition to another that is on a remote site. This will exercise whether the
        //       HStoreMessenger can properly discern that a partition is remote and should use
        //       a network message to send the data. You can stuff some data into the fragment
        //       and make sure that it arrives on the other side with the proper values.
    }
    
    /**
     * testSendMessage
     */
    @Test
    public void testSendMessage() throws Exception {
        // XXX
        if (DYLAN_REMOVE_FLAG == false) return; 
        
        // Send a StatusRequest message to each of our remote sites
        final Map<Integer, String> responses = new HashMap<Integer, String>();
        final Set<Integer> waiting = new HashSet<Integer>();
        
        // We will block on this until we get responses from all of the remote sites
        final CountDownLatch latch = new CountDownLatch(NUM_SITES);
        
        final RpcCallback<MessageAcknowledgement> callback = new RpcCallback<MessageAcknowledgement>() {
            @Override
            public void run(MessageAcknowledgement parameter) {
                int sender = parameter.getSenderId();
                String status = new String(parameter.getData().toByteArray());
                responses.put(sender, status);
                waiting.remove(sender);
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
                                                                .setSenderId(sender_id)
                                                                .setDestId(dest_id)
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
        ThreadUtil.run(threads);
        
        // BLOCK!
        latch.await();
        assert(group.exceptions.isEmpty()) : group.exceptions;
    }
}
