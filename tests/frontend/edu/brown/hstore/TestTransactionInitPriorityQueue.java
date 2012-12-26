package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.utils.EstTimeUpdater;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.hstore.TransactionInitPriorityQueue.QueueState;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;

public class TestTransactionInitPriorityQueue extends BaseTestCase {

    private static final int NUM_TXNS = 10;
    private static final int TXN_DELAY = 500;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = DeleteCallForwarding.class;
    private static final Random random = new Random(0);
    
    HStoreSite hstore_site;
    HStoreConf hstore_conf;
    TransactionIdManager idManager;
    TransactionQueueManager queueManager;
    TransactionInitPriorityQueue queue;
    TransactionInitPriorityQueue.Debug queueDbg;
    Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.hstore_conf = HStoreConf.singleton();
        this.hstore_conf.site.txn_incoming_delay = TXN_DELAY;
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        assertNotNull(catalog_site);
        this.hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, HStoreConf.singleton());
        this.idManager = hstore_site.getTransactionIdManager(0);
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.queue = this.queueManager.getInitQueue(0);
        this.queueDbg = this.queue.getDebugContext();
        assertTrue(this.queue.isEmpty());
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private Collection<AbstractTransaction> loadQueue(int num_txns) {
        Collection<AbstractTransaction> added = new TreeSet<AbstractTransaction>();
        for (long i = 0; i < num_txns; i++) {
            LocalTransaction txn = new LocalTransaction(this.hstore_site);
            Long txnId = this.idManager.getNextUniqueTransactionId();
            txn.testInit(txnId, 0, new PartitionSet(1), this.catalog_proc);
            
            // I think that we need to do this...
            this.queue.noteTransactionRecievedAndReturnLastSeen(txn.getTransactionId());
            
            boolean ret = this.queue.offer(txn);
            assert(ret);
            added.add(txn);
        } // FOR
        return (added);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testQueueState
     */
    @Test
    public void testQueueState() throws Exception {
        // Always start off as empty
        QueueState state = this.queue.checkQueueState();
        assertEquals(QueueState.BLOCKED_EMPTY, state);
        
        // Insert a bunch of txns that all have the same initiating timestamp
        Collection<AbstractTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        System.err.println(StringUtil.join("\n", added));
        
        // Because we haven't moved the current time up, we know that none of
        // the txns should be released now
        state = this.queue.checkQueueState();
        assertEquals(QueueState.BLOCKED_SAFETY, state);
        
        // Sleep for a little bit to make the current time move forward 
        ThreadUtil.sleep(TXN_DELAY);
        EstTimeUpdater.update(System.currentTimeMillis());

        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            // No matter how many times that we call checkQueueState, our 
            // blocked timestamp should not change since we haven't released
            // a transaction
            Long lastBlockTime = null;
            long nextBlockTime;
            for (int j = 0; j < 10; j++) {
                if (j != 0) TransactionInitPriorityQueue.LOG.info(StringUtil.SINGLE_LINE.trim());
                String debug = String.format("i=%d / j=%d", i, j);
                state = this.queue.checkQueueState();
                nextBlockTime = this.queueDbg.getBlockedTimestamp();
                // System.err.printf("%s => state=%s / lastBlock=%d\n", debug, state, lastBlockTime);
                
                assertEquals(debug, QueueState.UNBLOCKED, state);
                if (lastBlockTime != null) {
                    assertEquals(debug, lastBlockTime.longValue(), nextBlockTime);
                }
                lastBlockTime = nextBlockTime;
            } // FOR
            assertEquals("i="+i, it.next(), this.queue.poll());
            TransactionInitPriorityQueue.LOG.info(StringUtil.DOUBLE_LINE.trim());
        } // FOR
    }
    
    /**
     * testQueueStateAfterRemove
     */
    @Test
    public void testQueueStateAfterRemove() throws Exception {
        // Always start off as empty
        QueueState state = this.queue.checkQueueState();
        assertEquals(QueueState.BLOCKED_EMPTY, state);
        
        // Insert a bunch of txns that all have the same initiating timestamp
        Collection<AbstractTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        System.err.println(StringUtil.join("\n", added));
        
        // Because we haven't moved the current time up, we know that none of
        // the txns should be released now
        state = this.queue.checkQueueState();
        assertEquals(QueueState.BLOCKED_SAFETY, state);
        
        // Sleep for a little bit to make the current time move forward 
        ThreadUtil.sleep(TXN_DELAY);
        EstTimeUpdater.update(System.currentTimeMillis());

        TransactionInitPriorityQueue.LOG.info(StringUtil.DOUBLE_LINE.trim());
        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            String debug = String.format("i=%d", i);
            
            // Ok so what we're going to do here is peek into the
            // queue and make sure that that our expected txn 
            // is the next one that's suppose to pop out
            AbstractTransaction to_remove = it.next();
            state = this.queue.checkQueueState();
            assertEquals(debug, QueueState.UNBLOCKED, state);
            assertEquals(debug, to_remove, this.queue.peek());

            // Then we're going to delete it and make sure that the next txn
            // queued up is not the one that we just removed
            boolean result = this.queue.remove(to_remove);
            assertTrue(debug, result);
            if (i + 1 < NUM_TXNS) {
                assertFalse(debug, this.queue.isEmpty());
                assertEquals(debug, QueueState.UNBLOCKED, this.queue.getQueueState());
            }
            else {
                assertTrue(debug, this.queue.isEmpty());
            }
            TransactionInitPriorityQueue.LOG.info(StringUtil.DOUBLE_LINE.trim());
        } // FOR
    }
    
    /**
     * testOutOfOrderInsertion
     */
    @Test
    public void testOutOfOrderInsertion() throws Exception {
        // Create a bunch of txns and then insert them in the wrong order
        // We should be able to get them back in the right order
        Collection<AbstractTransaction> added = new TreeSet<AbstractTransaction>();
        for (long i = 0; i < NUM_TXNS; i++) {
            LocalTransaction txn = new LocalTransaction(this.hstore_site);
            Long txnId = this.idManager.getNextUniqueTransactionId();
            txn.testInit(txnId, 0, new PartitionSet(1), this.catalog_proc);
            added.add(txn);
        } // FOR
        List<AbstractTransaction> shuffled = new ArrayList<AbstractTransaction>(added);
        Collections.shuffle(shuffled, random);
        
        System.err.println(StringUtil.columns(
                "Expected Order:\n" + StringUtil.join("\n", added),
                "Insertion Order:\n" + StringUtil.join("\n", shuffled)
        ));
        System.err.flush();
        
        for (AbstractTransaction txn : shuffled) {
            this.queue.noteTransactionRecievedAndReturnLastSeen(txn.getTransactionId());
            boolean ret = this.queue.offer(txn);
            assert(ret);
            assertNull(this.queue.poll());
        } // FOR
        assertEquals(added.size(), this.queue.size());
        assertEquals(QueueState.BLOCKED_SAFETY, this.queue.getQueueState());

        // Now we should be able to remove the first of these mofos
        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            ThreadUtil.sleep(TXN_DELAY);
            EstTimeUpdater.update(System.currentTimeMillis());
            AbstractTransaction expected = it.next();
            assertNotNull(expected);
            if (i == 0) this.queue.checkQueueState();
            assertEquals("i="+i, expected, this.queue.poll());
        } // FOR
    }
    
    /**
     * testOutOfOrderRemoval
     */
    @Test
    public void testOutOfOrderRemoval() throws Exception {
        Collection<AbstractTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        
        // Now grab the last one and pop it out
        AbstractTransaction last = CollectionUtil.last(added);
        assertTrue(this.queue.remove(last));
        assertFalse(this.queue.contains(last));
        
        // Now we should be able to remove the first of these mofos
        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS-1; i++) {
            ThreadUtil.sleep(TXN_DELAY);
            EstTimeUpdater.update(System.currentTimeMillis());
            if (i == 0) this.queue.checkQueueState();
            assertEquals(it.next(), this.queue.poll());
        } // FOR
        assertTrue(this.queue.isEmpty());
    }
    
    /**
     * testRemove
     */
    @Test
    public void testRemove() throws Exception {
        Collection<AbstractTransaction> added = this.loadQueue(1);
        assertEquals(added.size(), this.queue.size());
        
        // Remove the first. Make sure that poll() doesn't return it
        ThreadUtil.sleep(TXN_DELAY*4);
        // System.err.println(StringUtil.repeat("-", 100));
        this.loadQueue(1);
        
        ThreadUtil.sleep(TXN_DELAY*2);
        EstTimeUpdater.update(System.currentTimeMillis());
        // System.err.println(StringUtil.repeat("-", 100));
        this.queue.checkQueueState();
        AbstractTransaction first = CollectionUtil.first(added);
        assertEquals(first, this.queue.peek());
        assertTrue(first.toString(), this.queue.remove(first));
        assertFalse(first.toString(), this.queue.contains(first));
        
        AbstractTransaction poll = this.queue.poll();
        assertNotSame(first, poll);
    }
    
    /**
     * testRemoveIterator
     */
    @Test
    public void testRemoveIterator() throws Exception {
        List<AbstractTransaction> added = new ArrayList<AbstractTransaction>(this.loadQueue(10));
        assertEquals(added.size(), this.queue.size());
        Collections.shuffle(added);
        
        // Remove them one by one and make sure that the iterator 
        // never returns an id that we removed
        Set<AbstractTransaction> removed = new HashSet<AbstractTransaction>();
        for (int i = 0, cnt = added.size(); i < cnt; i++) {
            AbstractTransaction next = added.get(i);
            assertFalse(next.toString(), removed.contains(next));
            assertTrue(next.toString(), this.queue.contains(next));
            assertTrue(next.toString(),this.queue.remove(next));
            removed.add(next);
            
            int it_ctr = 0;
            for (AbstractTransaction txn : this.queue) {
                assertNotNull(txn);
                assertFalse(txn.toString(), removed.contains(txn));
                assertTrue(txn.toString(), added.contains(txn));
                it_ctr++;
            } // FOR
            assertEquals(added.size() - removed.size(), it_ctr);
        } // FOR
    }
    
//    /**
//     * testConcurrentRemoveIterator
//     */
//    @Test
//    public void testConcurrentRemoveIterator() throws Exception {
//        List<AbstractTransaction> added = new ArrayList<AbstractTransaction>(this.loadQueue(10));
//        assertEquals(added.size(), this.queue.size());
//        Collections.shuffle(added);
//        AbstractTransaction toDelete = CollectionUtil.last(added);
//        
//        Set<AbstractTransaction> found = new HashSet<AbstractTransaction>();
//        for (AbstractTransaction txn : this.queue) {
//            if (found.isEmpty()) this.queue.remove(toDelete);
//            found.add(txn);
//        } // FOR
//        System.err.println("ToDelete: " + toDelete);
//        System.err.println("Found: " + found);
//        assertFalse(toDelete.toString(), found.contains(toDelete));
//        assertEquals(added.size()-1, found.size());
//    }
    
    /**
     * testConcurrentOfferIterator
     */
    @Test
    public void testConcurrentOfferIterator() throws Exception {
        Collection<AbstractTransaction> added = this.loadQueue(10);
        assertEquals(added.size(), this.queue.size());
        
        LocalTransaction toOffer = new LocalTransaction(this.hstore_site);
        Long txnId = this.idManager.getNextUniqueTransactionId();
        toOffer.testInit(txnId, 0, new PartitionSet(1), this.catalog_proc);
        assertFalse(this.queue.contains(toOffer));
        
        Set<AbstractTransaction> found = new HashSet<AbstractTransaction>();
        for (AbstractTransaction txn : this.queue) {
            if (found.isEmpty()) this.queue.offer(toOffer);
            found.add(txn);
        } // FOR
        assertFalse(found.contains(toOffer));
        assertEquals(added.size(), found.size());
    }
    
    /**
     * testPoll
     */
    @Test
    public void testPoll() throws Exception {
        Collection<AbstractTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        
        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            ThreadUtil.sleep(TXN_DELAY);
            EstTimeUpdater.update(System.currentTimeMillis());
            if (i == 0) this.queue.checkQueueState();
            assertEquals(it.next(), this.queue.poll());
        } // FOR
    }
    
    /**
     * testPollTooEarly
     */
    @Test
    public void testPollTooEarly() throws Exception {
        // Try polling *before* the appropriate wait time
        Collection<AbstractTransaction> added = this.loadQueue(1);
        ThreadUtil.sleep(TXN_DELAY);
        added.addAll(this.loadQueue(1));
        assertEquals(added.size(), this.queue.size());
        EstTimeUpdater.update(System.currentTimeMillis());
        
        Iterator<AbstractTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            // Our first poll should return back a txn
            if (i == 0) {
                this.queue.checkQueueState();
                assertEquals(it.next(), this.queue.poll());
            }
            // The second time should be null
            else {
                AbstractTransaction ts = this.queue.poll();
                assertNull("Unexpected txn returned: " + ts, ts);
                break;
            }
        } // FOR
    }
}
