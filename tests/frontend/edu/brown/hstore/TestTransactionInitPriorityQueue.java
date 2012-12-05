package edu.brown.hstore;

import java.util.Collection;
import java.util.Iterator;
import java.util.TreeSet;

import org.junit.Test;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.utils.EstTimeUpdater;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

public class TestTransactionInitPriorityQueue extends BaseTestCase {

    private static final int NUM_TXNS = 4;
    private static final int TXN_DELAY = 1000;
    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = DeleteCallForwarding.class;
    
    HStoreSite hstore_site;
    HStoreConf hstore_conf;
    TransactionIdManager idManager;
    TransactionQueueManager queueManager;
    TransactionInitPriorityQueue queue;
    Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.hstore_conf = HStoreConf.singleton();
        hstore_conf.site.txn_incoming_delay = TXN_DELAY;
        
        Site catalog_site = CollectionUtil.first(catalogContext.sites);
        assertNotNull(catalog_site);
        this.hstore_site = new MockHStoreSite(catalog_site.getId(), catalogContext, HStoreConf.singleton());
        this.idManager = hstore_site.getTransactionIdManager(0);
        this.queueManager = this.hstore_site.getTransactionQueueManager();
        this.queue = this.queueManager.getInitQueue(0);
        assertTrue(this.queue.isEmpty());
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
    }
    
    // --------------------------------------------------------------------------------------------
    // UTILITY METHODS
    // --------------------------------------------------------------------------------------------
    
    private Collection<LocalTransaction> loadQueue(int num_txns) {
        Collection<LocalTransaction> added = new TreeSet<LocalTransaction>();
        for (long i = 0; i < num_txns; i++) {
            LocalTransaction ts = new LocalTransaction(this.hstore_site);
            Long txnId = this.idManager.getNextUniqueTransactionId();
            ts.testInit(txnId, 0, new PartitionSet(1), this.catalog_proc);
            
            // I think that we need to do this...
            this.queue.noteTransactionRecievedAndReturnLastSeen(ts);
            
            boolean ret = this.queue.offer(ts, false);
            assert(ret);
            added.add(ts);
        } // FOR
        return (added);
    }
    
    // --------------------------------------------------------------------------------------------
    // TEST CASES
    // --------------------------------------------------------------------------------------------
    
    /**
     * testOutOfOrderExecution
     */
    @Test
    public void testOutOfOrderExecution() throws Exception {
        Collection<LocalTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        
        // Now grab the last one and pop it out
        LocalTransaction ts = CollectionUtil.last(added);
        assertTrue(this.queue.remove(ts));
        
        // Now we should be able to remove the first of these mofos
        Iterator<LocalTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS-1; i++) {
            ThreadUtil.sleep(TXN_DELAY);
            EstTimeUpdater.update(System.currentTimeMillis());
            assertEquals(it.next(), this.queue.poll());
        } // FOR
        assertTrue(this.queue.isEmpty());
    }
    
    /**
     * testRemove
     */
    @Test
    public void testRemove() throws Exception {
        Collection<LocalTransaction> added = this.loadQueue(1);
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
        assertTrue(this.queue.remove(first));
        
        AbstractTransaction poll = this.queue.poll();
        assertNotSame(first, poll);
    }
    
    /**
     * testPoll
     */
    @Test
    public void testPoll() throws Exception {
        Collection<LocalTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        
        Iterator<LocalTransaction> it = added.iterator();
        for (int i = 0; i < NUM_TXNS; i++) {
            ThreadUtil.sleep(TXN_DELAY);
            EstTimeUpdater.update(System.currentTimeMillis());
            assertEquals(it.next(), this.queue.poll());
        } // FOR
    }
}
