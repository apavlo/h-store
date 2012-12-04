package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.junit.Test;
import org.voltdb.TransactionIdManager;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;
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
    
    private Collection<LocalTransaction> loadQueue(int num_txns) {
        Collection<LocalTransaction> added = new TreeSet<LocalTransaction>();
        for (long i = 0; i < num_txns; i++) {
            LocalTransaction ts = new LocalTransaction(this.hstore_site);
            Long txnId = this.idManager.getNextUniqueTransactionId();
            ts.testInit(txnId, 0, new PartitionSet(1), this.catalog_proc);
            
            // I think that we need to do this...
            this.queue.noteTransactionRecievedAndReturnLastSeen(ts);
            
            boolean ret = this.queue.offer(ts);
            assert(ret);
            added.add(ts);
        } // FOR
        return (added);
    }
    
    /**
     * testOutOfOrderExecution
     */
    @Test
    public void testOutOfOrderExecution() throws Exception {
        Collection<LocalTransaction> added = this.loadQueue(NUM_TXNS);
        assertEquals(added.size(), this.queue.size());
        
        // If we peek in the queue, we should see our first guy
        ThreadUtil.sleep(TXN_DELAY*2);
        System.err.println(StringUtil.join("\n",this.queue)); 
        // assertEquals(CollectionUtil.first(added), this.queue.poll());
    }
}
