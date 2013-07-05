package edu.brown.hstore;

import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.StartTxnMessage;
import edu.brown.hstore.internal.UtilityWorkMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public class TestPartitionMessageQueue extends BaseTestCase {
    
    private static final int NUM_PARTITIONS = 5;
    private static final int BASE_PARTITION = 1;
    private static long NEXT_TXN_ID = 1;
    
    private final PartitionMessageQueue queue = new PartitionMessageQueue();
    private MockHStoreSite hstore_site;
    private Procedure catalog_proc;
    private LocalTransaction ts0;
    private LocalTransaction ts1;

    private final WorkFragment mockFragment = null;
    
    private StartTxnMessage startMsg;
    private WorkFragmentMessage workMsg;
    private UtilityWorkMessage utilMsg;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
        this.catalog_proc = this.getProcedure(UpdateLocation.class);
        
        this.ts0 = new LocalTransaction(this.hstore_site);
        this.ts0.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, catalogContext.getAllPartitionIds(), catalog_proc);
        
        this.ts1 = new LocalTransaction(this.hstore_site);
        this.ts1.testInit(NEXT_TXN_ID++, BASE_PARTITION, null, new PartitionSet(BASE_PARTITION), catalog_proc);
        
        // Initialize some messages that we can use
        this.utilMsg = new UtilityWorkMessage();
        this.startMsg = new StartTxnMessage(ts1);
        this.workMsg = new WorkFragmentMessage(ts1, mockFragment);
    }
    
    private void checkOutputOrder(InternalMessage target, InternalMessage messages[]) {
        boolean ret;
        InternalMessage next = null;
        
        // First try them one by one
        for (InternalMessage m : messages) {
            this.queue.clear();
            
            ret = this.queue.add(m);
            assertTrue(ret);
            assertEquals(m, this.queue.peek());
            
            // We should always get back the finish 
            ret = this.queue.add(target);
            System.err.println(this.queue);
            assertTrue(ret);
            assertEquals(target, this.queue.peek());
            next = this.queue.poll();
            assertEquals(target, next);
            
            // And our first guy is still there!
            assertEquals(m, this.queue.peek());
        } // FOR
        
        // Now add them all at once, just to make sure that always get the 
        // target message back first
        this.queue.clear();
        for (InternalMessage m : messages) {
            ret = this.queue.add(m);
            assertTrue(ret);
        } // FOR
        ret = this.queue.add(target);
        assertTrue(ret);
        assertEquals(target, this.queue.peek());
        next = this.queue.poll();
        assertEquals(target, next);
    }
    
    /**
     * testWorkBeforeOthers
     */
    public void testWorkBeforeOthers() throws Exception {
        InternalMessage messages[] = { utilMsg, startMsg };
        this.checkOutputOrder(workMsg, messages);
    }
    
    /**
     * testTransactionIdOrder
     */
    public void testTransactionIdOrder() throws Exception {
        StartTxnMessage start0 = new StartTxnMessage(ts1);
        StartTxnMessage start1 = new StartTxnMessage(ts0);
        assert(start1.getTransactionId() < start0.getTransactionId());
        
        // We'll add the our messages and make sure that we get the one with the 
        // smaller txnId back first
        boolean ret;
        ret = this.queue.add(start0);
        assertTrue(ret);
        ret = this.queue.add(start1);
        assertTrue(ret);
        
        assertEquals(start1, this.queue.peek());
        InternalMessage next = this.queue.poll();
        assertEquals(start1, next);
        
        next = this.queue.poll();
        assertEquals(start0, next);
    }
    
}
