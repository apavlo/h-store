package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.voltdb.messaging.FinishTaskMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;

import edu.brown.hstore.dtxn.LocalTransaction;

/**
 * @author ambell
 * There's probably no reason this needs to be its own class with overrides, except
 * this way we can experiment more easily for now.
 */
public class PartitionExecutorDeferredQueue extends PriorityQueue<LocalTransaction> {
    
    private static final long serialVersionUID = 1L;
    private final List<TransactionInfoBaseMessage> swap = new ArrayList<TransactionInfoBaseMessage>();
    
    public PartitionExecutorDeferredQueue() {
        super(10000); // FIXME
    }
    
    @Override
    public LocalTransaction poll() {
        return this.poll();
    }
    
   


}
