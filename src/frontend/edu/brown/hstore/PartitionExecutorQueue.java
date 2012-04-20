package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import org.voltdb.messaging.FinishTaskMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;

public class PartitionExecutorQueue extends PriorityBlockingQueue<TransactionInfoBaseMessage> {
    
    private static final long serialVersionUID = 1L;
    private final List<TransactionInfoBaseMessage> swap = new ArrayList<TransactionInfoBaseMessage>();
    
    public PartitionExecutorQueue() {
        super(10000, WORK_COMPARATOR); // FIXME
    }
    
    @Override
    public int drainTo(Collection<? super TransactionInfoBaseMessage> c) {
        assert(c != null);
        TransactionInfoBaseMessage msg = null;
        int ctr = 0;
        this.swap.clear();
        while ((msg = this.poll()) != null) {
            // All new transaction requests must be put in the new collection
            if (msg instanceof InitiateTaskMessage) {
                c.add(msg);
                ctr++;
            // Everything else will get added back in afterwards 
            } else {
                this.swap.add(msg);
            }
        } // WHILE
        if (this.swap.isEmpty() == false) this.addAll(this.swap);
        return (ctr);
    }
    
    private static final Comparator<TransactionInfoBaseMessage> WORK_COMPARATOR = new Comparator<TransactionInfoBaseMessage>() {
        @Override
        public int compare(TransactionInfoBaseMessage msg0, TransactionInfoBaseMessage msg1) {
            assert(msg0 != null);
            assert(msg1 != null);

            Class<? extends TransactionInfoBaseMessage> class0 = msg0.getClass();
            Class<? extends TransactionInfoBaseMessage> class1 = msg1.getClass();
            
            // (3) Otherwise, always let the FinishTaskMessage go first
            boolean isFinish0 = class0.equals(FinishTaskMessage.class);
            boolean isFinish1 = class1.equals(FinishTaskMessage.class);
            if (isFinish0 && !isFinish1) return (-1);
            else if (!isFinish0 && isFinish1) return (1);
            
            // (1) SysProcs always go first
            if (msg0.isSysProc() != msg1.isSysProc()) {
                if (msg0.isSysProc()) return (-1);
                else return (1);
            }
            
            // (2) If they're the same message type, go by their txnIds
            if (class0.equals(class1)) return (msg0.getTxnId().compareTo(msg1.getTxnId()));
            
            // (4) Then let a FragmentTaskMessage go before anything else
            boolean isWork0 = class0.equals(FragmentTaskMessage.class);
            boolean isWork1 = class1.equals(FragmentTaskMessage.class);
            if (isWork0 && !isWork1) return (-1);
            else if (!isWork0 && isWork1) return (1);
            
            // (5) They must be the same!
            assert(false) : String.format("%s <-> %s", class0, class1);
            return 0;
        }
    };


}
