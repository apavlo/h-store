package edu.brown.hstore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import edu.brown.hstore.internal.FinishTxnMessage;
import edu.brown.hstore.internal.InitializeTxnMessage;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.InternalTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;

public class PartitionMessageQueue extends PriorityBlockingQueue<InternalMessage> {
    
    private static final long serialVersionUID = 1L;
    private List<InternalMessage> swap = null;
    
    public PartitionMessageQueue() {
        super(1000, WORK_COMPARATOR); // FIXME
    }
    
    @Override
    public int drainTo(Collection<? super InternalMessage> c) {
        assert(c != null);
        InternalMessage msg = null;
        int ctr = 0;
        
        if (this.swap == null) {
            this.swap = new ArrayList<InternalMessage>();
        } else {
            this.swap.clear();
        }
        
        while ((msg = this.poll()) != null) {
            // All new transaction requests must be put in the new collection
            if (msg instanceof InitializeTxnMessage) {
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
    
    @Override
    public InternalMessage poll() {
        return super.poll();
    }
    
    private static final Comparator<InternalMessage> WORK_COMPARATOR = new Comparator<InternalMessage>() {
        @Override
        public int compare(InternalMessage msg0, InternalMessage msg1) {
            assert(msg0 != null);
            assert(msg1 != null);

            Class<?> class0 = msg0.getClass();
            Class<?> class1 = msg1.getClass();
            
            // (1) Always let the FinishTaskMessage go first so that we can release locks
            boolean isFinish0 = class0.equals(FinishTxnMessage.class);
            boolean isFinish1 = class1.equals(FinishTxnMessage.class);
            if (isFinish0 && !isFinish1) return (-1);
            else if (!isFinish0 && isFinish1) return (1);
            
            // (2) Then let a WorkFragmentMessage go before anything else
            boolean isWork0 = class0.equals(WorkFragmentMessage.class);
            boolean isWork1 = class1.equals(WorkFragmentMessage.class);
            if (isWork0 && !isWork1) return (-1);
            else if (!isWork0 && isWork1) return (1);
            
            // (3) Compare Transaction Ids
            boolean isTxn0 = (msg0 instanceof InternalTxnMessage);
            boolean isTxn1 = (msg1 instanceof InternalTxnMessage);
            if (isTxn0) {
                assert(((InternalTxnMessage)msg0).getTransactionId() != null) :
                    "Unexpected null txnId for " + msg0;
                if (isTxn1) {
                    assert(((InternalTxnMessage)msg1).getTransactionId() != null) :
                        "Unexpected null txnId for " + msg1;
                    return ((InternalTxnMessage)msg0).getTransactionId().compareTo(
                                ((InternalTxnMessage)msg1).getTransactionId());
                }
                return (-1); 
            } else if (isTxn1) {
                assert(((InternalTxnMessage)msg1).getTransactionId() != null) :
                    "Unexpected null txnId for " + msg1;
                return (1);
            }
            
            // (4) They must be the same!
            // assert(false) : String.format("%s <-> %s", class0, class1);
            return msg0.hashCode() - msg1.hashCode();
        }
    };


}
