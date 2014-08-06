package edu.brown.hstore;

import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;

import edu.brown.hstore.internal.FinishTxnMessage;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.InternalTxnMessage;
import edu.brown.hstore.internal.PrepareTxnMessage;
import edu.brown.hstore.internal.SetDistributedTxnMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.internal.UtilityWorkMessage;

public class PartitionMessageQueue extends PriorityBlockingQueue<InternalMessage> {
    
    private static final long serialVersionUID = 1L;
//    private List<InternalMessage> swap = null;
    
    public PartitionMessageQueue() {
        super(100000, WORK_COMPARATOR); // FIXME
    }
    
//    @Override
//    public int drainTo(Collection<? super InternalMessage> c) {
//        assert(c != null);
//        InternalMessage msg = null;
//        int ctr = 0;
//        
//        if (this.swap == null) {
//            this.swap = new ArrayList<InternalMessage>();
//        } else {
//            this.swap.clear();
//        }
//        
//        while ((msg = this.poll()) != null) {
//            // All new transaction requests must be put in the new collection
//            if (msg instanceof InitializeRequestMessage) {
//                c.add(msg);
//                ctr++;
//            // Everything else will get added back in afterwards 
//            } else {
//                this.swap.add(msg);
//            }
//        } // WHILE
//        if (this.swap.isEmpty() == false) this.addAll(this.swap);
//        return (ctr);
//    }
    
    private static final Comparator<InternalMessage> WORK_COMPARATOR = new Comparator<InternalMessage>() {
        @SuppressWarnings("unchecked")
        private final Class<? extends InternalMessage> compareOrder[] = (Class<? extends InternalMessage>[])new Class<?>[]{
            SetDistributedTxnMessage.class,
            PrepareTxnMessage.class,
            FinishTxnMessage.class,
            WorkFragmentMessage.class,
        };
        
        @Override
        public int compare(InternalMessage msg0, InternalMessage msg1) {
            assert(msg0 != null) : "Unexpected null message [msg0]";
            assert(msg1 != null) : "Unexpected null message [msg1]";

            Class<?> class0 = msg0.getClass();
            Class<?> class1 = msg1.getClass();

            boolean isUtl0 = class0.equals(UtilityWorkMessage.TableStatsRequestMessage.class);
            boolean isUtl1 = class1.equals(UtilityWorkMessage.TableStatsRequestMessage.class);
            if (isUtl0 && !isUtl1) return -1;
            if (!isUtl0 && isUtl1) return 1;
            
            // Always compare Transaction Ids first
            boolean isTxn0 = (msg0 instanceof InternalTxnMessage);
            boolean isTxn1 = (msg1 instanceof InternalTxnMessage);
            if (isTxn0 && isTxn1) {
                // If they're both for the same txn, then use the compareOrder
                Long txnId0 = ((InternalTxnMessage)msg0).getTransactionId();
                assert(txnId0 != null) : "Unexpected null txnId for " + msg0;
                Long txnId1 = ((InternalTxnMessage)msg1).getTransactionId();
                assert(txnId1 != null) : "Unexpected null txnId for " + msg1;

                // Compare TxnIds
                int result = txnId0.compareTo(txnId1);
                if (result != 0) return (result);
                
                // Rank them based on their message type
                // This prevents us from removing a txn before it's been added 
                if (class0.equals(class1) == false) {
                    for (Class<? extends InternalMessage> clazz : compareOrder) {
                        boolean isMatch0 = class0.equals(clazz);
                        boolean isMatch1 = class1.equals(clazz);
                        if (isMatch0 && !isMatch1) return (-1);
                        else if (!isMatch0 && isMatch1) return (1);                    
                    } // FOR
                }
            }
            else if (isTxn0) {
                return (-1); 
            } else if (isTxn1) {
                return (1);
            }

            // Last Resort: Just use hashCode
            return msg0.hashCode() - msg1.hashCode();
        }
    };


}
