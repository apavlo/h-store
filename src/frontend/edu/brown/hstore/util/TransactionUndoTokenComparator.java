package edu.brown.hstore.util;

import java.util.Comparator;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.txns.AbstractTransaction;

public class TransactionUndoTokenComparator implements Comparator<AbstractTransaction> {
    
    private static final int FIRST_TXN = -1;
    private static final int SECOND_TXN = 1;
    
    private final int partition;
    
    public TransactionUndoTokenComparator(int partition) {
        this.partition = partition;
    }
    
    @Override
    public int compare(AbstractTransaction ts0, AbstractTransaction ts1) {
        long undoToken0 = ts0.getFirstUndoToken(this.partition);
        long undoToken1 = ts1.getFirstUndoToken(this.partition);
        
        // Any transaction with a null undoToken should always go first
        if (undoToken0 == HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
            if (undoToken1 != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
                return (FIRST_TXN);
            } else {
                return (ts0.compareTo(ts1));
            }
        }
        else if (undoToken1 == HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
            if (undoToken0 != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
                return (SECOND_TXN);
            } else {
                return (ts0.compareTo(ts1));
            }
        }
        
        // Special Case
        if (ts0 == ts1) return (0);
        
        // They have real undoTokens. So let's just compare their order.
        assert(undoToken0 != undoToken1);
        return (undoToken0 < undoToken1 ? FIRST_TXN :  SECOND_TXN);
    }

}
