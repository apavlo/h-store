package edu.brown.hstore.internal;

import edu.brown.hstore.txns.AbstractTransaction;

/**
 * This message is used to indicate that the transaction as finished at 
 * this partition. 
 * @author pavlo
 */
public class PrepareTxnMessage extends InternalTxnMessage {
    
    public PrepareTxnMessage(AbstractTransaction ts) {
        super(ts);
    }
}
