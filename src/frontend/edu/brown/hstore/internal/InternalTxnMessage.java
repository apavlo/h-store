package edu.brown.hstore.internal;

import edu.brown.hstore.dtxn.AbstractTransaction;

public abstract class InternalTxnMessage extends InternalMessage {

    final AbstractTransaction ts;
    
    public InternalTxnMessage(AbstractTransaction ts) {
        super();
        this.ts = ts;
    }
    
    @SuppressWarnings("unchecked")
    public <T extends AbstractTransaction> T getTransaction() {
        return ((T)this.ts);
    }
    
    public Long getTransactionId() {
        return (this.ts.getTransactionId());
    }
    
}
