package edu.brown.hstore.internal;

import edu.brown.hstore.txns.AbstractTransaction;

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
    
    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "::" + this.ts;
    }
    
    
}
