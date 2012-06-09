package edu.brown.hstore.internal;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.dtxn.AbstractTransaction;

public class FinishTxnMessage extends InternalTxnMessage {
    
    private Status status;
    
    public FinishTxnMessage(AbstractTransaction ts, Status status) {
        super(ts);
        this.status = status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    public Status getStatus() {
        return (this.status);
    }

}
