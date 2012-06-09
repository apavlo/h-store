package edu.brown.hstore.internal;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.txns.AbstractTransaction;

public class WorkFragmentMessage extends InternalTxnMessage {
    
    private WorkFragment fragment;
    
    public WorkFragmentMessage(AbstractTransaction ts, WorkFragment fragment) {
        super(ts);
        this.fragment = fragment;
    }

    public void setFragment(WorkFragment fragment) {
        this.fragment = fragment;
    }
    
    public WorkFragment getFragment() {
        return (this.fragment);
    }
}
