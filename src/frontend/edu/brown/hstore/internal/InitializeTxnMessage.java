package edu.brown.hstore.internal;

import edu.brown.hstore.txns.AbstractTransaction;

public class InitializeTxnMessage extends InternalTxnMessage {

    public InitializeTxnMessage(AbstractTransaction ts) {
        super(ts);
    }
}
