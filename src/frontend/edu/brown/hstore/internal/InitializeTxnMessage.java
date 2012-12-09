package edu.brown.hstore.internal;

import edu.brown.hstore.txns.LocalTransaction;

public class InitializeTxnMessage extends InternalTxnMessage {

    public InitializeTxnMessage(LocalTransaction ts) {
        super(ts);
    }
}
