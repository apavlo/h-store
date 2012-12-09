package edu.brown.hstore.internal;

import edu.brown.hstore.txns.LocalTransaction;

public class StartTxnMessage extends InternalTxnMessage {

    public StartTxnMessage(LocalTransaction ts) {
        super(ts);
    }
}
