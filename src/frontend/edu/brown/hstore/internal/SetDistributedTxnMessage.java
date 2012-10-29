package edu.brown.hstore.internal;

import edu.brown.hstore.txns.AbstractTransaction;

public class SetDistributedTxnMessage extends InternalTxnMessage {

	// Do nothing but to mark a internal distributed transaction
	public SetDistributedTxnMessage (AbstractTransaction txn) {
		super(txn);
	}

}
