package edu.brown.hstore.callbacks;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;

public class TransactionPrefetchCallback implements RpcCallback<TransactionPrefetchAcknowledgement> {

    @Override
    public void run(TransactionPrefetchAcknowledgement parameter) {
        // I don't think there is anything we really want to do.
    }

}
