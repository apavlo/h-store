package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.MessageAcknowledgement;

public class ForwardTxnResponseCallback implements RpcCallback<byte[]> {
    private static final Logger LOG = Logger.getLogger(ForwardTxnResponseCallback.class);
    
    private final RpcCallback<MessageAcknowledgement> orig_callback;
    private final int source_id;
    private final int dest_id;
    
    /**
     * Constructor
     * @param source_id
     * @param dest_id
     * @param orig_callback
     */
    public ForwardTxnResponseCallback(int source_id, int dest_id, RpcCallback<MessageAcknowledgement> orig_callback) {
        this.orig_callback = orig_callback;
        this.source_id = source_id;
        this.dest_id = dest_id;
    }

    @Override
    public void run(byte[] parameter) {
        if (LOG.isTraceEnabled()) LOG.trace("Got ClientResponse callback! Sending back to Site #" + this.dest_id);
        ByteString bs = ByteString.copyFrom(parameter);
        Hstore.MessageAcknowledgement response = Hstore.MessageAcknowledgement.newBuilder()
                                                                              .setSenderId(this.source_id)
                                                                              .setDestId(this.dest_id)
                                                                              .setData(bs)
                                                                              .build();
        this.orig_callback.run(response);
        
    }
    
}
