package edu.mit.hstore.callbacks;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore.MessageAcknowledgement;

/**
 * 
 * @author pavlo
 */
public class ForwardTxnRequestCallback implements RpcCallback<MessageAcknowledgement> {
    private static final Logger LOG = Logger.getLogger(ForwardTxnRequestCallback.class);
    
    protected final RpcCallback<byte[]> orig_callback;
    
    
    public ForwardTxnRequestCallback(RpcCallback<byte[]> orig_callback) {
        this.orig_callback = orig_callback;
    }

    @Override
    public void run(MessageAcknowledgement parameter) {
        if (LOG.isTraceEnabled()) LOG.trace("Got back FORWARD_TXN response from Site #" + parameter.getSenderId() + ". Sending response to client " +
                                            "[bytes=" + parameter.getData().size() + "]");
        byte data[] = parameter.getData().toByteArray();
        this.orig_callback.run(data);
    }
    
}
