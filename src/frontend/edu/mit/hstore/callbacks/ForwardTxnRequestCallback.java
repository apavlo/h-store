package edu.mit.hstore.callbacks;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstore.MessageAcknowledgement;
import edu.mit.hstore.HStoreSite;

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
        if (LOG.isTraceEnabled()) LOG.trace("Got back FORWARD_TXN response from " + HStoreSite.getSiteName(parameter.getSenderSiteId()) + ". Sending response to client " +
                                            "[bytes=" + parameter.getData().size() + "]");
        byte data[] = parameter.getData().toByteArray();
        try {
            this.orig_callback.run(data);
        } catch (AssertionError ex) {
            FastDeserializer fds = new FastDeserializer(data);
            ClientResponseImpl cresponse = null;
            long txn_id = -1;
            try {
                cresponse = fds.readObject(ClientResponseImpl.class);
                txn_id = cresponse.getTransactionId();
            } catch (IOException e) {
                LOG.fatal("We're really falling apart here!", e);
            }
            LOG.fatal("Failed to forward ClientResponse data back for txn #" + txn_id, ex);
            // throw ex;
        }
    }
    
}
