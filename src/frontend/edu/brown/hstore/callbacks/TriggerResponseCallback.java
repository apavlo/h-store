/**
 * 
 */
package edu.brown.hstore.callbacks;

import org.voltdb.ClientResponseImpl;
import org.voltdb.exceptions.ClientConnectionLostException;
import org.voltdb.network.Connection;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.ClientInterface;

/**
 * Thin wrapper to sent a ClientResponse back to the client over a Connection handle
 * @author pavlo
 */
public class TriggerResponseCallback implements RpcCallback<ClientResponseImpl> {

    public TriggerResponseCallback() {
    }
    
    
    @Override
    public void run(ClientResponseImpl parameter) {
    }

}
