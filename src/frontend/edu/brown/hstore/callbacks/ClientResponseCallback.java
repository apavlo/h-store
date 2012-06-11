/**
 * 
 */
package edu.brown.hstore.callbacks;

import org.voltdb.ClientResponseImpl;
import org.voltdb.network.Connection;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.ClientInterface;

/**
 * Thin wrapper to sent a ClientResponse back to the client over a Connection handle
 * @author pavlo
 */
public class ClientResponseCallback implements RpcCallback<ClientResponseImpl> {

    private final ClientInterface clientInterface;
    private final Connection conn;
    private final int messageSize;
    
    public ClientResponseCallback(ClientInterface clientInterface, Connection conn, int messageSize) {
        this.clientInterface = clientInterface;
        this.conn = conn;
        this.messageSize = messageSize;
    }
    
    
    @Override
    public void run(ClientResponseImpl parameter) {
        boolean ret = this.conn.writeStream().enqueue(parameter);
        if (ret == false) {
            throw new RuntimeException("Unable to write ClientResponse on output stream?");
        }
        this.clientInterface.reduceBackpressure(this.messageSize);
    }

}
