/**
 * 
 */
package edu.brown.hstore.callbacks;

import java.nio.ByteBuffer;

import org.voltdb.network.Connection;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.ClientInterface;

/**
 * @author pavlo
 */
public class ClientResponseCallback implements RpcCallback<byte[]> {

    private final ClientInterface clientInterface;
    private final Connection conn;
    private final int messageSize;
    
    public ClientResponseCallback(ClientInterface clientInterface, Connection conn, int messageSize) {
        this.clientInterface = clientInterface;
        this.conn = conn;
        this.messageSize = messageSize;
    }
    
    
    @Override
    public void run(byte[] parameter) {
        // TODO: Switch to direct ByteBuffer input
        conn.writeStream().enqueue(ByteBuffer.wrap(parameter));
        this.clientInterface.reduceBackpressure(this.messageSize);
    }

}
