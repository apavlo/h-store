/**
 * 
 */
package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.network.Connection;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.ClientInterface;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public class ClientResponseCallback implements RpcCallback<ClientResponseImpl> {
    private static final Logger LOG = Logger.getLogger(ClientResponseCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

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
        LOG.info("Sending back ClientResponse to " + this.conn.getHostname() + "\n" + parameter);
        boolean ret = this.conn.writeStream().enqueue(parameter);
        if (ret == false) {
            throw new RuntimeException("Unable to write ClientResponse on output stream?");
        }
        this.clientInterface.reduceBackpressure(this.messageSize);
    }

}
