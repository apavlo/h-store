package ca.evanjones.protorpc;

import java.net.InetSocketAddress;

import ca.evanjones.db.*;
import ca.evanjones.db.Transactions.TransactionService;

public abstract class ProtoClient {
    protected final NIOEventLoop eventLoop = new NIOEventLoop();
    protected final TransactionRpcChannel transactionChannel;
    protected final TransactionRpcChannel.ClientRpcController rpc;
    
    /**
     * Constructor
     * @param host
     * @param port
     * @param id
     */
    public ProtoClient(String host, int port, int id) {
        InetSocketAddress address = new InetSocketAddress(host, port);
        ProtoRpcChannel channel = new ProtoRpcChannel(this.eventLoop, address);
        this.transactionChannel = new TransactionRpcChannel(TransactionService.newStub(channel), id);
        this.rpc = transactionChannel.newController();
        assert(this.rpc != null);
    }
    
    /**
     * Get the EventLoop
     * @return
     */
    public NIOEventLoop getEventLoop() {
        return this.eventLoop;
    }
    
    /**
     * Get the TransactionRpcChannel
     * @return
     */
    public TransactionRpcChannel getTransactionChannel() {
        return this.transactionChannel;
    }
    
    /**
     * Get the ClientRpcController
     * @return
     */
    public TransactionRpcChannel.ClientRpcController getRPC() {
        return this.rpc;
    }
    
}
