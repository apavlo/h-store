package edu.brown.hstore.callbacks;

import org.voltdb.ClientResponseImpl;

import com.google.protobuf.RpcCallback;

public class MockClientCallback implements RpcCallback<ClientResponseImpl> {
    private ClientResponseImpl response;
    
    @Override
    public void run(ClientResponseImpl parameter) {
        this.response = parameter;
    }
    
    public ClientResponseImpl getResponse() {
        return (this.response);
    }

}
