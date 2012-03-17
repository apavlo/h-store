package edu.brown.hstore.callbacks;

import com.google.protobuf.RpcCallback;

public class MockClientCallback implements RpcCallback<byte[]> {
    private byte response[];
    
    @Override
    public void run(byte[] parameter) {
        this.response = parameter;
    }
    
    public byte[] getResponse() {
        return (this.response);
    }

}
