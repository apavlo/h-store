package edu.brown.hstore.internal;

import java.nio.ByteBuffer;

import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;

import com.google.protobuf.RpcCallback;

public class InitializeRequestMessage extends InternalMessage {

    final ByteBuffer serializedRequest; 
    final long initiateTime;
    final Procedure catalog_proc;
    final ParameterSet procParams;
    final RpcCallback<ClientResponseImpl> clientCallback;
    
    
    public InitializeRequestMessage(ByteBuffer serializedRequest,
                                    long initiateTime,
                                    Procedure catalog_proc,
                                    ParameterSet procParams,
                                    RpcCallback<ClientResponseImpl> clientCallback) {
        
        assert(serializedRequest != null);
        assert(catalog_proc != null);
        assert(procParams != null);
        assert(clientCallback != null);
        
        this.serializedRequest = serializedRequest;
        this.initiateTime = initiateTime;
        this.catalog_proc = catalog_proc;
        this.procParams = procParams;
        this.clientCallback = clientCallback;
        
    }
    
    public ByteBuffer getSerializedRequest() {
        return (this.serializedRequest);
    }
    public long getInitiateTime() {
        return (this.initiateTime);
    }
    public Procedure getProcedure() {
        return (this.catalog_proc);
    }
    public ParameterSet getProcParams() {
        return (this.procParams);
    }
    public RpcCallback<ClientResponseImpl> getClientCallback() {
        return (this.clientCallback);
    }
    public long getClientHandle() {
        return StoredProcedureInvocation.getClientHandle(this.serializedRequest);
    }
}
