package edu.brown.protorpc;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

public final class ProtoMethodInvoker {
    private final Service service;
    private final MethodDescriptor method;
    private final Message requestPrototype;

    public ProtoMethodInvoker(Service service, MethodDescriptor method) {
        this.service = service;
        this.method = method;
        requestPrototype = service.getRequestPrototype(method);
    }

    public void invoke(RpcController controller, ByteString request,
            RpcCallback<Message> callback) throws InvalidProtocolBufferException {
        Message.Builder builder = requestPrototype.newBuilderForType();
        builder.mergeFrom(request);

        service.callMethod(method, controller, builder.build(), callback);
    }
}
