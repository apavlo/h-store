package edu.brown.protorpc;

import java.util.HashMap;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Service;


/** Registers methods and invokes callbacks. */
public final class ServiceRegistry {
    public void register(Service service) {
        // TODO: Support registering multiple local services? Needs "local 2PC" effectively. Yuck.
        Descriptors.ServiceDescriptor descriptor = service.getDescriptorForType();
        for (MethodDescriptor i : descriptor.getMethods()) {
            if (methods.containsKey(i.getFullName())) {
                throw new IllegalStateException(
                        "method " + i.getFullName() + " is already registered");
            }
            methods.put(i.getFullName(), new ProtoMethodInvoker(service, i));
        }
    }

    public ProtoMethodInvoker getInvoker(String fullMethodName) {
        ProtoMethodInvoker invoker = methods.get(fullMethodName);
        if (invoker == null) {
            throw new RuntimeException("method not found: " + fullMethodName);
        }
        return invoker;
    }

    private final HashMap<String, ProtoMethodInvoker> methods =
        new HashMap<String, ProtoMethodInvoker>();
}
