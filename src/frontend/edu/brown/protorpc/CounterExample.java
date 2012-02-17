package edu.brown.protorpc;

import java.net.InetSocketAddress;

import ca.evanjones.protorpc.Counter.CounterService;
import ca.evanjones.protorpc.Counter.GetRequest;
import ca.evanjones.protorpc.Counter.Value;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

class CounterExample extends CounterService {
    private int counter;

    @Override
    public void add(RpcController controller, Value request,
            RpcCallback<Value> done) {
        counter += request.getValue();
        System.out.println("add " + request.getValue() + " = " + counter);
        done.run(Value.newBuilder().setValue(counter).build());
    }

    @Override
    public void get(RpcController controller, GetRequest request,
            RpcCallback<Value> done) {
        System.out.println("get = " + counter);
        done.run(Value.newBuilder().setValue(counter).build());
    }

    public static void server(int port) {
        NIOEventLoop eventLoop = new NIOEventLoop();
        ProtoServer server = new ProtoServer(eventLoop);
        server.bind(port);
        CounterExample counter = new CounterExample();
        server.register(counter);
        eventLoop.setExitOnSigInt(true);
        eventLoop.run();
    }

    public static void client(String host, int port, boolean isGet, int increment) {
        // Set up the connection to the server
        NIOEventLoop eventLoop = new NIOEventLoop();
        ProtoRpcChannel[] channels = ProtoRpcChannel.connectParallel(eventLoop,
                new InetSocketAddress[] {new InetSocketAddress(host, port)});
//        ProtoRpcChannel channel = new ProtoRpcChannel(eventLoop, new InetSocketAddress(host, port));
        CounterService stub = CounterService.newStub(channels[0]);
        ProtoRpcController rpc = new ProtoRpcController();
        StoreResultCallback<Value> callback = new StoreResultCallback<Value>();

        // Build the request
        if (isGet) {
            stub.get(rpc, GetRequest.getDefaultInstance(), callback);
            rpc.block();
            System.out.println("get = " + callback.getResult().getValue());
        } else {
            stub.add(rpc, Value.newBuilder().setValue(increment).build(), callback);
            rpc.block();
            System.out.println("counter + " + increment + " = " + callback.getResult().getValue());
        }
    }

    public static void main(String[] args) {
        if (args.length == 1) {
            server(Integer.parseInt(args[0]));
        } else if (args.length == 2 || args.length == 3) {
            boolean isGet = args.length == 2;
            int increment = 0;
            if (!isGet) increment = Integer.parseInt(args[2]);
            client(args[0], Integer.parseInt(args[1]), isGet, increment);
        } else {
            System.err.println("[port]|[host port]|[host port increment]");
            System.exit(1);
        }
    }
}
