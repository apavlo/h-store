package edu.brown.protorpc;

import java.util.concurrent.LinkedBlockingQueue;

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;

/** Runs a service in a separate thread. */
public class ThreadChannel extends Thread implements RpcChannel {
    public final class TableTask implements RpcCallback<Message>, Runnable {
        private final MethodDescriptor method;
        private final Message request;
        private final RpcCallback<Message> finalCallback;
        private Message response;

        public TableTask(MethodDescriptor method, Message request,
                RpcCallback<Message> finalCallback) {
            this.method = method;
            this.request = request;
            this.finalCallback = finalCallback;
        }

        public RpcCallback<Message> getFinalCallback() { return finalCallback; }

        // RpcCallback: used when finished processing inside this thread
        @Override
        public void run(Message parameter) {
            assert response == null;
            assert parameter != null;
            response = parameter;
//            System.out.println("scheduling callback in original thread");
            outputEventLoop.runInEventThread(this);
        }

        // Callback: used to call the finalCallback in the EventLoop thread
        @Override
        public void run() {
//            System.out.println("calling callback in original thread");
            assert response != null;
            finalCallback.run(response);
            response = null;
        }
    }

    private final LinkedBlockingQueue<TableTask> inputQueue = new LinkedBlockingQueue<TableTask>();
//    private final TableService table = new TableServer();
    private final EventLoop outputEventLoop;
    private final Service service;

    public ThreadChannel(EventLoop outputEventLoop, Service service) {
        this.outputEventLoop = outputEventLoop;
        this.service = service;
    }

    public void run() {
        while (true) {
            TableTask task;
            try {
                task = inputQueue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (task.request == null) {
                // TODO: Abort outstanding transactions?
                return;
            }

//            System.out.println("calling task in thread");
            service.callMethod(task.method, null, task.request, task);
        }
    }

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller,
            Message request, Message responsePrototype,
            RpcCallback<Message> done) {
//        System.out.println("added task to thread");
        inputQueue.add(new TableTask(method, request, done));
    }

    public void shutDownThread() {
        inputQueue.add(new TableTask(null, null, null));
    }
}
