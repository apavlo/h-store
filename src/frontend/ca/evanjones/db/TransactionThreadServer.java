package ca.evanjones.db;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import ca.evanjones.db.Transactions.FinishRequest;
import ca.evanjones.db.Transactions.FinishResult;
import ca.evanjones.db.Transactions.Status;
import ca.evanjones.db.Transactions.TransactionRequest;
import ca.evanjones.db.Transactions.TransactionResult;
import ca.evanjones.db.Transactions.TransactionService;
import ca.evanjones.protorpc.EventLoop;
import ca.evanjones.protorpc.ProtoMethodInvoker;
import ca.evanjones.protorpc.ServiceRegistry;

/** Runs each transaction in its own thread, acquiring locks in a blocking fashion. */
public class TransactionThreadServer extends TransactionService {
    public TransactionThreadServer(EventLoop outputEventLoop, TransactionalService service) {
        this.outputEventLoop = outputEventLoop;
        this.service = service;

        // TODO: Support registering multiple local services? Needs "local 2PC" effectively. Yuck.
        serviceRegistry.register(service);
    }

    // TODO: Reuse threads.
    private final class StopThreadCallback implements RpcCallback<FinishResult> {
        private final TransactionThread thread;
        private final RpcCallback<FinishResult> finalCallback;

        public StopThreadCallback(TransactionThread thread,
                RpcCallback<FinishResult> finalCallback) {
            this.thread = thread;
            this.finalCallback = finalCallback;
        }

        public void run(FinishResult result) {
            // run the original callback
            finalCallback.run(result);

            // recycle the thread
            recycleThread(thread);
        }
    }

    @Override
    public void finish(RpcController controller, FinishRequest request,
            RpcCallback<FinishResult> done) {
        TransactionThread thread = transactionThreads.remove(request.getTransactionId());

        if (thread == null) {
            // This transaction must have already been aborted
            done.run(FinishResult.getDefaultInstance());
            return;
        }

        // TODO: Call done immediately? We would need to handle multiple requests in the thread ...
//        System.out.println("finish " + done);
        thread.addTask(request, new StopThreadCallback(thread, done));
    }

    @Override
    public void request(RpcController controller, TransactionRequest request,
            RpcCallback<TransactionResult> done) {
        TransactionThread thread = transactionThreads.get(request.getTransactionId());
        if (thread == null) {
            thread = allocateThread(request.getTransactionId());
        }

//        System.out.println("request " + done);
        thread.addTask(request, done);
    }

    public ProtoMethodInvoker getInvoker(String fullMethodName) {
        return serviceRegistry.getInvoker(fullMethodName);
    }

    public void runInThread(Runnable callback) {
        outputEventLoop.runInEventThread(callback);
    }

    // Called from TransactionThread when finishing a transaction
    public void finishTransaction(TransactionRpcController transaction, boolean commit) {
        service.finish(transaction, commit);
    }

    // Called in the main thread when a transaction aborts locally.
    public void localAbort(int transactionId) {
        recycleThread(transactionThreads.remove(transactionId));
    }

    /** Attempts to stop all threads. */
    public void shutdown() {
        for (TransactionThread thread : idleTransactionThreads) {
            thread.shutDownThread();
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        idleTransactionThreads.clear();

        // TODO: Should we gracefully shut down active transactions?
        assert transactionThreads.isEmpty();
    }

    private TransactionThread allocateThread(int transactionId) {
        TransactionThread thread = idleTransactionThreads.pollLast();
        if (thread == null) {
            thread = new TransactionThread(this);
            thread.start();
        }

        thread.reset(transactionId);
        TransactionThread previous = transactionThreads.put(transactionId, thread);
        assert previous == null;
        return thread;
    }

    private void recycleThread(TransactionThread thread) {
        assert thread != null;
        idleTransactionThreads.add(thread);
    }

    private final EventLoop outputEventLoop;
    private final TransactionalService service;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    private final HashMap<Integer, TransactionThread> transactionThreads =
            new HashMap<Integer, TransactionThread>();
    private final ArrayDeque<TransactionThread> idleTransactionThreads =
            new ArrayDeque<TransactionThread>();
}

final class TransactionThread extends Thread implements RpcCallback<Message> {
    public TransactionThread(TransactionThreadServer server) {
        this.server = server;
    }

    public synchronized void reset(int transactionId) {
        rpc.reset();
        rpc.startTransaction(transactionId);
        request = null;
        response = null;
        finalCallback = null;
    }

    @Override
    public void run() {
        while (true) {
            // Wait for the next request
            synchronized (this) {
                while (request == null) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            assert request != null;

            // Abuse the request and response fields to indicate when we should exit
            if (request == response) {
                break;
            }
            assert finalCallback != null;

            // Set request to null immediately because the callbacks can trigger the next request
            // before we have time to clear it
            Message currentRequest = request;
            request = null;

            if (currentRequest instanceof TransactionRequest) {
                TransactionRequest transactionRequest = (TransactionRequest) currentRequest;
                assert rpc.getId() == transactionRequest.getTransactionId();
                ProtoMethodInvoker invoker = server.getInvoker(
                        transactionRequest.getRequest().getMethodName());
                try {
                    invoker.invoke(rpc, transactionRequest.getRequest().getRequest(), this);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                } catch (Transaction.DeadlockException e) {
                    // This transaction was killed due to a deadlock: set the abort and return it
                    rpc.setAbort(Status.ABORT_DEADLOCK);
                    run(null);
                }
            } else if (currentRequest instanceof FinishRequest) {
                FinishRequest finish = (FinishRequest) currentRequest;
                localFinish(finish.getCommit());
                response = FinishResult.getDefaultInstance();
                server.runInThread(returnResultCallback);
            } else {
                throw new IllegalArgumentException("unsupported message type");
            }
        }
    }

    private void localFinish(boolean commit) {
        // Must apply undo before releasing locks, otherwise others could see our writes!
        server.finishTransaction(rpc, commit);
        List<Transaction> granted = rpc.synchronizedReleaseLocks();
        for (Transaction g : granted) {
            g.unblockLockGranted();
        }
    }

    public void shutDownThread() {
        // Abuse the response field to signal the thread to exit
        synchronized (this) {
            assert request == null;
            assert response == null;
            assert finalCallback == null;
            response = FinishResult.getDefaultInstance();
            request = response;
            notify();
        }
    }

    public void addTask(Message request, RpcCallback<? extends Message> finalCallback) {
        assert request != null;
        assert finalCallback != null;

        synchronized (this) {
            assert this.request == null;
            assert this.response == null;
            assert this.finalCallback == null;

            this.request = request;
            this.finalCallback = finalCallback;
            this.notify();
        }
    }

    /** Response callback from the service running in this thread: queue on event loop. */
    @Override
    public void run(Message response) {
        assert this.response == null;
        assert finalCallback != null;
        if (rpc.getStatus() != Status.PREPARED) {
            // This is a local abort: complete the abort
            localFinish(false);
        }

        this.response = TransactionServer.makeTransactionResult(response, rpc.getStatus());
        server.runInThread(returnResultCallback);
    }

    private final TransactionRpcController rpc = new TransactionRpcController();
    private final TransactionThreadServer server;
    // Wraps this to implement Callback (avoids conflict with Thread.run).
    private final Runnable returnResultCallback = new Runnable() {
        public void run() {
            Message currentResponse = response;
            @SuppressWarnings("unchecked")
            RpcCallback<Message> callback = (RpcCallback<Message>) finalCallback;

            // Must set to null before calling callback: may call back to this class
            response = null;
            finalCallback = null;

            if (rpc.getStatus() != Status.PREPARED) {
                // This is an abort response: remove this transaction
                server.localAbort(rpc.getId());
            }

            callback.run(currentResponse);
        }
    };

    private Message request;
    private Message response;
    private RpcCallback<? extends Message> finalCallback;
}
