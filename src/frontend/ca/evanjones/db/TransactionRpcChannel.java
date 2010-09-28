package ca.evanjones.db;

import ca.evanjones.db.Transactions.*;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.Protocol.RpcRequest;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.Descriptors;
import com.google.protobuf.RpcController;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;


public class TransactionRpcChannel implements RpcChannel {
    private final TransactionService serverStub;
    private final int clientId;
    private int transactionId;

    /** @param clientId an integer unique for this client. */
    public TransactionRpcChannel(TransactionService serverStub, int clientId) {
        this.serverStub = serverStub;
        this.clientId = clientId;
        // TODO: Use 64-bit transaction ids? Assign them somehow?
        assert clientId < (1 << 15);
    }

    public final class ClientRpcController implements RpcController, RpcCallback<TransactionResult> {
        public ClientRpcController(int id) {
            this.id = id;
        }

        void startRpc(Message responsePrototype, RpcCallback<Message> responseCallback) {
            // TODO: Should we require an explicit call  to .reset() after an abort?
            if (status == null) {
                status = Status.PREPARED;
            }

            assert this.responsePrototype == null;
            assert this.responseCallback == null;
            this.responsePrototype = responsePrototype;
            this.responseCallback = responseCallback;
        }

        @Override
        public String errorText() {
            throw new UnsupportedOperationException();
        }

        /** Note: aborted is not failed. An abort is a successful request. Check isAborted(). */
        @Override
        public boolean failed() {
            return rpc.failed();
        }

        @Override
        public boolean isCanceled() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyOnCancel(RpcCallback<Object> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setFailed(String reason) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void startCancel() {
            throw new UnsupportedOperationException();
        }

        public void setAbort(Status status) {
            assert this.status == Status.PREPARED;
            assert status != Status.PREPARED;
            this.status = status;
        }

        public void block() { rpc.block(); }

        public int getId() { return id; }
        public ProtoRpcController getRpc() { return rpc; }

        public void commit(final Runnable callback) {
            if (status != Status.PREPARED) {
                throw new IllegalStateException("Transaction is not active: cannot commit.");
            }

            FinishRequest.Builder finish = FinishRequest.newBuilder();
            finish.setTransactionId(id);
            finish.setCommit(true);

            RpcCallback<FinishResult> wrapper = new RpcCallback<FinishResult>() {
                @Override
                public void run(FinishResult parameter) {
                    assert parameter != null;
                    if (callback != null) callback.run();

                    // Reset status to null, so we indicate that the transaction completed.
                    assert status == Status.PREPARED;
                    status = null;
                }
            };
            serverStub.finish(rpc, finish.build(), wrapper);
        }

        // Called when one request completes
        @Override
        public void run(TransactionResult result) {
            assert !rpc.failed();

            // This might be an abort: copy the status; response might be missing for an abort
            status = result.getStatus();
            Message responseMessage = null;
            if (result.hasResponse()) {
                try {
                    Message.Builder response =
                            responsePrototype.newBuilderForType().mergeFrom(result.getResponse());
                    responseMessage = response.build();
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            } else {
                if (status == Status.PREPARED) {
                    // TODO: define our own exception types for this?
                    throw new IllegalArgumentException("Missing response: " +
                            "must have a response if the transaction is PREPARED");
                }
            }
            responseCallback.run(responseMessage);

            responsePrototype = null;
            responseCallback = null;
        }

        public boolean isAborted() { return status != Status.PREPARED; }

        private final int id;
        private final ProtoRpcController rpc = new ProtoRpcController();
        private Status status;
        private Message responsePrototype;
        private RpcCallback<Message> responseCallback;
    }

    public void callMethod(Descriptors.MethodDescriptor method,
            RpcController controller, Message request,
            Message responsePrototype, RpcCallback<Message> done) {
        ClientRpcController transaction = (ClientRpcController) controller;

        // Bundle the RPC request in a TransactionRequest
        TransactionRequest transactionRequest = makeTransactionRequest(
                transaction.getId(), method, request);

        transaction.startRpc(responsePrototype, done);
        serverStub.request(transaction.getRpc(), transactionRequest, transaction);
    }

    public static TransactionRequest makeTransactionRequest(int transactionId,
            Descriptors.MethodDescriptor method, Message request) {
        RpcRequest rpcRequest = ProtoRpcChannel.makeRpcRequest(0, method, request);
        TransactionRequest.Builder transactionRequest = TransactionRequest.newBuilder();
        transactionRequest.setTransactionId(transactionId);
        transactionRequest.setRequest(rpcRequest);
        return transactionRequest.build();
    }

    public ClientRpcController newController() {
        // Assign transactions sequential ids.
        int id = clientId << 16 | transactionId;
        transactionId += 1;
        return new ClientRpcController(id);
    }
}
