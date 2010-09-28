package ca.evanjones.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.Descriptors.MethodDescriptor;

import ca.evanjones.db.Transactions.*;
import ca.evanjones.protorpc.NullCallback;
import ca.evanjones.protorpc.ProtoMethodInvoker;
import ca.evanjones.protorpc.ProtoRpcController;
import ca.evanjones.protorpc.ServiceRegistry;

public class TransactionDistributor extends TransactionService {
    private final ArrayList<DtxnChannel> partitions = new ArrayList<DtxnChannel>();
    private final HashMap<Integer, DtxnController> transactions =
            new HashMap<Integer, DtxnController>();
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();

    private static final class DtxnChannel implements RpcChannel {
        private final int index;
        private final TransactionService service;

        public DtxnChannel(int index, TransactionService service) {
            this.index = index;
            this.service = service;
        }

        @Override
        public void callMethod(MethodDescriptor method,
                RpcController controller, Message request,
                Message responsePrototype, RpcCallback<Message> done) {
            // Record that this is a participant
            DtxnController dtxn = (DtxnController) controller;

            RpcCallback<TransactionResult> callback =
                    dtxn.addParticipantRequest(index, responsePrototype, done);

            // TODO: Wrap the callback to catch aborts
            TransactionRequest transactionRequest = TransactionRpcChannel.makeTransactionRequest(
                    dtxn.getTransactionId(), method, request);
            // TODO: Create specific controllers? Re-use them?
            service.request(new ProtoRpcController(), transactionRequest, callback);
        }

        public TransactionService getService() {
            return service;
        }
    }

    private final class DtxnController implements RpcController, RpcCallback<Message> {
        public DtxnController(int transactionId) {
            this.transactionId = transactionId;
        }

        private final class DtxnSubRequestCallback implements RpcCallback<TransactionResult> {
            private final int participantIndex;
            private final Message responsePrototype;
            private final RpcCallback<Message> callback;

            public DtxnSubRequestCallback(int participantIndex, Message responsePrototype,
                    RpcCallback<Message> callback) {
                this.participantIndex = participantIndex;
                this.responsePrototype = responsePrototype;
                this.callback = callback;
            }

            @Override
            public void run(TransactionResult result) {
                if (status != Status.PREPARED) {
                    // someone else already aborted: ignore this
                    return;
                }

                Message response = null;
                if (result.hasResponse()) {
                    try {
                        Message.Builder builder = responsePrototype.newBuilderForType();
                        builder.mergeFrom(result.getResponse());
                        response = builder.build();
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    // missing response: must be aborting
                    assert result.getStatus() != Status.PREPARED;
                }

                if (result.getStatus() != Status.PREPARED) {
                    abort(result.getStatus(), participantIndex, response);
                } else {
                    callback.run(response);
                }
            }
        }

        public RpcCallback<TransactionResult> addParticipantRequest(
                int participantIndex, Message responsePrototype, RpcCallback<Message> done) {
            assert status == Status.PREPARED;
            participants.add(participantIndex);
            return new DtxnSubRequestCallback(participantIndex, responsePrototype, done);
        }

        public int getTransactionId() {
            return transactionId;
        }

        @Override
        public String errorText() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean failed() {
            throw new UnsupportedOperationException();
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

        public void run(Message result) {
            // Reset the callback in case the callback causes more work to occur
            RpcCallback<TransactionResult> callback = requestCallback;
            requestCallback = null;

            TransactionResult.Builder transactionResult = TransactionResult.newBuilder();
            if (result != null) {
                transactionResult.setResponse(result.toByteString());
            }
            transactionResult.setStatus(status);
            callback.run(transactionResult.build());
        }

        public void startParentRequest(RpcCallback<TransactionResult> done) {
            assert requestCallback == null;
            requestCallback = done;
        }

        public HashSet<Integer> getParticipants() {
            return participants;
        }

        /** Called when a participant aborts. Aborts all other participants. */
        private void abort(Status status, int index, Message result) {
            assert status != Status.PREPARED;
            assert this.status == Status.PREPARED;
            this.status = status;

            // Remove the participant (already aborted)
            boolean success = participants.remove(index);
            assert success;

            // Call the request callback to return the abort
            requestCallback.run(TransactionServer.makeTransactionResult(result, status));

            // Abort all participants
            // TODO: Cancel outstanding transactions?
            FinishRequest.Builder builder = FinishRequest.newBuilder();
            builder.setTransactionId(transactionId);
            builder.setCommit(false);
            FinishRequest abortRequest = builder.build();
            localFinish(abortRequest);
        }

        private final int transactionId;
        private RpcCallback<TransactionResult> requestCallback;
        private final HashSet<Integer> participants = new HashSet<Integer>();
        private Status status = Status.PREPARED;
    }

    public TransactionDistributor(List<TransactionService> partitions) {
        for (TransactionService service : partitions) {
            this.partitions.add(new DtxnChannel(this.partitions.size(), service));
        }
    }

    public void register(Service service) {
        serviceRegistry.register(service);
    }

    public List<RpcChannel> getChannels() {
        return Collections.<RpcChannel>unmodifiableList(partitions);
    }

    @Override
    public void finish(RpcController controller, FinishRequest request,
            RpcCallback<FinishResult> done) {
        // respond to the request immediately: no need to wait for this to complete
        // TODO: Error checking?
        done.run(FinishResult.getDefaultInstance());
        localFinish(request);
    }

    private void localFinish(FinishRequest request) {
        // Get the participants and remove them from the map
        DtxnController dtxn = transactions.remove(request.getTransactionId());
        if (dtxn == null) {
            throw new IllegalArgumentException(
                    "no transaction with id: " + request.getTransactionId());
        }

        // Distribute the commit/abort to all participants
        for (Integer index : dtxn.getParticipants()) {
            // TODO: Allocate implementation-specific controllers? Reuse them?
            partitions.get(index).getService().finish(
                    new ProtoRpcController(), request, NullCallback.<FinishResult>getInstance());
        }
    }

    @Override
    public void request(RpcController controller, TransactionRequest request,
            RpcCallback<TransactionResult> done) {
        ProtoMethodInvoker invoker =
                serviceRegistry.getInvoker(request.getRequest().getMethodName());

        DtxnController dtxn = transactions.get(request.getTransactionId());
        if (dtxn == null) {
            dtxn = new DtxnController(request.getTransactionId());
            transactions.put(request.getTransactionId(), dtxn);
        }
        dtxn.startParentRequest(done);

        try {
            invoker.invoke(dtxn, request.getRequest().getRequest(), dtxn);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
    }
}
