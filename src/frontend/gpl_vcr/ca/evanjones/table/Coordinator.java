package ca.evanjones.table;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import ca.evanjones.db.TransactionDistributor;
import ca.evanjones.db.Transactions.TransactionService;
import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.ProtoServer;
import ca.evanjones.table.Table.*;

public class Coordinator extends TableService {
    private final int accountsPerPartition;
    private final ArrayList<TableService> partitions = new ArrayList<TableService>();

    public Coordinator(int maxAccount, List<RpcChannel> partitions) {
        // This function puts the excess accounts in the earlier partitions
        int partitionAccounts = maxAccount / partitions.size();
        if (maxAccount % partitions.size() != 0) {
            partitionAccounts += 1;
        }
        accountsPerPartition = partitionAccounts;

        for (RpcChannel channel : partitions) {
            this.partitions.add(TableService.newStub(channel));
        }
    }

    private int partitionFunction(int account) {
        return account / accountsPerPartition;
    }

    final static class MergeCallback<ParameterType extends Message> implements RpcCallback<ParameterType> {
        public MergeCallback(int numCallbacks, RpcCallback<ParameterType> finalCallback) {
            assert numCallbacks > 0;
            this.remaining = numCallbacks;
            this.finalCallback = finalCallback;
        }

        @SuppressWarnings("unchecked")
        public void run(ParameterType parameter) {
            if (builder == null) {
                builder = parameter.newBuilderForType();
            }
            builder.mergeFrom(parameter);

            assert remaining > 0;
            remaining -= 1;
            if (remaining == 0) {
                finalCallback.run((ParameterType) builder.build());
            }
        }

        private Message.Builder builder;
        private int remaining;
        private final RpcCallback<ParameterType> finalCallback;
    }

    @Override
    public void read(RpcController controller, ReadRequest request,
            RpcCallback<ReadResult> done) {
        int firstPartition = partitionFunction(request.getId(0));
        boolean onePartition = true;
        for (int i = 1; i < request.getIdCount(); ++i) {
            if (partitionFunction(request.getId(i)) != firstPartition) {
                onePartition = false;
                break;
            }
        }
        if (onePartition) {
            // Only one partition: dispatch directly
            partitions.get(firstPartition).read(controller, request, done);
            return;
        }

        HashMap<Integer, ReadRequest.Builder> partitionRequests =
                new HashMap<Integer, ReadRequest.Builder>();
        for (Integer accountId : request.getIdList()) {
            int partition = partitionFunction(accountId);
            ReadRequest.Builder partitionRequest = partitionRequests.get(partition);
            if (partitionRequest == null) {
                partitionRequest = ReadRequest.newBuilder();
                partitionRequests.put(partition, partitionRequest);
            }
            partitionRequest.addId(accountId);
        }

        MergeCallback<ReadResult> merger = new MergeCallback<ReadResult>(partitionRequests.size(), done);
        for (Entry<Integer, ReadRequest.Builder> e : partitionRequests.entrySet()) {
            partitions.get(e.getKey()).read(controller, e.getValue().build(), merger);
        }
    }

    @Override
    public void write(RpcController controller, WriteRequest request,
            RpcCallback<WriteResult> done) {
        int firstPartition = partitionFunction(request.getAccount(0).getId());
        boolean onePartition = true;
        for (int i = 1; i < request.getAccountCount(); ++i) {
            if (partitionFunction(request.getAccount(i).getId()) != firstPartition) {
                onePartition = false;
                break;
            }
        }
        if (onePartition) {
            // Only one partition: dispatch directly
            partitions.get(firstPartition).write(controller, request, done);
            return;
        }

        HashMap<Integer, WriteRequest.Builder> partitionRequests =
                new HashMap<Integer, WriteRequest.Builder>();
        for (Account account : request.getAccountList()) {
            int partition = partitionFunction(account.getId());
            WriteRequest.Builder partitionRequest = partitionRequests.get(partition);
            if (partitionRequest == null) {
                partitionRequest = WriteRequest.newBuilder();
                partitionRequests.put(partition, partitionRequest);
            }
            partitionRequest.addAccount(account);
        }

        MergeCallback<WriteResult> merger = new MergeCallback<WriteResult>(partitionRequests.size(), done);
        for (Entry<Integer, WriteRequest.Builder> e : partitionRequests.entrySet()) {
            partitions.get(e.getKey()).write(controller, e.getValue().build(), merger);
        }
    }

    public static void main(String[] arguments) {
        NIOEventLoop eventLoop = new NIOEventLoop();

        // Connect to the servers
        ArrayList<TransactionService> partitions = new ArrayList<TransactionService>();
        for (int i = 1; i < arguments.length; ++i) {
            String[] parts = arguments[i].split(":");
            assert parts.length == 2;
            int port = Integer.parseInt(parts[1]);

            ProtoRpcChannel channel = new ProtoRpcChannel(
                    eventLoop, new InetSocketAddress(parts[0], port));
            partitions.add(TransactionService.newStub(channel));
        }

        TransactionDistributor distributor = new TransactionDistributor(partitions);
        Coordinator coordinator = new Coordinator(Benchmark.MAX_ACCOUNT, distributor.getChannels());
        distributor.register(coordinator);

        // Listen for RPC requests
        eventLoop.setExitOnSigInt(true);
        ProtoServer server = new ProtoServer(eventLoop);
        server.bind(Integer.parseInt(arguments[0]));
        server.register(distributor);
        eventLoop.run();
    }
}
