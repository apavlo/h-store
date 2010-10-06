package ca.evanjones.table;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Random;

import ca.evanjones.db.TransactionRpcChannel;
import ca.evanjones.db.Transactions.TransactionService;
import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoRpcChannel;
import ca.evanjones.protorpc.StoreResultCallback;
import ca.evanjones.table.Table.*;
import edu.mit.benchmark.ThreadBench;

public class Benchmark {
    private static final int INITIAL_BALANCE = 10000;
    public static final int MAX_ACCOUNT = 100;
    private static final int WARM_UP_SECONDS = 5;
    private static final int MEASURE_SECONDS = 30;

    public Benchmark(String host, int port, int id) {
        ProtoRpcChannel channel = new ProtoRpcChannel(
                eventLoop, new InetSocketAddress(host, port));
        transactionChannel = new TransactionRpcChannel(TransactionService.newStub(channel), id);

        table = TableService.newStub(transactionChannel);
        rpc = transactionChannel.newController();
    }

    private int readBalance(int account_id) {
        ReadRequest.Builder read = ReadRequest.newBuilder().addId(account_id);
        table.read(rpc, read.build(), readCallback);
        rpc.block();
        assert !rpc.failed();
        assert !rpc.isAborted();
        assert readCallback.getResult().getAccountCount() == 1;
        assert readCallback.getResult().getAccount(0).getId() == account_id;
        return readCallback.getResult().getAccount(0).getBalance();
    }

    private void writeAccount(int account_id, int new_balance) {
        WriteRequest.Builder write = WriteRequest.newBuilder();
        write.addAccount(Account.newBuilder().setId(account_id).setBalance(new_balance).build());
        StoreResultCallback<WriteResult> writeCallback = new StoreResultCallback<WriteResult>();
        table.write(rpc, write.build(), writeCallback);
        rpc.block();
        assert !rpc.failed();
        assert !rpc.isAborted();
    }

    private void increment(int account_id, int amount) {
        int new_balance = readBalance(account_id);
        new_balance += amount;
        assert new_balance >= 0;
        writeAccount(account_id, new_balance);
    }

    private void transfer(int source_account, int destination_account, int amount) {
        assert amount > 0;
        assert source_account != destination_account;

        // Read the balances
        ReadRequest.Builder read = ReadRequest.newBuilder();
        read.addId(source_account);
        read.addId(destination_account);
        table.read(rpc, read.build(), readCallback);
        rpc.block();
        assert !rpc.failed();
        if (rpc.isAborted()) {
            // retry in case of deadlock
            transfer(source_account, destination_account, amount);
            return;
        }
        assert readCallback.getResult().getAccountCount() == 2;
        int sourceBalance;
        int destinationBalance;
        if (readCallback.getResult().getAccount(0).getId() == source_account) {
            sourceBalance = readCallback.getResult().getAccount(0).getBalance();
            destinationBalance = readCallback.getResult().getAccount(1).getBalance();
        } else {
            sourceBalance = readCallback.getResult().getAccount(1).getBalance();
            destinationBalance = readCallback.getResult().getAccount(0).getBalance();
        }

        // Do the transfer
        sourceBalance -= amount;
        assert sourceBalance >= 0;
        destinationBalance += amount;

        // Update the balances
        WriteRequest.Builder write = WriteRequest.newBuilder();
        write.addAccount(Account.newBuilder().
                setId(source_account).setBalance(sourceBalance).build());
        write.addAccount(Account.newBuilder().
                setId(destination_account).setBalance(destinationBalance).build());
        table.write(rpc, write.build(), writeCallback);
        rpc.block();
        assert !rpc.failed();
        if (rpc.isAborted()) {
            // retry in case of deadlock
            transfer(source_account, destination_account, amount);
            return;
        }

        // Commit and block waiting for response
        rpc.commit(null);
        rpc.block();
        assert !rpc.failed();
    }

    public void loadInitialData() {
        WriteRequest.Builder write = WriteRequest.newBuilder();
        for (int i = 0; i < MAX_ACCOUNT; ++i) {
            Account.Builder account = Account.newBuilder();
            account.setId(i);
            account.setBalance(INITIAL_BALANCE);
            write.addAccount(account);
        }

        table.write(rpc, write.build(), writeCallback);
        rpc.block();
        assert !rpc.failed();

        rpc.commit(null);
        rpc.block();
        assert !rpc.failed();
    }

    private static class BenchmarkWorker extends ThreadBench.Worker {
        private final Random rng = new Random();
        private final Benchmark client;

        public BenchmarkWorker(Benchmark client) {
            this.client = client;
        }

        @Override
        protected void doWork(boolean measure) {
            client.randomTransfer(rng);
        }
    }

    public void randomTransfer(Random rng) {
        int source = rng.nextInt(MAX_ACCOUNT);
        // select destination != source: one less account to choose from, then skip it
        int destination = rng.nextInt(MAX_ACCOUNT-1);
        if (destination == source) {
            destination = MAX_ACCOUNT - 1;
        }
        transfer(source, destination, 1);
    }

    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int threads = Integer.parseInt(args[2]);

        ArrayList<Benchmark> clients = new ArrayList<Benchmark>();
        ArrayList<BenchmarkWorker> benchmarkWorkers = new ArrayList<BenchmarkWorker>();
        for (int i = 0; i < threads; ++i) {
            clients.add(new Benchmark(host, port, i));
            benchmarkWorkers.add(new BenchmarkWorker(clients.get(i)));
        }

        // Load the initial database
        clients.get(0).loadInitialData();

        // Run the test
        ThreadBench.Results results =
                ThreadBench.runBenchmark(benchmarkWorkers, WARM_UP_SECONDS, MEASURE_SECONDS);
        System.out.println(results.getRequestsPerSecond() + " transactions/second");
    }

    private final NIOEventLoop eventLoop = new NIOEventLoop();
    private final TransactionRpcChannel transactionChannel;
    private final TableService table;

    private final TransactionRpcChannel.ClientRpcController rpc;
    private final StoreResultCallback<ReadResult> readCallback = new StoreResultCallback<ReadResult>();
    private final StoreResultCallback<WriteResult> writeCallback = new StoreResultCallback<WriteResult>();
}
