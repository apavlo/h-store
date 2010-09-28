package ca.evanjones.table;

import java.util.ArrayList;

import ca.evanjones.db.TransactionDistributor;
import ca.evanjones.db.TransactionServer;
import ca.evanjones.db.Transactions.TransactionService;
import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoServer;
import ca.evanjones.protorpc.ThreadChannel;

public class LocalPartitions {
    public static void main(String[] arguments) {
        NIOEventLoop eventLoop = new NIOEventLoop();

        int numPartitions = Integer.parseInt(arguments[1]);

        // Create local partitions in threads inside this JVM
        ArrayList<Server> tables = new ArrayList<Server>();
        ArrayList<ThreadChannel> threads = new ArrayList<ThreadChannel>();
        ArrayList<TransactionService> partitions = new ArrayList<TransactionService>();
        for (int i = 0; i < numPartitions; ++i) {
            tables.add(new Server());
            TransactionServer server = new TransactionServer(tables.get(i));
            threads.add(new ThreadChannel(eventLoop, server));
            threads.get(i).start();
            partitions.add(TransactionService.newStub(threads.get(i)));
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

        for (int i = 0; i < numPartitions; ++i) {
            threads.get(i).shutDownThread();
        }

        int total = 0;
        for (int i = 0; i < numPartitions; ++i) {
            try {
                threads.get(i).join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            total += tables.get(i).dumpBalances();
        }
        System.out.println("TOTAL: " + total);
    }
}
