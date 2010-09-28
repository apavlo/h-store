package ca.evanjones.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import ca.evanjones.db.Lock;
import ca.evanjones.db.TransactionRpcController;
import ca.evanjones.db.TransactionServer;
import ca.evanjones.db.TransactionalService;
import ca.evanjones.protorpc.NIOEventLoop;
import ca.evanjones.protorpc.ProtoServer;
import ca.evanjones.table.Table.*;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

public class Server extends TableService implements TransactionalService {
    private final HashMap<Integer, LockBalance> accounts = new HashMap<Integer, LockBalance>();

    private final static class LockBalance {
        public final Lock lock = new Lock();
        public int balance;
    }

    @Override
    public void read(RpcController controller, ReadRequest request,
            RpcCallback<ReadResult> done) {
        TransactionRpcController transaction = (TransactionRpcController) controller;

        ReadResult.Builder response = ReadResult.newBuilder();
        for (Integer account_id : request.getIdList()) {
            // TODO: For serializability, this lookup needs to acquire a lock
            LockBalance balance = accounts.get(account_id);
            if (balance != null) {
                // Try to acquire the read lock, return if we fail
                if (!transaction.tryShared(balance.lock)) {
                    return;
                }

                Account.Builder account = Account.newBuilder();
                account.setId(account_id);
                account.setBalance(balance.balance);
                response.addAccount(account);
            }
        }
        transaction.setWorkUnit(null);
        done.run(response.build());
    }

    @Override
    public void write(RpcController controller, WriteRequest request,
            RpcCallback<WriteResult> done) {
        TransactionRpcController transaction = (TransactionRpcController) controller;

        // Create an undo record if needed
        @SuppressWarnings("unchecked")
        HashMap<Integer, Integer> undo = (HashMap<Integer, Integer>) transaction.getUndo();
        if (undo == null) {
            undo = new HashMap<Integer, Integer>();
            transaction.setUndo(undo);
        }

        for (Account account : request.getAccountList()) {
            // TODO: For serializability, this lookup/insert needs to acquire a lock
            LockBalance balance = accounts.get(account.getId());
            boolean created = false;
            if (balance == null) {
                created = true;
                balance = new LockBalance();
                accounts.put(account.getId(), balance);
            }
            // Try to acquire the write lock, return if we fail
            if (!transaction.tryExclusive(balance.lock)) {
                return;
            }

            // Save the undo information
            Integer oldBalance = created ? null : balance.balance;
            undo.put(account.getId(), oldBalance);

            // Update the balance
            balance.balance = account.getBalance();
//            System.out.println(account.getId() + " " + accounts.get(account.getId()));
        }
        transaction.setWorkUnit(null);
        done.run(WriteResult.newBuilder().build());
    }

    @Override
    public void finish(TransactionRpcController transaction,
            boolean commit) {
        @SuppressWarnings("unchecked")
        HashMap<Integer, Integer> undo = (HashMap<Integer, Integer>) transaction.getUndo();
        if (!commit && undo != null) {
            // Aborting: apply the undo information
            for (Entry<Integer, Integer> i : undo.entrySet()) {
                if (i.getValue() == null) {
                    // TODO: Deletes need special lock handling for serializability
                    LockBalance balance = accounts.remove(i.getKey());
                    assert balance != null;
                } else {
                    // Replace the balance with the previous version
                    LockBalance balance = accounts.get(i.getKey());
                    balance.balance = i.getValue();
                }
            }
        }
    }

    public Map<Integer, Integer> getBalances() {
        HashMap<Integer, Integer> balances = new HashMap<Integer, Integer>();
        for (Entry<Integer, LockBalance> i : accounts.entrySet()) {
            balances.put(i.getKey(), i.getValue().balance);
        }
        return balances;
    }

    public int dumpBalances() {
        int total = 0;
        for (Entry<Integer, LockBalance> accountEntry : accounts.entrySet()) {
            System.out.println(accountEntry.getKey() + " = " + accountEntry.getValue().balance);
            total += accountEntry.getValue().balance;
        }
        return total;
    }

    public static void main(String[] args) {
        NIOEventLoop eventLoop = new NIOEventLoop();
        eventLoop.setExitOnSigInt(true);
        ProtoServer server = new ProtoServer(eventLoop);
        server.bind(Integer.parseInt(args[0]));
        Server table = new Server();
        TransactionServer transactionServer = new TransactionServer(table);
        server.register(transactionServer);
        eventLoop.run();

        int total = table.dumpBalances();
        System.out.println("TOTAL: " + total);
    }
}
