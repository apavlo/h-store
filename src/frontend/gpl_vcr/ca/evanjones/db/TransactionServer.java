package ca.evanjones.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import ca.evanjones.db.Transactions.*;
import ca.evanjones.protorpc.ProtoMethodInvoker;
import ca.evanjones.protorpc.ServiceRegistry;

public class TransactionServer extends TransactionService {
    public TransactionServer(TransactionalService service) {
        this.service = service;

        // TODO: Support registering multiple local services? Needs "local 2PC" effectively. Yuck.
        serviceRegistry.register(service);
    }

    @Override
    public void finish(RpcController controller, FinishRequest request,
            RpcCallback<FinishResult> done) {
        TransactionRpcController transaction = transactions.get(request.getTransactionId());
        if (transaction == null) {
            // NOTE: We ignore "abort" messages for missing transactions. We might have aborted
            // locally before receiving a remote abort
            if (request.getCommit()) {
                throw new IllegalArgumentException("No record of transaction: cannot commit");
            }
            done.run(FinishResult.getDefaultInstance());
            return;
        }

        assert transaction != null;
        assert transaction.getStatus() == Status.PREPARED;
        done.run(FinishResult.getDefaultInstance());
        localFinish(transaction, request.getCommit());
    }

    private final class TransactionServerWork implements
            RpcCallback<Message> {
        private final TransactionRpcController transaction;
        private final TransactionRequest request;
        private final ProtoMethodInvoker invoker;
        private final RpcCallback<TransactionResult> finalCallback;

        public TransactionServerWork(
                TransactionRpcController transaction,
                TransactionRequest request,
                ProtoMethodInvoker invoker,
                RpcCallback<TransactionResult> finalCallback) {
            this.transaction = transaction;
            this.request = request;
            this.invoker = invoker;
            this.finalCallback = finalCallback;
        }

        public void invoke() {
            try {
                invoker.invoke(transaction, request.getRequest().getRequest(), this);
                if (transaction.isBlocked()) {
                    // the transaction is blocked: detect deadlocks
                    List<Transaction> cycle = depthFirstSearch(transaction);
                    if (cycle != null) {
                        transaction.setAbort(Status.ABORT_DEADLOCK);
                        run(null);
                    }
                }
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run(Message parameter) {
            finalCallback.run(makeTransactionResult(parameter, transaction.getStatus()));

            // Local abort: abort immediately
            if (transaction.getStatus() != Status.PREPARED) {
                // abort this transaction
                localFinish(transaction, false);
            }
        }
    }

    public static TransactionResult makeTransactionResult(Message parameter, Status status) {
        TransactionResult.Builder result = TransactionResult.newBuilder();
        // Results may be omitted for aborts
        if (parameter != null) {
            result.setResponse(parameter.toByteString());
        } else {
            assert status != Status.PREPARED;
        }
        result.setStatus(status);
        return result.build();
    }

    @Override
    public void request(RpcController controller, TransactionRequest request,
            RpcCallback<TransactionResult> done) {
        // Find the method
        ProtoMethodInvoker invoker =
                serviceRegistry.getInvoker(request.getRequest().getMethodName());

        // Get the transaction object for this request
        TransactionRpcController transaction = transactions.get(request.getTransactionId());
        if (transactions.get(request.getTransactionId()) == null) {
            transaction = new TransactionRpcController();
            transaction.startTransaction(request.getTransactionId());
            transactions.put(request.getTransactionId(), transaction);
        }

        // Build the request object
        TransactionServerWork work = new TransactionServerWork(transaction, request, invoker, done);
        transaction.setWorkUnit(work);

        // Invoke the underlying method
        work.invoke();
    }

    private void localFinish(TransactionRpcController transaction, boolean commit) {
        service.finish(transaction, commit);
        TransactionRpcController removed = transactions.remove(transaction.getId());
        assert removed == transaction;

        // Grant all locks before re-executing. Required because if we look for deadlocks before
        // a transaction has unblocked itself, we do bad things.
        List<Transaction> granted = transaction.releaseLocks();
        for (Transaction g : granted) {
            g.lockGranted();
        }

        for (Transaction g : granted) {
            // Re-execute any blocked transactions
            TransactionRpcController unblocked = (TransactionRpcController) g;
            TransactionServerWork work = (TransactionServerWork) unblocked.getWorkUnit();
            work.invoke();
        }
    }

    private List<Transaction> depthFirstSearch(Transaction start) {
        HashSet<Transaction> visited = new HashSet<Transaction>();
        ArrayDeque<ArrayList<Transaction>> visitStack = new ArrayDeque<ArrayList<Transaction>>();
        visitStack.add(new ArrayList<Transaction>());
        visitStack.peekFirst().add(start);

        while (!visitStack.isEmpty()) {
            // Take the next transaction, record that we have visited it
            ArrayList<Transaction> path = visitStack.removeLast();
            Transaction last = path.get(path.size()-1);
            assert last.isBlocked();
            boolean unvisited = visited.add(last);
            if (!unvisited) {
                // we have already visited this node: skip it
                continue;
            }

            // Find all the transactions holding the lock this transaction wants
            Set<Transaction> waitsForSet = last.getBlockedLock().getHolders();
            assert !waitsForSet.isEmpty();

            // Visit all the "waits for" transactions
            for (Transaction waitsFor : waitsForSet) {
                if (waitsFor == last) {
                    // waiting for ourself: this *must* be a lock upgrade
                    // do not add a waits for edge to ourself for upgrades.
                    assert last.getBlockedLock().isShared();
                    assert waitsForSet.size() > 1;
                    continue;
                }

                if (!waitsFor.isBlocked()) {
                    // Not blocked: do not visit
                    continue;
                }

                // TODO: If we check for deadlocks each time we block, we should only form deadlocks
                // with ourselves, so we don't need to check the entire path.
                int previousIndex = path.indexOf(waitsFor);
                if (previousIndex != -1) {
                    // this is a cycle: return it after dropping extra transactions
                    assert previousIndex == 0;
                    assert waitsFor == start;
                    return path.subList(previousIndex, path.size());
                }

                if (!visited.contains(waitsFor)) {
                    // Another unvisited blocked transaction: visit it
                    ArrayList<Transaction> visitPath = new ArrayList<Transaction>(path);
                    visitPath.add(waitsFor);
                    visitStack.add(visitPath);
                }
            }
        }
        return null;
    }

    private final TransactionalService service;
    private final ServiceRegistry serviceRegistry = new ServiceRegistry();
    private final HashMap<Integer, TransactionRpcController> transactions =
        new HashMap<Integer, TransactionRpcController>();
}
