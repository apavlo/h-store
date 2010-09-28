package ca.evanjones.db;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class Lock {
    private final HashSet<Transaction> holders = new HashSet<Transaction>();
    private enum State {
        UNLOCKED, SHARED, EXCLUSIVE,
    }
    private State state = State.UNLOCKED;
    private final ArrayDeque<LockRequest> requestQueue = new ArrayDeque<LockRequest>();

    private final static class LockRequest {
        public final Transaction transaction;
        public final State lockRequest;

        public LockRequest(Transaction transaction, State lockRequest) {
            this.transaction = transaction;
            this.lockRequest = lockRequest;
        }
    }

    public boolean tryExclusive(Transaction transaction) {
        return tryOrQueueRequest(transaction, State.EXCLUSIVE);
    }

    public boolean tryShared(Transaction transaction) {
        return tryOrQueueRequest(transaction, State.SHARED);
    }

    public void unlock(Transaction transaction, List<Transaction> granted) {
        assert state != State.UNLOCKED;

        if (!holders.remove(transaction)) {
            throw new IllegalArgumentException("transaction does not hold lock");
        }
        if (holders.size() == 0) {
            state = State.UNLOCKED;
        }

        // Admit as many lock requests as possible
        admitQueuedRequests(granted);
    }

    public void cancelRequest(Transaction transaction, List<Transaction> granted) {
        if (!tryCancelRequest(transaction, granted)) {
            throw new IllegalArgumentException("transaction is not waiting for lock");
        }
    }

    /** When aborting a transaction in multi-threaded mode, we could be releasing locks at the
     * same time they are granted to us. Thus, we could go to cancel a request and discover it has
     * already been canceled.
     * @return true if we canceled a lock request.
     */
    protected boolean tryCancelRequest(Transaction transaction, List<Transaction> granted) {
        assert state != State.UNLOCKED;

        for (Iterator<LockRequest> it = requestQueue.iterator(); it.hasNext(); ) {
            LockRequest request = it.next();
            if (request.transaction == transaction) {
                it.remove();
                admitQueuedRequests(granted);
                return true;
            }
        }
        return false;
    }

    private boolean tryOrQueueRequest(Transaction transaction, State lockRequest) {
        if (tryRequest(transaction, lockRequest)) {
            return true;
        }

        // Queue the request if it is not already in the queue
        for (LockRequest request : requestQueue) {
            if (request.transaction == transaction) {
                assert request.lockRequest == lockRequest;
                return false;
            }
        }

        LockRequest request = new LockRequest(transaction, lockRequest);
        if (lockRequest == State.EXCLUSIVE && state == State.SHARED &&
                holders.contains(transaction)) {
            // this is a lock upgrade: prioritize it
            // NOTE: This works even for multiple lock upgrades, because multiple lock upgrades
            // cause a deadlock so all but one will need to be killed anyway.
            requestQueue.addFirst(request);
        } else {
            requestQueue.add(request);
        }
        return false;
    }

    private boolean tryRequest(Transaction transaction, State lockRequest) {
        if (lockRequest == State.SHARED) {
            // Acquire lock when:
            // * The lock is unlocked
            // * We already hold the lock (in either state)
            // * The lock is locked for reading and either the queue is empty OR we are admitting
            //    queued requests
            if (state == State.UNLOCKED
                    || holders.contains(transaction)
                    || (state == State.SHARED && (requestQueue.isEmpty()
                            || requestQueue.peek().lockRequest == State.SHARED))) {
                // Add transaction to set of lock holders: if locked more than once it doesn't matter.
                if (state == State.UNLOCKED) {
                    state = State.SHARED;
                }
                holders.add(transaction);
                return true;
            } else {
                return false;
            }
        } else {
            assert lockRequest == State.EXCLUSIVE;
            // Acquire the lock in one of three cases:
            // * The lock is unlocked
            // * The lock is held for writing by us already
            // * The lock is held for reading by us ONLY -> upgrade to write lock
            // This means that we either need:
            // * The lock is unlocked
            //    OR
            // * The lock is held by only us in either state.
            if (state == State.UNLOCKED ||
                    (holders.size() == 1 && holders.contains(transaction))) {
                // Add transaction to set of lock holders: if locked more than once it doesn't matter.
                if (state == State.UNLOCKED) {
                    assert holders.isEmpty();
                    holders.add(transaction);
                }
                state = State.EXCLUSIVE;
                assert(holders.size() == 1);
                return true;
            } else {
                return false;
            }
        }
    }

    private void admitQueuedRequests(List<Transaction> granted) {
        while (!requestQueue.isEmpty() &&
                tryRequest(requestQueue.peek().transaction, requestQueue.peek().lockRequest)) {
            granted.add(requestQueue.peek().transaction);
            requestQueue.poll();
        }
    }

    public Set<Transaction> getHolders() {
        return Collections.unmodifiableSet(holders);
    }

    public boolean isShared() { return state == State.SHARED; }
}
