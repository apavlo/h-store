package ca.evanjones.db;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Transaction {
    private HashSet<Lock> heldLocks = new HashSet<Lock>();
    private Lock blockedLock = null;
    private Object workUnit = null;
    private Object undo = null;

    /** Set to true when the transaction must be aborted due to a deadlock. */
    private boolean deadlockAbort = false;

    /** Thrown when a thread transaction is blocked waiting for a lock and must be aborted to
     * resolve a deadlock. */
    public static final class DeadlockException extends RuntimeException {
        private static final long serialVersionUID = 8806437102448283107L;
    }

    /** Resets this transaction so it can be reused. */
    public void reset() {
        if (blockedLock != null || !heldLocks.isEmpty()) {
            throw new IllegalStateException(
                    "can't reset a transaction holding or waiting for locks");
        }

        deadlockAbort = false;
        undo = null;
        workUnit = null;
    }

    public boolean tryExclusive(Lock lock) {
        return tryAcquire(lock, lock.tryExclusive(this));
    }

    public boolean tryShared(Lock lock) {
        return tryAcquire(lock, lock.tryShared(this));
    }

    private boolean tryAcquire(Lock lock, boolean acquired) {
        assert blockedLock == null;
        if (acquired) {
            heldLocks.add(lock);
        } else {
            blockedLock = lock;
        }
        return acquired;
    }

    public List<Transaction> releaseLocks() {
        ArrayList<Transaction> granted = new ArrayList<Transaction>();
        if (blockedLock != null) {
            blockedLock.cancelRequest(this, granted);
            blockedLock = null;
        }

        for (Lock lock : heldLocks) {
            lock.unlock(this, granted);
        }
        if (heldLocks.size() > 32) {
            heldLocks = new HashSet<Lock>();
        } else {
            heldLocks.clear();
        }
        return granted;
    }

    /** Must call this for threaded transactions. TODO: Make two separate implementations! */
    public synchronized List<Transaction> synchronizedReleaseLocks() {
        ArrayList<Transaction> granted = new ArrayList<Transaction>();
        if (blockedLock != null) {
            boolean canceled = blockedLock.tryCancelRequest(this, granted);
            if (!canceled) {
                assert blockedLock.getHolders().contains(this);
                heldLocks.add(blockedLock);
            }
            blockedLock = null;
        }

        for (Lock lock : heldLocks) {
            lock.unlock(this, granted);
        }
        heldLocks.clear();
        return granted;
    }

    public void lockGranted() {
        assert blockedLock != null;
        // the lock might already be in this set if this is a lock upgrade
        heldLocks.add(blockedLock);
        blockedLock = null;
    }

    public void blockExclusive(ThreadSafeLock lock) {
        boolean success = false;
        synchronized (this) {
            success = tryExclusive(lock);
        }
        if (!success) {
            // block the transaction until explicitly woken
            waitForLock();
            assert tryExclusive(lock);
        }
    }

    public void blockShared(ThreadSafeLock lock) {
        boolean success = false;
        synchronized (this) {
            success = tryShared(lock);
        }
        if (!success) {
            // block the transaction until explicitly woken
            waitForLock();
            assert tryShared(lock);
        }
    }

    public final static int DEADLOCK_DETECTION_DELAY_MS = 1;

    /** Must not be called from a synchronized method. */
    private void waitForLock() {
        // By the time we get here another thread could have granted our lock
        boolean lookedForDeadlock = false;
        while (true) {
            boolean isBlocked = true;
            synchronized (this) {
                if (blockedLock == null) break;
                if (deadlockAbort) {
                    throw new DeadlockException();
                }

                try{
                    if (!lookedForDeadlock) {
                        wait(DEADLOCK_DETECTION_DELAY_MS);
                    } else {
                        wait();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                isBlocked = blockedLock != null;
            }
            if (isBlocked && !lookedForDeadlock) {
                // Look for deadlock exactly once.
                lookedForDeadlock = true;
                // perform deadlock detection.
                // IMPORTANT: We do this *WITHOUT* holding our lock. Another thread might
                // decide to run deadlock detection concurrently, and this ensures they
                // won't deadlock during detection. However, they *both* detect the deadlock
                // and try to kill multiple transactions, or the same transaction, or a
                // transaction that has been unblocked by another transaction breaking
                // the cycle.
                List<Transaction> cycle = detectCycle();
                if (cycle != null) {
                    // We detected a deadlock: attempt to kill a transaction (could be this)
                    cycle.get(0).tryAbortForDeadlock();
                }
            }
        }
        assert blockedLock == null;
    }

    private List<Transaction> detectCycle() {
        // TODO: This is basically the same code as TransactionServer.depthFirstSearch
        // except this attempts to be thread safe.
        HashSet<Transaction> visited = new HashSet<Transaction>();
        ArrayDeque<ArrayList<Transaction>> visitStack = new ArrayDeque<ArrayList<Transaction>>();
        visitStack.add(new ArrayList<Transaction>());
        visitStack.peekFirst().add(this);

        while (!visitStack.isEmpty()) {
            // Take the next transaction, record that we have visited it
            ArrayList<Transaction> path = visitStack.removeLast();
            Transaction last = path.get(path.size()-1);
            boolean unvisited = visited.add(last);
            if (!unvisited) {
                // we have already visited this node: skip it
                continue;
            }

            Set<Transaction> waitsForSet;
            synchronized (last) {
                // It is possible that this is no longer blocked by the time we check it.
                // If so, skip this node
                if (!last.isBlocked()) {
                    continue;
                }

                // Find all the transactions holding the lock this transaction wants
                waitsForSet = last.getBlockedLock().getHolders();
                assert !waitsForSet.isEmpty();
            }

            // Visit all the "waits for" transactions
            for (Transaction waitsFor : waitsForSet) {
                if (waitsFor == last) {
                    // waiting for ourself: this *must* be a lock upgrade
                    // do not add a waits for edge to ourself for upgrades.
                    // assert last.getBlockedLock().isShared(); THREAD UNSAFE
                    // assert waitsForSet.size() > 1; THREAD UNSAFE: Unlock concurrent with this
                    continue;
                }

                // TODO: This is racy, but I think it is a "safe" race because we check again
                // while holding the lock
                if (!waitsFor.isBlocked()) {
                    // Not blocked: do not visit
                    continue;
                }

                int previousIndex = path.indexOf(waitsFor);
                if (previousIndex != -1) {
                    // found a cycle: return it after dropping extra transactions
                    List<Transaction> cycle = path.subList(previousIndex, path.size());
                    /* Can't do this check: Simultaneous deadlock aborts are possible.
                    for (Transaction t : cycle) {
                        synchronized (t) {
                            assert t.isBlocked();
                        }
                    }*/
                    return cycle;
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

    /** Call instead of lockGranted() for threaded operation. */
    public synchronized void unblockLockGranted() {
        // Must call lockGranted while synchronized to guarantee we see changes from
        // blockExclusive/blockShared
        if (blockedLock == null) {
            // It is possible that this transaction aborted while we are trying to give it the
            // lock. Ignore this.
            assert this.deadlockAbort;
            return;
        }

        lockGranted();
        notify();
    }

    public synchronized void tryAbortForDeadlock() {
        // If multiple threads detect deadlocks at the same time, this could be called multiple
        // times, or be called on a transaction this is not blocked.
        if (blockedLock != null && !deadlockAbort) {
            deadlockAbort = true;
            notify();
        }
    }

    public Object getWorkUnit() { return workUnit; }
    public void setWorkUnit(Object workUnit) { this.workUnit = workUnit; }

    public Object getUndo() { return undo; }
    public void setUndo(Object undo) { this.undo = undo; }

    public boolean isBlocked() { return blockedLock != null; }
    public Lock getBlockedLock() { return blockedLock; }
}
