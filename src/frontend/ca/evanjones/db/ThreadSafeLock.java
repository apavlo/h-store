package ca.evanjones.db;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ThreadSafeLock extends Lock {
    @Override
    public synchronized boolean tryExclusive(Transaction transaction) {
        return super.tryExclusive(transaction);
    }

    @Override
    public synchronized boolean tryShared(Transaction transaction) {
        return super.tryShared(transaction);
    }

    @Override
    public synchronized void unlock(Transaction transaction, List<Transaction> granted) {
        super.unlock(transaction, granted);
    }

    @Override
    public void cancelRequest(Transaction transaction, List<Transaction> granted) {
        throw new UnsupportedOperationException("Use tryCancelRequest for thread-safe locks.");
    }

    @Override
    public synchronized boolean tryCancelRequest(Transaction transaction, List<Transaction> granted) {
        return super.tryCancelRequest(transaction, granted);
    }

    @Override
    public synchronized Set<Transaction> getHolders() {
        // Return an unmodifiable copy of the set to avoid needing complex synchronization
        return Collections.unmodifiableSet(new HashSet<Transaction>(super.getHolders()));
    }

    @Override
    public synchronized boolean isShared() { return super.isShared(); }
}
