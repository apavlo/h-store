/**
 * 
 */
package edu.brown.hstore.util;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.catalog.Procedure;

import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public enum TransactionCounter {
    /** The number of transaction requests that have arrived at this site */
    RECEIVED,
    /** */
    THROTTLED,
    /** */
    REJECTED,
    /** Of the the received transactions, the number that we had to send somewhere else */
    REDIRECTED,
    /** The number of transactions that we executed locally */
    EXECUTED,
    /** The number of transactions that were completed (committed or aborted) */
    COMPLETED,
    /** Of the locally executed transactions, how many were single-partitioned */
    SINGLE_PARTITION,
    /** Of the locally executed transactions, how many were multi-partitioned */
    MULTI_PARTITION,
    /** Speculative Execution **/
    SPECULATIVE,
    /** The number of sysprocs that we executed */
    SYSPROCS,
    /** The number of transactions that were mispredicted (and thus re-executed) */
    MISPREDICTED,
    /** Of the locally executed transactions, how many were aborted by the user */
    ABORTED,
    /** The number of transactions that were unexpectedly aborted (e.g., because of an assert) */
    ABORT_UNEXPECTED,
    /** The number of transactions that were gracefully aborted  */
    ABORT_GRACEFUL,
    /** The number of transactions that were speculative and had to be restarted */
    RESTARTED,
    /** The number of transactions that were aborted because they tried to access evicted data */
    EVICTEDACCESS,
    /** No undo buffers! Naked transactions! */
    NO_UNDO,
    /** The number of transactions that were blocked due to the local partition's timestamps */
    BLOCKED_LOCAL,
    /** The number of transactions that were blocked due to a remote partition's timestamps */
    BLOCKED_REMOTE,
    /** The number of transactions that were sent out with prefetch queries */
    PREFETCH_LOCAL,
    /** The number of transactions with prefetch queries that were received and prefetched before needed by the sender */
    PREFETCH_REMOTE,
    ;
    
    private final Histogram<String> h = new Histogram<String>();
    private final String name;
    private TransactionCounter() {
        this.name = StringUtil.title(this.name().replace("_", "-"));
    }
    @Override
    public String toString() {
        return (this.name);
    }
    public Histogram<String> getHistogram() {
        return (this.h);
    }
    public int get() {
        return ((int)this.h.getSampleCount());
    }
    public Long get(Procedure catalog_proc) {
        return (this.h.get(catalog_proc.getName()));
    }
    public synchronized int inc(String procName) {
        this.h.put(procName);
        return (this.get());
    }
    public synchronized int inc(Procedure catalog_proc) {
        this.h.put(catalog_proc.getName());
        return (this.get());
    }
    public synchronized int dec(Procedure catalog_proc) {
        this.h.dec(catalog_proc.getName());
        return (this.get());
    }
    public synchronized void clear() {
        this.h.clear();
    }
    public static Collection<String> getAllProcedures() {
        Set<String> ret = new TreeSet<String>();
        for (TransactionCounter tc : TransactionCounter.values()) {
            ret.addAll(tc.h.values());
        }
        return (ret);
    }
    public Double ratio() {
        int total = -1;
        int cnt = this.get();
        switch (this) {
            case SINGLE_PARTITION:
            case MULTI_PARTITION:
                if (this.get() == 0) return (null);
                total = SINGLE_PARTITION.get() + MULTI_PARTITION.get();
                break;
            case SPECULATIVE:
                total = SINGLE_PARTITION.get();
                break;
            case NO_UNDO:
                total = EXECUTED.get();
                break;
            case SYSPROCS:
            case ABORTED:
            case ABORT_UNEXPECTED:
            case ABORT_GRACEFUL:
            case RESTARTED:
            case MISPREDICTED:
                total = EXECUTED.get() - SYSPROCS.get();
                break;
            case REDIRECTED:
            case REJECTED:
            case RECEIVED:
            case EXECUTED:
            case THROTTLED:
            case BLOCKED_LOCAL:
            case BLOCKED_REMOTE:
            case PREFETCH_LOCAL:
            case PREFETCH_REMOTE:
                total = RECEIVED.get();
                break;
            case COMPLETED:
                return (null);
            default:
                assert(false) : "Unexpected TxnCounter: " + this;
        }
        return (total == 0 ? null : cnt / (double)total);
    }
    
    protected static final Map<Integer, TransactionCounter> idx_lookup = new HashMap<Integer, TransactionCounter>();
    protected static final Map<String, TransactionCounter> name_lookup = new HashMap<String, TransactionCounter>();
    static {
        for (TransactionCounter vt : EnumSet.allOf(TransactionCounter.class)) {
            TransactionCounter.idx_lookup.put(vt.ordinal(), vt);
            TransactionCounter.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }
    public static TransactionCounter get(Integer idx) {
        return TransactionCounter.idx_lookup.get(idx);
    }
    public static TransactionCounter get(String name) {
        return TransactionCounter.name_lookup.get(name.toLowerCase());
    }
}