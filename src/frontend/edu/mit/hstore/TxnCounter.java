/**
 * 
 */
package edu.mit.hstore;

import java.util.Set;
import java.util.TreeSet;

import org.voltdb.catalog.Procedure;

import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public enum TxnCounter {
    /** The number of transaction requests that have arrived at this site */
    RECEIVED,
    /** Of the the received transactions, the number that we had to send somewhere else */
    REDIRECTED,
    /** The number of txns that we executed locally */
    EXECUTED,
    /** Of the locally executed transactions, how many were single-partitioned */
    SINGLE_PARTITION,
    /** Of the locally executed transactions, how many were multi-partitioned */
    MULTI_PARTITION,
    /** The number of sysprocs that we executed */
    SYSPROCS,
    /** The number of tranactions that were completed (committed or aborted) */
    COMPLETED,
    /** Of the locally executed transactions, how many were abort */
    ABORTED,
    /** The number of transactions that were speculative and had to be restarted */
    RESTARTED,
    /** The number of transactions that were mispredicted (and thus re-executed) */
    MISPREDICTED,
    /** Speculative Execution **/
    SPECULATIVE,
    /** No undo buffers! Naked transactions! */
    NO_UNDO,
    ;
    
    private final Histogram<String> h = new Histogram<String>();
    private final String name;
    private TxnCounter() {
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
    public int inc(Procedure catalog_proc) {
        this.h.put(catalog_proc.getName());
        return (this.get());
    }
    public int dec(Procedure catalog_proc) {
        this.h.remove(catalog_proc.getName());
        return (this.get());
    }
    public static Set<String> getAllProcedures() {
        Set<String> ret = new TreeSet<String>();
        for (TxnCounter tc : TxnCounter.values()) {
            ret.addAll(tc.h.values());
        }
        return (ret);
    }
    public Double ratio() {
        int total = -1;
        int cnt = this.get();
        switch (this) {
            case SINGLE_PARTITION:
                if (SINGLE_PARTITION.get() == 0) return (null);
                total = SINGLE_PARTITION.get() + MULTI_PARTITION.get();
                break;
            case MULTI_PARTITION:
                if (MULTI_PARTITION.get() == 0) return (null);
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
            case RESTARTED:
            case MISPREDICTED:
                total = EXECUTED.get() - SYSPROCS.get();
                break;
            case REDIRECTED:
            case RECEIVED:
            case EXECUTED:
                total = RECEIVED.get();
                break;
            case COMPLETED:
                return (null);
            default:
                assert(false) : "Unexpected TxnCounter: " + this;
        }
        return (total == 0 ? null : cnt / (double)total);
    }
}