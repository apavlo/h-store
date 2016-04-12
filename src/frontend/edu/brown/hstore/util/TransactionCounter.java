/**
 * 
 */
package edu.brown.hstore.util;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.statistics.FastIntHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.StringUtil;

/**
 * Internal counters for how transactions were executed.
 * These are updated in various parts of the txn's lifetime.
 * TODO: This should be better integrated into the Statistics framework.
 */
public enum TransactionCounter {
    /** The number of transaction requests that have arrived at this site */
    RECEIVED,
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
    /** Of the locally executed transactions, how many were multi-site */
    MULTI_SITE,
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
    /** The number of transactions that were aborted while being speculatively executed */
    ABORT_SPECULATIVE,
    /** The number of transactions that had to be restarted (non-speculative txns) */
    RESTARTED,
    /** The number of transactions that were aborted because they tried to access evicted data */
    EVICTEDACCESS,
    /** No undo buffers! Naked transactions! */
    NO_UNDO,
    /** The number of transactions that were sent out with prefetch queries */
    PREFETCH,
    
    // --------------------------------------------------------
    // Speculative Execution Stall Points
    // --------------------------------------------------------
    SPECULATIVE_SP1_IDLE,
    SPECULATIVE_SP1_LOCAL,
    SPECULATIVE_SP2,
    SPECULATIVE_SP3_BEFORE,
    SPECULATIVE_SP3_AFTER,
    SPECULATIVE_SP4_LOCAL,
    SPECULATIVE_SP4_REMOTE,
    ;
    
    private final FastIntHistogram h = new FastIntHistogram();
    private final String name;
    private TransactionCounter() {
        this.name = StringUtil.title(this.name().replace("_", "-"));
    }
    @Override
    public String toString() {
        return (this.name);
    }
    public Histogram<Procedure> getHistogram(CatalogContext catalogContext) {
        Histogram<Procedure> procHistogram = new ObjectHistogram<Procedure>();
        for (int procId : this.h.fastValues()) {
            Procedure catalog_proc = catalogContext.getProcedureById(procId);
            procHistogram.put(catalog_proc, this.h.get(procId));
        }
        return (procHistogram);
    }
    public int get() {
        return ((int)this.h.getSampleCount());
    }
    public Long get(Procedure catalog_proc) {
        long val = this.h.get(catalog_proc.getId());
        return (val < 0 ? null : val);
    }
    public int inc(Procedure catalog_proc) {
        synchronized (catalog_proc) {
            this.h.put(catalog_proc.getId());
        } // SYNCH
        return (this.get());
    }
    public int dec(Procedure catalog_proc) {
        synchronized (catalog_proc) {
            this.h.dec(catalog_proc.getId());
        } // SYNCH
        return (this.get());
    }
    public void clear() {
        this.h.clear();
    }
    public static Collection<Procedure> getAllProcedures(CatalogContext catalogContext) {
        Set<Procedure> ret = new TreeSet<Procedure>();
        for (TransactionCounter tc : TransactionCounter.values()) {
            for (int procId : tc.h.fastValues()) {
                Procedure catalog_proc = catalogContext.getProcedureById(procId);
                ret.add(catalog_proc);
            } // FOR
        } // fOR
        return (ret);
    }
    public Double ratio() {
        int total = -1;
        int cnt = this.get();
        switch (this) {
            case SINGLE_PARTITION:
            case MULTI_PARTITION:
            case MULTI_SITE:
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
            case ABORT_SPECULATIVE:
            case RESTARTED:
            case MISPREDICTED:
                total = EXECUTED.get() - SYSPROCS.get();
                break;
            case REDIRECTED:
            case REJECTED:
            case RECEIVED:
            case EXECUTED:
            case PREFETCH:
                total = RECEIVED.get();
                break;
            case SPECULATIVE_SP1_IDLE:
            case SPECULATIVE_SP1_LOCAL:
            case SPECULATIVE_SP2:
            case SPECULATIVE_SP3_AFTER:
            case SPECULATIVE_SP3_BEFORE:
            case SPECULATIVE_SP4_LOCAL:
            case SPECULATIVE_SP4_REMOTE:
                total = SPECULATIVE.get();
                break;
            case COMPLETED:
                return (null);
            default:
                assert(false) :
                    String.format("Unexpected %s.%s", this.getClass().getSimpleName(), this);
        }
        return (total == 0 ? null : cnt / (double)total);
    }
    
    protected static final Map<String, TransactionCounter> name_lookup = new HashMap<String, TransactionCounter>();
    static {
        for (TransactionCounter vt : EnumSet.allOf(TransactionCounter.class)) {
            TransactionCounter.name_lookup.put(vt.name().toLowerCase(), vt);
        }
    }
    public static TransactionCounter get(String name) {
        return TransactionCounter.name_lookup.get(name.toLowerCase());
    }
    public static void resetAll(CatalogContext catalogContext) {
        for (TransactionCounter tc : EnumSet.allOf(TransactionCounter.class)) {
            tc.clear();
            tc.h.ensureSize(catalogContext.procedures.size());
        } // FOR
    }
    public static String debug() {
        Map<String, Integer> m = new LinkedHashMap<String, Integer>();
        for (TransactionCounter tc : EnumSet.allOf(TransactionCounter.class)) {
            m.put(tc.name(), tc.get()); 
        } // FOR
        return (StringUtil.formatMaps(m));
    }
}