package edu.brown.hstore.specexec.checkers;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;

import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Unsafe conflict checker.
 * This will allow the DBMS to execute *any* transaction.
 * @author pavlo
 */
public class UnsafeConflictChecker extends AbstractConflictChecker {
    private static final Logger LOG = Logger.getLogger(UnsafeConflictChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final int limit;
    private int counter = 0;
    private AbstractTransaction lastDtxn;
    
    public UnsafeConflictChecker(CatalogContext catalogContext, int limit) {
        super(catalogContext);
        this.limit = limit;
    }

    @Override
    public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
        return (false);
    }
    @Override
    public boolean skipConflictBefore() {
        return (true);
    }
    @Override
    public boolean skipConflictAfter() {
        return (true);
    }

    @Override
    public boolean hasConflictBefore(AbstractTransaction dtxn, LocalTransaction candidate, int partitionId) {
        if (this.lastDtxn != dtxn) {
            this.counter = 1;
            this.lastDtxn = dtxn;
            return (false);
        }
        if (this.limit < 0 || ++this.counter < this.limit) {
            return (false);
        }
        return (true);
    }
    
    @Override
    public boolean hasConflictAfter(AbstractTransaction ts0, LocalTransaction ts1, int partitionId) {
        return (false);
    }
}
