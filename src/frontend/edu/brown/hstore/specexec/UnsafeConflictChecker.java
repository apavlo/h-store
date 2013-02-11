package edu.brown.hstore.specexec;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

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
    
    public UnsafeConflictChecker(CatalogContext catalogContext) {
        super(catalogContext);
    }

    @Override
    public boolean shouldIgnoreProcedure(Procedure proc) {
        return (false);
    }

    @Override
    public boolean canExecute(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
        return (true);
    }
}
