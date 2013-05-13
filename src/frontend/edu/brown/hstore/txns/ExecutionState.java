package edu.brown.hstore.txns;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.utils.Pair;

import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.util.ParameterSetArrayCache;
import edu.brown.interfaces.DebugContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;

/**
 * The internal state of a transaction while it is running at a PartitionExecutor
 * This will be removed from the LocalTransaction once its control code is finished executing.
 * If you need to track anything that may occur *before* the txn starts running, then you don't
 * want to put those data structures in here.
 * @author pavlo
 */
public class ExecutionState implements Poolable {
    private static final Logger LOG = Logger.getLogger(LocalTransaction.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    // ----------------------------------------------------------------------------
    // GLOBAL DATA MEMBERS
    // ----------------------------------------------------------------------------
    
    /**
     * The ExecutionSite that this TransactionState is tied to
     */
    protected final PartitionExecutor executor;
    
    // ----------------------------------------------------------------------------
    // TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Reusable cache of ParameterSet arrays for VoltProcedures
     */
    public final ParameterSetArrayCache procParameterSets = new ParameterSetArrayCache(10);
    
    /**
     * Reusable list of WorkFragment builders. The builders are not reusable
     * but the list is.
     */
    public final List<WorkFragment.Builder> tmp_partitionFragments = new ArrayList<WorkFragment.Builder>(); 
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     */
    public ExecutionState(PartitionExecutor executor) {
        this.executor = executor;
    }

    // ----------------------------------------------------------------------------
    // EXECUTION ROUNDS
    // ----------------------------------------------------------------------------
    
    @Override
    public boolean isInitialized() {
        return (true);
    }

    @Override
    public void finish() {
        this.procParameterSets.reset();
    }
}
