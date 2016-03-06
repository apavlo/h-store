/***************************************************************************
 *   Copyright (C) 2013 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package edu.brown.hstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.voltdb.AriesLog;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DependencySet;
import org.voltdb.HsqlBackend;
import org.voltdb.MemoryStats;
import org.voltdb.AntiCacheMemoryStats;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.AntiCacheDBType;
import org.voltdb.types.SpecExecSchedulerPolicyType;
import org.voltdb.types.SpeculationConflictCheckerType;
import org.voltdb.types.SpeculationType;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.PlanFragmentIdGenerator;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.callbacks.LocalFinishCallback;
import edu.brown.hstore.callbacks.LocalPrepareCallback;
import edu.brown.hstore.callbacks.PartitionCountingCallback;
import edu.brown.hstore.callbacks.RemotePrepareCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.EstimatorUtil;
import edu.brown.hstore.estimators.TransactionEstimator;
import edu.brown.hstore.internal.DeferredQueryMessage;
import edu.brown.hstore.internal.FinishTxnMessage;
import edu.brown.hstore.internal.InternalMessage;
import edu.brown.hstore.internal.InternalTxnMessage;
import edu.brown.hstore.internal.PotentialSnapshotWorkMessage;
import edu.brown.hstore.internal.PrepareTxnMessage;
import edu.brown.hstore.internal.SetDistributedTxnMessage;
import edu.brown.hstore.internal.StartTxnMessage;
import edu.brown.hstore.internal.UtilityWorkMessage;
import edu.brown.hstore.internal.UtilityWorkMessage.TableStatsRequestMessage;
import edu.brown.hstore.internal.UtilityWorkMessage.UpdateMemoryMessage;
import edu.brown.hstore.internal.WorkFragmentMessage;
import edu.brown.hstore.specexec.QueryTracker;
import edu.brown.hstore.specexec.checkers.AbstractConflictChecker;
import edu.brown.hstore.specexec.checkers.MarkovConflictChecker;
import edu.brown.hstore.specexec.checkers.OptimisticConflictChecker;
import edu.brown.hstore.specexec.checkers.TableConflictChecker;
import edu.brown.hstore.specexec.checkers.UnsafeConflictChecker;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.DependencyTracker;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.MapReduceTransaction;
import edu.brown.hstore.txns.PrefetchState;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.hstore.util.ArrayCache.IntArrayCache;
import edu.brown.hstore.util.ArrayCache.LongArrayCache;
import edu.brown.hstore.util.ParameterSetArrayCache;
import edu.brown.hstore.util.TransactionCounter;
import edu.brown.hstore.util.TransactionUndoTokenComparator;
import edu.brown.hstore.util.TransactionWorkRequestBuilder;
import edu.brown.interfaces.Configurable;
import edu.brown.interfaces.DebugContext;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.profilers.PartitionExecutorProfiler;
import edu.brown.protorpc.NullCallback;
import edu.brown.statistics.FastIntHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
/**
 * The main executor of transactional work in the system for a single partition.
 * Controls running stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class PartitionExecutor implements Runnable, Configurable, Shutdownable {
    private static final Logger LOG = Logger.getLogger(PartitionExecutor.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final long WORK_QUEUE_POLL_TIME = 10; // 0.5 milliseconds
    private static final TimeUnit WORK_QUEUE_POLL_TIMEUNIT = TimeUnit.MICROSECONDS;
    
    private static final UtilityWorkMessage UTIL_WORK_MSG = new UtilityWorkMessage();
    private static final UpdateMemoryMessage STATS_WORK_MSG = new UpdateMemoryMessage();
    
    // ----------------------------------------------------------------------------
    // INTERNAL EXECUTION STATE
    // ----------------------------------------------------------------------------

    /**
     * The current execution mode for this PartitionExecutor
     * This defines what level of speculative execution we have enabled.
     */
    public enum ExecutionMode {
        /**
         * Disable processing all transactions until told otherwise.
         * We will still accept new ones
         */
        DISABLED,
        /**
         * Reject any transaction that tries to get added
         */
        DISABLED_REJECT,
        /**
         * No speculative execution. All transactions are committed immediately
         */
        COMMIT_ALL,
        /**
         * Allow read-only txns to return results.
         */
        COMMIT_READONLY,
        /**
         * Allow non-conflicting txns to return results.
         */
        COMMIT_NONCONFLICTING,
        /**
         * All txn responses must wait until the current distributed txn is committed
         */ 
        COMMIT_NONE,
    };
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Thread self;

    /**
     * If this flag is enabled, then we need to shut ourselves down and stop running txns
     */
    private ShutdownState shutdown_state = Shutdownable.ShutdownState.INITIALIZED;
    private Semaphore shutdown_latch;
    
    /**
     * Catalog objects
     */
    protected final CatalogContext catalogContext;
    protected Site site;
    protected int siteId;
    private Partition partition;
    private int partitionId;

    private final BackendTarget backend_target;
    private final ExecutionEngine ee;
    private final HsqlBackend hsql;
    private final DBBPool buffer_pool = new DBBPool(false, false);
    private final FastSerializer fs = new FastSerializer(this.buffer_pool);
    
    /**
     * The PartitionEstimator is what we use to figure our what partitions each 
     * query invocation needs to be sent to at run time.
     * It is deterministic.
     */
    private final PartitionEstimator p_estimator;
    
    /**
     * The TransactionEstimator is the runtime piece that we use to keep track of
     * where a locally running transaction is in its execution workflow. This allows 
     * us to make predictions about what kind of things we expect the xact to do in 
     * the future
     */
    private final TransactionEstimator localTxnEstimator;
    
    private EstimationThresholds thresholds = EstimationThresholds.factory();
    
    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;

    /**
     * ProcedureId -> Queue<VoltProcedure>
     */
    private final Queue<VoltProcedure>[] procedures;
    
    // ----------------------------------------------------------------------------
    // H-Store Transaction Stuff
    // ----------------------------------------------------------------------------

    private HStoreSite hstore_site;
    private HStoreCoordinator hstore_coordinator;
    private HStoreConf hstore_conf;
    private TransactionQueueManager queueManager;
    private PartitionLockQueue lockQueue;
    private DependencyTracker depTracker;
    
    // ----------------------------------------------------------------------------
    // Work Queue
    // ----------------------------------------------------------------------------
    
    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * WorkFragment (i.e., execute some fragments on behalf of another transaction)
     * We will use this special wrapper around the PartitionExecutorQueue that can determine
     * whether this partition is overloaded and therefore new requests should be throttled
     */
    private final PartitionMessageQueue work_queue;
    
    // ----------------------------------------------------------------------------
    // Internal Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * The transaction id of the current transaction
     * This is mostly used for testing and should not be relied on from the outside.
     */
    private Long currentTxnId = null;
    
    /**
     * We can only have one active "parent" transaction at a time.
     * We can speculatively execute other transactions out of order, but the active parent
     * transaction will always be the same.  
     */
    private AbstractTransaction currentTxn;
    
    /**
     * We can only have one active distributed transactions at a time.  
     * The multi-partition TransactionState that is currently executing at this partition
     * When we get the response for these txn, we know we can commit/abort the speculatively executed transactions
     */
    private AbstractTransaction currentDtxn = null;
    private String lastDtxnDebug = null;
    
    /**
     * The current VoltProcedure handle that is executing at this partition
     * This will be set to null as soon as the VoltProcedure.run() method completes
     */
    private VoltProcedure currentVoltProc = null;
    
    /**
     * List of messages that are blocked waiting for the outstanding dtxn to commit
     */
    private final List<InternalMessage> currentBlockedTxns = new ArrayList<InternalMessage>();

    /**
     * The current ExecutionMode. This defines when transactions are allowed to execute
     * and whether they can return their results to the client immediately or whether they
     * must wait until the current_dtxn commits.
     */
    private ExecutionMode currentExecMode = ExecutionMode.COMMIT_ALL;

    /**
     * The time in ms since epoch of the last call to ExecutionEngine.tick(...)
     */
    private long lastTickTime = 0;

    /**
     * The time in ms since last stats update
     */
    private long lastStatsTime = 0;
    
    /**
     * The last txn id that we executed (either local or remote)
     */
    private volatile Long lastExecutedTxnId = null;
    
    /**
     * The last txn id that we committed
     */
    private volatile Long lastCommittedTxnId = Long.valueOf(-1l);
    
    /**
     * The last undoToken that we handed out
     */
    private long lastUndoToken = 0l;
    
    /**
     * The last undoToken that we committed at this partition
     */
    private long lastCommittedUndoToken = -1l;
    
    // ARIES    
    private boolean m_ariesRecovery;    
     
    private final String m_ariesDefaultLogFileName = "aries.log";
    
    public long getArieslogBufferLength() {
        return ee.getArieslogBufferLength();
    }

    public void getArieslogData(int bufferLength, byte[] arieslogDataArray) {
        ee.getArieslogData(bufferLength, arieslogDataArray);
    }

    public long readAriesLogForReplay(long[] size) {
        return ee.readAriesLogForReplay(size);
    }

    public void freePointerToReplayLog(long ariesReplayPointer) {
        ee.freePointerToReplayLog(ariesReplayPointer);
    }

    public boolean doingAriesRecovery() 
    {
        return m_ariesRecovery;
    }
    
    public void ariesRecoveryCompleted() 
    {
     //m_ariesRecovery = false;
    }
    
    // ----------------------------------------------------------------------------
    // SPECULATIVE EXECUTION STATE
    // ----------------------------------------------------------------------------
    
    private SpeculationConflictCheckerType specExecCheckerType;
    private AbstractConflictChecker specExecChecker;
    private boolean specExecSkipAfter = false;
    private SpecExecScheduler specExecScheduler;
    
    /**
     * Transactions that were speculatively executed before or after the current 
     * distributed transaction finished at this partition and are now waiting to be committed.
     * Any transaction in this list should have its ClientResponse member set.
     */
    private final LinkedList<LocalTransaction> specExecBlocked = new LinkedList<LocalTransaction>();
    
    /**
     * Special comparator that will sort txns in the order according to their undo tokens.
     */
    private final TransactionUndoTokenComparator specExecComparator;
    
    /**
     * If this flag is set to true, that means some txn has modified the database
     * in the current batch of speculatively executed txns. Any read-only specexec txn that 
     * is executed when this flag is set to false can be returned to the client immediately.
     * TODO: This should really be a bitmap of table ids so that we have finer grain control
     */
    private boolean specExecModified = false;

    /**
     * If set to true, then we should not check for speculative execution candidates
     * at run time. This needs to be set any time we change the currentDtxn
     */
    private boolean specExecIgnoreCurrent = false;
    
    // ----------------------------------------------------------------------------
    // SHARED VOLTPROCEDURE DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * Mapping from SQLStmt batch hash codes (computed by VoltProcedure.getBatchHashCode()) to BatchPlanners
     * The idea is that we can quickly derived the partitions for each unique set of SQLStmt list
     */
    private final Map<Integer, BatchPlanner> batchPlanners = new HashMap<Integer, BatchPlanner>(100);
    
    // ----------------------------------------------------------------------------
    // DISTRIBUTED TRANSACTION TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * WorkFragments that we need to send to a remote HStoreSite for execution
     */
    private final List<WorkFragment.Builder> tmp_remoteFragmentBuilders = new ArrayList<WorkFragment.Builder>();
    /**
     * WorkFragments that we need to send to our own PartitionExecutor
     */
    private final List<WorkFragment.Builder> tmp_localWorkFragmentBuilders = new ArrayList<WorkFragment.Builder>();
    /**
     * WorkFragments that we need to send to a different PartitionExecutor that is on this same HStoreSite
     */
    private final List<WorkFragment.Builder> tmp_localSiteFragmentBuilders = new ArrayList<WorkFragment.Builder>();
    /**
     * Temporary space used when calling removeInternalDependencies()
     */
    private final HashMap<Integer, List<VoltTable>> tmp_removeDependenciesMap = new HashMap<Integer, List<VoltTable>>();
    /**
     * Remote SiteId -> TransactionWorkRequest.Builder
     */
    private final TransactionWorkRequestBuilder tmp_transactionRequestBuilders[];
    /**
     * PartitionId -> List<VoltTable>
     */
    private final Map<Integer, List<VoltTable>> tmp_EEdependencies = new HashMap<Integer, List<VoltTable>>();
    /**
     * List of serialized ParameterSets
     */
    private final List<ByteString> tmp_serializedParams = new ArrayList<ByteString>();
    /**
     * Histogram for the number of WorkFragments that we're going to send to partitions
     * in the current batch.
     */
    private final FastIntHistogram tmp_fragmentsPerPartition = new FastIntHistogram(true);
    /**
     * Reusable int array for StmtCounters
     */
    private final IntArrayCache tmp_stmtCounters = new IntArrayCache(10);
    /**
     * Reusable ParameterSet array cache for WorkFragments
     */
    private final ParameterSetArrayCache tmp_fragmentParams = new ParameterSetArrayCache(5);
    /**
     * Reusable long array for fragment ids
     */
    private final LongArrayCache tmp_fragmentIds = new LongArrayCache(10);
    /**
     * Reusable long array for fragment id offsets
     */
    private final IntArrayCache tmp_fragmentOffsets = new IntArrayCache(10);
    /**
     * Reusable int array for output dependency ids
     */
    private final IntArrayCache tmp_outputDepIds = new IntArrayCache(10);
    /**
     * Reusable int array for input dependency ids
     */
    private final IntArrayCache tmp_inputDepIds = new IntArrayCache(10);
    
    /**
     * The following three arrays are used by utilityWork() to create transactions
     * for deferred queries
     */
    private final SQLStmt[] tmp_def_stmt = new SQLStmt[1];
    private final ParameterSet[] tmp_def_params = new ParameterSet[1];
    private LocalTransaction tmp_def_txn;
    
    // ----------------------------------------------------------------------------
    // INTERNAL CLASSES
    // ----------------------------------------------------------------------------
    
    private class DonePartitionsNotification {
        /**
         * All of the partitions that a transaction is currently done with.
         */
        private final PartitionSet donePartitions = new PartitionSet();
        
        /**
         * RemoteSiteId -> Partitions that we need to notify that this txn is done with.
         */
        private PartitionSet[] notificationsPerSite;
        
        /**
         * Site ids that we need to notify separately about the done partitions.
         */
        private Collection<Integer> _sitesToNotify;
        
        public void addSiteNotification(Site remoteSite, int partitionId, boolean noQueriesInBatch) {
            int remoteSiteId = remoteSite.getId();
            if (this.notificationsPerSite == null) {
                this.notificationsPerSite = new PartitionSet[catalogContext.numberOfSites];
            }
            if (this.notificationsPerSite[remoteSiteId] == null) {
                this.notificationsPerSite[remoteSiteId] = new PartitionSet();
            }
            this.notificationsPerSite[remoteSiteId].add(partitionId);
            if (noQueriesInBatch) {
                if (this._sitesToNotify == null) {
                    this._sitesToNotify = new HashSet<Integer>();
                }
                this._sitesToNotify.add(Integer.valueOf(remoteSiteId));
            }
        }
        
        /**
         * Return the set of partitions that needed to be notified separately
         * for the given site id. The return value may be null.
         * @param remoteSiteId
         * @return
         */
        public PartitionSet getNotifications(int remoteSiteId) {
            if (this.notificationsPerSite != null) {
                return (this.notificationsPerSite[remoteSiteId]);
            }
            return (null);
        }
        
        public boolean hasSitesToNotify() {
            return (this._sitesToNotify != null && this._sitesToNotify.isEmpty() == false);
        }
    }
    /**
     * Parse a string for size. Units accepted are K, M, G, T. Format is 4G.
     * @param size
     */
    private long parseSize(String size) {
        String unit = size.substring(size.length() - 1);
        long Size = Long.valueOf(size.substring(0, size.length() - 1));
        if (unit.equals("K")) {
            Size = Size * 1024;
        } else if (unit.equals("M")) {
            Size = Size * 1024*1024;
        } else if (unit.equals("G")) {
            Size = Size * 1024*1024*1024;
        } else if (unit.equals("T")) {
            Size = Size * 1024*1024*1024*1024;
        } else {
            //Throw error
        }
        return Size;
    }

 
    // ----------------------------------------------------------------------------
    // PROFILING OBJECTS
    // ----------------------------------------------------------------------------
    
    private final PartitionExecutorProfiler profiler = new PartitionExecutorProfiler();
    
    // ----------------------------------------------------------------------------
    // WORK REQUEST CALLBACK
    // ----------------------------------------------------------------------------
    
    /**
     * This will be invoked for each TransactionWorkResponse that comes back from
     * the remote HStoreSites. Note that we don't need to do any counting as to whether
     * a transaction has gotten back all of the responses that it expected. That logic is down
     * below in waitForResponses()
     */
    private final RpcCallback<TransactionWorkResponse> request_work_callback = new RpcCallback<TransactionWorkResponse>() {
        @Override
        public void run(TransactionWorkResponse msg) {
            Long txn_id = msg.getTransactionId();
            LocalTransaction ts = hstore_site.getTransaction(txn_id);
            
            // We can ignore anything that comes in for a transaction that we don't know about
            if (ts == null) {
                if (debug.val) LOG.debug("No transaction state exists for txn #" + txn_id);
                return;
            }
            
            if (debug.val)
                LOG.debug(String.format("Processing TransactionWorkResponse for %s with %d results%s",
                          ts, msg.getResultsCount(), (trace.val ? "\n"+msg : "")));
            for (int i = 0, cnt = msg.getResultsCount(); i < cnt; i++) {
                WorkResult result = msg.getResults(i); 
                if (debug.val)
                    LOG.debug(String.format("Got %s from partition %d for %s",
                              result.getClass().getSimpleName(), result.getPartitionId(), ts));
                PartitionExecutor.this.processWorkResult(ts, result);
            } // FOR
            if (hstore_conf.site.specexec_enable) { 
                specExecScheduler.interruptSearch(UTIL_WORK_MSG);
            }
        }
    }; // END CLASS

    // ----------------------------------------------------------------------------
    // SYSPROC STUFF
    // ----------------------------------------------------------------------------
    
    // Associate the system procedure planfragment ids to wrappers.
    // Planfragments are registered when the procedure wrapper is init()'d.
    private final Map<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments = new HashMap<Long, VoltSystemProcedure>();

    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            if (!m_registeredSysProcPlanFragments.containsKey(pfId)) {
                assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false) : "Trying to register the same sysproc more than once: " + pfId;
                m_registeredSysProcPlanFragments.put(pfId, proc);
                if (trace.val) LOG.trace(String.format("Registered %s sysproc handle at partition %d for FragmentId #%d",
                                 VoltSystemProcedure.procCallName(proc.getClass()), partitionId, pfId));
            }
        } // SYNCH
    }

    /**
     * SystemProcedures are "friends" with PartitionExecutors and granted
     * access to internal state via m_systemProcedureContext.
     * access to internal state via m_systemProcedureContext.
     */
    public interface SystemProcedureExecutionContext {
        public Catalog getCatalog();
        public Database getDatabase();
        public Cluster getCluster();
        public Site getSite();
        public Host getHost();
        public ExecutionEngine getExecutionEngine();
        public long getLastCommittedTxnId();
        public PartitionExecutor getPartitionExecutor();
        public HStoreSite getHStoreSite();
        public Long getCurrentTxnId();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Catalog getCatalog()                 { return catalogContext.catalog; }
        public Database getDatabase()               { return catalogContext.database; }
        public Cluster getCluster()                 { return catalogContext.cluster; }
        public Site getSite()                       { return site; }
        public Host getHost()                       { return site.getHost(); }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return lastCommittedTxnId; }
        public PartitionExecutor getPartitionExecutor() { return PartitionExecutor.this; }
        public HStoreSite getHStoreSite()           { return hstore_site; }
        public Long getCurrentTxnId()               { return PartitionExecutor.this.currentTxnId; }
    }

    private final SystemProcedureContext m_systemProcedureContext = new SystemProcedureContext();
    private AriesLog m_ariesLog ;

    public SystemProcedureExecutionContext getSystemProcedureExecutionContext(){
	return m_systemProcedureContext;
    }	
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Dummy constructor...
     */
    protected PartitionExecutor() {
        this.catalogContext = null;
        this.work_queue = null;
        this.ee = null;
        this.hsql = null;
        this.specExecChecker = null;
        this.specExecScheduler = null;
        this.specExecComparator = null;
        this.p_estimator = null;
        this.localTxnEstimator = null;
        this.m_snapshotter = null;
        this.thresholds = null;
        this.site = null;
        this.backend_target = BackendTarget.HSQLDB_BACKEND;
        this.siteId = 0;
        this.partitionId = 0;
        this.procedures = null;
        this.tmp_transactionRequestBuilders = null;
        this.m_ariesLog = null;
    }

    /**
     * Initialize the StoredProcedure runner and EE for this Site.
     * @param partitionId
     * @param t_estimator
     * @param coordinator
     * @param siteManager
     * @param serializedCatalog A list of catalog commands, separated by
     * newlines that, when executed, reconstruct the complete m_catalog.
     */
    public PartitionExecutor(final int partitionId,
                             final CatalogContext catalogContext,
                             final BackendTarget target,
                             final PartitionEstimator p_estimator,
                             final TransactionEstimator t_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        this.work_queue = new PartitionMessageQueue();
        this.backend_target = target;
        this.catalogContext = catalogContext;
        this.partition = catalogContext.getPartitionById(partitionId);
        assert(this.partition != null) : "Invalid Partition #" + partitionId;
        this.partitionId = this.partition.getId();
        this.site = this.partition.getParent();
        assert(site != null) : "Unable to get Site for Partition #" + partitionId;
        this.siteId = this.site.getId();

        this.lastUndoToken = this.partitionId * 1000000;
        this.p_estimator = p_estimator;
        this.localTxnEstimator = t_estimator;
        this.specExecComparator = new TransactionUndoTokenComparator(this.partitionId);
                
        // VoltProcedure Queues
        @SuppressWarnings("unchecked")
        Queue<VoltProcedure> voltProcQueues[] = new Queue[catalogContext.procedures.size()+1];
        this.procedures = voltProcQueues;
        
        // An execution site can be backed by HSQLDB, by volt's EE accessed
        // via JNI or by volt's EE accessed via IPC.  When backed by HSQLDB,
        // the VoltProcedure interface invokes HSQLDB directly through its
        // hsql Backend member variable.  The real volt backend is encapsulated
        // by the ExecutionEngine class. This class has implementations for both
        // JNI and IPC - and selects the desired implementation based on the
        // value of this.eeBackend.
        HsqlBackend hsqlTemp = null;
        ExecutionEngine eeTemp = null;
        SnapshotSiteProcessor snapshotter = null;
        try {
            if (trace.val) LOG.trace("Creating EE wrapper with target type '" + target + "'");
            if (this.backend_target == BackendTarget.HSQLDB_BACKEND) {
                hsqlTemp = new HsqlBackend(partitionId);
                final String hexDDL = catalogContext.database.getSchema();
                final String ddl = Encoder.hexDecodeToString(hexDDL);
                final String[] commands = ddl.split(";");
                for (String command : commands) {
                    if (command.length() == 0) {
                        continue;
                    }
                    hsqlTemp.runDDL(command);
                }
                eeTemp = new MockExecutionEngine();
                
            }
            else if (target == BackendTarget.NATIVE_EE_JNI) {
                org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
                // set up the EE
                eeTemp = new ExecutionEngineJNI(this,
                                                catalogContext.cluster.getRelativeIndex(),
                                                this.getSiteId(),
                                                this.getPartitionId(),
                                                this.site.getHost().getId(),
                                                "localhost");
                
               // Initialize Anti-Cache
                if (hstore_conf.site.anticache_enable) {
                    boolean blockMerge = hstore_conf.site.anticache_block_merge;
                    if (!hstore_conf.site.anticache_enable_multilevel) {
                        File acFile = AntiCacheManager.getDatabaseDir(this, 0);
                        long blockSize = hstore_conf.site.anticache_block_size;
                        AntiCacheDBType dbType = AntiCacheDBType.get(hstore_conf.site.anticache_dbtype);
                        boolean blocking = hstore_conf.site.anticache_db_blocks;
                        // XXX: MJG: TODO: We've got to get a sane value for maxSize. -1 is no bueno.
                        long dbSize = parseSize(hstore_conf.site.anticache_dbsize);
                        LOG.info(String.format("Creating AntiCacheDB type: %d blocking: %b blockmerge: %b blocksize: %d maxsize: %d @ %s (dbtype: %s)", 
                                  dbType.ordinal(), blocking, blockMerge, blockSize, dbSize, acFile.getAbsolutePath(), hstore_conf.site.anticache_dbtype));
                        eeTemp.antiCacheInitialize(acFile, dbType, blocking, blockSize, dbSize, blockMerge);
                    } else {
                    // if we are using multilevel, ignore single config options and parse string
                        String config = hstore_conf.site.anticache_levels;
                        String delims = "[;]";
                        String[] levels = config.split(delims);

                        for (int i = 0; i < levels.length; i++) {
                            delims = "[,]";
                            String[] opts = levels[i].split(delims); 
                            AntiCacheDBType dbType = AntiCacheDBType.get(opts[0]);
                            
                            String blockingstr = opts[1];
                            boolean blocking = Boolean.parseBoolean(blockingstr);

                            String blockstr = opts[2];
                            long blockSize = parseSize(blockstr);

                            String maxstr = opts[3];
                            long maxSize = parseSize(maxstr) / catalogContext.numberOfPartitions;
                            
                            File acFile = AntiCacheManager.getDatabaseDir(this, i);
                            LOG.info(String.format("Creating AntiCacheDB type: %d blocking: %b blockMerge: %b blocksize: %d maxsize: %d @ %s", 
                                  dbType.ordinal(), blocking, blockMerge, blockSize, maxSize, acFile.getAbsolutePath()));
                            if (i == 0) {
                                eeTemp.antiCacheInitialize(acFile, dbType, blocking, blockSize, maxSize, blockMerge);
                            } else {
                                eeTemp.antiCacheAddDB(acFile, dbType, blocking, blockSize, maxSize, blockMerge);
                        
                            }
                        }
                    }      
                }
                
                // Initialize STORAGE_MMAP
                if (hstore_conf.site.storage_mmap) {
                    File dbFile = getMMAPDir(this);
                    long mapSize = hstore_conf.site.storage_mmap_file_size;
                    long syncFrequency = hstore_conf.site.storage_mmap_sync_frequency;
                    eeTemp.MMAPInitialize(dbFile, mapSize, syncFrequency);
                }
                
                // Initialize ARIES
                if (hstore_conf.site.aries) {
                    File dbFile = getARIESDir(this);
                    File logFile = getARIESFile(this);
                    eeTemp.ARIESInitialize(dbFile, logFile);
                }                            
                
                // Important: This has to be called *after* we initialize the anti-cache
                //            and the storage information!
                eeTemp.loadCatalog(catalogContext.catalog.serialize());
                this.lastTickTime = System.currentTimeMillis();
                eeTemp.tick(this.lastTickTime, 0);
                
                snapshotter = new SnapshotSiteProcessor(new Runnable() {
                    final PotentialSnapshotWorkMessage msg = new PotentialSnapshotWorkMessage();
                    @Override
                    public void run() {
                        PartitionExecutor.this.work_queue.add(this.msg);
                    }
                });
            }
            else {
                // set up the EE over IPC
                eeTemp = new ExecutionEngineIPC(this,
                                                catalogContext.cluster.getRelativeIndex(),
                                                this.getSiteId(),
                                                this.getPartitionId(),
                                                this.site.getHost().getId(),
                                                "localhost",
                                                target);
                eeTemp.loadCatalog(catalogContext.catalog.serialize());
                this.lastTickTime = System.currentTimeMillis();
                eeTemp.tick(this.lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            throw new ServerFaultException("Failed to initialize PartitionExecutor", ex);
        }
        this.ee = eeTemp;
        this.hsql = hsqlTemp;
        m_snapshotter = snapshotter;
        assert(this.ee != null);
        assert(!(this.ee == null && this.hsql == null)) : "Both execution engine objects are empty. This should never happen";
        
        // Initialize temporary data structures
        int num_sites = this.catalogContext.numberOfSites;
        this.tmp_transactionRequestBuilders = new TransactionWorkRequestBuilder[num_sites];
    }

       
    /**
     * Link this PartitionExecutor with its parent HStoreSite
     * This will initialize the references the various components shared among the PartitionExecutors 
     * @param hstore_site
     */
    public void initHStoreSite(HStoreSite hstore_site) {
        if (trace.val)
            LOG.trace(String.format("Initializing HStoreSite components at partition %d", this.partitionId));
        assert(this.hstore_site == null) :
            String.format("Trying to initialize HStoreSite for PartitionExecutor #%d twice!", this.partitionId);
        this.hstore_site = hstore_site;
        this.depTracker = hstore_site.getDependencyTracker(this.partitionId);
        this.thresholds = hstore_site.getThresholds();
        this.queueManager = hstore_site.getTransactionQueueManager();
        this.lockQueue = this.queueManager.getLockQueue(this.partitionId);
        
        if (hstore_conf.site.exec_deferrable_queries) {
            tmp_def_txn = new LocalTransaction(hstore_site);
        }

        // ARIES        
        this.m_ariesLog = this.hstore_site.getAriesLogger();

        // -------------------------------
        // BENCHMARK START NOTIFICATIONS
        // -------------------------------
        
        // Poke ourselves to update the partition stats when the first
        // non-sysproc procedure shows up. I forget why we need to do this...
        EventObservable<HStoreSite> observable = this.hstore_site.getStartWorkloadObservable(); 
        observable.addObserver(new EventObserver<HStoreSite>() {
            @Override
            public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
                queueUtilityWork(STATS_WORK_MSG);
            }
        });
        
        // Reset our profiling information when we get the first non-sysproc
        this.profiler.resetOnEventObservable(observable);
        
        // Initialize speculative execution scheduler
        this.initSpecExecScheduler();
    }
    
    private void setSpecExecChecker(AbstractConflictChecker checker) {
        this.specExecChecker = checker;
        this.specExecSkipAfter = this.specExecChecker.skipConflictAfter();
        
        if (this.specExecScheduler != null) {
            this.specExecScheduler.getDebugContext().setConflictChecker(checker);
        }
    }
    
    /**
     * Initialize this PartitionExecutor' speculative execution scheduler
     */
    private void initSpecExecScheduler() {
        assert(this.specExecScheduler == null);
        assert(this.hstore_site != null);
        
        this.specExecCheckerType = SpeculationConflictCheckerType.get(hstore_conf.site.specexec_scheduler_checker);
        AbstractConflictChecker checker = null;
        switch (this.specExecCheckerType) {
            // -------------------------------
            // ROW-LEVEL
            // -------------------------------
            case MARKOV:
                // The MarkovConflictChecker is thread-safe, so we all of the partitions
                // at this site can reuse the same one.
                checker = MarkovConflictChecker.singleton(this.catalogContext, this.thresholds);
                break;
            // -------------------------------
            // TABLE-LEVEL
            // -------------------------------
            case TABLE:
                checker = new TableConflictChecker(this.catalogContext);
                break;
            // -------------------------------
            // UNSAFE
            // NOTE: You probably don't want to use this!
            // -------------------------------
            case UNSAFE:
                checker = new UnsafeConflictChecker(this.catalogContext, hstore_conf.site.specexec_unsafe_limit);
                LOG.warn(StringUtil.bold(String.format("Using %s in %s for partition %d. This is a bad idea!",
                         checker.getClass().getSimpleName(), this.getClass().getSimpleName(), this.partitionId)));
                break;
            // -------------------------------
            // OPTIMISTIC
            // -------------------------------
            case OPTIMISTIC:
                checker = new OptimisticConflictChecker(this.catalogContext, this.ee);
                break;
            // BUSTED!
            default: {
                String msg = String.format("Invalid %s '%s'",
                                           SpeculationConflictCheckerType.class.getSimpleName(),
                                           hstore_conf.site.specexec_scheduler_checker);
                throw new RuntimeException(msg);
            }
        } // SWITCH
        this.setSpecExecChecker(checker);
        assert(this.specExecChecker != null);
        
        SpecExecSchedulerPolicyType policy = SpecExecSchedulerPolicyType.get(hstore_conf.site.specexec_scheduler_policy);
        assert(policy != null) : String.format("Invalid %s '%s'",
                                               SpecExecSchedulerPolicyType.class.getSimpleName(),
                                               hstore_conf.site.specexec_scheduler_policy);
        assert(this.lockQueue.getPartitionId() == this.partitionId);
        this.specExecScheduler = new SpecExecScheduler(this.specExecChecker,
                                                       this.partitionId,
                                                       this.lockQueue,
                                                       policy,
                                                       hstore_conf.site.specexec_scheduler_window);
        this.specExecChecker.setEstimationThresholds(this.thresholds);
        this.specExecScheduler.updateConf(hstore_conf, null);
        
        if (debug.val && hstore_conf.site.specexec_enable)
            LOG.debug(String.format("Initialized %s for partition %d [checker=%s, policy=%s]",
                      this.specExecScheduler.getClass().getSimpleName(), this.partitionId,
                      this.specExecChecker.getClass().getSimpleName(), policy));
    }
    
    @Override
    public void updateConf(HStoreConf hstore_conf, String[] changed) {
        if (this.specExecScheduler != null) {
            this.specExecScheduler.updateConf(hstore_conf, changed);
        }
    }
    
    // ARIES
    public void waitForAriesRecoveryCompletion() {
        // wait for other threads to complete Aries recovery
        // ONLY called from main site.
        while (!m_ariesLog.isRecoveryCompleted()) {
            /*
            try {
                // don't sleep too long, shouldn't bias numbers
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
            }
            */
        }
    }
    
    public void doPartitionRecovery(long txnIdToBeginReplay) {    
        LOG.warn("ARIES : aries : " + this.hstore_conf.site.aries+ " aries forward only : "+this.hstore_conf.site.aries_forward_only );

        if (this.hstore_conf.site.aries && this.hstore_conf.site.aries_forward_only == false) {
            // long logReadStartTime = System.currentTimeMillis();

            // define an array so that we can pass to native code by reference
            long size[] = new long[1];
            long ariesReplayPointer = readAriesLogForReplay(size);

            // LOG.info("ARIES : replay pointer address: " +
            // ariesReplayPointer);
            LOG.info("ARIES : partition recovery started at partition : " + this.partitionId + " log size :" + size[0]);

            // long logReadEndTime = System.currentTimeMillis();
            // LOG.info("ARIES : log read in " + (logReadEndTime -
            // logReadStartTime) + " milliseconds");

            long ariesStartTime = System.currentTimeMillis();

            m_ariesLog.setPointerToReplayLog(ariesReplayPointer, size[0]);
            m_ariesLog.setTxnIdToBeginReplay(txnIdToBeginReplay);

            waitForAriesRecoveryCompletion();

            freePointerToReplayLog(ariesReplayPointer);

            long ariesEndTime = System.currentTimeMillis();
            LOG.info("ARIES : partition recovery finished in " + (ariesEndTime - ariesStartTime) + " milliseconds");

            m_ariesLog.init();
        }
    }
        
    // ----------------------------------------------------------------------------
    // MAIN EXECUTION LOOP
    // ----------------------------------------------------------------------------
    
    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public final void run() {
        if (this.hstore_site == null) {
            String msg = String.format("Trying to start %s for partition %d before its HStoreSite was initialized",
                                       this.getClass().getSimpleName(), this.partitionId);
            throw new RuntimeException(msg);
        }
        else if (this.self != null) {
            String msg = String.format("Trying to restart %s for partition %d after it was already running",
                                       this.getClass().getSimpleName(), this.partitionId);
            throw new RuntimeException(msg);
        }
        
        // Initialize all of our VoltProcedures handles
        // This needs to be done here so that the Workload trace handles can be 
        // set up properly
        this.initializeVoltProcedures();
        
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(this.hstore_site, this.partitionId));
        
        this.hstore_coordinator = hstore_site.getCoordinator();
        this.hstore_site.getThreadManager().registerEEThread(partition);
        this.shutdown_latch = new Semaphore(0);
        this.shutdown_state = ShutdownState.STARTED;
        if (hstore_conf.site.exec_profiling) profiler.start_time = System.currentTimeMillis();

        assert(this.hstore_site != null);
        assert(this.hstore_coordinator != null);
        assert(this.specExecScheduler != null);
        assert(this.queueManager != null);
        
        // ARIES :: Starts recovery on partition
        if(m_ariesLog != null){
            doPartitionRecovery(Long.MIN_VALUE);
        }
        
        // *********************************** DEBUG ***********************************
        if (hstore_conf.site.exec_validate_work) {
            LOG.warn("Enabled Distributed Transaction Validation Checker");
        }
        // *********************************** DEBUG ***********************************

        // Things that we will need in the loop below
        InternalMessage nextWork = null;
        AbstractTransaction nextTxn = null;
        if (debug.val)
            LOG.debug("Starting PartitionExecutor run loop...");
        try {
            while (this.shutdown_state == ShutdownState.STARTED) {
                this.currentTxnId = null;
                nextTxn = null;
                nextWork = null;
                
                // This is the starting state of the PartitionExecutor.
                // At this point here we currently don't have a txn to execute nor 
                // are we involved in a distributed txn running at another partition.
                // So we need to go our PartitionLockQueue and get back the next
                // txn that will have our lock.
                if (this.currentDtxn == null) {
                    this.tick();
                    
                    if (hstore_conf.site.exec_profiling) profiler.poll_time.start();
                    try {
                        nextTxn = this.queueManager.checkLockQueue(this.partitionId); // NON-BLOCKING
                    } finally {
                        if (hstore_conf.site.exec_profiling) profiler.poll_time.stopIfStarted();
                    }
                    
                    // If we get something back here, then it should become our current transaction.
                    if (nextTxn != null) {
                        // If it's a single-partition txn, then we can return the StartTxnMessage 
                        // so that we can fire it off right away.
                        if (nextTxn.isPredictSinglePartition()) {
                            LocalTransaction localTxn = (LocalTransaction)nextTxn;
                            nextWork = localTxn.getStartTxnMessage();
                            if (hstore_conf.site.txn_profiling && localTxn.profiler != null) 
                                localTxn.profiler.startQueueExec();
                        }
                        // If it's as distribued txn, then we'll want to just set it as our 
                        // current dtxn at this partition and then keep checking the queue
                        // for more work.
                        else {
                            this.setCurrentDtxn(nextTxn);
                        }
                    }
                }
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                
                // Check if we have anything to do right now
                if (nextWork == null) {
                    if (hstore_conf.site.exec_profiling) profiler.idle_time.start();
                    try {
                        // If we're allowed to speculatively execute txns, then we don't want to have
                        // to wait to see if anything will show up in our work queue.
                        if (hstore_conf.site.specexec_enable && this.lockQueue.approximateIsEmpty() == false) {
                            nextWork = this.work_queue.poll();
                            /*if (nextWork != null) {
                                        System.out.println(String.format("Polled a work %s from partition %d",
                                                                          nextWork.getClass().getSimpleName(), this.work_queue.size()));
                            } else {
                                System.out.println("Null work!");
                            }*/
                        } else {
                            nextWork = this.work_queue.poll(WORK_QUEUE_POLL_TIME, WORK_QUEUE_POLL_TIMEUNIT);    
                            /*if (nextWork != null) {
                                        LOG.info(String.format("Polled a work %s from partition %d",
                                                                          nextWork.getClass().getSimpleName(), this.work_queue.size()));
                            } else {
                                LOG.info("Null work!");
                            }*/
                        }
                    } catch (InterruptedException ex) {
                        continue;
                    } finally {
                        if (hstore_conf.site.exec_profiling) profiler.idle_time.stopIfStarted();
                    }
                }
                
                // -------------------------------
                // Process Work
                // -------------------------------
                if (nextWork != null) {
                    if (trace.val) LOG.trace("Next Work: " + nextWork);
                    if (hstore_conf.site.exec_profiling) {
                        profiler.numMessages.put(nextWork.getClass().getSimpleName());
                        profiler.exec_time.start();
                        if (this.currentDtxn != null) profiler.sp2_time.stopIfStarted();
                    }
                    try {
                        // -------------------------------
                        // TRANSACTIONAL WORK
                        // -------------------------------
                        if (nextWork instanceof InternalTxnMessage) {
                            this.processInternalTxnMessage((InternalTxnMessage)nextWork);
                        }
                        // -------------------------------
                        // EVERYTHING ELSE
                        // -------------------------------
                        else {
                            this.processInternalMessage(nextWork);
                        }
                    } finally {
                        if (hstore_conf.site.exec_profiling) {
                            profiler.exec_time.stopIfStarted();
                            if (this.currentDtxn != null) profiler.sp2_time.start();
                        }
                    }
                    if (this.currentTxnId != null) this.lastExecutedTxnId = this.currentTxnId;
                }
                // Check if we have any utility work to do while we wait
                else if (hstore_conf.site.specexec_enable) {
//                    if (trace.val)
//                        LOG.trace(String.format("The %s for partition %s empty. Checking for utility work...",
//                                  this.work_queue.getClass().getSimpleName(), this.partitionId));
                    if (this.utilityWork()) {
                        nextWork = UTIL_WORK_MSG;
                    }
                } else {
                    ThreadUtil.sleep(5);
                }
            } // WHILE
        } catch (final Throwable ex) {
            if (this.isShuttingDown() == false) {
                // ex.printStackTrace();
                LOG.fatal(String.format("Unexpected error at partition %d [current=%s, lastDtxn=%s]",
                          this.partitionId, this.currentTxn, this.lastDtxnDebug), ex);
                if (this.currentTxn != null) LOG.fatal("TransactionState Dump:\n" + this.currentTxn.debug());
            }
            this.shutdown_latch.release();
            this.hstore_coordinator.shutdownClusterBlocking(ex);
        } finally {
            if (debug.val) {
                String txnDebug = "";
                if (this.currentTxn != null && this.currentTxn.getBasePartition() == this.partitionId) {
                    txnDebug = " while a txn is still running\n" + this.currentTxn.debug();
                }
                LOG.warn(String.format("PartitionExecutor %d is stopping%s%s",
                         this.partitionId,
                         (this.currentTxnId != null ? " In-Flight Txn: #" + this.currentTxnId : ""),
                         txnDebug));
            }
            
            // Release the shutdown latch in case anybody waiting for us
            this.shutdown_latch.release();
        }
    }
    
    /**
     * Special function that allows us to do some utility work while 
     * we are waiting for a response or something real to do.
     * Note: this tracks how long the system spends doing utility work. It would
     * be interesting to have the system report on this before it shuts down.
     * @return true if there is more utility work that can be done
     */
    private boolean utilityWork() {
        if (hstore_conf.site.exec_profiling) this.profiler.util_time.start();
        
        // -------------------------------
        // Poll Lock Queue
        // -------------------------------

        LocalTransaction specTxn = null;
        InternalMessage work = null;
        
        // Check whether there is something we can speculatively execute right now
        if (this.specExecIgnoreCurrent == false && this.lockQueue.approximateIsEmpty() == false) {
//            if (trace.val)
//                LOG.trace(String.format("Checking %s for something to do at partition %d while %s",
//                          this.specExecScheduler.getClass().getSimpleName(),
//                          this.partitionId,
//                          (this.currentDtxn != null ? "blocked on " + this.currentDtxn : "idle")));
            assert(hstore_conf.site.specexec_enable) :
                "Trying to schedule speculative txn even though it is disabled";
            SpeculationType specType = this.calculateSpeculationType();
            if (hstore_conf.site.exec_profiling) this.profiler.conflicts_time.start();
            try {
                specTxn = this.specExecScheduler.next(this.currentDtxn, specType);
            } finally {
                if (hstore_conf.site.exec_profiling) this.profiler.conflicts_time.stopIfStarted();
            }
            
            // Because we don't have fine-grained undo support, we are just going
            // keep all of our speculative execution txn results around
            if (specTxn != null) {
                // TODO: What we really want to do is check to see whether we have anything
                // in our work queue before we go ahead and fire off this txn
                if (debug.val) {
                    if (this.work_queue.isEmpty() == false) {
                        LOG.warn(String.format("About to speculatively execute %s on partition %d but there " +
                                 "are %d messages in the work queue\n%s",
                                 specTxn, this.partitionId, this.work_queue.size(),
                                 CollectionUtil.first(this.work_queue)));
                    }
                    LOG.debug(String.format("Utility Work found speculative txn to execute on " +
                              "partition %d [%s, specType=%s]",
                              this.partitionId, specTxn, specType));
                    // IMPORTANT: We need to make sure that we remove this transaction for the lock queue
                    // before we execute it so that we don't try to run it again.
                    // We have to do this now because otherwise we may get the same transaction again
                    assert(this.lockQueue.contains(specTxn.getTransactionId()) == false) :
                        String.format("Failed to remove speculative %s before executing", specTxn);
                }
                assert(specTxn.getBasePartition() == this.partitionId) :
                    String.format("Trying to speculatively execute %s at partition %d but its base partition is %d\n%s",
                                  specTxn, this.partitionId, specTxn.getBasePartition(), specTxn.debug());
                assert(specTxn.isMarkedControlCodeExecuted() == false) :
                    String.format("Trying to speculatively execute %s at partition %d but was already executed\n%s",
                                  specTxn, this.partitionId, specTxn.getBasePartition(), specTxn.debug());
                assert(specTxn.isSpeculative() == false) :
                    String.format("Trying to speculatively execute %s at partition %d but was already speculative\n%s",
                                  specTxn, this.partitionId, specTxn.getBasePartition(), specTxn.debug());
                

                // It's also important that we cancel this txn's init queue callback, otherwise
                // it will never get cleaned up properly. This is necessary in order to support
                // sending out client results *before* the dtxn finishes
                specTxn.getInitCallback().cancel();
                
                // Ok now that that's out of the way, let's run this baby...
                specTxn.setSpeculative(specType);
                if (hstore_conf.site.exec_profiling) profiler.specexec_time.start();
                try {
                    this.executeTransaction(specTxn);
                } finally {
                    if (hstore_conf.site.exec_profiling) profiler.specexec_time.stopIfStarted();
                }
            }
//            else if (trace.val) {
//                LOG.trace(String.format("%s - No speculative execution candidates found at partition %d [queueSize=%d]",
//                          this.currentDtxn, this.partitionId, this.queueManager.getLockQueue(this.partitionId).size()));
//            }
        }
//        else if (trace.val && this.currentDtxn != null) {
//            LOG.trace(String.format("%s - Skipping check for speculative execution txns at partition %d " +
//                      "[lockQueue=%d, specExecIgnoreCurrent=%s]",
//                      this.currentDtxn, this.partitionId, this.lockQueue.size(), this.specExecIgnoreCurrent));
//        }
        
        if (hstore_conf.site.exec_profiling) this.profiler.util_time.stopIfStarted();
        return (specTxn != null || work != null);
    }
    
    // ----------------------------------------------------------------------------
    // MESSAGE PROCESSING METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * Process an InternalMessage
     * @param work
     */
    private final void processInternalMessage(InternalMessage work) {
        
        // -------------------------------
        // UTILITY WORK
        // -------------------------------
        if (work instanceof UtilityWorkMessage) {
            // UPDATE MEMORY STATS
            if (work instanceof UpdateMemoryMessage) {
                //LOG.info("Update mem stats");
                this.updateMemoryStats(EstTime.currentTimeMillis());
                if (this.hstore_site.getAntiCacheManager() != null && this.hstore_site.getAntiCacheManager().getEvictableTables().isEmpty() == false) {
                    this.updateAntiCacheMemoryStats(EstTime.currentTimeMillis());
                }
            }
            // TABLE STATS REQUEST
            else if (work instanceof TableStatsRequestMessage) {
                TableStatsRequestMessage stats_work = (TableStatsRequestMessage)work;
                VoltTable results[] = this.ee.getStats(SysProcSelector.TABLE,
                                                       stats_work.getLocators(),
                                                       false,
                                                       EstTime.currentTimeMillis());
                assert(results.length == 1);
                //results[0].advanceRow();
                //LOG.info(String.format("Notified ovserver at partition %d", results[0].getLong("PARTITION_ID")));
                stats_work.getObservable().notifyObservers(results[0]);
            }
            else {
                // IGNORE
            }
        }
        // -------------------------------
        // DEFERRED QUERIES
        // -------------------------------
        else if (work instanceof DeferredQueryMessage) {
            DeferredQueryMessage def_work = (DeferredQueryMessage)work;
            
            // Set the txnId in our handle to be what the original txn was that
            // deferred this query.
            tmp_def_stmt[0] = def_work.getStmt();
            tmp_def_params[0] = def_work.getParams();
            tmp_def_txn.init(def_work.getTxnId(), 
                       -1, // We don't really need the clientHandle
                       EstTime.currentTimeMillis(),
                       this.partitionId,
                       catalogContext.getPartitionSetSingleton(this.partitionId),
                       false,
                       false,
                       tmp_def_stmt[0].getProcedure(),
                       def_work.getParams(),
                       null // We don't need the client callback
            );
            this.executeSQLStmtBatch(tmp_def_txn, 1, tmp_def_stmt, tmp_def_params, false, false);
        }
        // -------------------------------
        // SNAPSHOT WORK
        // -------------------------------
        else if (work instanceof PotentialSnapshotWorkMessage) {
            m_snapshotter.doSnapshotWork(ee);
        }
        // -------------------------------
        // BAD MOJO!
        // -------------------------------
        else {
            String msg = "Unexpected work message in queue: " + work;
            throw new ServerFaultException(msg, this.currentTxnId);
        }
    }

    /**
     * Process an InternalTxnMessage
     * @param work
     */
    private void processInternalTxnMessage(InternalTxnMessage work) {
        //LOG.info("process a txn msg");
        AbstractTransaction ts = work.getTransaction();
        this.currentTxn = ts;
        this.currentTxnId = ts.getTransactionId();

        // If this transaction has already been aborted and they are trying to give us
        // something that isn't a FinishTaskMessage, then we won't bother processing it
        if (ts.isAborted() && (work instanceof FinishTxnMessage) == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Cannot process %s on partition %d because txn was marked as aborted",
                          ts, work.getClass().getSimpleName(), this.partitionId));
            return;
        }
                    
        if (debug.val)
            LOG.debug(String.format("Processing %s at partition %d", work, this.partitionId));
        
        // -------------------------------
        // Start Transaction
        // -------------------------------
        if (work instanceof StartTxnMessage) {
            if (hstore_conf.site.specexec_enable && ts.isPredictSinglePartition()) this.specExecScheduler.reset();
            if (hstore_conf.site.exec_profiling) profiler.txn_time.start();
            try {
                this.executeTransaction((LocalTransaction)ts);
            } finally {
                if (hstore_conf.site.exec_profiling) profiler.txn_time.stopIfStarted();
            }
        }
        // -------------------------------
        // Execute Query Plan Fragments
        // -------------------------------
        else if (work instanceof WorkFragmentMessage) {
            WorkFragment fragment = ((WorkFragmentMessage)work).getFragment();
            assert(fragment != null);
            
            // HACK HACK HACK
            if (ts.isInitialized() == false) {
                LOG.warn(String.format("Skipping %s at partition %d for unitialized txn",
                         work.getClass().getSimpleName(), this.partitionId));
                return;
            }

            // Get the ParameterSet array for this WorkFragment
            // It can either be attached to the AbstractTransaction handle if it came
            // over the wire directly from the txn's base partition, or it can be attached
            // as for prefetch WorkFragments 
            ParameterSet parameters[] = null;
            if (fragment.getPrefetch()) {
                parameters = ts.getPrefetchParameterSets();
                ts.markExecPrefetchQuery(this.partitionId);
                if (trace.val && ts.isSysProc() == false)
                    LOG.trace(ts + " - Prefetch Parameters:\n" + StringUtil.join("\n", parameters));
            } else {
                parameters = ts.getAttachedParameterSets();
                if (trace.val && ts.isSysProc() == false) 
                    LOG.trace(ts + " - Attached Parameters:\n" + StringUtil.join("\n", parameters));
            }
            
            // At this point we know that we are either the current dtxn or the current dtxn is null
            // We will allow any read-only transaction to commit if
            // (1) The WorkFragment for the remote txn is read-only
            // (2) This txn has always been read-only up to this point at this partition
            ExecutionMode newMode = null;
            if (hstore_conf.site.specexec_enable) {
                if (fragment.getReadOnly() && ts.isExecReadOnly(this.partitionId)) {
                    newMode = ExecutionMode.COMMIT_READONLY ;
                } else {
                    newMode = ExecutionMode.COMMIT_NONE;
                }
            } else {
                newMode = ExecutionMode.DISABLED;
            }
            // There is no current DTXN, so that means its us!
            if (this.currentDtxn == null) {
                this.setCurrentDtxn(ts);
                if (debug.val)
                    LOG.debug(String.format("Marking %s as current DTXN on partition %d [nextMode=%s]",
                              ts, this.partitionId, newMode));                    
            }
            // There is a current DTXN but it's not us!
            // That means we need to block ourselves until it finishes
            else if (this.currentDtxn != ts) {
                if (debug.val)
                    LOG.debug(String.format("%s - Blocking on partition %d until current Dtxn %s finishes",
                              ts, this.partitionId, this.currentDtxn));
                this.blockTransaction(work);
                return;
            }
            assert(this.currentDtxn == ts) :
                String.format("Trying to execute a second Dtxn %s before the current one has finished [current=%s]",
                              ts, this.currentDtxn);
            this.setExecutionMode(ts, newMode);
            this.processWorkFragment(ts, fragment, parameters);
        }
        // -------------------------------
        // Finish Transaction
        // -------------------------------
        else if (work instanceof FinishTxnMessage) {
            FinishTxnMessage fTask = (FinishTxnMessage)work;
            this.finishDistributedTransaction(fTask.getTransaction(), fTask.getStatus());
        }
        // -------------------------------
        // Prepare Transaction
        // -------------------------------
        else if (work instanceof PrepareTxnMessage) {
            PrepareTxnMessage pTask = (PrepareTxnMessage)work;
            this.prepareTransaction(pTask.getTransaction(), pTask.getCallback());
        }
        // -------------------------------
        // Set Distributed Transaction 
        // -------------------------------
        else if (work instanceof SetDistributedTxnMessage) {
            if (this.currentDtxn != null) {
                this.blockTransaction(work);
            } else {
                this.setCurrentDtxn(((SetDistributedTxnMessage)work).getTransaction());
            }
        }
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------
    
    public final ExecutionEngine getExecutionEngine() {
        return (this.ee);
    }
    public final Thread getExecutionThread() {
        return (this.self);
    }
    public final HsqlBackend getHsqlBackend() {
        return (this.hsql);
    }
    public final PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public final TransactionEstimator getTransactionEstimator() {
        return (this.localTxnEstimator);
    }
    public final BackendTarget getBackendTarget() {
        return (this.backend_target);
    }
    public final HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public final HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public final CatalogContext getCatalogContext() {
        return (this.catalogContext);
    }
    public final int getSiteId() {
        return (this.siteId);
    }
    public final Partition getPartition() {
        return (this.partition);
    }
    public final int getPartitionId() {
        return (this.partitionId);
    }
    public final DependencyTracker getDependencyTracker() {
        return (this.depTracker);
    }
    public final PartitionExecutorProfiler getProfiler() {
        return profiler;
    }
    
    // ----------------------------------------------------------------------------
    // VOLT PROCEDURE HELPER METHODS
    // ----------------------------------------------------------------------------
    
    
    protected void initializeVoltProcedures() {
        // load up all the stored procedures
        for (final Procedure catalog_proc : catalogContext.procedures) {
            VoltProcedure volt_proc = this.initializeVoltProcedure(catalog_proc);
            Queue<VoltProcedure> queue = new LinkedList<VoltProcedure>();
            queue.add(volt_proc);
            this.procedures[catalog_proc.getId()] = queue;
        } // FOR
    }

    @SuppressWarnings("unchecked")
    protected VoltProcedure initializeVoltProcedure(Procedure catalog_proc) {
        VoltProcedure volt_proc = null;
        
        if (catalog_proc.getHasjava()) {
            // Only try to load the Java class file for the SP if it has one
            Class<? extends VoltProcedure> p_class = null;
            final String className = catalog_proc.getClassname();
            try {
                p_class = (Class<? extends VoltProcedure>)Class.forName(className);
                volt_proc = (VoltProcedure)p_class.newInstance();
            } catch (Exception e) {
                throw new ServerFaultException("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
            }
            
        } else {
            volt_proc = new VoltProcedure.StmtProcedure();
        }
        volt_proc.init(this, catalog_proc, this.backend_target);
        return (volt_proc);
    }
    
    /**
     * Returns a new VoltProcedure instance for a given stored procedure name
     * <B>Note:</B> You will get a new VoltProcedure for each invocation
     * @param proc_name
     * @return
     */
    protected VoltProcedure getVoltProcedure(int proc_id) {
        VoltProcedure voltProc = this.procedures[proc_id].poll();
        if (voltProc == null) {
            Procedure catalog_proc = catalogContext.getProcedureById(proc_id);
            voltProc = this.initializeVoltProcedure(catalog_proc);
        }
        return (voltProc);
    }
    
    /**
     * Return the given VoltProcedure back into the queue to be re-used again
     * @param voltProc
     */
    protected void finishVoltProcedure(VoltProcedure voltProc) {
        voltProc.finish();
        this.procedures[voltProc.getProcedureId()].offer(voltProc);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    private void tick() {
        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        long elapsed = time - this.lastTickTime; 
        if (elapsed >= 1000) {
            if ((this.lastTickTime != 0) && (this.ee != null)) {
                this.ee.tick(time, this.lastCommittedTxnId);
                                
                if ((time - this.lastStatsTime) >= 20000) {
                    this.updateMemoryStats(time);
                    if (this.hstore_site.getAntiCacheManager() != null && this.hstore_site.getAntiCacheManager().getEvictableTables().isEmpty() == false) {
                        this.updateAntiCacheMemoryStats(time);
                    }
                }
            }
            this.lastTickTime = time;
        }
        
        // LOGICAL
        // do other periodic work
        if (m_snapshotter != null)
            m_snapshotter.doSnapshotWork(this.ee);
                
    }
        
    
    
    private void updateMemoryStats(long time) {
        //if (trace.val)
            LOG.trace("Updating memory stats for partition " + this.partitionId);
        
        Collection<Table> tables = this.catalogContext.database.getTables();
        int[] tableIds = new int[tables.size()];
        int i = 0;
        for (Table table : tables) {
            tableIds[i++] = table.getRelativeIndex();
        }

        // data to aggregate
        long tupleCount = 0;
        @SuppressWarnings("unused")
        long tupleAccessCount = 0;
        int tupleDataMem = 0;
        int tupleAllocatedMem = 0;
        int indexMem = 0;
        int stringMem = 0;
        
        // ACTIVE
        long tuplesEvicted = 0;
        long blocksEvicted = 0;
        long bytesEvicted = 0;
        
        // GLOBAL WRITTEN
        long tuplesWritten = 0;
        long blocksWritten = 0;
        long bytesWritten = 0;
        
        // GLOBAL READ
        long tuplesRead = 0;
        long blocksRead = 0;
        long bytesRead = 0;

        // update table stats
        VoltTable[] s1 = null;
        try {
            s1 = this.ee.getStats(SysProcSelector.TABLE, tableIds, false, time);
        } catch (RuntimeException ex) {
            LOG.warn("Unexpected error when trying to retrieve EE stats for partition " + this.partitionId, ex);
        }
        if (s1 != null) {
            VoltTable stats = s1[0];
            assert(stats != null);

            // rollup the table memory stats for this site
            while (stats.advanceRow()) {
                tupleCount += stats.getLong("TUPLE_COUNT");
                tupleAccessCount += stats.getLong("TUPLE_ACCESSES");
                tupleAllocatedMem += (int) stats.getLong("TUPLE_ALLOCATED_MEMORY");
                tupleDataMem += (int) stats.getLong("TUPLE_DATA_MEMORY");
                stringMem += (int) stats.getLong("STRING_DATA_MEMORY");
                indexMem += (int) stats.getLong("INDEX_MEMORY");

                // ACTIVE
                if (hstore_conf.site.anticache_enable) {
                    tuplesEvicted += (long) stats.getLong("ANTICACHE_TUPLES_EVICTED");
                    blocksEvicted += (long) stats.getLong("ANTICACHE_BLOCKS_EVICTED");
                    bytesEvicted += (long) stats.getLong("ANTICACHE_BYTES_EVICTED");
                
                    // GLOBAL WRITTEN
                    tuplesWritten += (long) stats.getLong("ANTICACHE_TUPLES_WRITTEN");
                    blocksWritten += (long) stats.getLong("ANTICACHE_BLOCKS_WRITTEN");
                    bytesWritten += (long) stats.getLong("ANTICACHE_BYTES_WRITTEN");
                    
                    // GLOBAL READ
                    tuplesRead += (long) stats.getLong("ANTICACHE_TUPLES_READ");
                    blocksRead += (long) stats.getLong("ANTICACHE_BLOCKS_READ");
                    bytesRead += (long) stats.getLong("ANTICACHE_BYTES_READ");
                }
            }
            stats.resetRowPosition();
        }

        // update the rolled up memory statistics
        MemoryStats memoryStats = hstore_site.getMemoryStatsSource();
        memoryStats.eeUpdateMemStats(this.partitionId,
                                     tupleCount,
                                     tupleDataMem,
                                     tupleAllocatedMem,
                                     indexMem,
                                     stringMem,
                                     0, // FIXME
                                     
                                     // ACTIVE
                                     tuplesEvicted, blocksEvicted, bytesEvicted,
                                     
                                     // GLOBAL WRITTEN
                                     tuplesWritten, blocksWritten, bytesWritten,
                                     
                                     // GLOBAL READ
                                     tuplesRead, blocksRead, bytesRead
        );
        
        this.lastStatsTime = time;
    }
    
    
    private void updateAntiCacheMemoryStats(long time) {
        if (trace.val)
            LOG.trace("Updating memory stats for partition " + this.partitionId);
        
        int numDBs = AntiCacheManager.getNumDBs();
        int[] acdbIDs = new int[numDBs];
        for (int i = 0; i < numDBs; ++i) {
            acdbIDs[i] = i;
        }

        // data to aggregate
        int anticacheID = 0;
        int anticacheType = 0;

        // ACTIVE
        long blocksEvicted = 0;
        long bytesEvicted = 0;
        
        // GLOBAL WRITTEN
        long blocksWritten = 0;
        long bytesWritten = 0;
        
        // GLOBAL READ
        long blocksRead = 0;
        long bytesRead = 0;

        // FREE 
        long blocksFree= 0;
        long bytesFree = 0;

        //System.out.println("From java (before update): " + anticacheID + " " + anticacheType + " " + bytesWritten);

        // update table stats
        VoltTable[] s1 = null;
        try {
            s1 = this.ee.getStats(SysProcSelector.MULTITIER_ANTICACHE, acdbIDs, false, time);
        } catch (RuntimeException ex) {
            LOG.warn("Unexpected error when trying to retrieve EE stats for partition " + this.partitionId, ex);
        }
        if (s1 != null) {
            VoltTable stats = s1[0];
            assert(stats != null);

            // rollup the table memory stats for this site
            while (stats.advanceRow()) {
                // ACTIVE
                if (hstore_conf.site.anticache_enable) {
                    anticacheID = (int) stats.getLong("ANTICACHE_ID");
                    anticacheType = (int) stats.getLong("ANTICACHEDB_TYPE");

                    blocksEvicted = (long) stats.getLong("ANTICACHE_BLOCKS_STORED");
                    bytesEvicted = (long) stats.getLong("ANTICACHE_BYTES_STORED");
                
                    // GLOBAL WRITTEN
                    blocksWritten = (long) stats.getLong("ANTICACHE_TOTAL_BLOCKS_EVICTED");
                    bytesWritten = (long) stats.getLong("ANTICACHE_TOTAL_BYTES_EVICTED");
                    
                    // GLOBAL READ
                    blocksRead = (long) stats.getLong("ANTICACHE_TOTAL_BLOCKS_UNEVICTED");
                    bytesRead = (long) stats.getLong("ANTICACHE_TOTAL_BYTES_UNEVICTED");

                    blocksFree = (long) stats.getLong("ANTICACHE_BLOCKS_FREE");
                    bytesFree = (long) stats.getLong("ANTICACHE_BYTES_FREE");
                    //System.out.println("From java: partition: " + this.partitionId + " " + anticacheID + " " + anticacheType + " " + blocksWritten + " " + blocksRead);
                    // update the anticache memory statistics
                    AntiCacheMemoryStats acmemoryStats = hstore_site.getAntiCacheMemoryStatsSource();
                    acmemoryStats.eeUpdateMemStats(this.partitionId,
                                     anticacheID,
                                     anticacheType,
                                     
                                     // ACTIVE
                                     blocksEvicted, bytesEvicted,
                                     
                                     // GLOBAL WRITTEN
                                     blocksWritten, bytesWritten,
                                     
                                     // GLOBAL READ
                                     blocksRead, bytesRead,

                                     blocksFree, bytesFree
                    );
                }
            }
            stats.resetRowPosition();
        } else {
            LOG.warn("Getting anticache memory stats failed!");
        }

        
        this.lastStatsTime = time;
    }
    
    
    public void haltProcessing() {
//        if (debug.val)
            LOG.warn("Halting transaction processing at partition " + this.partitionId);
        
        ExecutionMode origMode = this.currentExecMode;
        this.setExecutionMode(this.currentTxn, ExecutionMode.DISABLED_REJECT);
        List<InternalMessage> toKeep = new ArrayList<InternalMessage>(); 
        InternalMessage msg = null;
        while ((msg = this.work_queue.poll()) != null) {
            // -------------------------------
            // StartTxnMessage
            // -------------------------------
            if (msg instanceof StartTxnMessage) {
                StartTxnMessage startMsg = (StartTxnMessage)msg;
                hstore_site.transactionReject((LocalTransaction)startMsg.getTransaction(), Status.ABORT_REJECT);
            }
            // -------------------------------
            // Things to keep
            // -------------------------------
            else {
                toKeep.add(msg);
            }
        } // WHILE
        // assert(this.work_queue.isEmpty());
        this.work_queue.addAll(toKeep);
        
        // For now we'll set it back so that we can execute new stuff. Clearing out
        // the queue should enough for now
        this.setExecutionMode(this.currentTxn, origMode);
    }
    
    /**
     * Figure out the current speculative execution mode for this partition 
     * @return
     */
    private SpeculationType calculateSpeculationType() {
        SpeculationType specType = SpeculationType.NULL;

        // IDLE
        if (this.currentDtxn == null) {
            // If the lock queue is not empty, then that doesn't mean we're really idle I suppose...
            // if (this.lockQueue.approximateIsEmpty() == false) {
            specType = SpeculationType.SP1_IDLE;
        }
        // LOCAL
        else if (this.currentDtxn.getBasePartition() == this.partitionId) {
            // Check whether the DTXN has started executing
            if (((LocalTransaction)this.currentDtxn).isMarkedControlCodeExecuted() == false) {
                specType = SpeculationType.SP1_LOCAL;
            } else if (this.currentDtxn.isMarkedPrepared(this.partitionId)) {
                specType = SpeculationType.SP4_LOCAL;
            } else {
                specType = SpeculationType.SP2_LOCAL;
            }
        }
        // REMOTE
        else {
            if (this.currentDtxn.isMarkedPrepared(this.partitionId)) {
                specType = SpeculationType.SP4_REMOTE;
            } else if (this.currentDtxn.hasExecutedWork(this.partitionId) == false) {
                specType = SpeculationType.SP3_REMOTE_BEFORE;
            } else {
                specType = SpeculationType.SP3_REMOTE_AFTER;
            }
        }

        return (specType);
    }

    /**
     * Set the current ExecutionMode for this executor. The transaction handle given as an input
     * argument is the transaction that caused the mode to get changed. It is only used for debug
     * purposes.
     * @param newMode
     * @param txn_id
     */
    private void setExecutionMode(AbstractTransaction ts, ExecutionMode newMode) {
        if (debug.val && this.currentExecMode != newMode) {
            LOG.debug(String.format("Setting ExecutionMode for partition %d to %s because of %s [origMode=%s]",
                      this.partitionId, newMode, ts, this.currentExecMode));
        }
        assert(newMode != ExecutionMode.COMMIT_READONLY ||
              (newMode == ExecutionMode.COMMIT_READONLY && this.currentDtxn != null)) :
            String.format("%s is trying to set partition %d to %s when the current DTXN is null?", ts, this.partitionId, newMode);
        this.currentExecMode = newMode;
    }
    
    /**
     * Returns the next undo token to use when hitting up the EE with work
     * MAX_VALUE = no undo
     * @param txn_id
     * @return
     */
    private long getNextUndoToken() {
        if (trace.val) LOG.trace(String.format("Next Undo for Partition %d: %d", this.partitionId, this.lastUndoToken+1));
        return (++this.lastUndoToken);
    }
    
    /**
     * For the given txn, return the next undo token to use for its next execution round
     * @param ts
     * @param readOnly
     * @return
     */
    private long calculateNextUndoToken(AbstractTransaction ts, boolean readOnly) {
        long undoToken = HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN;
        long lastUndoToken = ts.getLastUndoToken(this.partitionId);
        boolean singlePartition = ts.isPredictSinglePartition();
        
        // Speculative txns always need an undo token
        // It's just easier this way...
        if (ts.isSpeculative()) {
            undoToken = this.getNextUndoToken();
        }
        // If this plan is read-only, then we don't need a new undo token (unless
        // we don't have one already)
        else if (readOnly) {
            if (lastUndoToken == HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
                lastUndoToken = HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN;
//                lastUndoToken = this.getNextUndoToken();
            }
            undoToken = lastUndoToken;
        }
        // Otherwise, we need to figure out whether we want to be a brave soul and 
        // not use undo logging at all
        else {
            // If one of the following conditions are true, then we need to get a new token:
            //  (1) If this our first time up at bat
            //  (2) If we're a distributed transaction
            //  (3) The force undo logging option is enabled
            if (lastUndoToken == HStoreConstants.NULL_UNDO_LOGGING_TOKEN ||
                    singlePartition == false ||
                    hstore_conf.site.exec_force_undo_logging_all) {
                undoToken = this.getNextUndoToken();
            }
            // If we originally executed this transaction with undo buffers and we have a MarkovEstimate,
            // then we can go back and check whether we want to disable undo logging for the rest of the transaction
            else if (ts.getEstimatorState() != null && singlePartition && ts.isSpeculative() == false) {
                Estimate est = ts.getEstimatorState().getLastEstimate();
                assert(est != null) : "Got back null MarkovEstimate for " + ts;
                if (hstore_conf.site.exec_no_undo_logging == false ||
                    est.isValid() == false ||
                    est.isAbortable(this.thresholds) ||
                    est.isReadOnlyPartition(this.thresholds, this.partitionId) == false) {
                    undoToken = lastUndoToken;
                } else if (debug.val) {
                    LOG.warn(String.format("Bold! Disabling undo buffers for inflight %s\n%s", ts, est));
                }
            }
        }
        // Make sure that it's at least as big as the last one handed out
        if (undoToken < this.lastUndoToken) undoToken = this.lastUndoToken;
        
        if (debug.val)
            LOG.debug(String.format("%s - Next undo token at partition %d is %s [readOnly=%s]",
                      ts, this.partitionId,
                      (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN ? "<DISABLED>" :
                          (undoToken == HStoreConstants.NULL_UNDO_LOGGING_TOKEN ? "<NULL>" : undoToken)),
                      readOnly));
        
        return (undoToken);
    }
    
    /**
     * Populate the provided inputs map with the VoltTables needed for the give 
     * input DependencyId. If the txn is a LocalTransaction, then we will
     * get the data we need from the base partition's DependencyTracker. 
     * @param ts
     * @param input_dep_ids
     * @param inputs
     * @return
     */
    private void getFragmentInputs(AbstractTransaction ts,
                                   int input_dep_id,
                                   Map<Integer, List<VoltTable>> inputs) {
        if (input_dep_id == HStoreConstants.NULL_DEPENDENCY_ID) return;
        
        if (trace.val)
            LOG.trace(String.format("%s - Attempting to retrieve input dependencies for DependencyId #%d",
                      ts, input_dep_id));

        // If the Transaction is on the same HStoreSite, then all the 
        // input dependencies will be internal and can be retrieved locally
        if (ts instanceof LocalTransaction) {
            DependencyTracker txnTracker = null;
            if (ts.getBasePartition() != this.partitionId) {
                txnTracker = hstore_site.getDependencyTracker(ts.getBasePartition());
            } else {
                txnTracker = this.depTracker;
            }
            List<VoltTable> deps = txnTracker.getInternalDependency((LocalTransaction)ts, input_dep_id);
            assert(deps != null);
            assert(inputs.containsKey(input_dep_id) == false);
            inputs.put(input_dep_id, deps);
            if (trace.val)
                LOG.trace(String.format("%s - Retrieved %d INTERNAL VoltTables for DependencyId #%d",
                          ts, deps.size(), input_dep_id,
                          (trace.val ? "\n" + deps : "")));
        }
        // Otherwise they will be "attached" inputs to the RemoteTransaction handle
        // We should really try to merge these two concepts into a single function call
        else if (ts.getAttachedInputDependencies().containsKey(input_dep_id)) {
            List<VoltTable> deps = ts.getAttachedInputDependencies().get(input_dep_id);
            List<VoltTable> pDeps = null;
            // We have to copy the tables if we have debugging enabled
            if (trace.val) { // this.firstPartition == false) {
                pDeps = new ArrayList<VoltTable>();
                for (VoltTable vt : deps) {
                    ByteBuffer buffer = vt.getTableDataReference();
                    byte arr[] = new byte[vt.getUnderlyingBufferSize()];
                    buffer.get(arr, 0, arr.length);
                    pDeps.add(new VoltTable(ByteBuffer.wrap(arr), true));
                }
            } else {
                pDeps = deps;
            }
            inputs.put(input_dep_id, pDeps); 
            if (trace.val)
                LOG.trace(String.format("%s - Retrieved %d ATTACHED VoltTables for DependencyId #%d in %s",
                          ts, deps.size(), input_dep_id));
        }
    }
    
    /**
     * Set the given AbstractTransaction handle as the current distributed txn
     * that is running at this partition. Note that this will check to make sure
     * that no other txn is marked as the currentDtxn.
     * @param ts
     */
    private void setCurrentDtxn(AbstractTransaction ts) {
        // There can never be another current dtxn still unfinished at this partition!
        assert(this.currentBlockedTxns.isEmpty()) :
            String.format("Concurrent multi-partition transactions at partition %d: " +
                          "Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.currentDtxn, ts, this.currentBlockedTxns.size());
        assert(this.currentDtxn == null) :
            String.format("Concurrent multi-partition transactions at partition %d: " +
                          "Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.currentDtxn, ts, this.currentBlockedTxns.size());
        
        // Check whether we should check for speculative txns to execute whenever this
        // dtxn is idle at this partition
        this.currentDtxn = ts;
        if (hstore_conf.site.specexec_enable && ts.isSysProc() == false && this.specExecScheduler.isDisabled() == false) {
            this.specExecIgnoreCurrent = this.specExecChecker.shouldIgnoreTransaction(ts);
        } else {
            this.specExecIgnoreCurrent = true;
        }
        if (debug.val) {
            LOG.debug(String.format("Set %s as the current DTXN for partition %d [specExecIgnore=%s, previous=%s]",
                      ts, this.partitionId, this.specExecIgnoreCurrent, this.lastDtxnDebug));
            this.lastDtxnDebug = this.currentDtxn.toString();
        }
        if (hstore_conf.site.exec_profiling && ts.getBasePartition() != this.partitionId) {
            profiler.sp2_time.start();
        }
    }
    
    /**
     * Reset the current dtxn for this partition
     */
    private void resetCurrentDtxn() {
        assert(this.currentDtxn != null) :
            "Trying to reset the currentDtxn when it is already null";
        if (debug.val)
            LOG.debug(String.format("Resetting current DTXN for partition %d to null [previous=%s]",
                      this.partitionId, this.lastDtxnDebug));
        this.currentDtxn = null;
    }

    /**
     * Store a new prefetch result for a transaction
     * @param txnId
     * @param fragmentId
     * @param partitionId
     * @param params
     * @param result
     */
    public void addPrefetchResult(LocalTransaction ts,
                                  int stmtCounter,
                                  int fragmentId,
                                  int partitionId,
                                  int paramsHash,
                                  VoltTable result) {
        if (debug.val)
            LOG.debug(String.format("%s - Adding prefetch result for %s with %d rows from partition %d " +
                      "[stmtCounter=%d / paramsHash=%d]",
                      ts, CatalogUtil.getPlanFragment(catalogContext.catalog, fragmentId).fullName(),
                      result.getRowCount(), partitionId, stmtCounter, paramsHash));
        this.depTracker.addPrefetchResult(ts, stmtCounter, fragmentId, partitionId, paramsHash, result);
    }
    
    /**
     * Returns the directory where the EE should store the mmap'ed files
     * for this PartitionExecutor
     * @return
     */
    public static File getMMAPDir(PartitionExecutor executor) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getPartition());

        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.storage_mmap_dir +
                File.separatorChar +
                catalog_db.getProject());

        //synchronized (AntiCacheManager.class) {
        FileUtil.makeDirIfNotExists(base_dir);
        //} // SYNC

        // Then each partition will have a separate directory inside of the base one
        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(),
                executor.getPartitionId());
        
        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);
        if (hstore_conf.site.storage_mmap_reset) {
            LOG.warn(String.format("Deleting storage mmap directory '%s'", dbDirPath));
            FileUtil.deleteDirectory(dbDirPath);
        }
        FileUtil.makeDirIfNotExists(dbDirPath);

        return (dbDirPath);
    }
    
    /**
     * Returns the directory where the EE should store the ARIES log files
     * for this PartitionExecutor
     * @return
     */
    public static File getARIESDir(PartitionExecutor executor) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        Database catalog_db = CatalogUtil.getDatabase(executor.getPartition());

        // First make sure that our base directory exists
        String base_dir = FileUtil.realpath(hstore_conf.site.aries_dir + File.separatorChar + catalog_db.getProject());

        synchronized (PartitionExecutor.class) {
            FileUtil.makeDirIfNotExists(base_dir);
        } // SYNC

        String partitionName = HStoreThreadManager.formatPartitionName(executor.getSiteId(), executor.getPartitionId());

        File dbDirPath = new File(base_dir + File.separatorChar + partitionName);

        if (hstore_conf.site.aries_reset) {
            LOG.warn(String.format("Deleting aries directory '%s'", dbDirPath));
            FileUtil.deleteDirectory(dbDirPath);
        }
        FileUtil.makeDirIfNotExists(dbDirPath);

        return (dbDirPath);
    }    
      

    /**
     * Returns the file where the EE should store the ARIES log for this
     * PartitionExecutor
     * 
     * @return
     */
    public static File getARIESFile(PartitionExecutor executor) {

        File dbDir = getARIESDir(executor);
        File logFile = new File(dbDir.getAbsolutePath() + File.separatorChar + executor.m_ariesDefaultLogFileName);

        return (logFile);
    }
    
    // ---------------------------------------------------------------
    // PartitionExecutor API
    // ---------------------------------------------------------------

    /**
     * Queue a new transaction initialization at this partition. This will cause the 
     * transaction to get added to this partition's lock queue. This PartitionExecutor does 
     * not have to be this txn's base partition/
     * @param ts
     */
    public void queueSetPartitionLock(AbstractTransaction ts) {
        assert(ts.isInitialized()) : "Unexpected uninitialized transaction: " + ts;
        SetDistributedTxnMessage work = ts.getSetDistributedTxnMessage();
        boolean success = this.work_queue.offer(work);
        assert(success) :
            String.format("Failed to queue %s at partition %d for %s",
                          work, this.partitionId, ts);
        if (debug.val)
            LOG.debug(String.format("%s - Added %s to front of partition %d " +
                      "work queue [size=%d]",
                      ts, work.getClass().getSimpleName(), this.partitionId,
                      this.work_queue.size()));
        if (hstore_conf.site.specexec_enable) this.specExecScheduler.interruptSearch(work);
    }
    
    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * We should not be sent an InitiateTaskMessage here!
     * @param ts
     * @param task
     */
    public void queueWork(AbstractTransaction ts, WorkFragment fragment) {
        assert(ts.isInitialized()) : "Unexpected uninitialized transaction: " + ts;
        WorkFragmentMessage work = ts.getWorkFragmentMessage(fragment);
        boolean success = this.work_queue.offer(work); // , true);
        assert(success) :
            String.format("Failed to queue %s at partition %d for %s",
                          work, this.partitionId, ts);
        ts.markQueuedWork(this.partitionId);
        if (debug.val)
            LOG.debug(String.format("%s - Added %s to partition %d " +
                      "work queue [size=%d]",
                      ts, work.getClass().getSimpleName(), this.partitionId,
                      this.work_queue.size()));
        if (hstore_conf.site.specexec_enable) this.specExecScheduler.interruptSearch(work);
    }

    /**
     * Add a new work message to our utility queue 
     * @param work
     */
    public void queueUtilityWork(InternalMessage work) {
        this.work_queue.add(work);
        if (debug.val)
            LOG.warn(String.format("Added utility work %s to partition %d with size %d",
                      work.getClass().getSimpleName(), this.partitionId, this.work_queue.size()));
    }

    
    /**
     * Put the prepare request for the transaction into the queue
     * @param task
     * @param status The final status of the transaction
     */
    public void queuePrepare(AbstractTransaction ts, PartitionCountingCallback<? extends AbstractTransaction> callback) {
        assert(ts.isInitialized()) : "Uninitialized transaction: " + ts;
        assert(callback.isInitialized()) : "Uninitialized callback: " + ts;
        
        PrepareTxnMessage work = new PrepareTxnMessage(ts, callback);
        boolean success = this.work_queue.offer(work);
        assert(success) :
            String.format("Failed to queue %s at partition %d for %s",
                          work, this.partitionId, ts);
        if (debug.val)
            LOG.debug(String.format("%s - Added %s to partition %d " +
                      "work queue [size=%d]",
                      ts, work.getClass().getSimpleName(), this.partitionId,
                      this.work_queue.size()));
        // if (hstore_conf.site.specexec_enable) this.specExecScheduler.interruptSearch();
    }
    
    /**
     * Put the finish request for the transaction into the queue
     * @param task
     * @param status The final status of the transaction
     */
    public void queueFinish(AbstractTransaction ts, Status status) {
        assert(ts.isInitialized()) : "Unexpected uninitialized transaction: " + ts;
        FinishTxnMessage work = ts.getFinishTxnMessage(status);
        boolean success = this.work_queue.offer(work); // , true);
        assert(success) :
            String.format("Failed to queue %s at partition %d for %s",
                          work, this.partitionId, ts);
        if (debug.val)
            LOG.debug(String.format("%s - Added %s to partition %d " +
                      "work queue [size=%d]",
                      ts, work.getClass().getSimpleName(), this.partitionId,
                      this.work_queue.size()));
        // if (success) this.specExecScheduler.haltSearch();
    }

    /**
     * Queue a new transaction invocation request at this partition
     * @param ts
     * @param task
     * @param callback
     */
    public boolean queueStartTransaction(LocalTransaction ts) {
        assert(ts != null) : "Unexpected null transaction handle!";
        boolean singlePartitioned = ts.isPredictSinglePartition();
        boolean force = (singlePartitioned == false) || ts.isMapReduce() || ts.isSysProc();
        
        // UPDATED 2012-07-12
        // We used to have a bunch of checks to determine whether we needed
        // put the new request in the blocked queue or not. This required us to
        // acquire the exec_lock to do the check and then another lock to actually put 
        // the request into the work_queue. Now we'll just throw it right in
        // the queue (checking for throttling of course) and let the main
        // thread sort out the mess of whether the txn should get blocked or not
        if (this.currentExecMode == ExecutionMode.DISABLED_REJECT) {
            if (debug.val)
                LOG.warn(String.format("%s - Not queuing txn at partition %d because current mode is %s",
                         ts, this.partitionId, this.currentExecMode));
            return (false);
        }
        
        StartTxnMessage work = ts.getStartTxnMessage();
        if (debug.val)
            LOG.debug(String.format("Queuing %s for '%s' request on partition %d " +
                      "[currentDtxn=%s, queueSize=%d, mode=%s]",
                      work.getClass().getSimpleName(), ts.getProcedure().getName(), this.partitionId,
                      this.currentDtxn, this.work_queue.size(), this.currentExecMode));
        boolean success = this.work_queue.offer(work); // , force);
        if (debug.val && force && success == false) {
            String msg = String.format("Failed to add %s even though force flag was true!", ts);
            throw new ServerFaultException(msg, ts.getTransactionId());
        }
        if (success && hstore_conf.site.specexec_enable) this.specExecScheduler.interruptSearch(work);
        return (success);
    }

    // ---------------------------------------------------------------
    // WORK QUEUE PROCESSING METHODS
    // ---------------------------------------------------------------
    
    /**
     * Process a WorkResult and update the internal state the LocalTransaction accordingly
     * Note that this will always be invoked by a thread other than the main execution thread
     * for this PartitionExecutor. That means if something comes back that's bad, we need a way
     * to alert the other thread so that it can act on it. 
     * @param ts
     * @param result
     */
    private void processWorkResult(LocalTransaction ts, WorkResult result) {
        boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.profiler != null);
        if (debug.val)
            LOG.debug(String.format("Processing WorkResult for %s on partition %d [srcPartition=%d, deps=%d]",
                      ts, this.partitionId, result.getPartitionId(), result.getDepDataCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (result.getStatus() != Status.OK) {
            if (trace.val)
                LOG.trace(String.format("Received non-success response %s from partition %d for %s",
                          result.getStatus(), result.getPartitionId(), ts));

            SerializableException error = null;
            if (needs_profiling) ts.profiler.startDeserialization();
            try {
                ByteBuffer buffer = result.getError().asReadOnlyByteBuffer();
                error = SerializableException.deserializeFromBuffer(buffer);
            } catch (Exception ex) {
                String msg = String.format("Failed to deserialize SerializableException from partition %d " +
                                                   "for %s [bytes=%d]",
                                           result.getPartitionId(), ts, result.getError().size());
                throw new ServerFaultException(msg, ex);
            } finally {
                if (needs_profiling) ts.profiler.stopDeserialization();
            }
            // At this point there is no need to even deserialize the rest of the message because 
            // we know that we're going to have to abort the transaction
            if (error == null) {
                LOG.warn(ts + " - Unexpected null SerializableException\n" + result);
            } else {
                if (debug.val)
                    LOG.error(String.format("%s - Got error from partition %d in %s",
                              ts, result.getPartitionId(), result.getClass().getSimpleName()), error);
                if (error instanceof EvictedTupleAccessException){
                	EvictedTupleAccessException evta = (EvictedTupleAccessException) error;
                    LOG.error(String.format("Evicted tuple access exception error has partition id set as %d", evta.getPartitionId()));                	
                }
                ts.setPendingError(error, true);
            }
            return;
        }
        
        if (needs_profiling) ts.profiler.startDeserialization();
        for (int i = 0, cnt = result.getDepDataCount(); i < cnt; i++) {
            if (trace.val)
                LOG.trace(String.format("Storing intermediate results from partition %d for %s",
                          result.getPartitionId(), ts));
            int depId = result.getDepId(i);
            ByteString bs = result.getDepData(i);
            VoltTable vt = null;
            if (bs.isEmpty() == false) {
                FastDeserializer fd = new FastDeserializer(bs.asReadOnlyByteBuffer());
                try {
                    vt = fd.readObject(VoltTable.class);
                    if (trace.val)
                        LOG.trace(String.format("Displaying results from partition %d for %s :: \n %s",
                                  result.getPartitionId(), ts, vt.toString()));                    
                } catch (Exception ex) {
                    throw new ServerFaultException("Failed to deserialize VoltTable from partition " + result.getPartitionId() + " for " + ts, ex);
                }
            }
            this.depTracker.addResult(ts, result.getPartitionId(), depId, vt);
        } // FOR (dependencies)
        if (needs_profiling) ts.profiler.stopDeserialization();
    }

    /**
     * Execute a new transaction at this partition.
     * This will invoke the run() method define in the VoltProcedure for this txn and 
     * then process the ClientResponse. Only the PartitionExecutor itself should be calling
     * this directly, since it's the only thing that knows what's going on with the world...
     * @param ts
     */
    private void executeTransaction(LocalTransaction ts) {
        assert(ts.isInitialized()) :
            String.format("Trying to execute uninitialized transaction %s at partition %d",
                          ts, this.partitionId);
        assert(ts.isMarkedReleased(this.partitionId)) :
            String.format("Transaction %s was not marked released at partition %d before being executed",
                          ts, this.partitionId);
        
        if (trace.val)
            LOG.debug(String.format("%s - Attempting to start transaction on partition %d",
                      ts, this.partitionId));
        
        // If this is a MapReduceTransaction handle, we actually want to get the 
        // inner LocalTransaction handle for this partition. The MapReduceTransaction
        // is just a placeholder
        if (ts instanceof MapReduceTransaction) {
            MapReduceTransaction mr_ts = (MapReduceTransaction)ts; 
            ts = mr_ts.getLocalTransaction(this.partitionId);
            assert(ts != null) : 
                "Unexpected null LocalTransaction handle from " + mr_ts; 
        }
        
        ExecutionMode before_mode = this.currentExecMode;
        boolean predict_singlePartition = ts.isPredictSinglePartition();
            
        // -------------------------------
        // DISTRIBUTED TXN
        // -------------------------------
        if (predict_singlePartition == false) {
            // If there is already a dtxn running, then we need to throw this
            // mofo back into the blocked txn queue
            // TODO: If our dtxn is on the same site as us, then at this point we know that 
            //       it is done executing the control code and is sending around 2PC messages 
            //       to commit/abort. That means that we could assume that all of the other 
            //       remote partitions are going to agree on the same outcome and we can start 
            //       speculatively executing this dtxn. After all, if we're at this point in 
            //       the PartitionExecutor then we know that we got this partition's locks 
            //       from the TransactionQueueManager.
            if (this.currentDtxn != null && this.currentDtxn.equals(ts) == false) {
                assert(this.currentDtxn.equals(ts) == false) :
                    String.format("New DTXN %s != Current DTXN %s", ts, this.currentDtxn);
                
                // If this is a local txn, then we can finagle things a bit.
                if (this.currentDtxn.isExecLocal(this.partitionId)) {
                    // It would be safe for us to speculative execute this DTXN right here
                    // if the currentDtxn has aborted... but we can never be in this state.
                    assert(this.currentDtxn.isAborted() == false) : // Sanity Check
                        String.format("We want to execute %s on partition %d but aborted %s is still hanging around\n",
                                      ts, this.partitionId, this.currentDtxn, this.work_queue);
                    
                    // So that means we know that it committed, which doesn't necessarily mean
                    // that it will still commit, but we'll be able to abort, rollback, and requeue
                    // if that happens.
                    // TODO: Right now our current dtxn marker is a single value. We may want to 
                    //       switch it to a FIFO queue so that we can multiple guys hanging around.
                    //       For now we will just do the default thing and block this txn
                    this.blockTransaction(ts);
                    return;
                }
                // If it's not local, then we just have to block it right away
                else {
                    this.blockTransaction(ts);
                    return;
                }
            }
            // If there is no other DTXN right now, then we're it!
            else if (this.currentDtxn == null) { //  || this.currentDtxn.equals(ts) == false) {
                this.setCurrentDtxn(ts);
            
            }
            // 2011-11-14: We don't want to set the execution mode here, because we know that we
            //             can check whether we were read-only after the txn finishes
            this.setExecutionMode(this.currentDtxn, ExecutionMode.COMMIT_NONE);
            
            if (debug.val)
                LOG.debug(String.format("Marking %s as current DTXN on Partition %d [isLocal=%s, execMode=%s]",
                          ts, this.partitionId, true, this.currentExecMode));                    
        }
        // -------------------------------
        // SINGLE-PARTITION TXN
        // -------------------------------
        else {
            // If this is a single-partition transaction, then we need to check whether we are
            // being executed under speculative execution mode. We have to check this here 
            // because it may be the case that we queued a bunch of transactions when speculative 
            // execution was enabled, but now the transaction that was ahead of this one is finished,
            // so now we're just executing them regularly
            if (this.currentDtxn != null) {
                // HACK: If we are currently under DISABLED mode when we get this, then we just 
                // need to block the transaction and return back to the queue. This is easier than 
                // having to set all sorts of crazy locks
                if (this.currentExecMode == ExecutionMode.DISABLED || hstore_conf.site.specexec_enable == false) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Blocking single-partition %s until dtxn finishes [mode=%s]",
                                  this.currentDtxn, ts, this.currentExecMode));
                    this.blockTransaction(ts);
                    return;
                }
                assert(ts.getSpeculationType() != null);
                if (debug.val)
                    LOG.debug(String.format("Speculatively executing %s while waiting for dtxn %s [%s]",
                              ts, this.currentDtxn, ts.getSpeculationType()));
                assert(ts.isSpeculative()) : ts + " was not marked as being speculative!";
            }
        }
        
        // If we reach this point, we know that we're about to execute our homeboy here...
        if (hstore_conf.site.txn_profiling && ts.profiler != null) {
            ts.profiler.startExec();
        }
        if (hstore_conf.site.exec_profiling) this.profiler.numTransactions++;
        
        // Make sure the dependency tracker knows about us
        if (ts.hasDependencyTracker()) this.depTracker.addTransaction(ts);
        
        // Grab a VoltProcedure handle for this txn
        // Two txns can't use the same VoltProcedure at the same time.
        VoltProcedure volt_proc = this.getVoltProcedure(ts.getProcedure().getId());
        assert(volt_proc != null) : "No VoltProcedure for " + ts;
        
        if (debug.val) {
            LOG.debug(String.format("%s - Starting execution of txn on partition %d " +
                      "[txnMode=%s, mode=%s]",
                      ts, this.partitionId, before_mode, this.currentExecMode));
            if (trace.val)
                LOG.trace(String.format("Current Transaction at partition #%d\n%s",
                          this.partitionId, ts.debug()));
        }
        
        if (hstore_conf.site.txn_counters) TransactionCounter.EXECUTED.inc(ts.getProcedure());
        ClientResponseImpl cresponse = null;
        VoltProcedure previous = this.currentVoltProc;
        try {
            this.currentVoltProc = volt_proc;
            ts.markControlCodeExecuted();
            cresponse = volt_proc.call(ts, ts.getProcedureParameters().toArray()); // Blocking...
        // VoltProcedure.call() should handle any exceptions thrown by the transaction
        // If we get anything out here then that's bad news
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) {
                SQLStmt last[] = volt_proc.voltLastQueriesExecuted();
                LOG.fatal("Unexpected error while executing " + ts, ex);
                if (last.length > 0) {
                    LOG.fatal(String.format("Last Queries Executed [%d]: %s",
                              last.length, Arrays.toString(last)));
                }
                LOG.fatal("LocalTransactionState Dump:\n" + ts.debug());
                this.crash(ex);
            }
        } finally {
            this.currentVoltProc = previous;
            this.finishVoltProcedure(volt_proc);
            if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startPost();
            
//            if (cresponse.getStatus() == Status.ABORT_UNEXPECTED) {
//                cresponse.getException().printStackTrace();
//            }
        }
        
        // If this is a MapReduce job, then we can just ignore the ClientResponse
        // and return immediately. The VoltMapReduceProcedure is responsible for storing
        // the result at the proper location.
        if (ts.isMapReduce()) {
            return;
        } else if (cresponse == null) {
            assert(this.isShuttingDown()) : String.format("No ClientResponse for %s???", ts);
            return;
        }
        
        // -------------------------------
        // PROCESS RESPONSE AND FIGURE OUT NEXT STEP
        // -------------------------------
        
        Status status = cresponse.getStatus();
        if (debug.val) {
            LOG.debug(String.format("%s - Finished execution of transaction control code " +
                                    "[status=%s, beforeMode=%s, currentMode=%s]",
                                    ts, status, before_mode, this.currentExecMode));
            if (ts.hasPendingError()) {
                LOG.debug(String.format("%s - Txn finished with pending error: %s",
                          ts, ts.getPendingErrorMessage()));
            }
        }

        // We assume that most transactions are not speculatively executed and are successful
        // Therefore we don't want to grab the exec_mode lock here.
        if (predict_singlePartition == false || this.canProcessClientResponseNow(ts, status, before_mode)) {
            this.processClientResponse(ts, cresponse);
        }
        // Otherwise always queue our response, since we know that whatever thread is out there
        // is waiting for us to finish before it drains the queued responses
        else {
            // If the transaction aborted, then we can't execute any transaction that touch the tables
            // that this guy touches. But since we can't just undo this transaction without undoing 
            // everything that came before it, we'll just disable executing all transactions until the 
            // current distributed transaction commits
            if (status != Status.OK && ts.isExecReadOnly(this.partitionId) == false) {
                this.setExecutionMode(ts, ExecutionMode.DISABLED);
                int blocked = this.work_queue.drainTo(this.currentBlockedTxns);
                if (debug.val) {
                    if (trace.val && blocked > 0)
                        LOG.trace(String.format("Blocking %d transactions at partition %d because ExecutionMode is now %s",
                                  blocked, this.partitionId, this.currentExecMode));
                    LOG.debug(String.format("Disabling execution on partition %d because speculative %s aborted",
                              this.partitionId, ts));
                }
            }
            if (trace.val)
                LOG.trace(String.format("%s - Queuing ClientResponse [status=%s, origMode=%s, newMode=%s, dtxn=%s]",
                          ts, cresponse.getStatus(), before_mode, this.currentExecMode, this.currentDtxn));
            this.blockClientResponse(ts, cresponse);
        }
    }
    
    /**
     * Determines whether a finished transaction that executed locally can have their ClientResponse processed immediately
     * or if it needs to wait for the response from the outstanding multi-partition transaction for this partition 
     * (1) This is the multi-partition transaction that everyone is waiting for
     * (2) The transaction was not executed under speculative execution mode 
     * (3) The transaction does not need to wait for the multi-partition transaction to finish first
     * @param ts
     * @param status
     * @param before_mode
     * @return
     */
    private boolean canProcessClientResponseNow(LocalTransaction ts, Status status, ExecutionMode before_mode) {
        if (debug.val) LOG.debug(String.format("%s - Checking whether to process %s response now at partition %d " +
                         "[singlePartition=%s, readOnly=%s, specExecModified=%s, before=%s, current=%s]",
                         ts, status, this.partitionId,
                         ts.isPredictSinglePartition(),
                         ts.isExecReadOnly(this.partitionId),
                         this.specExecModified,
                         before_mode, this.currentExecMode));
        // Commit All
        if (this.currentExecMode == ExecutionMode.COMMIT_ALL) {
            return (true);
        }
        // SPECIAL CASE
        // Any user-aborted, speculative single-partition transaction should be processed immediately.
        else if (status == Status.ABORT_USER && ts.isSpeculative()) {
            return (true);
        }
//        // SPECIAL CASE
//        // If this txn threw a user abort, and the current outstanding dtxn is read-only
//        // then it's safe for us to rollback
//        else if (status == Status.ABORT_USER &&
//                  this.currentDtxn != null &&
//                  this.currentDtxn.isExecReadOnly(this.partitionId)) {
//            return (true);
//        }
        // SPECIAL CASE
        // Anything mispredicted should be processed right away
        else if (status == Status.ABORT_MISPREDICT) {
            return (true);
        }    
        // Process successful txns based on the mode that it was executed under
        else if (status == Status.OK) {
            switch (before_mode) {
                case COMMIT_ALL:
                    return (true);
                case COMMIT_READONLY:
                    // Read-only speculative txns can be committed right now
                    // TODO: Right now we're going to use the specExecModified flag to disable
                    // sending out any results from spec execed txns that may have read from 
                    // a modified database. We should switch to a bitmap of table ids so that we
                    // have can be more selective.
                    // return (false);
                    return (this.specExecModified == false && ts.isExecReadOnly(this.partitionId));
                case COMMIT_NONE: {
                    // If this txn does not conflict with the current dtxn, then we should be able
                    // to let it commit but we can't because of the way our undo tokens work
                    return (false);
                }
                default:
                    throw new ServerFaultException("Unexpected execution mode: " + before_mode, ts.getTransactionId()); 
            } // SWITCH
        }
//        // If the transaction aborted and it was read-only thus far, then we want to process it immediately
//        else if (status != Status.OK && ts.isExecReadOnly(this.partitionId)) {
//            return (true);
//        }

        assert(this.currentExecMode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.currentExecMode, status);
        return (false);
    }

    /**
     * Process a WorkFragment for a transaction and execute it in this partition's underlying EE. 
     * @param ts
     * @param fragment
     * @param allParameters The array of all the ParameterSets for the current SQLStmt batch.
     */
    private void processWorkFragment(AbstractTransaction ts, WorkFragment fragment, ParameterSet allParameters[]) {
        assert(this.partitionId == fragment.getPartitionId()) :
            String.format("Tried to execute WorkFragment %s for %s at partition %d but it was suppose " +
                          "to be executed on partition %d",
                          fragment.getFragmentIdList(), ts, this.partitionId, fragment.getPartitionId());
        assert(ts.isMarkedPrepared(this.partitionId) == false) :
            String.format("Tried to execute WorkFragment %s for %s at partition %d after it was marked 2PC:PREPARE",
                          fragment.getFragmentIdList(), ts, this.partitionId);
        
        // A txn is "local" if the Java is executing at the same partition as this one
        boolean is_basepartition = (ts.getBasePartition() == this.partitionId);
        boolean is_remote = (ts instanceof LocalTransaction == false);
        boolean is_prefetch = fragment.getPrefetch();
        boolean is_readonly = fragment.getReadOnly();
        if (debug.val)
            LOG.debug(String.format("%s - Executing %s [isBasePartition=%s, isRemote=%s, isPrefetch=%s, isReadOnly=%s, fragments=%s]",
                      ts, fragment.getClass().getSimpleName(),
                      is_basepartition, is_remote, is_prefetch, is_readonly,
                      fragment.getFragmentIdCount()));
        
        // If this WorkFragment isn't being executed at this txn's base partition, then
        // we need to start a new execution round
        if (is_basepartition == false) {
            long undoToken = this.calculateNextUndoToken(ts, is_readonly);
            ts.initRound(this.partitionId, undoToken);
            ts.startRound(this.partitionId);
        }
        
        DependencySet result = null;
        Status status = Status.OK;
        SerializableException error = null;
        
        // Check how many fragments are not marked as ignored
        // If the fragment is marked as ignore then it means that it was already
        // sent to this partition for prefetching. We need to make sure that we remove
        // it from the list of fragmentIds that we need to execute.
        int fragmentCount = fragment.getFragmentIdCount();
        for (int i = 0; i < fragmentCount; i++) {
            if (fragment.getStmtIgnore(i)) {
                fragmentCount--;
            }
        } // FOR
        final ParameterSet parameters[] = tmp_fragmentParams.getParameterSet(fragmentCount);
        assert(parameters.length == fragmentCount);
        
        // Construct data given to the EE to execute this work fragment
        this.tmp_EEdependencies.clear();
        long fragmentIds[] = tmp_fragmentIds.getArray(fragmentCount);
        int fragmentOffsets[] = tmp_fragmentOffsets.getArray(fragmentCount);
        int outputDepIds[] = tmp_outputDepIds.getArray(fragmentCount);
        int inputDepIds[] = tmp_inputDepIds.getArray(fragmentCount);
        int offset = 0;

        for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
            if (fragment.getStmtIgnore(i) == false) {
                fragmentIds[offset] = fragment.getFragmentId(i);
                fragmentOffsets[offset] = i;
                outputDepIds[offset] = fragment.getOutputDepId(i);
                inputDepIds[offset] = fragment.getInputDepId(i);
                parameters[offset] = allParameters[fragment.getParamIndex(i)];
                this.getFragmentInputs(ts, inputDepIds[offset], this.tmp_EEdependencies);
                
                if (trace.val && ts.isSysProc() == false && is_basepartition == false)
                    LOG.trace(String.format("%s - Offset:%d FragmentId:%d OutputDep:%d/%d InputDep:%d/%d",
                              ts, offset, fragmentIds[offset],
                              outputDepIds[offset], fragment.getOutputDepId(i),
                              inputDepIds[offset], fragment.getInputDepId(i)));
                offset++;
            }
        } // FOR
        assert(offset == fragmentCount);
        
        try {
            result = this.executeFragmentIds(ts,
                                             ts.getLastUndoToken(this.partitionId),
                                             fragmentIds,
                                             parameters,
                                             outputDepIds,
                                             inputDepIds,
                                             this.tmp_EEdependencies);
        } catch (EvictedTupleAccessException ex) {
            // XXX: What do we do if this is not a single-partition txn?
            status = Status.ABORT_EVICTEDACCESS;
            error = ex;
        } catch (ConstraintFailureException ex) {
        	LOG.info("Found the abort!!!"+ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (SQLException ex) {
        	LOG.info("Found the abort!!!"+ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (EEException ex) {
            // this.crash(ex);
        	LOG.info("Found the abort!!!"+ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (Throwable ex) {
        	LOG.info("Found the abort!!!"+ex);
            status = Status.ABORT_UNEXPECTED;
            if (ex instanceof SerializableException) {
                error = (SerializableException)ex;
            } else {
                error = new SerializableException(ex);
            }
        } finally {
            if (error != null) {
                // error.printStackTrace();
//            	if (error instanceof EvictedTupleAccessException){
//            		EvictedTupleAccessException ex = (EvictedTupleAccessException) error;
//            	}
            	
                LOG.warn(String.format("%s - Unexpected %s on partition %d",
                         ts, error.getClass().getSimpleName(), this.partitionId),
                         error); // (debug.val ? error : null));
            }
            // Success, but without any results???
            if (result == null && status == Status.OK) {
                String msg = String.format("The WorkFragment %s executed successfully on Partition %d but " +
                                           "result is null for %s",
                                           fragment.getFragmentIdList(), this.partitionId, ts);
                Exception ex = new Exception(msg);
                if (debug.val) LOG.warn(ex);
                LOG.info("Found the abort!!!"+ex);
                status = Status.ABORT_UNEXPECTED;
                error = new SerializableException(ex);
            }
        }
        
        // For single-partition INSERT/UPDATE/DELETE queries, we don't directly
        // execute the SendPlanNode in order to get back the number of tuples that
        // were modified. So we have to rely on the output dependency ids set in the task
        assert(status != Status.OK ||
              (status == Status.OK && result.size() == fragmentIds.length)) :
           "Got back " + result.size() + " results but was expecting " + fragmentIds.length;
        
        // Make sure that we mark the round as finished before we start sending results
        if (is_basepartition == false) {
            ts.finishRound(this.partitionId);
        }
        
        // -------------------------------
        // PREFETCH QUERIES
        // -------------------------------
        if (is_prefetch) {
            // Regardless of whether this txn is running at the same HStoreSite as this PartitionExecutor,
            // we always need to put the result inside of the local query cache
            // This is so that we can identify if we get request for a query that we have already executed
            // We'll only do this if it succeeded. If it failed, then we won't do anything and will
            // just wait until they come back to execute the query again before 
            // we tell them that something went wrong. It's ghetto, but it's just easier this way...
            if (status == Status.OK) {
                // We're going to store the result in the base partition cache if they're 
                // on the same HStoreSite as us
                if (is_remote == false) {
                    PartitionExecutor other = this.hstore_site.getPartitionExecutor(ts.getBasePartition());
                    for (int i = 0, cnt = result.size(); i < cnt; i++) {
                        if (trace.val)
                            LOG.trace(String.format("%s - Storing %s prefetch result [params=%s]",
                                      ts, CatalogUtil.getPlanFragment(catalogContext.catalog, fragment.getFragmentId(fragmentOffsets[i])).fullName(),
                                      parameters[i]));
                        other.addPrefetchResult((LocalTransaction)ts,
                                                fragment.getStmtCounter(fragmentOffsets[i]),
                                                fragment.getFragmentId(fragmentOffsets[i]),
                                                this.partitionId,
                                                parameters[i].hashCode(),
                                                result.dependencies[i]);
                    } // FOR
                }
            }
            
            // Now if it's a remote transaction, we need to use the coordinator to send
            // them our result. Note that we want to send a single message per partition. Unlike
            // with the TransactionWorkRequests, we don't need to wait until all of the partitions
            // that are prefetching for this txn at our local HStoreSite to finish.
            if (is_remote) {
                WorkResult wr = this.buildWorkResult(ts, result, status, error);
                TransactionPrefetchResult.Builder builder = TransactionPrefetchResult.newBuilder()
                                                                .setTransactionId(ts.getTransactionId().longValue())
                                                                .setSourcePartition(this.partitionId)
                                                                .setResult(wr)
                                                                .setStatus(status)
                                                                .addAllFragmentId(fragment.getFragmentIdList())
                                                                .addAllStmtCounter(fragment.getStmtCounterList());
                for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
                    builder.addParamHash(parameters[i].hashCode());
                }
                if (debug.val)
                    LOG.debug(String.format("%s - Sending back %s to partition %d [numResults=%s, status=%s]",
                              ts, wr.getClass().getSimpleName(), ts.getBasePartition(),
                              result.size(), status));
                hstore_coordinator.transactionPrefetchResult((RemoteTransaction)ts, builder.build());
            }
        }
        // -------------------------------
        // LOCAL TRANSACTION
        // -------------------------------
        else if (is_remote == false) {
            LocalTransaction local_ts = (LocalTransaction)ts;
            
            // If the transaction is local, store the result directly in the local TransactionState
            if (status == Status.OK) {
                if (trace.val)
                    LOG.trace(String.format("%s - Storing %d dependency results locally for successful work fragment",
                              ts, result.size()));
                assert(result.size() == outputDepIds.length);
                DependencyTracker otherTracker = this.hstore_site.getDependencyTracker(ts.getBasePartition());
                for (int i = 0; i < outputDepIds.length; i++) {
                    if (trace.val)
                        LOG.trace(String.format("%s - Storing DependencyId #%d [numRows=%d]\n%s",
                                  ts, outputDepIds[i], result.dependencies[i].getRowCount(),
                                  result.dependencies[i]));
                    try {
                        otherTracker.addResult(local_ts, this.partitionId, outputDepIds[i], result.dependencies[i]);
                    } catch (Throwable ex) {
//                        ex.printStackTrace();
                        String msg = String.format("Failed to stored Dependency #%d for %s [idx=%d, fragmentId=%d]",
                                                   outputDepIds[i], ts, i, fragmentIds[i]);
                        LOG.error(String.format("%s - WorkFragment:%d\nExpectedIds:%s\nOutputDepIds: %s\nResultDepIds: %s\n%s",
                                  msg, fragment.hashCode(),
                                  fragment.getOutputDepIdList(), Arrays.toString(outputDepIds),
                                  Arrays.toString(result.depIds), fragment));
                        throw new ServerFaultException(msg, ex);
                    }
                } // FOR
            } else {
                local_ts.setPendingError(error, true);
            }
        }
        // -------------------------------
        // REMOTE TRANSACTION
        // -------------------------------
        else {
            if (trace.val)
                LOG.trace(String.format("%s - Constructing WorkResult with %d bytes from partition %d to send " +
                          "back to initial partition %d [status=%s]",
                          ts, (result != null ? result.size() : null),
                          this.partitionId, ts.getBasePartition(), status));
            
            RpcCallback<WorkResult> callback = ((RemoteTransaction)ts).getWorkCallback();
            if (callback == null) {
                LOG.fatal("Unable to send FragmentResponseMessage for " + ts);
                LOG.fatal("Orignal WorkFragment:\n" + fragment);
                LOG.fatal(ts.toString());
                throw new ServerFaultException("No RPC callback to HStoreSite for " + ts, ts.getTransactionId());
            }
            WorkResult response = this.buildWorkResult((RemoteTransaction)ts, result, status, error);
            assert(response != null);
            callback.run(response);
        }
        
        // Check whether this is the last query that we're going to get
        // from this transaction. If it is, then we can go ahead and prepare the txn
        if (is_basepartition == false && fragment.getLastFragment()) {
            if (debug.val)
                LOG.debug(String.format("%s - Invoking early 2PC:PREPARE at partition %d",
                          ts, this.partitionId));
            PartitionCountingCallback<? extends AbstractTransaction> callback = ts.getPrepareCallback();
            
            // If we are at a remote site, then we have to be careful here.
            // We don't actually have the real callback that the RemotePrepareCallback needs.
            // So that we have to use a null callback that doesn't actually do anything. The 
            // RemotePrepareCallback will make sure that we mark the partition as prepared.
            if (ts instanceof RemoteTransaction) {
                PartitionSet partitions = catalogContext.getPartitionSetSingleton(this.partitionId);
                RpcCallback<TransactionPrepareResponse> origCallback = NullCallback.getInstance(); 
                ((RemotePrepareCallback)callback).init((RemoteTransaction)ts, partitions, origCallback);
            }
            this.queuePrepare(ts, callback);
        }
    }
    
    /**
     * Executes a WorkFragment on behalf of some remote site and returns the
     * resulting DependencySet
     * @param fragment
     * @return
     * @throws Exception
     */
    private DependencySet executeFragmentIds(AbstractTransaction ts,
                                             long undoToken,
                                             long fragmentIds[],
                                             ParameterSet parameters[],
                                             int output_depIds[],
                                             int input_depIds[],
                                             Map<Integer, List<VoltTable>> input_deps) throws Exception {
        
        if (fragmentIds.length == 0) {
            LOG.warn(String.format("Got a fragment batch for %s that does not have any fragments?", ts));
            return (null);
        }
        
        // *********************************** DEBUG ***********************************
        if (trace.val) {
            LOG.trace(String.format("%s - Getting ready to kick %d fragments to partition %d EE [undoToken=%d]",
                      ts, fragmentIds.length, this.partitionId,
                      (undoToken != HStoreConstants.NULL_UNDO_LOGGING_TOKEN ? undoToken : "null")));
//            if (trace.val) {
//                LOG.trace("WorkFragmentIds: " + Arrays.toString(fragmentIds));
//                Map<String, Object> m = new LinkedHashMap<String, Object>();
//                for (int i = 0; i < parameters.length; i++) {
//                    m.put("Parameter[" + i + "]", parameters[i]);
//                } // FOR
//                LOG.trace("Parameters:\n" + StringUtil.formatMaps(m));
//            }
        }
        // *********************************** DEBUG ***********************************
        
        DependencySet result = null;
        
        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ts.isSysProc()) {
            result = this.executeSysProcFragments(ts,
                                                  undoToken,
                                                  fragmentIds.length,
                                                  fragmentIds,
                                                  parameters,
                                                  output_depIds,
                                                  input_depIds,
                                                  input_deps);
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            result = this.executePlanFragments(ts,
                                               undoToken,
                                               fragmentIds.length,
                                               fragmentIds,
                                               parameters,
                                               output_depIds,
                                               input_depIds,
                                               input_deps);
            if (result == null) {
                LOG.warn(String.format("Output DependencySet for %s in %s is null?",
                         Arrays.toString(fragmentIds), ts));
            }
        }
        return (result);
    }
    
    /**
     * Execute a BatchPlan directly on this PartitionExecutor without having to covert it
     * to WorkFragments first. This is big speed improvement over having to queue things up
     * @param ts
     * @param plan
     * @return
     */
    private VoltTable[] executeLocalPlan(LocalTransaction ts,
                                         BatchPlanner.BatchPlan plan,
                                         ParameterSet parameterSets[]) {

        // Start the new execution round
        long undoToken = this.calculateNextUndoToken(ts, plan.isReadOnly());
        ts.initFirstRound(undoToken, plan.getBatchSize());
      
        int fragmentCount = plan.getFragmentCount();
        long fragmentIds[] = plan.getFragmentIds();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        
        // Mark that we touched the local partition once for each query in the batch
        // ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not
        // a single-partition transaction
        if (hstore_conf.site.specexec_enable && ts.isPredictSinglePartition() == false) {
            //FIXME
            //PartitionSet new_done = ts.calculateDonePartitions(this.thresholds);
            //if (new_done != null && new_done.isEmpty() == false) {
            //   LocalPrepareCallback callback = ts.getPrepareCallback();
            //   assert(callback.isInitialized());
            //   this.hstore_coordinator.transactionPrepare(ts, callback, new_done);
            //}
        }

        if (trace.val)
            LOG.trace(String.format("Txn #%d - BATCHPLAN:\n" +
                                    "  fragmentIds:   %s\n" + 
                                    "  fragmentCount: %s\n" +
                                    "  output_depIds: %s\n" +
                                    "  input_depIds:  %s",
                                    ts.getTransactionId(),
                                    Arrays.toString(plan.getFragmentIds()),
                                    plan.getFragmentCount(),
                                    Arrays.toString(plan.getOutputDependencyIds()),
                                    Arrays.toString(plan.getInputDependencyIds())));
        
        // NOTE: There are no dependencies that we need to pass in because the entire
        // batch is local to this partition.
        DependencySet result = null;
        try {
            result = this.executePlanFragments(ts,
                                               undoToken,
                                               fragmentCount,
                                               fragmentIds,
                                               parameterSets,
                                               output_depIds,
                                               input_depIds,
                                               null);
        
        } finally {
            ts.fastFinishRound(this.partitionId);    
        }
        // assert(result != null) : "Unexpected null DependencySet result for " + ts; 
        if (trace.val)
            LOG.trace("Output:\n" + result);
        return (result != null ? result.dependencies : null);
    }
    
    /**
     * Execute the given fragment tasks on this site's underlying EE
     * @param ts
     * @param undoToken
     * @param batchSize
     * @param fragmentIds
     * @param parameterSets
     * @param output_depIds
     * @param input_depIds
     * @return
     */
    private DependencySet executeSysProcFragments(AbstractTransaction ts,
                                                  long undoToken,
                                                  int batchSize, 
                                                  long fragmentIds[],
                                                  ParameterSet parameters[],
                                                  int output_depIds[],
                                                  int input_depIds[],
                                                  Map<Integer, List<VoltTable>> input_deps) {
        assert(fragmentIds.length == 1);
        assert(fragmentIds.length == parameters.length) :
            String.format("%s - Fragments:%d / Parameters:%d",
                          ts, fragmentIds.length, parameters.length);

        VoltSystemProcedure volt_proc = this.m_registeredSysProcPlanFragments.get(fragmentIds[0]);
        if (volt_proc == null) {
            String msg = "No sysproc handle exists for FragmentID #" + fragmentIds[0] + " :: " + this.m_registeredSysProcPlanFragments;
            throw new ServerFaultException(msg, ts.getTransactionId());
        }
        
        ts.markExecNotReadOnly(this.partitionId);
        DependencySet result = null;
        try {
            result = volt_proc.executePlanFragment(ts.getTransactionId(),
                                                   this.tmp_EEdependencies,
                                                   (int)fragmentIds[0],
                                                   parameters[0],
                                                   this.m_systemProcedureContext);
        } catch (Throwable ex) {
            String msg = "Unexpected error when executing system procedure";
            throw new ServerFaultException(msg, ex, ts.getTransactionId());
        }
        if (debug.val)
            LOG.debug(String.format("%s - Finished executing sysproc fragment for %s (#%d)%s",
                      ts, m_registeredSysProcPlanFragments.get(fragmentIds[0]).getClass().getSimpleName(),
                      fragmentIds[0], (trace.val ? "\n" + result : "")));
        
        return (result);
    }
    
    /**
     * Execute the given fragment tasks on this site's underlying EE
     * @param ts
     * @param undoToken
     * @param batchSize
     * @param fragmentIds
     * @param parameterSets
     * @param output_depIds
     * @param input_depIds
     * @return
     */
    private DependencySet executePlanFragments(AbstractTransaction ts,
                                               long undoToken,
                                               int batchSize, 
                                               long fragmentIds[],
                                               ParameterSet parameterSets[],
                                               int output_depIds[],
                                               int input_depIds[],
                                               Map<Integer, List<VoltTable>> input_deps) {
        assert(this.ee != null) : "The EE object is null. This is bad!";
        Long txn_id = ts.getTransactionId();

        // *********************************** DEBUG ***********************************
        if (debug.val) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s - Executing %d fragments [lastTxnId=%d, undoToken=%d]",
                      ts, batchSize, this.lastCommittedTxnId, undoToken));
            // if (trace.val) {
                Map<String, Object> m = new LinkedHashMap<String, Object>();
                m.put("Fragments", Arrays.toString(fragmentIds));
                
                Map<Integer, Object> inner = new LinkedHashMap<Integer, Object>();
                for (int i = 0; i < batchSize; i++)
                    inner.put(i, parameterSets[i].toString());
                m.put("Parameters", inner);
                
                if (batchSize > 0 && input_depIds[0] != HStoreConstants.NULL_DEPENDENCY_ID) {
                    inner = new LinkedHashMap<Integer, Object>();
                    for (int i = 0; i < batchSize; i++) {
                        List<VoltTable> deps = input_deps.get(input_depIds[i]);
                        inner.put(input_depIds[i], (deps != null ? StringUtil.join("\n", deps) : "???"));
                    } // FOR
                    m.put("Input Dependencies", inner);
                }
                m.put("Output Dependencies", Arrays.toString(output_depIds));
                sb.append("\n" + StringUtil.formatMaps(m)); 
            // }
            LOG.debug(sb.toString().trim());
        }
        // *********************************** DEBUG ***********************************

        // pass attached dependencies to the EE (for non-sysproc work).
        if (input_deps != null && input_deps.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Stashing %d InputDependencies at partition %d",
                          ts, input_deps.size(), this.partitionId));
            this.ee.stashWorkUnitDependencies(input_deps);
        }
        
        // Java-based Table Read-Write Sets
        boolean readonly = true;
        boolean speculative = ts.isSpeculative();
        boolean singlePartition = ts.isPredictSinglePartition();
        int tableIds[] = null;
        for (int i = 0; i < batchSize; i++) {
            boolean fragReadOnly = PlanFragmentIdGenerator.isPlanFragmentReadOnly(fragmentIds[i]);
            // We don't need to maintain read/write sets for non-speculative txns
            if (speculative || singlePartition == false) {
                if (fragReadOnly) {
                    tableIds = catalogContext.getReadTableIds(Long.valueOf(fragmentIds[i]));
                    if (tableIds != null) ts.markTableIdsRead(this.partitionId, tableIds);
                } else {
                    tableIds = catalogContext.getWriteTableIds(Long.valueOf(fragmentIds[i]));
                    if (tableIds != null) ts.markTableIdsWritten(this.partitionId, tableIds);
                }
            }
            readonly = readonly && fragReadOnly;
        }
        
        // Enable read/write set tracking
        if (hstore_conf.site.exec_readwrite_tracking && ts.hasExecutedWork(this.partitionId) == false) {
            if (trace.val)
                LOG.trace(String.format("%s - Enabling read/write set tracking in EE at partition %d",
                          ts, this.partitionId));
            this.ee.trackingEnable(txn_id);
        }
        
        // Check whether the txn has only exeuted read-only queries up to this point
        if (ts.isExecReadOnly(this.partitionId)) {
            if (readonly == false) {
                if (trace.val)
                    LOG.trace(String.format("%s - Marking txn as not read-only %s",
                              ts, Arrays.toString(fragmentIds))); 
                ts.markExecNotReadOnly(this.partitionId);
            }
            
            // We can do this here because the only way that we're not read-only is if
            // we actually modify data at this partition
            ts.markExecutedWork(this.partitionId);
        }
        
        DependencySet result = null;
        boolean needs_profiling = false;
        if (ts.isExecLocal(this.partitionId)) {
            if (hstore_conf.site.txn_profiling && ((LocalTransaction)ts).profiler != null) {
                needs_profiling = true;
                ((LocalTransaction)ts).profiler.startExecEE();
            }
        }
        
        Throwable error = null;
        try {
            assert(this.lastCommittedUndoToken < undoToken) :
                String.format("Trying to execute work using undoToken %d for %s but " +
                              "it is less than the last committed undoToken %d at partition %d",
                              undoToken, ts, this.lastCommittedUndoToken, this.partitionId);
            if (trace.val)
                LOG.trace(String.format("%s - Executing fragments %s at partition %d [undoToken=%d]",
                          ts, Arrays.toString(fragmentIds), this.partitionId, undoToken));
            result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                            fragmentIds,
                            batchSize,
                            input_depIds,
                            output_depIds,
                            parameterSets,
                            batchSize,
                            txn_id.longValue(),
                            this.lastCommittedTxnId.longValue(),
                            undoToken);
        } catch (AssertionError ex) {
            LOG.error("Fatal error when processing " + ts + "\n" + ts.debug());
            error = ex;
            throw ex;
        } catch (EvictedTupleAccessException ex) {
            if (debug.val) LOG.warn("Caught EvictedTupleAccessException.");
            ((EvictedTupleAccessException)ex).setPartitionId(this.partitionId);
            error = ex;
            throw ex;
        } catch (SerializableException ex) {
            if (debug.val)
                LOG.error(String.format("%s - Unexpected error in the ExecutionEngine on partition %d",
                          ts, this.partitionId), ex);
            error = ex;
            throw ex;
        } catch (Throwable ex) {
            error = ex;
            String msg = String.format("%s - Failed to execute PlanFragments: %s", ts, Arrays.toString(fragmentIds));
            throw new ServerFaultException(msg, ex);
        } finally {
            if (needs_profiling) ((LocalTransaction)ts).profiler.stopExecEE();
            if (error == null && result == null) {
                LOG.warn(String.format("%s - Finished executing fragments but got back null results [fragmentIds=%s]",
                         ts, Arrays.toString(fragmentIds)));
            }
        }
        
        // *********************************** DEBUG ***********************************
        if (debug.val) {
            if (result != null) {
                LOG.debug(String.format("%s - Finished executing fragments and got back %d results",
                                        ts, result.depIds.length));
            } else {
                LOG.warn(String.format("%s - Finished executing fragments but got back null results? That seems bad...", ts));
            }
        }
        // *********************************** DEBUG ***********************************
        return (result);
    }
    
    /**
     * Load a VoltTable directly into the EE at this partition.
     * <B>NOTE:</B> This should only be invoked by a system stored procedure.
     * @param txn_id
     * @param clusterName
     * @param databaseName
     * @param tableName
     * @param data
     * @param allowELT
     * @throws VoltAbortException
     */
    public void loadTable(AbstractTransaction ts, String clusterName, String databaseName, String tableName, VoltTable data, int allowELT) throws VoltAbortException {
        Table table = this.catalogContext.database.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("Table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }
        if (data == null || data.getRowCount() == 0) {
            return;
        }
        
        if (debug.val)
            LOG.debug(String.format("Loading %d row(s) into %s [txnId=%d]",
                      data.getRowCount(), table.getName(), ts.getTransactionId()));
        ts.markExecutedWork(this.partitionId);
        this.ee.loadTable(table.getRelativeIndex(), data,
                          ts.getTransactionId(),
                          this.lastCommittedTxnId.longValue(),
                          ts.getLastUndoToken(this.partitionId),
                          allowELT != 0);
    }

    /**
     * Load a VoltTable directly into the EE at this partition.
     * <B>NOTE:</B> This should only be used for testing
     * @param txnId
     * @param table
     * @param data
     * @param allowELT
     * @throws VoltAbortException
     */
    protected void loadTable(Long txnId, Table table, VoltTable data, boolean allowELT) throws VoltAbortException {
        if (debug.val)
            LOG.debug(String.format("Loading %d row(s) into %s [txnId=%d]",
                      data.getRowCount(), table.getName(), txnId));
        this.ee.loadTable(table.getRelativeIndex(),
                          data,
                          txnId.longValue(),
                          this.lastCommittedTxnId.longValue(),
                          HStoreConstants.NULL_UNDO_LOGGING_TOKEN,
                          allowELT);
    }

    /**
     * Execute a SQLStmt batch at this partition. This is the main entry point from 
     * VoltProcedure for where we will execute a SQLStmt batch from a txn.
     * @param ts The txn handle that is executing this query batch
     * @param batchSize The number of SQLStmts that the txn queued up using voltQueueSQL()
     * @param batchStmts The SQLStmts that the txn is trying to execute
     * @param batchParams The input parameters for the SQLStmts
     * @param finalTask Whether the txn has marked this as the last batch that they will ever execute
     * @param forceSinglePartition Whether to force the BatchPlanner to only generate a single-partition plan  
     * @return
     */
    public VoltTable[] executeSQLStmtBatch(LocalTransaction ts,
                                           int batchSize,
                                           SQLStmt batchStmts[],
                                           ParameterSet batchParams[],
                                           boolean finalTask,
                                           boolean forceSinglePartition) {
        
        boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.profiler != null);
        if (needs_profiling) {
            ts.profiler.addBatch(batchSize);
            ts.profiler.stopExecJava();
            ts.profiler.startExecPlanning();
        }

        // HACK: This is needed to handle updates on replicated tables properly
        // when there is only one partition in the cluster.
        if (catalogContext.numberOfPartitions == 1) {
            this.depTracker.addTransaction(ts);
        }
        
        if (hstore_conf.site.exec_deferrable_queries) {
            // TODO: Loop through batchStmts and check whether their corresponding Statement
            // is marked as deferrable. If so, then remove them from batchStmts and batchParams
            // (sliding everyone over by one in the arrays). Queue up the deferred query.
            // Be sure decrement batchSize after you finished processing this.
            // EXAMPLE: batchStmts[0].getStatement().getDeferrable()    
        }
        
        // Calculate the hash code for this batch to see whether we already have a planner
        final Integer batchHashCode = VoltProcedure.getBatchHashCode(batchStmts, batchSize);
        BatchPlanner planner = this.batchPlanners.get(batchHashCode);
        if (planner == null) { // Assume fast case
            planner = new BatchPlanner(batchStmts,
                                       batchSize,
                                       ts.getProcedure(),
                                       this.p_estimator,
                                       forceSinglePartition);
            this.batchPlanners.put(batchHashCode, planner);
        }
        assert(planner != null);
        
        // At this point we have to calculate exactly what we need to do on each partition
        // for this batch. So somehow right now we need to fire this off to either our
        // local executor or to Evan's magical distributed transaction manager
        BatchPlanner.BatchPlan plan = planner.plan(ts.getTransactionId(),
                                                   this.partitionId,
                                                   ts.getPredictTouchedPartitions(), 
                                                   ts.getTouchedPartitions(),
                                                   batchParams);
        assert(plan != null);
        if (trace.val) {
            LOG.trace(ts + " - Touched Partitions: " + ts.getTouchedPartitions().values());
            LOG.trace(ts + " - Next BatchPlan:\n" + plan.toString());
        }
        if (needs_profiling) ts.profiler.stopExecPlanning();
        
        // Tell the TransactionEstimator that we're about to execute these mofos
        EstimatorState t_state = ts.getEstimatorState();
        if (this.localTxnEstimator != null && t_state != null && t_state.isUpdatesEnabled()) {
            if (needs_profiling) ts.profiler.startExecEstimation();
            try {
                this.localTxnEstimator.executeQueries(t_state,
                                                      planner.getStatements(),
                                                      plan.getStatementPartitions());
            } finally {
                if (needs_profiling) ts.profiler.stopExecEstimation();
            }
        } else if (t_state != null && t_state.shouldAllowUpdates()) {
            LOG.warn("Skipping estimator updates for " + ts);
        }

        // Check whether our plan was caused a mispredict
        // Doing it this way allows us to update the TransactionEstimator before we abort the txn
        if (plan.getMisprediction() != null) {
            MispredictionException ex = plan.getMisprediction(); 
            ts.setPendingError(ex, false);
            assert(ex.getPartitions().isEmpty() == false) :
                "Unexpected empty PartitionSet for mispredicated txn " + ts;

            // Print Misprediction Debug
            if (hstore_conf.site.exec_mispredict_crash) {
                // Use a lock so that only dump out the first txn that fails
                synchronized (PartitionExecutor.class) {
                    LOG.warn("\n" + EstimatorUtil.mispredictDebug(ts, planner, batchStmts, batchParams));
                    LOG.fatal(String.format("Crashing because site.exec_mispredict_crash is true [txn=%s]", ts));
                    this.crash(ex);
                } // SYNCH
            }
            else if (debug.val) {
                if (trace.val)
                    LOG.warn("\n" + EstimatorUtil.mispredictDebug(ts, planner, batchStmts, batchParams));
                LOG.debug(ts + " - Aborting and restarting mispredicted txn.");
            }
            throw ex;
        }
        
        // Keep track of the number of times that we've executed each query for this transaction
        int stmtCounters[] = this.tmp_stmtCounters.getArray(batchSize);
        for (int i = 0; i < batchSize; i++) {
            stmtCounters[i] = ts.updateStatementCounter(batchStmts[i].getStatement());
        } // FOR
        
        if (ts.hasPrefetchQueries()) {
            PartitionSet stmtPartitions[] = plan.getStatementPartitions();
            PrefetchState prefetchState = ts.getPrefetchState();
            QueryTracker queryTracker = prefetchState.getExecQueryTracker();
            assert(prefetchState != null);
            for (int i = 0; i < batchSize; i++) {
                // We always have to update the query tracker regardless of whether
                // the query was prefetched or not. This is so that we can ensure
                // that we execute the queries in the right order.
                Statement stmt = batchStmts[i].getStatement();
                stmtCounters[i] = queryTracker.addQuery(stmt, stmtPartitions[i], batchParams[i]);
            } // FOR
            // FIXME PrefetchQueryUtil.checkSQLStmtBatch(this, ts, plan, batchSize, batchStmts, batchParams);
        } // PREFETCH
        
        VoltTable results[] = null;
        
        // FAST-PATH: Single-partition + Local
        // If the BatchPlan only has WorkFragments that are for this partition, then
        // we can use the fast-path executeLocalPlan() method
        if (plan.isSingledPartitionedAndLocal()) {
            if (trace.val)
                LOG.trace(String.format("%s - Sending %s directly to the ExecutionEngine at partition %d",
                          ts, plan.getClass().getSimpleName(), this.partitionId));
            
            // If this the finalTask flag is set to true, and we're only executing queries at this
            // partition, then we need to notify the other partitions that we're done with them.
            if (hstore_conf.site.exec_early_prepare &&
                    finalTask == true &&
                    ts.isPredictSinglePartition() == false &&
                    ts.isSysProc() == false &&
                    ts.allowEarlyPrepare() == true) {
                tmp_fragmentsPerPartition.clearValues();
                tmp_fragmentsPerPartition.put(this.partitionId, batchSize);
                DonePartitionsNotification notify = this.computeDonePartitions(ts, null, tmp_fragmentsPerPartition, finalTask);
                if (notify != null && notify.hasSitesToNotify()) {
                    this.notifyDonePartitions(ts, notify);
                }
            }

            // Execute the queries right away.
            results = this.executeLocalPlan(ts, plan, batchParams);
        }
        // DISTRIBUTED EXECUTION
        // Otherwise, we need to generate WorkFragments and then send the messages out 
        // to our remote partitions using the HStoreCoordinator
        else {
            List<WorkFragment.Builder> partitionFragments = new ArrayList<WorkFragment.Builder>();
            plan.getWorkFragmentsBuilders(ts.getTransactionId(), stmtCounters, partitionFragments);
            if (debug.val)
                LOG.debug(String.format("%s - Using dispatchWorkFragments to execute %d %ss",
                          ts, partitionFragments.size(), WorkFragment.class.getSimpleName()));
            
            if (needs_profiling) {
                int remote_cnt = 0;
                PartitionSet stmtPartitions[] = plan.getStatementPartitions();
                for (int i = 0; i < batchSize; i++) {
                    if (stmtPartitions[i].get() != ts.getBasePartition()) remote_cnt++;
                    if (trace.val)
                        LOG.trace(String.format("%s - [%02d] stmt:%s / partitions:%s",
                                 ts, i, batchStmts[i].getStatement().getName(), stmtPartitions[i]));
                } // FOR
                if (trace.val) LOG.trace(String.format("%s - Remote Queries Count = %d", ts, remote_cnt));
                ts.profiler.addRemoteQuery(remote_cnt);
            }
            
            // This will block until we get all of our responses.
            results = this.dispatchWorkFragments(ts, batchSize, batchParams, partitionFragments, finalTask);
        }
        if (debug.val && results == null)
            LOG.warn("Got back a null results array for " + ts + "\n" + plan.toString());

        if (needs_profiling) ts.profiler.startExecJava();
        
        return (results);
    }
    
    /**
     * 
     * @param fresponse
     */
    protected WorkResult buildWorkResult(AbstractTransaction ts, DependencySet result, Status status, SerializableException error) {
        WorkResult.Builder builder = WorkResult.newBuilder();
        
        // Partition Id
        builder.setPartitionId(this.partitionId);
        
        // Status
        builder.setStatus(status);
        
        // SerializableException 
        if (error != null) {
            int size = error.getSerializedSize();
            BBContainer bc = this.buffer_pool.acquire(size);
            try {
                error.serializeToBuffer(bc.b);
            } catch (IOException ex) {
                String msg = "Failed to serialize error for " + ts;
                throw new ServerFaultException(msg, ex);
            }
            bc.b.rewind();
            builder.setError(ByteString.copyFrom(bc.b));
            bc.discard();
        }
        
        // Push dependencies back to the remote partition that needs it
        if (status == Status.OK) {
            for (int i = 0, cnt = result.size(); i < cnt; i++) {
                builder.addDepId(result.depIds[i]);
                this.fs.clear();
                try {
                    result.dependencies[i].writeExternal(this.fs);
                    ByteString bs = ByteString.copyFrom(this.fs.getBBContainer().b);
                    builder.addDepData(bs);
                } catch (Exception ex) {
                    throw new ServerFaultException(String.format("Failed to serialize output dependency %d for %s", result.depIds[i], ts), ex);
                }
                if (trace.val)
                    LOG.trace(String.format("%s - Serialized Output Dependency %d\n%s",
                              ts, result.depIds[i], result.dependencies[i]));  
            } // FOR
            this.fs.getBBContainer().discard();
        }
        
        return (builder.build());
    }

    /**
     * This method is invoked when the PartitionExecutor wants to execute work at a remote HStoreSite.
     * The doneNotificationsPerSite is an array where each offset (based on SiteId) may contain
     * a PartitionSet of the partitions that this txn is finished with at the remote node and will
     * not be executing any work in the current batch. 
     * @param ts
     * @param fragmentBuilders
     * @param parameterSets
     * @param doneNotificationsPerSite
     */
    private void requestWork(LocalTransaction ts,
                             Collection<WorkFragment.Builder> fragmentBuilders,
                             List<ByteString> parameterSets,
                             DonePartitionsNotification notify) {
        assert(fragmentBuilders.isEmpty() == false);
        assert(ts != null);
        Long txn_id = ts.getTransactionId();

        if (trace.val)
            LOG.trace(String.format("%s - Wrapping %d %s into a %s",
                      ts, fragmentBuilders.size(),
                      WorkFragment.class.getSimpleName(),
                      TransactionWorkRequest.class.getSimpleName()));
        
        // If our transaction was originally designated as a single-partitioned, then we need to make
        // sure that we don't touch any partition other than our local one. If we do, then we need abort
        // it and restart it as multi-partitioned
        boolean need_restart = false;
        boolean predict_singlepartition = ts.isPredictSinglePartition(); 
        PartitionSet done_partitions = ts.getDonePartitions();
        Estimate t_estimate = ts.getLastEstimate();
        
        // Now we can go back through and start running all of the WorkFragments that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each WorkFragment in its own message
        for (WorkFragment.Builder fragmentBuilder : fragmentBuilders) {
            assert(this.depTracker.isBlocked(ts, fragmentBuilder) == false);
            final int target_partition = fragmentBuilder.getPartitionId();
            final int target_site = catalogContext.getSiteIdForPartitionId(target_partition);
            final PartitionSet doneNotifications = (notify != null ? notify.getNotifications(target_site) : null);
            
            // Make sure that this isn't a single-partition txn trying to access a remote partition
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (debug.val)
                    LOG.debug(String.format("%s - Txn on partition %d is suppose to be " +
                              "single-partitioned, but it wants to execute a fragment on partition %d",
                              ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure that this txn isn't trying to access a partition that we said we were
            // done with earlier
            else if (done_partitions.contains(target_partition)) {
                if (debug.val)
                    LOG.warn(String.format("%s on partition %d was marked as done on partition %d " +
                             "but now it wants to go back for more!",
                             ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure we at least have something to do!
            else if (fragmentBuilder.getFragmentIdCount() == 0) {
                LOG.warn(String.format("%s - Trying to send a WorkFragment request with 0 fragments", ts));
                continue;
            }
            
            // Add in the specexec query estimate at this partition if needed
            if (hstore_conf.site.specexec_enable && t_estimate != null && t_estimate.hasQueryEstimate(target_partition)) {
                List<CountedStatement> queryEst = t_estimate.getQueryEstimate(target_partition);
                // if (debug.val)
                if (target_partition == 0)
                    if (debug.val)
                        LOG.debug(String.format("%s - Sending remote query estimate to partition %d " +
                                  "containing %d queries\n%s",
                                  ts, target_partition, queryEst.size(), StringUtil.join("\n", queryEst)));
                assert(queryEst.isEmpty() == false);
                QueryEstimate.Builder estBuilder = QueryEstimate.newBuilder();
                for (CountedStatement countedStmt : queryEst) {
                    estBuilder.addStmtIds(countedStmt.statement.getId());
                    estBuilder.addStmtCounters(countedStmt.counter);
                } // FOR
                fragmentBuilder.setFutureStatements(estBuilder);
            }
           
            // Get the TransactionWorkRequest.Builder for the remote HStoreSite
            // We will use this store our serialized input dependencies
            TransactionWorkRequestBuilder requestBuilder = tmp_transactionRequestBuilders[target_site];
            if (requestBuilder == null) {
                requestBuilder = tmp_transactionRequestBuilders[target_site] = new TransactionWorkRequestBuilder();
            }
            TransactionWorkRequest.Builder builder = requestBuilder.getBuilder(ts, doneNotifications);
            
            // Also keep track of what Statements they are executing so that we know
            // we need to send over the wire to them.
            requestBuilder.addParamIndexes(fragmentBuilder.getParamIndexList());
            
            // Input Dependencies
            if (fragmentBuilder.getNeedsInput()) {
                if (debug.val)
                    LOG.debug(String.format("%s - Retrieving input dependencies at partition %d",
                              ts, this.partitionId));
                
                tmp_removeDependenciesMap.clear();
                for (int i = 0, cnt = fragmentBuilder.getInputDepIdCount(); i < cnt; i++) {
                    this.getFragmentInputs(ts, fragmentBuilder.getInputDepId(i), tmp_removeDependenciesMap);
                } // FOR

                for (Entry<Integer, List<VoltTable>> e : tmp_removeDependenciesMap.entrySet()) {
                    if (requestBuilder.hasInputDependencyId(e.getKey())) continue;

                    if (debug.val)
                        LOG.debug(String.format("%s - Attaching %d input dependencies to be sent to %s",
                                  ts, e.getValue().size(), HStoreThreadManager.formatSiteName(target_site)));
                    for (VoltTable vt : e.getValue()) {
                        this.fs.clear();
                        try {
                            this.fs.writeObject(vt);
                            builder.addAttachedDepId(e.getKey().intValue());
                            builder.addAttachedData(ByteString.copyFrom(this.fs.getBBContainer().b));
                        } catch (Exception ex) {
                            String msg = String.format("Failed to serialize input dependency %d for %s", e.getKey(), ts);
                            throw new ServerFaultException(msg, ts.getTransactionId());
                        }
                        if (debug.val)
                            LOG.debug(String.format("%s - Storing %d rows for InputDependency %d to send " +
                                      "to partition %d [bytes=%d]",
                                      ts, vt.getRowCount(), e.getKey(), fragmentBuilder.getPartitionId(),
                                      CollectionUtil.last(builder.getAttachedDataList()).size()));
                    } // FOR
                    requestBuilder.addInputDependencyId(e.getKey());
                } // FOR
                this.fs.getBBContainer().discard();
            }
            builder.addFragments(fragmentBuilder);
        } // FOR (tasks)
        
        // Bad mojo! We need to throw a MispredictionException so that the VoltProcedure
        // will catch it and we can propagate the error message all the way back to the HStoreSite
        if (need_restart) {
            if (trace.val)
                LOG.trace(String.format("Aborting %s because it was mispredicted", ts));
            // This is kind of screwy because we don't actually want to send the touched partitions
            // histogram because VoltProcedure will just do it for us...
            throw new MispredictionException(txn_id, null);
        }

        // Stick on the ParameterSets that each site needs into the TransactionWorkRequest
        for (int target_site = 0; target_site < tmp_transactionRequestBuilders.length; target_site++) {
            TransactionWorkRequestBuilder builder = tmp_transactionRequestBuilders[target_site]; 
            if (builder == null || builder.isDirty() == false) {
                continue;
            }
            assert(builder != null);
            builder.addParameterSets(parameterSets);
            
            // Bombs away!
            this.hstore_coordinator.transactionWork(ts, target_site, builder.build(), this.request_work_callback);
            if (debug.val)
                LOG.debug(String.format("%s - Sent Work request to remote site %s",
                          ts, HStoreThreadManager.formatSiteName(target_site)));

        } // FOR
    }

    /**
     * Figure out what partitions this transaction is done with. This will only return
     * a PartitionSet of what partitions we think we're done with.
     * For each partition that we idenitfy that the txn is done with, we will check to see
     * whether the txn is going to execute a query at its site in this batch. If it's not,
     * then we will notify that HStoreSite through the HStoreCoordinator.
     * If the partition that it doesn't need anymore is local (i.e., it's at the same
     * HStoreSite that we're at right now), then we'll just pass them a quick message
     * to let them know that they can prepare the txn. 
     * @param ts
     * @param estimate
     * @param fragmentsPerPartition A histogram of the number of PlanFragments the
     *                              txn will execute in this batch at each partition.
     * @param finalTask Whether the txn has marked this as the last batch that they will ever execute
     * @return A notification object that can be used to notify partitions that this txn is done with them.
     */
    private DonePartitionsNotification computeDonePartitions(final LocalTransaction ts,
                                                             final Estimate estimate,
                                                             final FastIntHistogram fragmentsPerPartition,
                                                             final boolean finalTask) {
        final PartitionSet touchedPartitions = ts.getPredictTouchedPartitions();
        final PartitionSet donePartitions = ts.getDonePartitions();
        
        // Compute the partitions that the txn will be finished with after this batch
        PartitionSet estDonePartitions = null;
        
        // If the finalTask flag is set to true, then the new done partitions
        // is every partition that this txn has locked
        if (finalTask) {
            estDonePartitions = new PartitionSet(touchedPartitions);
        }
        // Otherwise, we'll rely on the transaction's current estimate to figure it out.
        else {
            if (estimate == null || estimate.isValid() == false) {
                if (debug.val && estimate != null)
                    LOG.debug(String.format("%s - Unable to compute new done partitions because there " +
                   		  "is no valid estimate for the txn",
                              ts, estimate.getClass().getSimpleName()));
                return (null);
            }
            
            estDonePartitions = estimate.getDonePartitions(this.thresholds);
            if (estDonePartitions == null || estDonePartitions.isEmpty()) {
                if (debug.val)
                    LOG.debug(String.format("%s - There are no new done partitions identified by %s",
                              ts, estimate.getClass().getSimpleName()));
                return (null);
            }
        }
        assert(estDonePartitions != null) : "Null done partitions for " + ts;
        assert(estDonePartitions.isEmpty() == false) : "Empty done partitions for " + ts;
        
        if (debug.val)
            LOG.debug(String.format("%s - New estimated done partitions %s%s",
                      ts, estDonePartitions,
                      (trace.val ? "\n"+estimate : "")));

        // Note that we can actually be done with ourself, if this txn is only going to execute queries
        // at remote partitions. But we can't actually execute anything because this partition's only 
        // execution thread is going to be blocked. So we always do this so that we're not sending a 
        // useless message
        estDonePartitions.remove(this.partitionId);
        
        // Make sure that we only tell partitions that we actually touched, otherwise they will
        // be stuck waiting for a finish request that will never come!
        DonePartitionsNotification notify = new DonePartitionsNotification();
        LocalPrepareCallback callback = null;
        for (int partition : estDonePartitions.values()) {
            // Only mark the txn done at this partition if the Estimate says we were done
            // with it after executing this batch and it's a partition that we've locked.
            if (donePartitions.contains(partition) || touchedPartitions.contains(partition) == false)
                continue;
                
            if (trace.val)
                LOG.trace(String.format("%s - Marking partition %d as done for txn", ts, partition));
            notify.donePartitions.add(partition);
            if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.markEarly2PCPartition(partition);
            
            // Check whether we're executing a query at this partition in this batch.
            // If we're not, then we need to check whether we can piggyback the "done" message
            // in another WorkFragment going to that partition or whether we have to
            // send a separate TransactionPrepareRequest
            if (fragmentsPerPartition.get(partition, 0) == 0) {
                // We need to let them know that the party is over!
                if (hstore_site.isLocalPartition(partition)) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Notifying local partition %d that the txn is finished with it",
                                  ts, partition));
                    if (callback == null) callback = ts.getPrepareCallback();
                    hstore_site.getPartitionExecutor(partition).queuePrepare(ts, callback);
                }
                // Check whether we can piggyback on another WorkFragment that is going to
                // the same site
                else {
                    Site remoteSite = catalogContext.getSiteForPartition(partition);
                    boolean found = false;
                    for (Partition remotePartition : remoteSite.getPartitions().values()) {
                        if (fragmentsPerPartition.get(remotePartition.getId(), 0) != 0) {
                            found = true;
                            break;
                        }
                    } // FOR
                    notify.addSiteNotification(remoteSite, partition, (found == false));
                }
            }
        } // FOR
        return (notify);
    }
    
    /**
     * Send asynchronous notification messages to any remote site to tell them that we
     * are done with partitions that they have. 
     * @param ts
     * @param notify
     */
    private void notifyDonePartitions(LocalTransaction ts, DonePartitionsNotification notify) {
        if (debug.val)
            LOG.debug(String.format("%s - Sending done partitions notifications to remote sites %s",
                      ts, notify._sitesToNotify));
        
        // BLAST OUT NOTIFICATIONS!
        for (int remoteSiteId : notify._sitesToNotify) {
            assert(notify.notificationsPerSite[remoteSiteId] != null);
            if (debug.val)
                LOG.debug(String.format("%s - Notifying %s that txn is finished with partitions %s",
                          ts, HStoreThreadManager.formatSiteName(remoteSiteId),
                          notify.notificationsPerSite[remoteSiteId]));
            hstore_coordinator.transactionPrepare(ts, ts.getPrepareCallback(),
                                                  notify.notificationsPerSite[remoteSiteId]);
            
            // Make sure that we remove the PartitionSet for this site so that we don't
            // try to send the notifications again.
            notify.notificationsPerSite[remoteSiteId] = null; 
        } // FOR
    }
    
    
    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do...
     * This is the slowest way to execute a bunch of WorkFragments and therefore should only be invoked
     * for batches that need to access non-local partitions
     * @param ts The txn handle that is executing this query batch
     * @param batchSize The number of SQLStmts that the txn queued up using voltQueueSQL()
     * @param batchParams The input parameters for the SQLStmts
     * @param allFragmentBuilders
     * @param finalTask Whether the txn has marked this as the last batch that they will ever execute
     * @return
     */
    public VoltTable[] dispatchWorkFragments(final LocalTransaction ts,
                                             final int batchSize,
                                             final ParameterSet batchParams[],
                                             final Collection<WorkFragment.Builder> allFragmentBuilders,
                                             boolean finalTask) {
        assert(allFragmentBuilders.isEmpty() == false) :
            "Unexpected empty WorkFragment list for " + ts;
        final boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.profiler != null);
        
        // *********************************** DEBUG ***********************************
        if (debug.val) {
            LOG.debug(String.format("%s - Preparing to dispatch %d messages and wait for the results [needsProfiling=%s]",
                      ts, allFragmentBuilders.size(), needs_profiling));
            if (trace.val) {
                StringBuilder sb = new StringBuilder();
                sb.append(ts + " - WorkFragments:\n");
                for (WorkFragment.Builder fragment : allFragmentBuilders) {
                    sb.append(StringBoxUtil.box(fragment.toString()) + "\n");
                } // FOR
                sb.append(ts + " - ParameterSets:\n");
                for (ParameterSet ps : batchParams) {
                    sb.append(ps + "\n");
                } // FOR
                LOG.trace(sb);
            }
        }
        // *********************************** DEBUG *********************************** 
        
        // OPTIONAL: Check to make sure that this request is valid 
        //  (1) At least one of the WorkFragments needs to be executed on a remote partition
        //  (2) All of the PlanFragments ids in the WorkFragments match this txn's Procedure
        if (hstore_conf.site.exec_validate_work && ts.isSysProc() == false) {
            LOG.warn(String.format("%s - Checking whether all of the WorkFragments are valid", ts));
            boolean has_remote = false; 
            for (WorkFragment.Builder frag : allFragmentBuilders) {
                if (frag.getPartitionId() != this.partitionId) {
                    has_remote = true;
                }
                for (int frag_id : frag.getFragmentIdList()) {
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(catalogContext.database, frag_id);
                    Statement catalog_stmt = catalog_frag.getParent();
                    assert(catalog_stmt != null);
                    Procedure catalog_proc = catalog_stmt.getParent();
                    if (catalog_proc.equals(ts.getProcedure()) == false) {
                        LOG.warn(ts.debug() + "\n" + allFragmentBuilders + "\n---- INVALID ----\n" + frag);
                        String msg = String.format("%s - Unexpected %s", ts, catalog_frag.fullName());
                        throw new ServerFaultException(msg, ts.getTransactionId());
                    }
                }
            } // FOR
            if (has_remote == false) {
                LOG.warn(ts.debug() + "\n" + allFragmentBuilders);
                String msg = ts + "Trying to execute all local single-partition queries using the slow-path!";
                throw new ServerFaultException(msg, ts.getTransactionId());
            }
        }

        boolean first = true;
        boolean serializedParams = false;
        CountDownLatch latch = null;
        boolean all_local = true;
        boolean is_localSite;
        boolean is_localPartition;
        boolean is_localReadOnly = true;
        int num_localPartition = 0;
        int num_localSite = 0;
        int num_remote = 0;
        int num_skipped = 0;
        int total = 0;
        Collection<WorkFragment.Builder> fragmentBuilders = allFragmentBuilders;

        // Make sure our txn is in our DependencyTracker
        if (trace.val)
            LOG.trace(String.format("%s - Added transaction to %s",
                      ts, this.depTracker.getClass().getSimpleName()));
        this.depTracker.addTransaction(ts);
        
        // Count the number of fragments that we're going to send to each partition and
        // figure out whether the txn will always be read-only at this partition
        tmp_fragmentsPerPartition.clearValues();
        for (WorkFragment.Builder fragmentBuilder : allFragmentBuilders) {
            int partition = fragmentBuilder.getPartitionId();
            tmp_fragmentsPerPartition.put(partition);
            if (this.partitionId == partition && fragmentBuilder.getReadOnly() == false) {
                is_localReadOnly = false;
            }
        } // FOR
        long undoToken = this.calculateNextUndoToken(ts, is_localReadOnly);
        ts.initFirstRound(undoToken, batchSize);
        final boolean predict_singlePartition = ts.isPredictSinglePartition();
        
        // Calculate whether we are finished with partitions now
        final Estimate lastEstimate = ts.getLastEstimate();
        DonePartitionsNotification notify = null;
        if (hstore_conf.site.exec_early_prepare && ts.isSysProc() == false && ts.allowEarlyPrepare()) {
            notify = this.computeDonePartitions(ts, lastEstimate, tmp_fragmentsPerPartition, finalTask);
            if (notify != null && notify.hasSitesToNotify())
                this.notifyDonePartitions(ts, notify);
        }
        
        // Attach the ParameterSets to our transaction handle so that anybody on this HStoreSite
        // can access them directly without needing to deserialize them from the WorkFragments
        ts.attachParameterSets(batchParams);
        
        // Now if we have some work sent out to other partitions, we need to wait until they come back
        // In the first part, we wait until all of our blocked WorkFragments become unblocked
        final BlockingDeque<Collection<WorkFragment.Builder>> queue = this.depTracker.getUnblockedWorkFragmentsQueue(ts);

        // Run through this loop if:
        //  (1) We have no pending errors
        //  (2) This is our first time in the loop (first == true)
        //  (3) If we know that there are still messages being blocked
        //  (4) If we know that there are still unblocked messages that we need to process
        //  (5) The latch for this round is still greater than zero
        while (ts.hasPendingError() == false && 
              (first == true || this.depTracker.stillHasWorkFragments(ts) || (latch != null && latch.getCount() > 0))) {
            if (trace.val)
                LOG.trace(String.format("%s - %s loop [first=%s, stillHasWorkFragments=%s, latch=%s]",
                          ts, ClassUtil.getCurrentMethodName(),
                          first, this.depTracker.stillHasWorkFragments(ts), queue.size(), latch));
            
            // If this is the not first time through the loop, then poll the queue
            // to get our list of fragments
            if (first == false) {
                all_local = true;
                is_localSite = false;
                is_localPartition = false;
                num_localPartition = 0;
                num_localSite = 0;
                num_remote = 0;
                num_skipped = 0;
                total = 0;
                
                if (trace.val)
                    LOG.trace(String.format("%s - Waiting for unblocked tasks on partition %d",
                              ts, this.partitionId));
                fragmentBuilders = queue.poll(); // NON-BLOCKING
                
                // If we didn't get back a list of fragments here, then we will spin through
                // and invoke utilityWork() to try to do something useful until what we need shows up
                if (needs_profiling) ts.profiler.startExecDtxnWork();
                if (hstore_conf.site.exec_profiling) this.profiler.sp1_time.start();
                try {
                    while (fragmentBuilders == null) {
                        // If there is more work that we could do, then we'll just poll the queue
                        // without waiting so that we can go back and execute it again if we have
                        // more time.
                        if (this.utilityWork()) {
                            fragmentBuilders = queue.poll();
                        }
                        // Otherwise we will wait a little so that we don't spin the CPU
                        else {
                            fragmentBuilders = queue.poll(WORK_QUEUE_POLL_TIME, TimeUnit.MILLISECONDS);
                        }
                    } // WHILE
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) {
                        LOG.error(String.format("%s - We were interrupted while waiting for blocked tasks", ts), ex);
                    }
                    return (null);
                } finally {
                    if (needs_profiling) ts.profiler.stopExecDtxnWork();
                    if (hstore_conf.site.exec_profiling) this.profiler.sp1_time.stopIfStarted();
                }
            }
            assert(fragmentBuilders != null);
            
            // If the list to fragments unblock is empty, then we 
            // know that we have dispatched all of the WorkFragments for the
            // transaction's current SQLStmt batch. That means we can just wait 
            // until all the results return to us.
            if (fragmentBuilders.isEmpty()) {
                if (trace.val)
                    LOG.trace(String.format("%s - Got an empty list of WorkFragments at partition %d. " +
                              "Blocking until dependencies arrive",
                              ts, this.partitionId));
                break;
            }

            this.tmp_localWorkFragmentBuilders.clear();
            if (predict_singlePartition == false) {
                this.tmp_remoteFragmentBuilders.clear();
                this.tmp_localSiteFragmentBuilders.clear();
            }
            
            // -------------------------------
            // FAST PATH: Assume everything is local
            // -------------------------------
            if (predict_singlePartition) {
                for (WorkFragment.Builder fragmentBuilder : fragmentBuilders) {
                    if (first == false || this.depTracker.addWorkFragment(ts, fragmentBuilder, batchParams)) {
                        this.tmp_localWorkFragmentBuilders.add(fragmentBuilder);
                        total++;
                        num_localPartition++;
                    }
                } // FOR
                
                // We have to tell the transaction handle to start the round before we send off the
                // WorkFragments for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = this.depTracker.getDependencyLatch(ts);
                }
                
                // Execute all of our WorkFragments quickly at our local ExecutionEngine
                for (WorkFragment.Builder fragmentBuilder : this.tmp_localWorkFragmentBuilders) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Got unblocked %s to execute locally",
                                  ts, fragmentBuilder.getClass().getSimpleName()));
                    assert(fragmentBuilder.getPartitionId() == this.partitionId) :
                        String.format("Trying to process %s for %s on partition %d but it should have been " +
                                      "sent to partition %d [singlePartition=%s]\n%s",
                                      fragmentBuilder.getClass().getSimpleName(), ts, this.partitionId,
                                      fragmentBuilder.getPartitionId(), predict_singlePartition, fragmentBuilder);
                    WorkFragment fragment = fragmentBuilder.build();
                    this.processWorkFragment(ts, fragment, batchParams);
                } // FOR
            }
            // -------------------------------
            // SLOW PATH: Mixed local and remote messages
            // -------------------------------
            else {
                // Look at each task and figure out whether it needs to be executed at a remote
                // HStoreSite or whether we can execute it at one of our local PartitionExecutors.
                for (WorkFragment.Builder fragmentBuilder : fragmentBuilders) {
                    int partition = fragmentBuilder.getPartitionId();
                    is_localSite = hstore_site.isLocalPartition(partition);
                    is_localPartition = (partition == this.partitionId);
                    all_local = all_local && is_localPartition;
                    
                    // If this is the last WorkFragment that we're going to send to this partition for
                    // this batch, then we will want to check whether we know that this is the last
                    // time this txn will ever need to go to that txn. If so, then we'll want to 
                    if (notify != null && notify.donePartitions.contains(partition) &&
                        tmp_fragmentsPerPartition.dec(partition) == 0) {
                        if (debug.val)
                            LOG.debug(String.format("%s - Setting last fragment flag in %s for partition %d",
                                      ts, WorkFragment.class.getSimpleName(), partition));
                        fragmentBuilder.setLastFragment(true);
                    }
                    
                    if (first == false || this.depTracker.addWorkFragment(ts, fragmentBuilder, batchParams)) {
                        total++;
                        
                        // At this point we know that all the WorkFragment has been registered
                        // in the LocalTransaction, so then it's safe for us to look to see
                        // whether we already have a prefetched result that we need
//                        if (prefetch && is_localPartition == false) {
//                            boolean skip_queue = true;
//                            for (int i = 0, cnt = fragmentBuilder.getFragmentIdCount(); i < cnt; i++) {
//                                int fragId = fragmentBuilder.getFragmentId(i);
//                                int paramIdx = fragmentBuilder.getParamIndex(i);
//                                
//                                VoltTable vt = this.queryCache.getResult(ts.getTransactionId(),
//                                                                         fragId,
//                                                                         partition,
//                                                                         parameters[paramIdx]);
//                                if (vt != null) {
//                                    if (trace.val)
//                                        LOG.trace(String.format("%s - Storing cached result from partition %d for fragment %d",
//                                                  ts, partition, fragId));
//                                    this.depTracker.addResult(ts, partition, fragmentBuilder.getOutputDepId(i), vt);
//                                } else {
//                                    skip_queue = false;
//                                }
//                            } // FOR
//                            // If we were able to get cached results for all of the fragmentIds in
//                            // this WorkFragment, then there is no need for us to send the message
//                            // So we'll just skip queuing it up! How nice!
//                            if (skip_queue) {
//                                if (debug.val)
//                                    LOG.debug(String.format("%s - Using prefetch result for all fragments from partition %d",
//                                              ts, partition));
//                                num_skipped++;
//                                continue;
//                            }
//                        }
                        
                        // Otherwise add it to our list of WorkFragments that we want
                        // queue up right now
                        if (is_localPartition) {
                            is_localReadOnly = (is_localReadOnly && fragmentBuilder.getReadOnly());
                            this.tmp_localWorkFragmentBuilders.add(fragmentBuilder);
                            num_localPartition++;
                        } else if (is_localSite) {
                            this.tmp_localSiteFragmentBuilders.add(fragmentBuilder);
                            num_localSite++;
                        } else {
                            this.tmp_remoteFragmentBuilders.add(fragmentBuilder);
                            num_remote++;
                        }
                    }
                } // FOR
                assert(total == (num_remote + num_localSite + num_localPartition + num_skipped)) :
                    String.format("Total:%d / Remote:%d / LocalSite:%d / LocalPartition:%d / Skipped:%d",
                                  total, num_remote, num_localSite, num_localPartition, num_skipped);

                // We have to tell the txn to start the round before we send off the
                // WorkFragments for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = this.depTracker.getDependencyLatch(ts);
                }
        
                // Now request the fragments that aren't local
                // We want to push these out as soon as possible
                if (num_remote > 0) {
                    // We only need to serialize the ParameterSets once
                    if (serializedParams == false) {
                        if (needs_profiling) ts.profiler.startSerialization();
                        tmp_serializedParams.clear();
                        for (int i = 0; i < batchParams.length; i++) {
                            if (batchParams[i] == null) {
                                tmp_serializedParams.add(ByteString.EMPTY);
                            } else {
                                this.fs.clear();
                                try {
                                    batchParams[i].writeExternal(this.fs);
                                    ByteString bs = ByteString.copyFrom(this.fs.getBBContainer().b);
                                    tmp_serializedParams.add(bs);
                                } catch (Exception ex) {
                                    String msg = "Failed to serialize ParameterSet " + i + " for " + ts;
                                    throw new ServerFaultException(msg, ex, ts.getTransactionId());
                                }
                            }
                        } // FOR
                        if (needs_profiling) ts.profiler.stopSerialization();
                    }
                    
                    //if (trace.val)
                    //    LOG.trace(String.format("%s - Requesting %d %s to be executed on remote partitions " +
                    //              "[doneNotifications=%s]",
                    //              ts, WorkFragment.class.getSimpleName(), num_remote, notify!=null));
                    this.requestWork(ts, tmp_remoteFragmentBuilders, tmp_serializedParams, notify);
                    if (needs_profiling) ts.profiler.markRemoteQuery();
                }
                
                // Then dispatch the task that are needed at the same HStoreSite but 
                // at a different partition than this one
                if (num_localSite > 0) {
                    if (trace.val)
                        LOG.trace(String.format("%s - Executing %d WorkFragments on local site's partitions",
                                  ts, num_localSite));
                    for (WorkFragment.Builder builder : this.tmp_localSiteFragmentBuilders) {
                        PartitionExecutor other = hstore_site.getPartitionExecutor(builder.getPartitionId());
                        other.queueWork(ts, builder.build());
                    } // FOR
                    if (needs_profiling) ts.profiler.markRemoteQuery();
                }
        
                // Then execute all of the tasks need to access the partitions at this HStoreSite
                // We'll dispatch the remote-partition-local-site fragments first because they're going
                // to need to get queued up by at the other PartitionExecutors
                if (num_localPartition > 0) {
                    if (trace.val)
                        LOG.trace(String.format("%s - Executing %d WorkFragments on local partition",
                                  ts, num_localPartition));
                    for (WorkFragment.Builder fragmentBuilder : this.tmp_localWorkFragmentBuilders) {
                        this.processWorkFragment(ts, fragmentBuilder.build(), batchParams);
                    } // FOR
                }
            }
            if (trace.val)
                LOG.trace(String.format("%s - Dispatched %d WorkFragments " +
                          "[remoteSite=%d, localSite=%d, localPartition=%d]",
                          ts, total, num_remote, num_localSite, num_localPartition));
            first = false;
        } // WHILE
        this.fs.getBBContainer().discard();
        
        if (trace.val)
            LOG.trace(String.format("%s - BREAK OUT [first=%s, stillHasWorkFragments=%s, latch=%s]",
                      ts, first, this.depTracker.stillHasWorkFragments(ts), latch));
//        assert(ts.stillHasWorkFragments() == false) :
//            String.format("Trying to block %s before all of its WorkFragments have been dispatched!\n%s\n%s",
//                          ts,
//                          StringUtil.join("** ", "\n", tempDebug),
//                          this.getVoltProcedure(ts.getProcedureName()).getLastBatchPlan());
                
        // Now that we know all of our WorkFragments have been dispatched, we can then
        // wait for all of the results to come back in.
        if (latch == null) latch = this.depTracker.getDependencyLatch(ts);
        assert(latch != null) :
            String.format("Unexpected null dependency latch for " + ts);
        if (latch.getCount() > 0) {
            if (debug.val) {
                LOG.debug(String.format("%s - All blocked messages dispatched. Waiting for %d dependencies",
                          ts, latch.getCount()));
                if (trace.val) LOG.trace(ts.toString());
            }
            boolean timeout = false;
            long startTime = EstTime.currentTimeMillis();
            
            if (needs_profiling) ts.profiler.startExecDtxnWork();
            if (hstore_conf.site.exec_profiling) this.profiler.sp1_time.start();
            try {
                while (latch.getCount() > 0 && ts.hasPendingError() == false) {
                    if (this.utilityWork() == false) {
                        timeout = latch.await(WORK_QUEUE_POLL_TIME, TimeUnit.MILLISECONDS);
                        if (timeout == false) break;
                    }
                    if ((EstTime.currentTimeMillis() - startTime) > hstore_conf.site.exec_response_timeout) {
                        timeout = true;
                        break;
                    }
                } // WHILE
            } catch (InterruptedException ex) {
                if (this.hstore_site.isShuttingDown() == false) {
                    LOG.error(String.format("%s - We were interrupted while waiting for results", ts), ex);
                }
                timeout = true;
            } catch (Throwable ex) {
                String msg = String.format("Fatal error for %s while waiting for results", ts);
                throw new ServerFaultException(msg, ex);
            } finally {
                if (needs_profiling) ts.profiler.stopExecDtxnWork();
                if (hstore_conf.site.exec_profiling) this.profiler.sp1_time.stopIfStarted();
            }
            
            if (timeout && this.isShuttingDown() == false) {
                LOG.warn(String.format("Still waiting for responses for %s after %d ms [latch=%d]\n%s",
                         ts, hstore_conf.site.exec_response_timeout, latch.getCount(), ts.debug()));
                LOG.warn("Procedure Parameters:\n" + ts.getProcedureParameters());
                hstore_conf.site.exec_profiling = true;
                LOG.warn(hstore_site.statusSnapshot());
                
                String msg = "The query responses for " + ts + " never arrived!";
                throw new ServerFaultException(msg, ts.getTransactionId());
            }
        }
        // Update done partitions
        if (notify != null && notify.donePartitions.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Marking new done partitions %s", ts, notify.donePartitions));
            ts.getDonePartitions().addAll(notify.donePartitions);
        }
        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (debug.val) LOG.warn(String.format("%s was hit with a %s",
                                          ts, ts.getPendingError().getClass().getSimpleName()));
            throw ts.getPendingError();
        }
        
        // IMPORTANT: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        VoltTable results[] = null;
        try {
            results = this.depTracker.getResults(ts);
        } catch (AssertionError ex) {
            LOG.error("Failed to get final results for batch\n" + ts.debug());
            throw ex;
        }
        ts.finishRound(this.partitionId);
         if (debug.val) {
            if (trace.val) LOG.trace(ts + " is now running and looking for love in all the wrong places...");
            LOG.debug(String.format("%s - Returning back %d tables to VoltProcedure", ts, results.length));
        }
        return (results);
    }

    // ---------------------------------------------------------------
    // COMMIT + ABORT METHODS
    // ---------------------------------------------------------------

    /**
     * Queue a speculatively executed transaction to send its ClientResponseImpl message
     */
    private void blockClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        assert(ts.isPredictSinglePartition() == true) :
            String.format("Specutatively executed multi-partition %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(ts.isSpeculative() == true) :
            String.format("Blocking ClientResponse for non-specutative %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(ts.getClientResponse() != null) :
            String.format("Missing ClientResponse for %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(cresponse.getStatus() != Status.ABORT_MISPREDICT) : 
            String.format("Trying to block ClientResponse for mispredicted %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(this.currentExecMode != ExecutionMode.COMMIT_ALL) :
            String.format("Blocking ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        
        this.specExecBlocked.push(ts);
        this.specExecModified = (this.specExecModified && ts.isExecReadOnly(this.partitionId));
        if (debug.val)
            LOG.debug(String.format("%s - Blocking %s ClientResponse [partitions=%s, blockQueue=%d]",
                      ts, cresponse.getStatus(),
                      ts.getTouchedPartitions().values(), this.specExecBlocked.size()));
    }
    
    /**
     * For the given transaction's ClientResponse, figure out whether we can send it back to the client
     * right now or whether we need to initiate two-phase commit.
     * @param ts
     * @param cresponse
     */
    protected void processClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        Status status = cresponse.getStatus();

        if (debug.val) {
            LOG.debug(String.format("%s - Processing ClientResponse at partition %d " +
                      "[status=%s, singlePartition=%s, local=%s, clientHandle=%d]",
                      ts, this.partitionId, status, ts.isPredictSinglePartition(),
                      ts.isExecLocal(this.partitionId), cresponse.getClientHandle()));
            if (trace.val) {
                LOG.trace(ts + " Touched Partitions: " + ts.getTouchedPartitions().values());
                if (ts.isPredictSinglePartition() == false)
                    LOG.trace(ts + " Done Partitions: " + ts.getDonePartitions());
            }
        }
        
        // -------------------------------
        // ALL: Transactions that need to be internally restarted
        // -------------------------------
        if (status == Status.ABORT_MISPREDICT ||
            status == Status.ABORT_SPECULATIVE ||
            status == Status.ABORT_EVICTEDACCESS) {
            
            // If the txn was mispredicted, then we will pass the information over to the
            // HStoreSite so that it can re-execute the transaction. We want to do this 
            // first so that the txn gets re-executed as soon as possible...
            if (debug.val)
                LOG.debug(String.format("%s - Restarting because transaction was hit with %s",
                          ts, (ts.getPendingError() != null ? ts.getPendingError().getClass().getSimpleName() : "")));

            // We don't want to delete the transaction here because whoever is going to requeue it for
            // us will need to know what partitions that the transaction touched when it executed before
            if (ts.isPredictSinglePartition()) {
                if (ts.isMarkedFinished(this.partitionId) == false)
                    this.finishTransaction(ts, status);
                this.hstore_site.transactionRequeue(ts, status);
            }
            // Send a message all the partitions involved that the party is over
            // and that they need to abort the transaction. We don't actually care when we get the
            // results back because we'll start working on new txns right away.
            // Note that when we call transactionFinish() right here this thread will then go on 
            // to invoke HStoreSite.transactionFinish() for us. That means when it returns we will
            // have successfully aborted the txn at least at all of the local partitions at this site.
            else {
                if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startPostFinish();
                LocalFinishCallback finish_callback = ts.getFinishCallback();
                finish_callback.init(ts, status);
                finish_callback.markForRequeue();
                if (hstore_conf.site.exec_profiling) this.profiler.network_time.start();
                this.hstore_coordinator.transactionFinish(ts, status, finish_callback);
                if (hstore_conf.site.exec_profiling) this.profiler.network_time.stopIfStarted();
            }
        }
        // -------------------------------
        // ALL: Single-Partition Transactions
        // -------------------------------
        else if (ts.isPredictSinglePartition()) {
            // Commit or abort the transaction only if we haven't done it already
            // This can happen when we commit speculative txns out of order
            if (ts.isMarkedFinished(this.partitionId) == false) {
                this.finishTransaction(ts, status);
            }
            
            // We have to mark it as loggable to prevent the response
            // from getting sent back to the client
            if (hstore_conf.site.commandlog_enable) ts.markLogEnabled();
            
            if (hstore_conf.site.exec_profiling) this.profiler.network_time.start();
            this.hstore_site.responseSend(ts, cresponse);
            if (hstore_conf.site.exec_profiling) this.profiler.network_time.stopIfStarted();
            this.hstore_site.queueDeleteTransaction(ts.getTransactionId(), status);
        } 
        // -------------------------------
        // COMMIT: Distributed Transaction
        // -------------------------------
        else if (status == Status.OK) {
            // We need to set the new ExecutionMode before we invoke transactionPrepare
            // because the LocalTransaction handle might get cleaned up immediately
            ExecutionMode newMode = null;
            if (hstore_conf.site.specexec_enable) {
                newMode = (ts.isExecReadOnly(this.partitionId) ? ExecutionMode.COMMIT_READONLY :
                                                                 ExecutionMode.COMMIT_NONE);
            } else {
                newMode = ExecutionMode.DISABLED;
            }
            this.setExecutionMode(ts, newMode);

            if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startPostPrepare();
            if (hstore_conf.site.exec_profiling) {
                this.profiler.network_time.start();
                this.profiler.sp3_local_time.start();
            }
            
            // We will send a prepare message to all of our remote HStoreSites
            // The coordinator needs to be smart enough to know whether a txn 
            // has already been marked as prepared at a partition (i.e., it's gotten
            // responses).
            PartitionSet partitions = ts.getPredictTouchedPartitions();
            LocalPrepareCallback callback = ts.getPrepareCallback();
            this.hstore_coordinator.transactionPrepare(ts, callback, partitions);
            if (hstore_conf.site.exec_profiling) this.profiler.network_time.stopIfStarted();
            ts.getDonePartitions().addAll(partitions);
        }
        // -------------------------------
        // ABORT: Distributed Transaction
        // -------------------------------
        else {
            // Send back the result to the client right now, since there's no way 
            // that we're magically going to be able to recover this and get them a result
            // This has to come before the network messages above because this will clean-up the 
            // LocalTransaction state information
            this.hstore_site.responseSend(ts, cresponse);
            
            // Send a message all the partitions involved that the party is over
            // and that they need to abort the transaction. We don't actually care when we get the
            // results back because we'll start working on new txns right away.
            // Note that when we call transactionFinish() right here this thread will then go on 
            // to invoke HStoreSite.transactionFinish() for us. That means when it returns we will
            // have successfully aborted the txn at least at all of the local partitions at this site.
            if (hstore_conf.site.txn_profiling && ts.profiler != null) ts.profiler.startPostFinish();
            LocalFinishCallback callback = ts.getFinishCallback();
            callback.init(ts, status);
            if (hstore_conf.site.exec_profiling) this.profiler.network_time.start();
            try {
                this.hstore_coordinator.transactionFinish(ts, status, callback);
            } finally {
                if (hstore_conf.site.exec_profiling) this.profiler.network_time.stopIfStarted();
            }
        }
    }
    
    /**
     * Enable speculative execution mode for this partition. The given transaction is 
     * the one that we will need to wait to finish before we can release the ClientResponses 
     * for any speculatively executed transactions. 
     * @param txn_id
     * @return true if speculative execution was enabled at this partition
     */
    private Status prepareTransaction(AbstractTransaction ts,
                                      PartitionCountingCallback<? extends AbstractTransaction> callback) {
        assert(ts != null) :
            "Unexpected null transaction handle at partition " + this.partitionId;
        assert(ts.isInitialized()) :
            String.format("Trying to prepare uninitialized transaction %s at partition %d", ts, this.partitionId);
        assert(ts.isMarkedFinished(this.partitionId) == false) :
            String.format("Trying to prepare %s again after it was already finished at partition %d", ts, this.partitionId);
        
        Status status = Status.OK; 
        
        // Skip if we've already invoked prepared for this txn at this partition
        if (ts.isMarkedPrepared(this.partitionId) == false) {
            if (debug.val)
                LOG.debug(String.format("%s - Preparing to commit txn at partition %d [specBlocked=%d]",
                          ts, this.partitionId, this.specExecBlocked.size()));
            
            ExecutionMode newMode = ExecutionMode.COMMIT_NONE;
            
            if (hstore_conf.site.exec_profiling && 
                   this.partitionId != ts.getBasePartition() &&
                   ts.needsFinish(this.partitionId)) {
                profiler.sp3_remote_time.start();
            }
            
            if (hstore_conf.site.specexec_enable) {
                // Check to see if there were any conflicts with the dtxn and any of its speculative
                // txns at this partition. If there were, then we know that we can't commit the txn here.
                if (this.specExecSkipAfter == false) {
                    for (LocalTransaction spec_ts : this.specExecBlocked) {
                        // Check whether we can quickly ignore this speculative txn because
                        // it was executed at a stall point where conflicts don't matter.
                        SpeculationType specType = spec_ts.getSpeculationType();
                        if (specType != SpeculationType.SP3_REMOTE_AFTER && specType != SpeculationType.SP2_LOCAL) {
                            continue;
                        }
                        
                        if (debug.val)
                            LOG.debug(String.format("%s - Checking for conflicts with speculative %s at partition %d [%s]",
                                      ts, spec_ts, this.partitionId,
                                      this.specExecChecker.getClass().getSimpleName()));
                        if (this.specExecChecker.hasConflictAfter(ts, spec_ts, this.partitionId)) {
                            if (debug.val)
                                LOG.debug(String.format("%s - Conflict found with speculative txn %s at partition %d",
                                          ts, spec_ts, this.partitionId));
                            status = Status.ABORT_RESTART;
                            break;
                        }
                    } // FOR
                }
                // Check whether the txn that we're waiting for is read-only.
                // If it is, then that means all read-only transactions can commit right away
                if (status == Status.OK && ts.isExecReadOnly(this.partitionId)) {
                    if (debug.val)
                        LOG.debug(String.format("%s - Txn is read-only at partition %d [readOnly=%s]",
                                  ts, this.partitionId, ts.isExecReadOnly(this.partitionId)));
                    newMode = ExecutionMode.COMMIT_READONLY;
                }
            }
            if (this.currentDtxn != null) this.setExecutionMode(ts, newMode);
        }
        // It's ok if they try to prepare the txn twice. That might just mean that they never
        // got the acknowledgement back in time if they tried to send an early commit message.
        else if (debug.val) {
            LOG.debug(String.format("%s - Already marked 2PC:PREPARE at partition %d", ts, this.partitionId));
        }

        // IMPORTANT
        // When we do an early 2PC-PREPARE, we won't have this callback ready
        // because we don't know what callback to use to send the acknowledgements
        // back over the network
        if (status == Status.OK) {
            if (callback.isInitialized()) {
                try {
                    callback.run(this.partitionId);
                } catch (Throwable ex) {
                    LOG.warn("Unexpected error for " + ts, ex);
                }
            }
            // But we will always mark ourselves as prepared at this partition
            ts.markPrepared(this.partitionId);
        } else {
            if (debug.val)
                LOG.debug(String.format("%s - Aborting txn from partition %d [%s]",
                          ts, this.partitionId, status));
            callback.abort(this.partitionId, status);
        }
        
        return (status);
    }
        
    /**
     * Internal call to abort/commit the transaction down in the execution engine
     * @param ts
     * @param commit
     */
    private void finishTransaction(AbstractTransaction ts, Status status) {
        assert(ts != null) :
            "Unexpected null transaction handle at partition " + this.partitionId;
        assert(ts.isInitialized()) :
            String.format("Trying to commit uninitialized transaction %s at partition %d", ts, this.partitionId);
        assert(ts.isMarkedFinished(this.partitionId) == false) :
            String.format("Trying to commit %s twice at partition %d", ts, this.partitionId);
        
        // Figure out what undoToken we need to process. This can be null if they haven't
        // submitted any work to the EE at this partition.
        // The logic for what we're doing is as follows:
        // (1) If we are committing, then we want the *last* undoToken because that
        //     will automatically commit everything up to that token (i.e., all the earlier
        //     tokens used by the txn.
        // (2) If we are aborting, then we want the *first* undo token
        //     because that will automatically rollback all of the tokens used by the txn
        //     that came after it.
        boolean commit = (status == Status.OK);
        long undoToken = (commit ? ts.getLastUndoToken(this.partitionId) :
                                   ts.getFirstUndoToken(this.partitionId));
        
        // Only commit/abort this transaction if:
        //  (2) We have the last undo token used by this transaction
        //  (3) The transaction was executed with undo buffers
        //  (4) The transaction actually submitted work to the EE
        //  (5) The transaction modified data at this partition
        if (ts.needsFinish(this.partitionId) && undoToken != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
            if (trace.val)
                LOG.trace(String.format("%s - Invoking EE to finish work for txn [%s / speculative=%s]",
                          ts, status, ts.isSpeculative()));
            this.finishWorkEE(ts, undoToken, commit);
        }
        
        // We always need to do the following things regardless if we hit up the EE or not
        if (commit) this.lastCommittedTxnId = ts.getTransactionId();
        
        if (trace.val)
            LOG.trace(String.format("%s - Telling queue manager that txn is finished at partition %d",
                      ts, this.partitionId));
        this.queueManager.lockQueueFinished(ts, status, this.partitionId);
        
        if (debug.val)
            LOG.debug(String.format("%s - Successfully %sed transaction at partition %d",
                      ts, (commit ? "committ" : "abort"), this.partitionId));
        this.markTransactionFinished(ts);

    }
    
    /**
     * The real method that actually reaches down into the EE and commits/undos the changes 
     * for the given token.
     * Unless you know what you're doing, you probably want to be calling finishTransaction()
     * instead of calling this directly.
     * @param ts
     * @param undoToken
     * @param commit If true, then this txn will be committed. If false, the txn will be aborted.
     */
    private void finishWorkEE(AbstractTransaction ts, long undoToken, boolean commit) {
        assert(ts.isMarkedFinished(this.partitionId) == false) :
            String.format("Trying to commit %s twice at partition %d", ts, this.partitionId);
        
        // If the txn is completely read-only and they didn't use undo-logging, then
        // there is nothing that we need to do, except to check to make sure we aren't
        // trying to abort this txn
        if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
            // SANITY CHECK: Make sure that they're not trying to undo a transaction that
            // modified the database but did not use undo logging
            if (ts.isExecReadOnly(this.partitionId) == false && commit == false) {
                String msg = String.format("TRYING TO ABORT TRANSACTION ON PARTITION %d WITHOUT UNDO LOGGING [undoToken=%d]",
                                           this.partitionId, undoToken); 
                LOG.fatal(msg + "\n" + ts.debug());
                this.crash(new ServerFaultException(msg, ts.getTransactionId()));
            }
            if (debug.val) LOG.debug(String.format("%s - undoToken == DISABLE_UNDO_LOGGING_TOKEN", ts));
        }
        // COMMIT / ABORT
        else {
            boolean needs_profiling = false;
            if (hstore_conf.site.txn_profiling && ts.isExecLocal(this.partitionId) && ((LocalTransaction)ts).profiler != null) {
                needs_profiling = true;
                ((LocalTransaction)ts).profiler.startPostEE();
            }
            assert(this.lastCommittedUndoToken != undoToken) :
                String.format("Trying to %s undoToken %d for %s twice at partition %d",
                              (commit ? "COMMIT" : "ABORT"), undoToken, ts, this.partitionId);
            
            // COMMIT!
            if (commit) {
                if (debug.val) {
                    LOG.debug(String.format("%s - COMMITING txn on partition %d with undoToken %d " +
                              "[lastTxnId=%d, lastUndoToken=%d, dtxn=%s]%s",
                              ts, this.partitionId, undoToken,
                              this.lastCommittedTxnId, this.lastCommittedUndoToken, this.currentDtxn,
                              (ts instanceof LocalTransaction ? " - " + ((LocalTransaction)ts).getSpeculationType() : "")));
                    if (this.specExecBlocked.isEmpty() == false && ts.isPredictSinglePartition() == false) {
                        LOG.debug(String.format("%s - # of Speculatively Executed Txns: %d ", ts, this.specExecBlocked.size()));
                    }
                }
                    
                assert(this.lastCommittedUndoToken < undoToken) :
                    String.format("Trying to commit undoToken %d for %s but it is less than the " +
                                  "last committed undoToken %d at partition %d\n" +
                                  "Last Committed Txn: %d",
                                  undoToken, ts, this.lastCommittedUndoToken, this.partitionId,
                                  this.lastCommittedTxnId);
                this.ee.releaseUndoToken(undoToken);
                this.lastCommittedUndoToken = undoToken;
            }
            // ABORT!
            else {
                // Evan says that txns will be aborted LIFO. This means the first txn that
                // we get in abortWork() will have a the greatest undoToken, which means that 
                // it will automagically rollback all other outstanding txns.
                // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
                // we should be able to check whether our undoToken has already been rolled back
                if (debug.val) {
                    LOG.debug(String.format("%s - ABORTING txn on partition %d with undoToken %d " +
                              "[lastTxnId=%d, lastUndoToken=%d, dtxn=%s]%s",
                              ts, this.partitionId, undoToken,
                              this.lastCommittedTxnId, this.lastCommittedUndoToken, this.currentDtxn,
                              (ts instanceof LocalTransaction ? " - " + ((LocalTransaction)ts).getSpeculationType() : "")));
                    if (this.specExecBlocked.isEmpty() == false && ts.isPredictSinglePartition() == false) {
                        LOG.debug(String.format("%s - # of Speculatively Executed Txns: %d ", ts, this.specExecBlocked.size()));
                    }
                }
                assert(this.lastCommittedUndoToken < undoToken) :
                    String.format("Trying to abort undoToken %d for %s but it is less than the " +
                                  "last committed undoToken %d at partition %d " +
                                  "[lastTxnId=%d, lastUndoToken=%d, dtxn=%s]%s",
                                  undoToken, ts, this.lastCommittedUndoToken, this.partitionId,
                                  this.lastCommittedTxnId, this.lastCommittedUndoToken, this.currentDtxn,
                                  (ts instanceof LocalTransaction ? " - " + ((LocalTransaction)ts).getSpeculationType() : ""));
                this.ee.undoUndoToken(undoToken);
            }
            if (needs_profiling) ((LocalTransaction)ts).profiler.stopPostEE();
        }
    }
    
    /**
     * Somebody told us that our partition needs to abort/commit the given transaction id.
     * This method should only be used for distributed transactions, because
     * it will do some extra work for speculative execution
     * @param ts - The transaction to finish up.
     * @param status - The final status of the transaction
     */
    private void finishDistributedTransaction(final AbstractTransaction ts, final Status status) {
        if (debug.val)
            LOG.debug(String.format("%s - Processing finish request at partition %d " +
                      "[status=%s, readOnly=%s]",
                      ts, this.partitionId,
                      status, ts.isExecReadOnly(this.partitionId)));
        if (this.currentDtxn == ts) {
            // 2012-11-22 -- Yes, today is Thanksgiving and I'm working on my database.
            // That's just grad student life I guess. Anyway, if you're reading this then 
            // you know that this is an important part of the system. We have a dtxn that 
            // we have been told is completely finished and now we need to either commit 
            // or abort any changes that it may have made at this partition. The tricky thing 
            // is that if we have speculative execution enabled, then we need to make sure
            // that we process any transactions that were executed while the dtxn was running
            // in the right order to ensure that we maintain serializability.
            // Here is the basic logic of what's about to happen:
            // 
            //  (1) If the dtxn is commiting, then we just need to commit the the last txn that 
            //      was executed (since this will have the largest undo token).
            //      The EE will automatically commit all undo tokens less than that.
            //  (2) If the dtxn is aborting, then we can commit any speculative txn that was 
            //      executed before the dtxn's first non-readonly undo token.
            //  
            //  Note that none of the speculative txns in the blocked queue will need to be
            //  aborted at this point, because we will have rolled back their changes immediately 
            //  when they aborted, so that our dtxn doesn't read dirty data.  
            if (this.specExecBlocked.isEmpty() == false) {
                // First thing we need to do is get the latch that will be set by any transaction
                // that was in the middle of being executed when we were called
                if (debug.val)
                    LOG.debug(String.format("%s - Checking %d blocked speculative transactions at " +
                              "partition %d [currentMode=%s]",
                              ts, this.specExecBlocked.size(), this.partitionId, this.currentExecMode));
                
                // -------------------------------
                // DTXN NON-READ-ONLY ABORT
                // If the dtxn did not modify this partition, then everthing can commit 
                // Otherwise, we want to commit anything that was executed before the dtxn started
                // -------------------------------
                if (status != Status.OK && ts.isExecReadOnly(this.partitionId) == false) {
                    // We need to get the first undo tokens for our distributed transaction
                    long dtxnUndoToken = ts.getFirstUndoToken(this.partitionId);
                    if (debug.val)
                        LOG.debug(String.format("%s - Looking for speculative txns to commit before we rollback undoToken %d",
                                  ts, dtxnUndoToken));
                    
                    // Queue of speculative txns that should be committed/aborted either 
                    // *before* or *after* we abort the distributed transaction
                    Collection<AbstractTransaction> allTxns = new TreeSet<AbstractTransaction>(this.specExecComparator);
                    allTxns.addAll(this.specExecBlocked);
                    allTxns.add(ts);
                    final List<LocalTransaction> toCommit = new ArrayList<LocalTransaction>();
                    final List<LocalTransaction> toAbortBefore = new ArrayList<LocalTransaction>();
                    final List<LocalTransaction> toAbortAfter = new ArrayList<LocalTransaction>();
                    
                    // Go through once and figure out which txns we need to abort
                    // We have to do this first because if we abort our dtxn then we
                    // could lose its read/write tracking set if we're using OCC
                    boolean useAfterQueue = true;
                    for (AbstractTransaction next : allTxns) {
                        if (ts == next) {
                            useAfterQueue = false;
                            continue;
                        }
                        // Otherwise it's as speculative txn.
                        // Let's figure out what we need to do with it.
                        LocalTransaction spec_ts = (LocalTransaction)next;
                        boolean shouldCommit = false;
                        long spec_token = spec_ts.getFirstUndoToken(this.partitionId);
                        if (debug.val)
                            LOG.debug(String.format("Speculative Txn %s [undoToken=%d, %s]",
                                      spec_ts, spec_token, spec_ts.getSpeculationType()));
                        
                        // Speculative txns should never be executed without an undo token
                        assert(spec_token != HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN);
                        assert(spec_ts.isSpeculative()) : spec_ts + " is not marked as speculative!";
                        
                        // If the speculative undoToken is null, then this txn didn't execute
                        // any queries. That means we can always commit it
                        if (spec_token == HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
                            if (debug.val)
                                LOG.debug(String.format("Speculative Txn %s has a null undoToken at partition %d",
                                          spec_ts, this.partitionId));
                            toCommit.add(spec_ts);
                            continue;
                        }
                        
                        // Otherwise, look to see if this txn was speculatively executed before the 
                        // first undo token of the distributed txn. That means we know that this guy
                        // didn't read any modifications made by the dtxn.
                        if (spec_token < dtxnUndoToken) {
                            if (debug.val)
                                LOG.debug(String.format("Speculative Txn %s has an undoToken less than the dtxn %s " +
                                          "at partition %d [%d < %d]",
                                          spec_ts, ts, this.partitionId, spec_token, dtxnUndoToken));
                            shouldCommit = true;
                        }
                        // Ok so at this point we know that our spec txn came *after* the distributed txn
                        // started. So we need to use our checker to see whether there is a conflict
                        else if (this.specExecSkipAfter || this.specExecChecker.hasConflictAfter(ts, spec_ts, this.partitionId) == false) {
                            if (debug.val)
                                LOG.debug(String.format("Speculative Txn %s does not conflict with dtxn %s at partition %d",
                                          spec_ts, ts, this.partitionId));
                            shouldCommit = true;
                        }
                        if (useAfterQueue == false || shouldCommit == false) {
                            ClientResponseImpl spec_cr = spec_ts.getClientResponse();
                            MispredictionException error = new MispredictionException(spec_ts.getTransactionId(),
                                                                                      spec_ts.getTouchedPartitions());
                            spec_ts.setPendingError(error, false);
                            spec_cr.setStatus(Status.ABORT_SPECULATIVE);
                            (useAfterQueue ? toAbortAfter : toAbortBefore).add(spec_ts);
                        } else {
                            toCommit.add(spec_ts);
                        }
                        
                    } // FOR
                    
                    // (1) Process all of the aborting txns that need to come *before* 
                    //     we abort the dtxn
                    if (toAbortBefore.isEmpty() == false)
                        this.processClientResponseBatch(toAbortBefore, Status.ABORT_SPECULATIVE);
                    
                    // (2) Now abort the dtxn
                    this.finishTransaction(ts, status);
                    
                    // (3) Then abort all of the txn that need to come *after* we abort the dtxn
                    if (toAbortAfter.isEmpty() == false)
                        this.processClientResponseBatch(toAbortAfter, Status.ABORT_SPECULATIVE);
                    
                    // (4) Then blast out all of the txns that we want to commit
                    if (toCommit.isEmpty() == false)
                        this.processClientResponseBatch(toCommit, Status.OK);
                }
                // -------------------------------
                // DTXN READ-ONLY ABORT or DTXN COMMIT
                // -------------------------------
                else {
                    // **IMPORTANT**
                    // If the dtxn needs to commit, then all we need to do is get the 
                    // last undoToken that we've generated (since we know that it had to 
                    // have been used either by our distributed txn or for one of our 
                    // speculative txns).
                    //
                    // If the read-only dtxn needs to abort, then there's nothing we need to
                    // do, because it didn't make any changes. That means we can just
                    // commit the last speculatively executed transaction
                    //
                    // Once we have this token, we can just make a direct call to the EE
                    // to commit any changes that came before it. Note that we are using our
                    // special 'finishWorkEE' method that does not require us to provide
                    // the transaction that we're committing.
                    long undoToken = this.lastUndoToken;
                    if (debug.val)
                        LOG.debug(String.format("%s - Last undoToken at partition %d => %d",
                                  ts, this.partitionId, undoToken));
                    // Bombs away!
                    if (undoToken != this.lastCommittedUndoToken) {
                        this.finishWorkEE(ts, undoToken, true);
                        
                        // IMPORTANT: Make sure that we remove the dtxn from the lock queue!
                        // This is normally done in finishTransaction() but because we're trying
                        // to be clever and invoke the EE directly, we have to make sure that
                        // we call it ourselves.
                        this.queueManager.lockQueueFinished(ts, status, this.partitionId);
                    }
                    
                    // Make sure that we mark the dtxn as finished so that we don't
                    // try to do anything with it later on.
                    if (hstore_conf.site.exec_readwrite_tracking)
                        this.markTransactionFinished(ts);
                    else
                        ts.markFinished(this.partitionId);
                
                    // Now make sure that all of the speculative txns are processed without 
                    // committing (since we just committed any change that they could have made
                    // up above).
                    LocalTransaction spec_ts = null;
                    while ((spec_ts = this.specExecBlocked.pollFirst()) != null) {
                        ClientResponseImpl spec_cr = spec_ts.getClientResponse();
                        assert(spec_cr != null);
                        if (hstore_conf.site.exec_readwrite_tracking) 
                            this.markTransactionFinished(spec_ts);
                        else
                            spec_ts.markFinished(this.partitionId);
                            
                        try {
                            if (trace.val)
                                LOG.trace(String.format("%s - Releasing blocked ClientResponse for %s [status=%s]",
                                          ts, spec_ts, spec_cr.getStatus()));
                            this.processClientResponse(spec_ts, spec_cr);
                        } catch (Throwable ex) {
                            String msg = "Failed to complete queued response for " + spec_ts;
                            throw new ServerFaultException(msg, ex, ts.getTransactionId());
                        }
                    } // WHILE
                }
                this.specExecBlocked.clear();
                this.specExecModified = false;
                if (trace.val)
                    LOG.trace(String.format("Finished processing all queued speculative txns for dtxn %s", ts));
            }
            // -------------------------------
            // NO SPECULATIVE TXNS
            // -------------------------------
            else {
                // There are no speculative txns waiting for this dtxn, 
                // so we can just commit it right away
                if (debug.val)
                    LOG.debug(String.format("%s - No speculative txns at partition %d. Just %s txn by itself",
                              ts, this.partitionId, (status == Status.OK ? "commiting" : "aborting")));
                this.finishTransaction(ts, status);
            }
            
            // Clear our cached query results that are specific for this transaction
            // this.queryCache.purgeTransaction(ts.getTransactionId());
            
            // TODO: Remove anything in our queue for this txn
            // if (ts.hasQueuedWork(this.partitionId)) {
            // }
            
            // Check whether this is the response that the speculatively executed txns have been waiting for
            // We could have turned off speculative execution mode beforehand 
            if (debug.val)
                LOG.debug(String.format("%s - Attempting to unmark as the current DTXN at partition %d and " +
                          "setting execution mode to %s",
                          ts, this.partitionId, ExecutionMode.COMMIT_ALL));
            try {
                // Resetting the current_dtxn variable has to come *before* we change the execution mode
                this.resetCurrentDtxn();
                this.setExecutionMode(ts, ExecutionMode.COMMIT_ALL);
    
                // Release blocked transactions
                this.releaseBlockedTransactions(ts);
            } catch (Throwable ex) {
                String msg = String.format("Failed to finish %s at partition %d", ts, this.partitionId);
                throw new ServerFaultException(msg, ex, ts.getTransactionId());
            }
            
            if (hstore_conf.site.exec_profiling) {
                this.profiler.sp3_local_time.stopIfStarted();
                this.profiler.sp3_remote_time.stopIfStarted();
            }
        }
        // We were told told to finish a dtxn that is not the current one
        // at this partition. That's ok as long as it's aborting and not trying
        // to commit.
        else {
            assert(status != Status.OK) :
                String.format("Trying to commit %s at partition %d but the current dtxn is %s",
                              ts, this.partitionId, this.currentDtxn);
            this.queueManager.lockQueueFinished(ts, status, this.partitionId);
        }
        
        // -------------------------------
        // FINISH CALLBACKS
        // -------------------------------
        
        // MapReduceTransaction
        if (ts instanceof MapReduceTransaction) {
            PartitionCountingCallback<AbstractTransaction> callback = ((MapReduceTransaction)ts).getCleanupCallback();
            // We don't want to invoke this callback at the basePartition's site
            // because we don't want the parent txn to actually get deleted.
            if (this.partitionId == ts.getBasePartition()) {
                if (debug.val) 
                    LOG.debug(String.format("%s - Notifying %s that the txn is finished at partition %d",
                              ts, callback.getClass().getSimpleName(), this.partitionId));
                callback.run(this.partitionId);
            }
        }
        else {
            PartitionCountingCallback<AbstractTransaction> callback = ts.getFinishCallback();
            if (debug.val)
                LOG.debug(String.format("%s - Notifying %s that the txn is finished at partition %d",
                          ts, callback.getClass().getSimpleName(), this.partitionId));
            callback.run(this.partitionId);
        }
    }
    
    /**
     * Mark a transaction as being finished at this partition and clear out
     * any internal tracking stuff that we may have down in the EE.
     * @param ts
     */
    private void markTransactionFinished(AbstractTransaction ts) {
        if (hstore_conf.site.exec_readwrite_tracking && ts.hasExecutedWork(this.partitionId)) {
            this.ee.trackingFinish(ts.getTransactionId());
        }
        ts.markFinished(this.partitionId);
    }
    
    /**
     * Process a batch of completed txns. The first txn in the batch will be
     * committed/aborted in the EE. The rest will be marked as finished. 
     * @param batch
     * @param status
     */
    private void processClientResponseBatch(Collection<LocalTransaction> batch, Status status) {
        // Only processs the last txn in the list, since it will have the
        // the greatest undo token value.
        LocalTransaction targetTxn = null;
        if (status == Status.OK) {
            targetTxn = CollectionUtil.last(batch);
        } else {
            targetTxn = CollectionUtil.first(batch);
        }
        assert(targetTxn != null);
        long lastUndoToken = targetTxn.getFirstUndoToken(this.partitionId);
        this.finishWorkEE(targetTxn, lastUndoToken, (status == Status.OK));
        
        for (LocalTransaction ts : batch) {
            // Marking the txn as finished will prevent us from going down
            // into the EE to finish up the transaction.
            if (hstore_conf.site.exec_readwrite_tracking)
                this.markTransactionFinished(ts);
            else
                ts.markFinished(this.partitionId);
            
            // Send out the ClientResponse to whomever wants it!
            if (debug.val)
                LOG.debug(String.format("%s - Releasing blocked ClientResponse for %s [status=%s]",
                          ts, ts, ts.getClientResponse().getStatus()));
            try {
                this.processClientResponse(ts, ts.getClientResponse());
            } catch (Throwable ex) {
                String msg = "Failed to complete queued response for " + ts;
                throw new ServerFaultException(msg, ex, ts.getTransactionId());
            }
        } // FOR
    }
    
    private void blockTransaction(InternalTxnMessage work) {
        if (debug.val)
            LOG.debug(String.format("%s - Adding %s work to blocked queue",
                      work.getTransaction(), work.getClass().getSimpleName()));
        assert(this.currentDtxn != null) :
            String.format("Trying to block %s for %s at partition %d but the current dtxn is null",
                          work, work.getTransaction(), this.partitionId);
        assert(this.currentDtxn != work.getTransaction()) :
            String.format("Trying to block %s for %s at partition %d but it is the current dtxn",
                          work, work.getTransaction(), this.partitionId);
        this.currentBlockedTxns.add(work);
    }
    
    private void blockTransaction(LocalTransaction ts) {
        this.blockTransaction(new StartTxnMessage(ts));
    }

    /**
     * Release all the transactions that are currently in this partition's blocked queue 
     * into the work queue.
     * @param ts
     */
    private void releaseBlockedTransactions(AbstractTransaction ts) {
        if (this.currentBlockedTxns.isEmpty() == false) {
            if (debug.val)
                LOG.debug(String.format("Attempting to release %d blocked transactions at partition %d because of %s",
                          this.currentBlockedTxns.size(), this.partitionId, ts));
            this.work_queue.addAll(this.currentBlockedTxns);
            int released = this.currentBlockedTxns.size();
            this.currentBlockedTxns.clear();
            if (debug.val) LOG.debug(String.format("Released %d blocked transactions at partition %d because of %s",
                                           released, this.partitionId, ts));
        }
        assert(this.currentBlockedTxns.isEmpty());
    }
    
    // ---------------------------------------------------------------
    // SNAPSHOT METHODS
    // ---------------------------------------------------------------
    
    /**
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    public void initiateSnapshots(Deque<SnapshotTableTask> tasks) {
        m_snapshotter.initiateSnapshots(ee, tasks);
    }

    public Collection<Exception> completeSnapshotWork() throws InterruptedException {
        LOG.warn("completeSnapshotWork at partition :"+this.getPartitionId());
        return m_snapshotter.completeSnapshotWork(ee);
    }    
    
    
    // ---------------------------------------------------------------
    // SHUTDOWN METHODS
    // ---------------------------------------------------------------
    
    /**
     * Cause this PartitionExecutor to make the entire HStore cluster shutdown
     * This won't return!
     */
    public synchronized void crash(Throwable ex) {
        String msg = String.format("PartitionExecutor for Partition #%d is crashing", this.partitionId);
        if (ex == null) LOG.warn(msg);
        else LOG.warn(msg, ex);
        
        assert(this.hstore_coordinator != null);
        this.hstore_coordinator.shutdownClusterBlocking(ex);
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.hstore_site.isShuttingDown()); // shutdown_state == State.PREPARE_SHUTDOWN || this.shutdown_state == State.SHUTDOWN);
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        this.shutdown_state = ShutdownState.PREPARE_SHUTDOWN;
    }
    
    /**
     * Somebody from the outside wants us to shutdown
     */
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (debug.val) LOG.debug(String.format("Partition #%d told to shutdown again. Ignoring...", this.partitionId));
            return;
        }
        this.shutdown_state = ShutdownState.SHUTDOWN;
        
        if (debug.val) LOG.debug(String.format("Shutting down PartitionExecutor for Partition #%d", this.partitionId));
        
        // Clear the queue
        this.work_queue.clear();
        
        // Knock out this ma
        if (this.m_snapshotter != null) this.m_snapshotter.shutdown();
        
        // Make sure we shutdown our threadpool
        // this.thread_pool.shutdownNow();
        if (this.self != null) this.self.interrupt();
        
        if (this.shutdown_latch != null) {
            try {
                this.shutdown_latch.acquire();
            } catch (InterruptedException ex) {
                // Ignore
            } catch (Exception ex) {
                LOG.fatal("Unexpected error while shutting down", ex);
            }
        }
    }
    
    // ----------------------------------------------------------------------------
    // DEBUG METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public String toString() {
        return String.format("%s{%s}", this.getClass().getSimpleName(),
                                       HStoreThreadManager.formatPartitionName(siteId, partitionId));
    }
    
    public class Debug implements DebugContext {
        public VoltProcedure getVoltProcedure(String procName) {
            Procedure proc = catalogContext.procedures.getIgnoreCase(procName);
            return (PartitionExecutor.this.getVoltProcedure(proc.getId()));
        }
        public SpecExecScheduler getSpecExecScheduler() {
            return (PartitionExecutor.this.specExecScheduler);
        }
        public AbstractConflictChecker getSpecExecConflictChecker() {
            return (PartitionExecutor.this.specExecChecker);
        }
        public Collection<BatchPlanner> getBatchPlanners() {
            return (PartitionExecutor.this.batchPlanners.values());
        }
        public PartitionExecutorProfiler getProfiler() {
            return (PartitionExecutor.this.profiler);
        }
        public Thread getExecutionThread() {
            return (PartitionExecutor.this.self);
        }
        public Queue<InternalMessage> getWorkQueue() {
            return (PartitionExecutor.this.work_queue);
        }
        public void setExecutionMode(AbstractTransaction ts, ExecutionMode newMode) {
            PartitionExecutor.this.setExecutionMode(ts, newMode);
        }
        public ExecutionMode getExecutionMode() {
            return (PartitionExecutor.this.currentExecMode);
        }
        public Long getLastExecutedTxnId() {
            return (PartitionExecutor.this.lastExecutedTxnId);
        }
        public Long getLastCommittedTxnId() {
            return (PartitionExecutor.this.lastCommittedTxnId);
        }
        public long getLastCommittedIndoToken() {
            return (PartitionExecutor.this.lastCommittedUndoToken);
        }
        /**
         * Get the VoltProcedure handle of the current running txn. This could be null.
         * <B>FOR TESTING ONLY</B> 
         */
        public VoltProcedure getCurrentVoltProcedure() {
            return (PartitionExecutor.this.currentVoltProc);
        }
        /**
         * Get the txnId of the current distributed transaction at this partition
         * <B>FOR TESTING ONLY</B> 
         */
        public AbstractTransaction getCurrentDtxn() {
            return (PartitionExecutor.this.currentDtxn);
        }
        /**
         * Get the txnId of the current distributed transaction at this partition
         * <B>FOR TESTING ONLY</B>
         */
        public Long getCurrentDtxnId() {
            Long ret = null;
            // This is a race condition, so we'll just ignore any errors
            if (PartitionExecutor.this.currentDtxn != null) { 
                try {
                    ret = PartitionExecutor.this.currentDtxn.getTransactionId();
                } catch (NullPointerException ex) {
                    // IGNORE
                }
            } 
            return (ret);
        }
        public Long getCurrentTxnId() {
            return (PartitionExecutor.this.currentTxnId);
        }
        public int getBlockedWorkCount() {
            return (PartitionExecutor.this.currentBlockedTxns.size());
        }
        /**
         * Return the number of spec exec txns have completed but are waiting
         * for the distributed txn to finish at this partition
         */
        public int getBlockedSpecExecCount() {
            return (PartitionExecutor.this.specExecBlocked.size());
        }
        public int getWorkQueueSize() {
            return (PartitionExecutor.this.work_queue.size());
        }
        public void updateMemory() {
            PartitionExecutor.this.updateMemoryStats(EstTime.currentTimeMillis());
        }
        /**
         * Replace the ConflictChecker. This should only be used for testing
         * @param checker
         */
        protected void setConflictChecker(AbstractConflictChecker checker) {
            LOG.warn(String.format("Replacing original checker %s with %s at partition %d",
                     specExecChecker.getClass().getSimpleName(),
                     checker.getClass().getSimpleName(),
                     partitionId));
            setSpecExecChecker(checker);
        }
    }
    
    private Debug cachedDebugContext;
    public Debug getDebugContext() {
        if (this.cachedDebugContext == null) {
            // We don't care if we're thread-safe here...
            this.cachedDebugContext = new Debug();
        }
        return this.cachedDebugContext;
    }
}
