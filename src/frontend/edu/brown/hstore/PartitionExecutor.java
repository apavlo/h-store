/***************************************************************************
 *   Copyright (C) 2012 by H-Store Project                                 *
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DependencySet;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotSiteProcessor.SnapshotTableTask;
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
import org.voltdb.messaging.FinishTaskMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.PotentialSnapshotWorkMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.WorkFragment;
import edu.brown.hstore.Hstoreservice.WorkResult;
import edu.brown.hstore.callbacks.TransactionFinishCallback;
import edu.brown.hstore.callbacks.TransactionPrepareCallback;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.dtxn.AbstractTransaction;
import edu.brown.hstore.dtxn.ExecutionState;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.hstore.dtxn.MapReduceTransaction;
import edu.brown.hstore.dtxn.RemoteTransaction;
import edu.brown.hstore.interfaces.Loggable;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.hstore.util.ArrayCache.IntArrayCache;
import edu.brown.hstore.util.ArrayCache.LongArrayCache;
import edu.brown.hstore.util.ParameterSetArrayCache;
import edu.brown.hstore.util.QueryCache;
import edu.brown.hstore.util.ThrottlingQueue;
import edu.brown.hstore.util.TransactionWorkRequestBuilder;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TypedPoolableObjectFactory;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class PartitionExecutor implements Runnable, Shutdownable, Loggable {
    private static final Logger LOG = Logger.getLogger(PartitionExecutor.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    private static boolean d;
    private static boolean t;
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
        d = debug.get();
        t = trace.get();
    }

    // ----------------------------------------------------------------------------
    // INTERNAL EXECUTION STATE
    // ----------------------------------------------------------------------------

    /**
     * The current execution mode for this PartitionExecutor
     * This defines what level of speculative execution we have enabled.
     */
    protected enum ExecutionMode {
        /** Disable processing all transactions until... **/
        DISABLED,
        /** No speculative execution. All transactions are committed immediately **/
        COMMIT_ALL,
        /** Allow read-only txns to return results. **/
        COMMIT_READONLY,
        /** All txn responses must wait until the current distributed txn is committed **/ 
        COMMIT_NONE,
    };
    
    // ----------------------------------------------------------------------------
    // GLOBAL CONSTANTS
    // ----------------------------------------------------------------------------
    
    /**
     * Create a new instance of the corresponding VoltProcedure for the given Procedure catalog object
     */
    public class VoltProcedureFactory extends TypedPoolableObjectFactory<VoltProcedure> {
        private final Procedure catalog_proc;
        private final boolean has_java;
        private final Class<? extends VoltProcedure> proc_class;
        
        @SuppressWarnings("unchecked")
        public VoltProcedureFactory(Procedure catalog_proc) {
            super(hstore_conf.site.pool_profiling);
            this.catalog_proc = catalog_proc;
            this.has_java = this.catalog_proc.getHasjava();
            
            // Only try to load the Java class file for the SP if it has one
            Class<? extends VoltProcedure> p_class = null;
            if (catalog_proc.getHasjava()) {
                final String className = catalog_proc.getClassname();
                try {
                    p_class = (Class<? extends VoltProcedure>)Class.forName(className);
                } catch (final ClassNotFoundException e) {
                    LOG.fatal("Failed to load procedure class '" + className + "'", e);
                    System.exit(1);
                }
            }
            this.proc_class = p_class;

        }
        @Override
        public VoltProcedure makeObjectImpl() throws Exception {
            VoltProcedure volt_proc = null;
            try {
                if (this.has_java) {
                    volt_proc = (VoltProcedure)this.proc_class.newInstance();
                } else {
                    volt_proc = new VoltProcedure.StmtProcedure();
                }
                volt_proc.globalInit(PartitionExecutor.this,
                               this.catalog_proc,
                               PartitionExecutor.this.backend_target,
                               PartitionExecutor.this.hsql,
                               PartitionExecutor.this.p_estimator);
            } catch (Exception e) {
                if (d) LOG.warn("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                throw e;
            }
            return (volt_proc);
        }
    };

    /**
     * Procedure Name -> VoltProcedure
     */
    private final Map<String, VoltProcedure> procedures = new HashMap<String, VoltProcedure>(16, (float) .1);
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Thread self;

    /**
     * If this flag is enabled, then we need to shut ourselves down and stop running txns
     */
    private Shutdownable.ShutdownState shutdown_state = Shutdownable.ShutdownState.INITIALIZED;
    private Semaphore shutdown_latch;
    
    /**
     * Catalog objects
     */
    protected Catalog catalog;
    protected Cluster cluster;
    protected Database database;
    protected Site site;
    protected int siteId;
    private Partition partition;
    private int partitionId;
    private Integer partitionIdObj;

    private final BackendTarget backend_target;
    private final ExecutionEngine ee;
    private final HsqlBackend hsql;
    private final DBBPool buffer_pool = new DBBPool(false, false);
    private final FastSerializer fs = new FastSerializer(this.buffer_pool);
    
    /**
     * Runtime Estimators
     */
    private final PartitionEstimator p_estimator;
    private final TransactionEstimator t_estimator;
    private EstimationThresholds thresholds;
    
    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private final SnapshotSiteProcessor m_snapshotter;
    
    // ----------------------------------------------------------------------------
    // H-Store Transaction Stuff
    // ----------------------------------------------------------------------------

    protected HStoreSite hstore_site;
    protected HStoreCoordinator hstore_coordinator;
    protected HStoreConf hstore_conf;
    
    // ----------------------------------------------------------------------------
    // Shared VoltProcedure Data Members
    // ----------------------------------------------------------------------------

    /**
     * This is the execution state for the current transaction.
     * There is only one of these per partition, so it must be cleared out for each new txn
     */
    private final ExecutionState execState;
    
    /**
     * Mapping from SQLStmt batch hash codes (computed by VoltProcedure.getBatchHashCode()) to BatchPlanners
     * The idea is that we can quickly derived the partitions for each unique set of SQLStmt list
     */
    public final Map<Integer, BatchPlanner> batchPlanners = new HashMap<Integer, BatchPlanner>(100);
    
    /**
     * Reusable cache of ParameterSet arrays
     */
    private final ParameterSetArrayCache procParameterSets;
    
    // ----------------------------------------------------------------------------
    // Internal Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * The transaction id of the current transaction
     * This is mostly used for testing and should not be relied on from the outside.
     */
    private Long currentTxnId = null;
    
    /**
     * We can only have one active distributed transactions at a time.  
     * The multi-partition TransactionState that is currently executing at this partition
     * When we get the response for these txn, we know we can commit/abort the speculatively executed transactions
     */
    private AbstractTransaction currentDtxn = null;
    
    /**
     * List of InitiateTaskMessages that are blocked waiting for the outstanding dtxn to commit
     */
    private List<VoltMessage> currentBlockedTxns = new ArrayList<VoltMessage>();

    /**
     * The current ExecutionMode. This defines when transactions are allowed to execute
     * and whether they can return their results to the client immediately or whether they
     * must wait until the current_dtxn commits.
     */
    private ExecutionMode currentExecMode = ExecutionMode.COMMIT_ALL;
    
    /**
     * The main lock used for critical areas in this PartitionExecutor
     * This should only be used sparingly.
     * Note that you do not need this lock in order to execute something in this partition's ExecutionEngine.
     */
    private final ReentrantLock exec_lock = new ReentrantLock();
    
    /**
     * ClientResponses from speculatively executed transactions that are waiting to be committed 
     */
    private final LinkedBlockingDeque<Pair<LocalTransaction, ClientResponseImpl>> queued_responses = new LinkedBlockingDeque<Pair<LocalTransaction, ClientResponseImpl>>();

    /**
     * The time in ms since epoch of the last call to ExecutionEngine.tick(...)
     */
    private long lastTickTime = 0;
    
    /**
     * The last txn id that we executed (either local or remote)
     */
    private volatile Long lastExecutedTxnId = null;
    
    /**
     * The last txn id that we committed
     */
    private volatile long lastCommittedTxnId = -1;
    
    /**
     * The last undoToken that we handed out
     */
    private long lastUndoToken = 0l;
    
    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * FragmentTaskMessage (i.e., execute some fragments on behalf of another transaction)
     */
    private final PartitionExecutorQueue work_queue = new PartitionExecutorQueue();
    
    /**
     * This is the queue for work deferred .
     */
    private final PartitionExecutorDeferredQueue deferred_queue = new PartitionExecutorDeferredQueue();
        
    /**
     * Special wrapper around the PartitionExecutorQueue that can determine whether this
     * partition is overloaded and therefore new requests should be throttled
     */
    private final ThrottlingQueue<VoltMessage> work_throttler;
    
    /**
     * 
     */
    private final QueryCache queryCache = new QueryCache(10, 10); // FIXME
    
    // ----------------------------------------------------------------------------
    // TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     */
    private final List<WorkFragment> partitionFragments = new ArrayList<WorkFragment>(); 
    
    /**
     * WorkFragments that we need to send to a remote HStoreSite for execution
     */
    private final List<WorkFragment> tmp_remoteFragmentList = new ArrayList<WorkFragment>();
    /**
     * WorkFragments that we need to send to our own PartitionExecutor
     */
    private final List<WorkFragment> tmp_localWorkFragmentList = new ArrayList<WorkFragment>();
    /**
     * WorkFragments that we need to send to a different PartitionExecutor that is on this same HStoreSite
     */
    private final List<WorkFragment> tmp_localSiteFragmentList = new ArrayList<WorkFragment>();
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
     * List of PartitionIds that need to be notified that the transaction is preparing to commit
     */
    private final List<Integer> tmp_preparePartitions = new ArrayList<Integer>();
    /**
     * Reusable ParameterSet array cache for WorkFragments
     */
    private final ParameterSetArrayCache tmp_fragmentParams;
    
    /**
     * Reusable long array for fragment ids
     */
    private final LongArrayCache tmp_fragmentIds = new LongArrayCache(10); // FIXME
    /**
     * Reusable int array for output dependency ids
     */
    private final IntArrayCache tmp_outputDepIds = new IntArrayCache(10); // FIXME
    /**
     * Reusable int array for input dependency ids
     */
    private final IntArrayCache tmp_inputDepIds = new IntArrayCache(10);
    
    // ----------------------------------------------------------------------------
    // PROFILING OBJECTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much time the PartitionExecutor was idle waiting for work to do in its queue
     */
    private final ProfileMeasurement work_idle_time = new ProfileMeasurement("EE_IDLE");
    /**
     * How much time it takes for this PartitionExecutor to execute a transaction
     */
    private final ProfileMeasurement work_exec_time = new ProfileMeasurement("EE_EXEC");
    
    // ----------------------------------------------------------------------------
    // CALLBACKS
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
            AbstractTransaction ts = hstore_site.getTransaction(txn_id);
            
            // We can ignore anything that comes in for a transaction that we don't know about
            if (ts == null) {
                if (d) LOG.debug("No transaction state exists for txn #" + txn_id);
                return;
            }
            
            if (d) LOG.debug(String.format("Processing TransactionWorkResponse for %s with %d results",
                                        ts, msg.getResultsCount()));
            for (int i = 0, cnt = msg.getResultsCount(); i < cnt; i++) {
                WorkResult result = msg.getResults(i); 
                if (t) LOG.trace(String.format("Got %s from partition %d for %s",
                                               result.getClass().getSimpleName(), result.getPartitionId(), ts));
                PartitionExecutor.this.processWorkResult((LocalTransaction)ts, result);
            } // FOR
        }
    }; // END CLASS

    // ----------------------------------------------------------------------------
    // SYSPROC STUFF
    // ----------------------------------------------------------------------------
    
    // Associate the system procedure planfragment ids to wrappers.
    // Planfragments are registered when the procedure wrapper is init()'d.
    private final HashMap<Long, VoltSystemProcedure> m_registeredSysProcPlanFragments = new HashMap<Long, VoltSystemProcedure>();

    public void registerPlanFragment(final long pfId, final VoltSystemProcedure proc) {
        synchronized (m_registeredSysProcPlanFragments) {
            if (!m_registeredSysProcPlanFragments.containsKey(pfId)) {
                assert(m_registeredSysProcPlanFragments.containsKey(pfId) == false) : "Trying to register the same sysproc more than once: " + pfId;
                m_registeredSysProcPlanFragments.put(pfId, proc);
                LOG.trace("Registered @" + proc.getClass().getSimpleName() + " sysproc handle for FragmentId #" + pfId);
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
        public long getNextUndo();
        public PartitionExecutor getExecutionSite();
        public Long getCurrentTxnId();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Catalog getCatalog()                 { return catalog; }
        public Database getDatabase()               { return cluster.getDatabases().get("database"); }
        public Cluster getCluster()                 { return cluster; }
        public Site getSite()                       { return site; }
        public Host getHost()                       { return PartitionExecutor.this.getHost(); }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return PartitionExecutor.this.getLastCommittedTxnId(); }
        public long getNextUndo()                   { return getNextUndoToken(); }
        public PartitionExecutor getExecutionSite() { return PartitionExecutor.this; }
        public Long getCurrentTxnId()               { return PartitionExecutor.this.currentTxnId; }
    }

    private final SystemProcedureContext m_systemProcedureContext = new SystemProcedureContext();

    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------

    /**
     * Dummy constructor...
     */
    protected PartitionExecutor() {
        this.work_throttler = null;
        this.ee = null;
        this.hsql = null;
        this.p_estimator = null;
        this.t_estimator = null;
        this.m_snapshotter = null;
        this.thresholds = null;
        this.catalog = null;
        this.cluster = null;
        this.site = null;
        this.database = null;
        this.backend_target = BackendTarget.HSQLDB_BACKEND;
        this.siteId = 0;
        this.partitionId = 0;
        this.execState = null;
        this.procParameterSets = null;
        this.tmp_fragmentParams = null;
        this.tmp_transactionRequestBuilders = null;
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
    public PartitionExecutor(final int partitionId, final Catalog catalog, final BackendTarget target, PartitionEstimator p_estimator, TransactionEstimator t_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        
        this.work_throttler = new ThrottlingQueue<VoltMessage>(
                this.work_queue,
                hstore_conf.site.queue_incoming_max_per_partition,
                hstore_conf.site.queue_incoming_release_factor,
                hstore_conf.site.queue_incoming_increase,
                hstore_conf.site.queue_incoming_increase_max
        );
        
        this.catalog = catalog;
        this.partition = CatalogUtil.getPartitionById(this.catalog, partitionId);
        assert(this.partition != null) : "Invalid Partition #" + partitionId;
        this.partitionId = this.partition.getId();
        this.partitionIdObj = Integer.valueOf(this.partitionId);
        this.site = this.partition.getParent();
        assert(site != null) : "Unable to get Site for Partition #" + partitionId;
        this.siteId = this.site.getId();
        
        this.execState = new ExecutionState(this);
        
        this.backend_target = target;
        this.cluster = CatalogUtil.getCluster(catalog);
        this.database = CatalogUtil.getDatabase(cluster);

        // The PartitionEstimator is what we use to figure our where our transactions are going to go
        this.p_estimator = p_estimator; // t_estimator.getPartitionEstimator();
        
        // The TransactionEstimator is the runtime piece that we use to keep track of where the 
        // transaction is in its execution workflow. This allows us to make predictions about
        // what kind of things we expect the xact to do in the future
        if (t_estimator == null) { // HACK
            this.t_estimator = new TransactionEstimator(partitionId, p_estimator);    
        } else {
            this.t_estimator = t_estimator; 
        }
        
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
            if (d) LOG.debug("Creating EE wrapper with target type '" + target + "'");
            if (this.backend_target == BackendTarget.HSQLDB_BACKEND) {
                hsqlTemp = new HsqlBackend(partitionId);
                final String hexDDL = database.getSchema();
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
                eeTemp = new ExecutionEngineJNI(this, cluster.getRelativeIndex(), this.getSiteId(), this.getPartitionId(), this.getHostId(), "localhost");
                eeTemp.loadCatalog(catalog.serialize());
                lastTickTime = System.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
                
                snapshotter = new SnapshotSiteProcessor(new Runnable() {
                    final PotentialSnapshotWorkMessage msg = new PotentialSnapshotWorkMessage();
                    @Override
                    public void run() {
                        PartitionExecutor.this.work_queue.add(msg);
                    }
                });
            }
            else {
                // set up the EE over IPC
                eeTemp = new ExecutionEngineIPC(this, cluster.getRelativeIndex(), this.getSiteId(), this.getPartitionId(), this.getHostId(), "localhost", target);
                eeTemp.loadCatalog(catalog.serialize());
                lastTickTime = System.currentTimeMillis();
                eeTemp.tick( lastTickTime, 0);
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
        
        // ParameterSet Array Caches
        this.procParameterSets = new ParameterSetArrayCache(10);
        this.tmp_fragmentParams = new ParameterSetArrayCache(5);

        // Initialize temporary data structures
        int num_sites = CatalogUtil.getNumberOfSites(this.catalog);
        this.tmp_transactionRequestBuilders = new TransactionWorkRequestBuilder[num_sites];
        
        
    }
    
    @SuppressWarnings("unchecked")
    protected void initializeVoltProcedures() {
        // load up all the stored procedures
        for (final Procedure catalog_proc : database.getProcedures()) {
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
            volt_proc.globalInit(PartitionExecutor.this,
                                 catalog_proc,
                                 this.backend_target,
                                 this.hsql,
                                 this.p_estimator);
            this.procedures.put(catalog_proc.getName(), volt_proc);
        } // FOR
    }

    /**
     * Link this PartitionExecutor with its parent HStoreSite
     * This will initialize the references the various components shared among the PartitionExecutors 
     * @param hstore_site
     */
    public void initHStoreSite(HStoreSite hstore_site) {
        if (t) LOG.trace(String.format("Initializing HStoreSite components at partition %d", this.partitionId));
        assert(this.hstore_site == null);
        this.hstore_site = hstore_site;
        this.hstore_coordinator = hstore_site.getHStoreCoordinator();
        this.thresholds = (hstore_site != null ? hstore_site.getThresholds() : null);
        
        if (hstore_conf.site.exec_profiling) {
            EventObservable<AbstractTransaction> eo = this.hstore_site.getStartWorkloadObservable();
            this.work_idle_time.resetOnEvent(eo);
            this.work_exec_time.resetOnEvent(eo);
        }
        
        this.initializeVoltProcedures();
    }
    
    // ----------------------------------------------------------------------------
    // MAIN EXECUTION LOOP
    // ----------------------------------------------------------------------------
    
    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        assert(this.hstore_site != null);
        assert(this.hstore_coordinator != null);
        assert(this.self == null);
        this.self = Thread.currentThread();
        this.self.setName(HStoreThreadManager.getThreadName(this.hstore_site, this.partitionId));
        
        if (hstore_conf.site.cpu_affinity) {
            this.hstore_site.getThreadManager().registerEEThread(partition);
        }
        
        // *********************************** DEBUG ***********************************
        if (hstore_conf.site.exec_validate_work) {
            LOG.warn("Enabled Distributed Transaction Checking");
        }
        // *********************************** DEBUG ***********************************
        
        // Things that we will need in the loop below
        AbstractTransaction current_txn = null;
        VoltMessage work = null;
        boolean stop = false;
        
        try {
            // Setup shutdown lock
            this.shutdown_latch = new Semaphore(0);
            
            if (d) LOG.debug("Starting PartitionExecutor run loop...");
            while (stop == false && this.isShuttingDown() == false) {
                this.currentTxnId = null;
                work = null;
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                try {
                    work = this.work_queue.poll();
                    if (work == null) {
                        // See if there is anything that we can do while we wait
                        // XXX this.utilityWork(null);
                        
                        if (t) LOG.trace("Partition " + this.partitionId + " queue is empty. Waiting...");
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.start();
                        work = this.work_queue.take();
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.stop();
                    }
                } catch (InterruptedException ex) {
                    if (d && this.isShuttingDown() == false) 
                        LOG.debug("Unexpected interuption while polling work queue. Halting PartitionExecutor...", ex);
                    stop = true;
                    break;
                }
                
                // -------------------------------
                // Transactional Work
                // -------------------------------
                if (work instanceof TransactionInfoBaseMessage) {
                    this.currentTxnId = ((TransactionInfoBaseMessage)work).getTxnId();
                    current_txn = hstore_site.getTransaction(this.currentTxnId);
                    if (current_txn == null) {
                        String msg = String.format("No transaction state for txn #%d [%s]",
                                                   this.currentTxnId, work.getClass().getSimpleName());
                        LOG.error(msg + "\n" + work.toString());
                        throw new ServerFaultException(msg, this.currentTxnId);
                    }
                    // If this transaction has already been aborted and they are trying to give us
                    // something that isn't a FinishTaskMessage, then we won't bother processing it
                    else if (current_txn.isAborted() && (work instanceof FinishTaskMessage) == false) {
                        if (d) LOG.debug(String.format("%s - Was marked as aborted. Will not process %s on partition %d",
                                                       current_txn, work.getClass().getSimpleName(), this.partitionId));
                        continue;
                    }
                    
                    // -------------------------------
                    // Execute Query Plan Fragments
                    // -------------------------------
                    if (work instanceof FragmentTaskMessage) {
                        FragmentTaskMessage ftask = (FragmentTaskMessage)work;
                        WorkFragment fragment = ftask.getWorkFragment();
                        assert(fragment != null);

                        // Get the ParameterSet array for this WorkFragment
                        // It can either be attached to the AbstractTransaction handle if it came
                        // over the wire directly from the txn's base partition, or it can be attached
                        // as for prefetch WorkFragments 
                        ParameterSet parameters[] = null;
                        if (fragment.getPrefetch()) {
                            parameters = current_txn.getPrefetchParameterSets();
                            current_txn.markExecPrefetchQuery(this.partitionId);
                        } else {
                            parameters = current_txn.getAttachedParameterSets();
                        }
                        parameters = this.getFragmentParameters(current_txn, fragment, parameters);
                        assert(parameters != null);
                        
                        // At this point we know that we are either the current dtxn or the current dtxn is null
                        // We will allow any read-only transaction to commit if
                        // (1) The WorkFragment for the remote txn is read-only
                        // (2) This txn has always been read-only up to this point at this partition
                        ExecutionMode newMode = null;
                        if (hstore_conf.site.exec_speculative_execution) { 
                            newMode = (fragment.getReadOnly() && current_txn.isExecReadOnly(this.partitionId) ?
                                                          ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
                        } else {
                            newMode = ExecutionMode.DISABLED;
                        }
                        exec_lock.lock();
                        try {
                            // There is no current DTXN, so that means its us!
                            if (this.currentDtxn == null) {
                                this.setCurrentDtxn(current_txn);
                                if (d) LOG.debug(String.format("Marking %s as current DTXN on partition %d [nextMode=%s]",
                                                                        current_txn, this.partitionId, newMode));                    
                            }
                            // There is a current DTXN but it's not us!
                            // That means we need to block ourselves until it finishes
                            else if (this.currentDtxn != current_txn) {
                                if (d) LOG.warn(String.format("%s - Blocking on partition %d until current Dtxn %s finishes",
                                                              current_txn, this.partitionId, this.currentDtxn));
                                this.currentBlockedTxns.add(ftask);
                                continue;
                            }
                            assert(this.currentDtxn == current_txn) :
                                String.format("Trying to execute a second Dtxn %s before the current one has finished [current=%s]",
                                              current_txn, this.currentDtxn);
                            this.setExecutionMode(current_txn, newMode);
                        } finally {
                            exec_lock.unlock();
                        } // SYNCH
                        
                        this.processWorkFragment(current_txn, fragment, parameters);
                        
                    // -------------------------------
                    // Invoke Stored Procedure
                    // -------------------------------
                    } else if (work instanceof InitiateTaskMessage) {
                        if (hstore_conf.site.exec_profiling) this.work_exec_time.start();
                        InitiateTaskMessage itask = (InitiateTaskMessage)work;
                        
                        // If this is a MapReduceTransaction handle, we actually want to get the 
                        // inner LocalTransaction handle for this partition. The MapReduceTransaction
                        // is just a placeholder
                        if (current_txn instanceof MapReduceTransaction) {
                            MapReduceTransaction orig_ts = (MapReduceTransaction)current_txn; 
                            current_txn = orig_ts.getLocalTransaction(this.partitionId);
                            assert(current_txn != null) : "Unexpected null LocalTransaction handle from " + orig_ts; 
                        }
    
                        try {
                            this.processInitiateTaskMessage((LocalTransaction)current_txn, itask);
                        } catch (Throwable ex) {
                            LOG.error(String.format("Unexpected error when executing %s\n%s", current_txn, current_txn.debug()));
                            throw ex;
                        } finally {
                            if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                        }
                        
                    // -------------------------------
                    // Finish Transaction
                    // -------------------------------
                    } else if (work instanceof FinishTaskMessage) {
                        FinishTaskMessage ftask = (FinishTaskMessage)work;
                        this.finishTransaction(current_txn, (ftask.getStatus() == Status.OK));
                    }
                
                // -------------------------------
                // PotentialSnapshotWorkMessage
                // -------------------------------
                } else if (work instanceof PotentialSnapshotWorkMessage) {
                    m_snapshotter.doSnapshotWork(ee);
                    
                // -------------------------------
                // BAD MOJO!
                // -------------------------------
                } else if (work != null) {
                    throw new ServerFaultException("Unexpected work message in queue: " + work, this.currentTxnId);
                }

                // Is there a better way to do this?
                this.work_throttler.checkThrottling(false);
                
                if (hstore_conf.site.exec_profiling && this.currentTxnId != null) {
                    this.lastExecutedTxnId = this.currentTxnId;
                    this.currentTxnId = null;
                }
            } // WHILE
        } catch (final Throwable ex) {
            if (this.isShuttingDown() == false) {
                ex.printStackTrace();
                LOG.fatal(String.format("Unexpected error for PartitionExecutor partition #%d [%s]%s",
                                        this.partitionId, (current_txn != null ? " - " + current_txn : ""), ex), ex);
                if (current_txn != null) LOG.fatal("TransactionState Dump:\n" + current_txn.debug());
                
            }
            this.hstore_coordinator.shutdownCluster(ex);
        } finally {
            if (d) {
                String txnDebug = "";
                if (current_txn != null && current_txn.getBasePartition() == this.partitionId) {
                    txnDebug = "\n" + current_txn.debug();
                }
                LOG.warn(String.format("PartitionExecutor %d is stopping.%s%s",
                                       this.partitionId,
                                       (this.currentTxnId != null ? " In-Flight Txn: #" + this.currentTxnId : ""),
                                       txnDebug));
            }
            
            // Release the shutdown latch in case anybody waiting for us
            this.shutdown_latch.release();
            
            // Stop HStoreMessenger (because we're nice)
            if (this.isShuttingDown() == false) {
                if (this.hstore_coordinator != null) this.hstore_coordinator.shutdown();
            }
        }
    }
    
    /**
     * Special function that allows us to do some utility work while 
     * we are waiting for a response or something real to do.
     */
    protected void utilityWork(CountDownLatch dtxnLatch) {
        // TODO: Set the txnId in our handle to be what the original txn was that
        //       deferred this query.
        
       /* We need to start popping from the deferred_queue here. There is no need
        * for a while loop if we're going to requeue each popped txn in wthe work_queue,
        * because we know we this.work_queue.isEmpty() will be false as soon as we
        * pop one local txn off of deferred_queue. We will arrive back in utilityWork() 
        * when that txn finishes if no new txn's have entered.*/
    	do {
    		LocalTransaction ts = deferred_queue.poll();
    		if (ts == null) break;
    		this.queueNewTransaction(ts);
        } while ((dtxnLatch != null && dtxnLatch.getCount() > 0) || (dtxnLatch == null && this.work_queue.isEmpty()));
        //while (this.work_queue.isEmpty()) {
        //}
	     // Try to free some memory
//	        this.tmp_fragmentParams.reset();
//	        this.tmp_serializedParams.clear();
//	        this.tmp_EEdependencies.clear();
    }

    public void tick() {
        // invoke native ee tick if at least one second has passed
        final long time = EstTime.currentTimeMillis();
        if ((time - lastTickTime) >= 1000) {
            if ((lastTickTime != 0) && (ee != null)) {
                ee.tick(time, lastCommittedTxnId);
            }
            lastTickTime = time;
        }
        
        // do other periodic work
        m_snapshotter.doSnapshotWork(ee);
    }

    @Override
    public void updateLogging() {
        d = debug.get();
        t = trace.get();
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------------
    
    public ExecutionEngine getExecutionEngine() {
        return (this.ee);
    }
    public Thread getExecutionThread() {
        return (this.self);
    }
    public PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public TransactionEstimator getTransactionEstimator() {
        return (this.t_estimator);
    }
    public ThrottlingQueue<VoltMessage> getThrottlingQueue() {
        return (this.work_throttler);
    }
    
    public HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public HStoreCoordinator getHStoreCoordinator() {
        return (this.hstore_coordinator);
    }

    public Site getCatalogSite() {
        return (this.site);
    }
    public int getHostId() {
        return (this.site.getHost().getRelativeIndex());
    }
    public Host getHost() {
        return (this.site.getHost());
    }
    public int getSiteId() {
        return (this.siteId);
    }
    public Partition getPartition() {
        return (this.partition);
    }
    public int getPartitionId() {
        return (this.partitionId);
    }
    
    public Long getLastExecutedTxnId() {
        return (this.lastExecutedTxnId);
    }
    public Long getLastCommittedTxnId() {
        return (this.lastCommittedTxnId);
    }

    public ParameterSetArrayCache getProcedureParameterSetArrayCache() {
        return (this.procParameterSets);
    }
    
    /**
     * Returns the next undo token to use when hitting up the EE with work
     * MAX_VALUE = no undo
     * @param txn_id
     * @return
     */
    public long getNextUndoToken() {
        return (++this.lastUndoToken);
    }
    
    /**
     * Set the current ExecutionMode for this executor
     * @param newMode
     * @param txn_id
     */
    private void setExecutionMode(AbstractTransaction ts, ExecutionMode newMode) {
        if (d && this.currentExecMode != newMode) {
            LOG.debug(String.format("Setting ExecutionMode for partition %d to %s because of %s [currentDtxn=%s, origMode=%s]",
                                    this.partitionId, newMode, ts, this.currentDtxn, this.currentExecMode));
        }
        assert(newMode != ExecutionMode.COMMIT_READONLY || (newMode == ExecutionMode.COMMIT_READONLY && this.currentDtxn != null)) :
            String.format("%s is trying to set partition %d to %s when the current DTXN is null?", ts, this.partitionId, newMode);
        this.currentExecMode = newMode;
    }
    public ExecutionMode getExecutionMode() {
        return (this.currentExecMode);
    }
    
    /**
     * Get the txnId of the current distributed transaction at this partition
     * <B>FOR TESTING ONLY</B> 
     */
    public AbstractTransaction getCurrentDtxn() {
        return (this.currentDtxn);
    }
    /**
     * Get the txnId of the current distributed transaction at this partition
     * <B>FOR TESTING ONLY</B>
     */
    public Long getCurrentDtxnId() {
        Long ret = null;
        // This is a race condition, so we'll just ignore any errors
        if (this.currentDtxn != null) { 
            try {
                ret = this.currentDtxn.getTransactionId();
            } catch (NullPointerException ex) {
                // IGNORE
            }
        } 
        return (ret);
    }
    public Long getCurrentTxnId() {
        return (this.currentTxnId);
    }
    
    public int getBlockedQueueSize() {
        return (this.currentBlockedTxns.size());
    }
    public int getWaitingQueueSize() {
        return (this.queued_responses.size());
    }
    public int getWorkQueueSize() {
        return (this.work_queue.size());
    }
    public ProfileMeasurement getWorkIdleTime() {
        return (this.work_idle_time);
    }
    public ProfileMeasurement getWorkExecTime() {
        return (this.work_exec_time);
    }
    /**
     * Returns the number of txns that have been invoked on this partition
     * @return
     */
    public int getTransactionCounter() {
        return (this.work_exec_time.getInvocations());
    }
    
    /**
     * Returns the VoltProcedure instance for a given stored procedure name
     * This is slow and should not be used at run time
     * @param proc_name
     * @return
     */
    public VoltProcedure getVoltProcedure(String proc_name) {
        return (this.procedures.get(proc_name));
    }
    
    private ParameterSet[] getFragmentParameters(AbstractTransaction ts, WorkFragment fragment, ParameterSet allParams[]) {
        int num_fragments = fragment.getFragmentIdCount();
        ParameterSet fragmentParams[] = tmp_fragmentParams.getParameterSet(num_fragments);
        assert(fragmentParams != null);
        assert(fragmentParams.length == num_fragments);
        
        for (int i = 0; i < num_fragments; i++) {
            int param_index = fragment.getParamIndex(i);
            assert(param_index < allParams.length) :
                String.format("StatementIndex is %d but there are only %d ParameterSets for %s",
                              param_index, allParams.length, ts); 
            fragmentParams[i].setParameters(allParams[param_index]);
        } // FOR
        return (fragmentParams);
    }
    
    private Map<Integer, List<VoltTable>> getFragmentInputs(AbstractTransaction ts, WorkFragment fragment, Map<Integer, List<VoltTable>> inputs) {
        Map<Integer, List<VoltTable>> attachedInputs = ts.getAttachedInputDependencies();
        assert(attachedInputs != null);
        boolean is_local = (ts instanceof LocalTransaction);
        
        if (d) LOG.debug(String.format("%s - Attempting to retrieve input dependencies for WorkFragment [isLocal=%s]",
                                       ts, is_local));
        for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
            WorkFragment.InputDependency input_dep_ids = fragment.getInputDepId(i);
            for (int input_dep_id : input_dep_ids.getIdsList()) {
                if (input_dep_id == HStoreConstants.NULL_DEPENDENCY_ID) continue;

                // If the Transaction is on the same HStoreSite, then all the 
                // input dependencies will be internal and can be retrieved locally
                if (is_local) {
                    List<VoltTable> deps = ((LocalTransaction)ts).getInternalDependency(input_dep_id);
                    assert(deps != null);
                    assert(inputs.containsKey(input_dep_id) == false);
                    inputs.put(input_dep_id, deps);
                    if (d) LOG.debug(String.format("%s - Retrieved %d INTERNAL VoltTables for DependencyId #%d\n" + deps,
                                                   ts, deps.size(), input_dep_id));
                }
                // Otherwise they will be "attached" inputs to the RemoteTransaction handle
                // We should really try to merge these two concepts into a single function call
                else if (attachedInputs.containsKey(input_dep_id)) {
                    List<VoltTable> deps = attachedInputs.get(input_dep_id);
                    List<VoltTable> pDeps = null;
                    // XXX: Do we actually need to copy these???
                    // XXX: I think we only need to copy if we're debugging the tables!
                    if (d) { // this.firstPartition == false) {
                        pDeps = new ArrayList<VoltTable>();
                        for (VoltTable vt : deps) {
                            // TODO: Move into VoltTableUtil
                            ByteBuffer buffer = vt.getTableDataReference();
                            byte arr[] = new byte[vt.getUnderlyingBufferSize()]; // FIXME
                            buffer.get(arr, 0, arr.length);
                            pDeps.add(new VoltTable(ByteBuffer.wrap(arr), true));
                        }
                    } else {
                        pDeps = deps;
                    }
                    inputs.put(input_dep_id, pDeps); 
                    if (d) LOG.debug(String.format("%s - Retrieved %d ATTACHED VoltTables for DependencyId #%d in %s",
                                                   ts, deps.size(), input_dep_id));
                }

            } // FOR (inputs)
        } // FOR (fragments)
        if (d) {
            if (inputs.isEmpty() == false) {

                LOG.debug(String.format("%s - Retrieved %d InputDependencies for %s on partition %d",
                                        ts, inputs.size(), fragment.getFragmentIdList(), fragment.getPartitionId())); // StringUtil.formatMaps(inputs)));
            } else if (fragment.getNeedsInput()) {
                LOG.warn(String.format("%s - No InputDependencies retrieved for %s on partition %d",
                                       ts, fragment.getFragmentIdList(), fragment.getPartitionId()));
            }
        }
        return (inputs);
    }
    
    
    /**
     * 
     * @param ts
     */
    private void setCurrentDtxn(AbstractTransaction ts) {
        // There can never be another current dtxn still unfinished at this partition!
        assert(this.currentBlockedTxns.isEmpty()) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.currentDtxn, ts, this.currentBlockedTxns.size());
        assert(this.currentDtxn == null) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.currentDtxn, ts, this.currentBlockedTxns.size());
        if (d) LOG.debug(String.format("Setting %s as the current DTXN for partition #%d [previous=%s]",
                                       ts, this.partitionId, this.currentDtxn));
        this.currentDtxn = ts;
    }
    
    private void resetCurrentDtxn() {
        assert(this.currentDtxn != null) :
            "Trying to reset the currentDtxn when it is already null";
        if (d) LOG.debug(String.format("Resetting current DTXN for partition #%d to null [previous=%s]",
                                       this.partitionId, this.currentDtxn));
        this.currentDtxn = null;
    }
    
    
    // ---------------------------------------------------------------
    // PartitionExecutor API
    // ---------------------------------------------------------------

    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * We should not be sent an InitiateTaskMessage here!
     * @param ts
     * @param task
     */
    public void queueWork(AbstractTransaction ts, FragmentTaskMessage task) {
        assert(ts.isInitialized());
        this.work_queue.add(task);
        if (d) LOG.debug(String.format("%s - Added distributed txn %s to front of partition %d work queue [size=%d]",
                                       ts, task.getClass().getSimpleName(), this.partitionId, this.work_queue.size()));
    }
    
    /**
     * Put the finish request for the transaction into the queue
     * @param task
     * @param status The final status of the transaction
     */
    public void queueFinish(AbstractTransaction ts, Status status) {
        assert(ts.isInitialized());
        FinishTaskMessage task = ts.getFinishTaskMessage(status);
        this.work_queue.add(task);
        if (d) LOG.debug(String.format("%s - Added distributed %s to front of partition %d work queue [size=%d]",
                                       ts, task.getClass().getSimpleName(), this.partitionId, this.work_queue.size()));
    }

    /**
     * New work for a local transaction
     * @param ts
     * @param task
     * @param callback
     */
    public boolean queueNewTransaction(LocalTransaction ts) {
        assert(ts != null) : "Unexpected null transaction handle!";
        final InitiateTaskMessage task = ts.getInitiateTaskMessage();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        final boolean mapreduce_part = ts.isPartOfMapreduce();
        boolean success = true;
        
        if (d) LOG.debug(String.format("%s - Queuing new transaction execution request on partition %d [currentDtxn=%s, mode=%s, taskHash=%d]",
                                       ts, this.partitionId, this.currentDtxn, this.currentExecMode, task.hashCode()));
        
        // If we're a single-partition and speculative execution is enabled, then we can always set it up now
        if (hstore_conf.site.exec_speculative_execution && singlePartitioned && this.currentExecMode != ExecutionMode.DISABLED) {
            if (d) LOG.debug(String.format("%s - Adding to work queue at partition %d [size=%d]", ts, this.partitionId, this.work_queue.size()));
            if (d) LOG.debug(String.format("Is part of mapreduce: " + mapreduce_part));
            
            if (mapreduce_part) success = this.work_throttler.offer(task, true);
            else success = this.work_throttler.offer(task, false);
            

        // Otherwise figure out whether this txn needs to be blocked or not
        }
        else {
            if (d) LOG.debug(String.format("%s - Attempting to add %s to partition %d queue [currentTxn=%s]",
                                           ts, task.getClass().getSimpleName(), this.partitionId, this.currentTxnId));
            
            exec_lock.lock();
            try {
                // No outstanding DTXN
                if (this.currentDtxn == null && this.currentExecMode != ExecutionMode.DISABLED) {
                    if (d) LOG.debug(String.format("%s - Adding %s to work queue [size=%d]",
                                                   ts, task.getClass().getSimpleName(), this.work_queue.size()));
                    // Only use the throttler for single-partition txns
                    if (singlePartitioned) {
                        if (d) LOG.debug(String.format("Is part of mapreduce: " + mapreduce_part));
                        if (mapreduce_part) success = this.work_throttler.offer(task, true);
                        else success = this.work_throttler.offer(task, false);
                        
                    } else {
                        // this.work_queue.addFirst(task);
                        this.work_queue.add(task);
                    }
                }
                // Add the transaction request to the blocked queue
                else {
                    // TODO: This is where we can check whether this new transaction request is commutative 
                    //       with the current dtxn. If it is, then we know that we don't
                    //       need to block it or worry about whether it will conflict with the current dtxn
                    if (d) LOG.debug(String.format("%s - Blocking until dtxn %s finishes", ts, this.currentDtxn));
                    this.currentBlockedTxns.add(task);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        if (success == false) {
            // Depending on what we need to do for this type txn, we will send
            // either an ABORT_THROTTLED or an ABORT_REJECT in our response
            // An ABORT_THROTTLED means that the client will back-off of a bit
            // before sending another txn request, where as an ABORT_REJECT means
            // that it will just try immediately
            Status status = ((singlePartitioned ? hstore_conf.site.queue_incoming_throttle : hstore_conf.site.queue_dtxn_throttle) ? 
                                        Status.ABORT_THROTTLED :
                                        Status.ABORT_REJECT);
            
            if (d) LOG.debug(String.format("%s - Hit with a %s response from partition %d [currentTxn=%s, throttled=%s, queueSize=%d]",
                                           ts, status, this.partitionId, this.currentTxnId,
                                           this.work_throttler.isThrottled(), this.work_throttler.size()));
            if (singlePartitioned == false) {
                TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(Status.ABORT_THROTTLED);
                hstore_coordinator.transactionFinish(ts, status, finish_callback);
            }
            // We will want to delete this transaction after we reject it if it is a single-partition txn
            // Otherwise we will let the normal distributed transaction process clean things up 
            hstore_site.transactionReject(ts, status);
        }
        return (success);
    }

    // ---------------------------------------------------------------
    // WORK QUEUE PROCESSING METHODS
    // ---------------------------------------------------------------
    

    /**
     * Enable speculative execution mode for this partition
     * The given TransactionId is the transaction that we need to wait to finish before
     * we can release the speculatively executed transactions
     * Returns true if speculative execution was enabled at this partition
     * @param txn_id
     * @param force
     * @return
     */
    public boolean enableSpeculativeExecution(AbstractTransaction ts, boolean force) {
        assert(ts != null) : "Null transaction handle???";
        // assert(this.speculative_execution == SpeculateType.DISABLED) : "Trying to enable spec exec twice because of txn #" + txn_id;
        
        if (d) LOG.debug(String.format("%s - Checking whether txn is read-only at partition %d [readOnly=%s]",
                                       ts, this.partitionId, ts.isExecReadOnly(this.partitionId)));
        
        // Check whether the txn that we're waiting for is read-only.
        // If it is, then that means all read-only transactions can commit right away
        if (ts.isExecReadOnly(this.partitionId)) {
            ExecutionMode newMode = ExecutionMode.COMMIT_READONLY;
            if (d) LOG.debug(String.format("%s - Attempting to enable %s speculative execution at partition %d [currentMode=%s]",
                                           ts, newMode, partitionId, this.currentExecMode));
            exec_lock.lock();
            try {
                if (this.currentDtxn == ts && this.currentExecMode != ExecutionMode.DISABLED) {
                    this.setExecutionMode(ts, newMode);
                    this.releaseBlockedTransactions(ts, true);
                    if (d) LOG.debug(String.format("%s - Enabled %s speculative execution at partition %d",
                                                   ts, this.currentExecMode, partitionId));
                    return (true);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        return (false);
    }
    
    /**
     * Process a FragmentResponseMessage and update the TransactionState accordingly
     * @param ts
     * @param result
     */
    private void processWorkResult(LocalTransaction ts, WorkResult result) {
        if (d) LOG.debug(String.format("Processing FragmentResponseMessage for %s on partition %d [srcPartition=%d, deps=%d]",
                                       ts, this.partitionId, result.getPartitionId(), result.getDepDataCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (result.getStatus() != Status.OK) {
            if (t) LOG.trace(String.format("Received non-success response %s from partition %d for %s",
                                                    result.getStatus(), result.getPartitionId(), ts));

            SerializableException error = null;
            if (hstore_conf.site.txn_profiling) ts.profiler.startDeserialization();
            try {
                ByteBuffer buffer = result.getError().asReadOnlyByteBuffer();
                error = SerializableException.deserializeFromBuffer(buffer);
            } catch (Exception ex) {
                throw new ServerFaultException(String.format("Failed to deserialize SerializableException from partition %d for %s [bytes=%d]",
                                                             result.getPartitionId(), ts, result.getError().size()), ex);
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopDeserialization();
            }
            // At this point there is no need to even deserialize the rest of the message because 
            // we know that we're going to have to abort the transaction
            if (error == null) {
                LOG.warn(ts + " - Unexpected null SerializableException\n" + result);
            } else {
                if (d) LOG.error(ts + " - Got error from partition " + result.getPartitionId(), error);
                ts.setPendingError(error, true);
            }
            return;
        }
        
        if (hstore_conf.site.txn_profiling) ts.profiler.startDeserialization();
        for (int i = 0, cnt = result.getDepDataCount(); i < cnt; i++) {
            if (t) LOG.trace(String.format("Storing intermediate results from partition %d for %s",
                                                    result.getPartitionId(), ts));
            int depId = result.getDepId(i);
            ByteString bs = result.getDepData(i);
            VoltTable vt = null;
            if (bs.isEmpty() == false) {
                FastDeserializer fd = new FastDeserializer(bs.asReadOnlyByteBuffer());
                try {
                    vt = fd.readObject(VoltTable.class);
                } catch (Exception ex) {
                    throw new ServerFaultException("Failed to deserialize VoltTable from partition " + result.getPartitionId() + " for " + ts, ex);
                }
            }
            ts.addResult(result.getPartitionId(), depId, vt);
        } // FOR (dependencies)
        if (hstore_conf.site.txn_profiling) ts.profiler.stopDeserialization();
    }
    
    /**
     * Execute a new transaction based on an InitiateTaskMessage
     * @param itask
     */
    private void processInitiateTaskMessage(LocalTransaction ts, InitiateTaskMessage itask) throws InterruptedException {
        if (hstore_conf.site.txn_profiling) ts.profiler.startExec();
        
        ExecutionMode before_mode = ExecutionMode.COMMIT_ALL;
        boolean predict_singlePartition = ts.isPredictSinglePartition();
        
        if (t) LOG.trace(String.format("%s - Attempting to begin processing %s on partition %d [taskHash=%d]",
                                       ts, itask.getClass().getSimpleName(), this.partitionId, itask.hashCode()));
        // If this is going to be a multi-partition transaction, then we will mark it as the current dtxn
        // for this PartitionExecutor.
        if (predict_singlePartition == false) {
            this.exec_lock.lock();
            try {
                if (this.currentDtxn != null) {
                    this.currentBlockedTxns.add(itask);
                    return;
                }
                this.setCurrentDtxn(ts);
                // 2011-11-14: We don't want to set the execution mode here, because we know that we
                //             can check whether we were read-only after the txn finishes
                if (d) LOG.debug(String.format("Marking %s as current DTXN on Partition %d [isLocal=%s, execMode=%s]",
                                               ts, this.partitionId, true, this.currentExecMode));                    
                before_mode = this.currentExecMode;
            } finally {
                exec_lock.unlock();
            } // SYNCH
        } else {
            exec_lock.lock();
            try {
                // If this is a single-partition transaction, then we need to check whether we are being executed
                // under speculative execution mode. We have to check this here because it may be the case that we queued a
                // bunch of transactions when speculative execution was enabled, but now the transaction that was ahead of this 
                // one is finished, so now we're just executing them regularly
                if (this.currentExecMode != ExecutionMode.COMMIT_ALL) {
                    assert(this.currentDtxn != null) : String.format("Invalid execution mode %s without a dtxn at partition %d", this.currentExecMode, this.partitionId);
                    
                    // HACK: If we are currently under DISABLED mode when we get this, then we just need to block the transaction
                    // and return back to the queue. This is easier than having to set all sorts of crazy locks
                    if (this.currentExecMode == ExecutionMode.DISABLED) {
                        if (d) LOG.debug(String.format("Blocking single-partition %s until dtxn %s finishes [mode=%s]", ts, this.currentDtxn, this.currentExecMode));
                        this.currentBlockedTxns.add(itask);
                        return;
                    }
                    
                    before_mode = this.currentExecMode;
                    if (hstore_conf.site.exec_speculative_execution) {
                        ts.setSpeculative(true);
                        if (d) LOG.debug(String.format("Marking %s as speculatively executed on partition %d [txnMode=%s, dtxn=%s]", ts, this.partitionId, before_mode, this.currentDtxn));
                    }
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        // Always clear+set the ExecutionState
        this.execState.clear();
        ts.setExecutionState(this.execState);
        
        VoltProcedure volt_proc = this.procedures.get(itask.getStoredProcedureName());
        assert(volt_proc != null) : "No VoltProcedure for " + ts;
        
        if (d) {
            LOG.debug(String.format("%s - Starting execution of txn [txnMode=%s, mode=%s]",
                                    ts, before_mode, this.currentExecMode));
            if (t) LOG.trace("Current Transaction at partition #" + this.partitionId + "\n" + ts.debug());
        }
            
        ClientResponseImpl cresponse = null;
        try {
            cresponse = (ClientResponseImpl)volt_proc.call(ts, itask.getParameters()); // Blocking...
        // VoltProcedure.call() should handle any exceptions thrown by the transaction
        // If we get anything out here then that's bad news
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) {
                SQLStmt last[] = volt_proc.voltLastQueriesExecuted();
                LOG.fatal("Unexpected error while executing " + ts, ex);
                if (last.length > 0) {
                    LOG.fatal(String.format("Last Queries Executed [%d]: %s", last.length, Arrays.toString(last)));
                }
                LOG.fatal("LocalTransactionState Dump:\n" + ts.debug());
                this.crash(ex);
            }
        } finally {
            ts.resetExecutionState();
        }
        // If this is a MapReduce job, then we can just ignore the ClientResponse
        // and return immediately
        if (ts.isMapReduce()) {
            return;
        } else if (cresponse == null) {
            assert(this.isShuttingDown()) : String.format("No ClientResponse for %s???", ts);
            return;
        }
        
        Status status = cresponse.getStatus();
        if (d) LOG.debug(String.format("Finished execution of %s [status=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, before_mode, this.currentExecMode));

        // We assume that most transactions are not speculatively executed and are successful
        // Therefore we don't want to grab the exec_mode lock here.
        if (predict_singlePartition == false || this.canProcessClientResponseNow(ts, status, before_mode)) {
            this.processClientResponse(ts, cresponse);
        }
        // Otherwise acquire the lock and then figure out what we can do with this guy
        else {
            exec_lock.lock();
            try {
                if (this.canProcessClientResponseNow(ts, status, before_mode)) {
                    this.processClientResponse(ts, cresponse);
                // Otherwise always queue our response, since we know that whatever thread is out there
                // is waiting for us to finish before it drains the queued responses
                } else {
                    // If the transaction aborted, then we can't execute any transaction that touch the tables that this guy touches
                    // But since we can't just undo this transaction without undoing everything that came before it, we'll just
                    // disable executing all transactions until the multi-partition transaction commits
                    // NOTE: We don't need acquire the 'exec_mode' lock here, because we know that we either executed in non-spec mode, or 
                    // that there already was a multi-partition transaction hanging around.
                    if (status != Status.OK && ts.isExecReadOnlyAllPartitions() == false) {
                        this.setExecutionMode(ts, ExecutionMode.DISABLED);
                        int blocked = this.work_queue.drainTo(this.currentBlockedTxns);
                        if (t && blocked > 0)
                            LOG.trace(String.format("Blocking %d transactions at partition %d because ExecutionMode is now %s",
                                                    blocked, this.partitionId, this.currentExecMode));
                        if (d) LOG.debug(String.format("Disabling execution on partition %d because speculative %s aborted", this.partitionId, ts));
                    }
                    if (t) LOG.trace(String.format("%s - Queuing ClientResponse [status=%s, origMode=%s, newMode=%s, dtxn=%s]",
                                                   ts, cresponse.getStatus(), before_mode, this.currentExecMode, this.currentDtxn));
                    this.queueClientResponse(ts, cresponse);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        volt_proc.finish();
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
        if (d) LOG.debug(String.format("%s - Checking whether to process response now [status=%s, singlePartition=%s, readOnly=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, ts.isExecSinglePartition(), ts.isExecReadOnly(this.partitionId), before_mode, this.currentExecMode));
        // Commit All
        if (this.currentExecMode == ExecutionMode.COMMIT_ALL) {
            return (true);
            
        // Process successful txns based on the mode that it was executed under
        } else if (status == Status.OK) {
            switch (before_mode) {
                case COMMIT_ALL:
                    return (true);
                case COMMIT_READONLY:
                    return (ts.isExecReadOnly(this.partitionId));
                case COMMIT_NONE: {
                    return (false);
                }
                default:
                    throw new ServerFaultException("Unexpected execution mode: " + before_mode, ts.getTransactionId()); 
            } // SWITCH
        }
        // Anything mispredicted should be processed right away
        else if (status == Status.ABORT_MISPREDICT) {
            return (true);
        }    
        // If the transaction aborted and it was read-only thus far, then we want to process it immediately
        else if (status != Status.OK && ts.isExecReadOnly(this.partitionId)) {
            return (true);
        }
        // If this txn threw a user abort, and the current outstanding dtxn is read-only
        // then it's safe for us to rollback
        else if (status == Status.ABORT_USER && (this.currentDtxn != null && this.currentDtxn.isExecReadOnly(this.partitionId))) {
            return (true);
        }
        
        assert(this.currentExecMode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.currentExecMode, status);
        return (false);
    }
    
    /**
     * Execute a WorkFragment for a distributed transaction
     * @param fragment
     * @throws Exception
     */
    private void processWorkFragment(AbstractTransaction ts, WorkFragment fragment, ParameterSet parameters[]) {
        assert(this.partitionId == fragment.getPartitionId()) :
            String.format("Tried to execute WorkFragment %s for %s on partition %d but it was suppose to be executed on partition %d",
                          fragment.getFragmentIdList(), ts, this.partitionId, fragment.getPartitionId());
        
        // A txn is "local" if the Java is executing at the same partition as this one
        boolean is_local = ts.isExecLocal(this.partitionId);
        boolean is_dtxn = (ts instanceof LocalTransaction == false);
        boolean is_prefetch = fragment.getPrefetch();
        if (d) LOG.debug(String.format("%s - Executing %s [isLocal=%s, isDtxn=%s, isPrefetch=%s, fragments=%s]",
                                       ts, fragment.getClass().getSimpleName(),
                                       is_local, is_dtxn, is_prefetch,
                                       fragment.getFragmentIdCount()));
        
        // If this txn isn't local, then we have to update our undoToken
        if (is_local == false) {
            ts.initRound(this.partitionId, this.getNextUndoToken());
            ts.startRound(this.partitionId);
        }
        
        DependencySet result = null;
        Status status = Status.OK;
        SerializableException error = null;
        
        try {
            result = this.executeWorkFragment(ts, fragment, parameters);
        } catch (ConstraintFailureException ex) {
            if (d) LOG.warn(String.format("%s - Unexpected ConstraintFailureException error on partition %d", ts, this.partitionId), ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (EEException ex) {
            LOG.error(String.format("%s - Unexpected ExecutionEngine error on partition %d", ts, this.partitionId), ex);
            this.crash(ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (SQLException ex) {
            LOG.error(String.format("%s - Unexpected SQL error on partition %d", ts, this.partitionId), ex);
            status = Status.ABORT_UNEXPECTED;
            error = ex;
        } catch (Throwable ex) {
            LOG.error(String.format("%s - Unexpected error on partition %d", ts, this.partitionId), ex);
            status = Status.ABORT_UNEXPECTED;
            if (ex instanceof SerializableException) {
                error = (SerializableException)ex;
            } else {
                error = new SerializableException(ex);
            }
        } finally {
            // Success, but without any results???
            if (result == null && status == Status.OK) {
                Exception ex = new Exception(String.format("The WorkFragment %s executed successfully on Partition %d but result is null for %s",
                                                           fragment.getFragmentIdList(), this.partitionId, ts));
                if (d) LOG.warn(ex);
                status = Status.ABORT_UNEXPECTED;
                error = new SerializableException(ex);
            }
        }
        
        // For single-partition INSERT/UPDATE/DELETE queries, we don't directly
        // execute the SendPlanNode in order to get back the number of tuples that
        // were modified. So we have to rely on the output dependency ids set in the task
        assert(status != Status.OK ||
              (status == Status.OK && result.size() == fragment.getFragmentIdCount())) :
           "Got back " + result.size() + " results but was expecting " + fragment.getFragmentIdCount();
        
        // Make sure that we mark the round as finished before we start sending results
        if (is_local == false) {
            ts.finishRound(this.partitionId);
        }
        
        // -------------------------------
        // PREFETCH QUERIES
        // -------------------------------
        if (is_prefetch) {
            // Regardless of whether this txn is running at the same HStoreSite as this PartitionExecutor,
            // we always need to put the result inside of the AbstractTransaction
            // This is so that we can identify if we get request for a query that we have already executed
            // We'll only do this if it succeeded. If it failed, then we won't do anything and will
            // just wait until they come back to execute the query again before 
            // we tell them that something went wrong. It's ghetto, but it's just easier this way...
            if (status == Status.OK) {
                if (d) LOG.debug(String.format("%s - Storing %d prefetch query results in partition %d query cache",
                                               ts, result.size(), ts.getBasePartition()));
                PartitionExecutor other = null; // 
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    // We're going to store the result in the base partition cache if they're 
                    // on the same HStoreSite as us
                    if (hstore_site.isLocalPartition(ts.getBasePartition())) {
                        if (other == null) other = this.hstore_site.getPartitionExecutor(ts.getBasePartition());
                        other.queryCache.addTransactionQueryResult(ts.getTransactionId(),
                                                                   fragment.getFragmentId(i),
                                                                   fragment.getPartitionId(),
                                                                   parameters[i],
                                                                   result.dependencies[i]);
                    }
                    // We also need to store it in our own cache in case we need to retrieve it
                    // if they come at us with the same query request
                    this.queryCache.addTransactionQueryResult(ts.getTransactionId(),
                                                              fragment.getFragmentId(i),
                                                              fragment.getPartitionId(),
                                                              parameters[i],
                                                              result.dependencies[i]);

                } // FOR
            }
            
            // Now if it's a remote transaction, we need to use the coordinator to send
            // them our result. Note that we want to send a single message per partition. Unlike
            // with the TransactionWorkRequests, we don't need to wait until all of the partitions
            // that are prefetching for this txn at our local HStoreSite to finish.
            if (is_dtxn) {
                WorkResult wr = this.buildWorkResult(ts, result, status, error);
                TransactionPrefetchResult prefetchResult = TransactionPrefetchResult.newBuilder()
                                                                .setTransactionId(ts.getTransactionId().longValue())
                                                                .setSourcePartition(this.partitionId)
                                                                .setResult(wr)
                                                                .setStatus(status)
                                                                .build();
                hstore_coordinator.transactionPrefetchResult((RemoteTransaction)ts, prefetchResult);
            }
        }
        // -------------------------------
        // LOCAL TRANSACTION
        // -------------------------------
        else if (is_dtxn == false) {
            LocalTransaction local_ts = (LocalTransaction)ts;
            
            // If the transaction is local, store the result directly in the local TransactionState
            if (status == Status.OK) {
                if (t) LOG.trace("Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                assert(result.size() == fragment.getOutputDepIdCount());
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    int dep_id = fragment.getOutputDepId(i);
                    if (t) LOG.trace("Storing DependencyId #" + dep_id  + " for " + ts);
                    try {
                        local_ts.addResult(this.partitionId, dep_id, result.dependencies[i]);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                        String msg = String.format("Failed to stored Dependency #%d for %s [idx=%d, fragmentId=%d]",
                                                   dep_id, ts, i, fragment.getFragmentId(i));
                        LOG.error(msg + "\n" + fragment.toString());
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
            if (d) LOG.debug(String.format("Constructing WorkResult %s with %d bytes from partition %d to send back to initial partition %d [status=%s]",
                                                    ts,
                                                    (result != null ? result.size() : null),
                                                    this.partitionId, ts.getBasePartition(),
                                                    status));
            
            RpcCallback<WorkResult> callback = ((RemoteTransaction)ts).getFragmentTaskCallback();
            if (callback == null) {
                LOG.fatal("Unable to send FragmentResponseMessage for " + ts);
                LOG.fatal("Orignal FragmentTaskMessage:\n" + fragment);
                LOG.fatal(ts.toString());
                throw new ServerFaultException("No RPC callback to HStoreSite for " + ts, ts.getTransactionId());
            }
            WorkResult response = this.buildWorkResult((RemoteTransaction)ts, result, status, error);
            assert(response != null);
            callback.run(response);
            
        }
    }
    
    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the
     * resulting DependencySet
     * @param fragment
     * @return
     * @throws Exception
     */
    private DependencySet executeWorkFragment(AbstractTransaction ts, WorkFragment fragment, ParameterSet parameters[]) throws Exception {
        DependencySet result = null;
        final long undoToken = ts.getLastUndoToken(this.partitionId);
        int fragmentCount = fragment.getFragmentIdCount();
        if (fragmentCount == 0) {
            LOG.warn(String.format("Got a FragmentTask for %s that does not have any fragments?!?", ts));
            return (result);
        }
        
        // Construct arrays given to the EE
        long fragmentIds[] = tmp_fragmentIds.getArray(fragmentCount);
        int outputDepIds[] = tmp_outputDepIds.getArray(fragmentCount);
        int inputDepIds[] = tmp_inputDepIds.getArray(fragmentCount); // Is this ok?
        for (int i = 0; i < fragmentCount; i++) {
            fragmentIds[i] = fragment.getFragmentId(i);
            outputDepIds[i] = fragment.getOutputDepId(i);
            for (int input_depId : fragment.getInputDepId(i).getIdsList()) {
                inputDepIds[i] = input_depId; // FIXME!
            } // FOR
        } // FOR
        
        // Input Dependencies
        this.tmp_EEdependencies.clear();
        this.getFragmentInputs(ts, fragment, this.tmp_EEdependencies);
        
        // *********************************** DEBUG ***********************************
        if (d) {
            LOG.debug(String.format("%s - Getting ready to kick %d fragments to EE",
                                    ts, fragmentCount));
//            if (t) {
//                LOG.trace("FragmentTaskIds: " + Arrays.toString(fragmentIds));
//                Map<String, Object> m = new ListOrderedMap<String, Object>();
//                for (int i = 0; i < parameters.length; i++) {
//                    m.put("Parameter[" + i + "]", parameters[i]);
//                } // FOR
//                LOG.trace("Parameters:\n" + StringUtil.formatMaps(m));
//            }
        }
        // *********************************** DEBUG ***********************************
        
        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ts.isSysProc()) {
            assert(fragmentCount == 1);
            long fragment_id = fragmentIds[0];
            assert(fragmentCount == parameters.length) :
                String.format("%s - Fragments:%d / Parameters:%d",
                              ts, fragmentCount, parameters.length);
            ParameterSet fragmentParams = parameters[0];

            VoltSystemProcedure volt_proc = this.m_registeredSysProcPlanFragments.get(fragment_id);
            if (volt_proc == null) {
                String msg = "No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments;
                throw new ServerFaultException(msg, ts.getTransactionId());
            }
            
            // HACK: We have to set the TransactionState for sysprocs manually
            volt_proc.setTransactionState(ts);
            ts.markExecNotReadOnly(this.partitionId);
            try {
                result = volt_proc.executePlanFragment(ts.getTransactionId(),
                                                       this.tmp_EEdependencies,
                                                       (int)fragment_id,
                                                       fragmentParams,
                                                       this.m_systemProcedureContext);
            } catch (Throwable ex) {
                String msg = "Unexpected error when executing system procedure";
                throw new ServerFaultException(msg, ex, ts.getTransactionId());
            }
            if (d) LOG.debug(String.format("%s - Finished executing sysproc fragment %d\n%s",
                                           ts, fragment_id, result));
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            result = this.executePlanFragments(ts,
                                               undoToken,
                                               fragmentCount,
                                               fragmentIds,
                                               parameters,
                                               outputDepIds,
                                               inputDepIds,
                                               this.tmp_EEdependencies);
            if (result == null) {
                LOG.warn(String.format("Output DependencySet for %s in %s is null?", Arrays.toString(fragmentIds), ts));
            }
        }
        return (result);
    }
    
    /**
     * Execute a BatchPlan directly on this PartitionExecutor without having to covert it
     * to FragmentTaskMessages first. This is big speed improvement over having to queue things up
     * @param ts
     * @param plan
     * @return
     */
    public VoltTable[] executeLocalPlan(LocalTransaction ts, BatchPlanner.BatchPlan plan, ParameterSet parameterSets[]) {
        long undoToken = HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN;
        
        // If we originally executed this transaction with undo buffers and we have a MarkovEstimate,
        // then we can go back and check whether we want to disable undo logging for the rest of the transaction
        // We can do this regardless of whether the transaction has written anything <-- NOT TRUE!
        if (ts.getEstimatorState() != null && ts.isPredictSinglePartition() && ts.isSpeculative() == false && hstore_conf.site.exec_no_undo_logging) {
            MarkovEstimate est = ts.getEstimatorState().getLastEstimate();
            assert(est != null) : "Got back null MarkovEstimate for " + ts;
            if (est.isAbortable(this.thresholds) || est.isReadOnlyPartition(this.thresholds, this.partitionId) == false) {
                undoToken = this.getNextUndoToken();
            } else if (d) {
                LOG.debug(String.format("Bold! Disabling undo buffers for inflight %s [prob=%f]\n%s\n%s",
                                        ts, est.getAbortProbability(), est, plan.toString()));
            }
        }
        // If the transaction is predicted to be read-only, then we won't bother with an undo buffer
        else if ((ts.isPredictReadOnly() == false && hstore_conf.site.exec_no_undo_logging_all == false) ||
                 hstore_conf.site.exec_force_undo_logging_all) {
            undoToken = this.getNextUndoToken();
        }
        ts.fastInitRound(this.partitionId, undoToken);
        ts.setBatchSize(plan.getBatchSize());
      
        int fragmentCount = plan.getFragmentCount();
        long fragmentIds[] = plan.getFragmentIds();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        
        // Mark that we touched the local partition once for each query in the batch
        // ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not a single-partition transaction
        if (hstore_conf.site.exec_speculative_execution && ts.isPredictSinglePartition() == false) {
            // TODO: We need to notify the remote HStoreSites that we are done with their partitions
            ts.calculateDonePartitions(this.thresholds);
        }

        if (t) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("Parameters:");
//            for (int i = 0; i < parameterSets.length; i++) {
//                sb.append(String.format("\n [%02d] %s", i, parameterSets[i].toString()));
//            }
//            LOG.trace(sb.toString());
            LOG.trace(String.format("Txn #%d - BATCHPLAN:\n" +
                     "  fragmentIds:   %s\n" + 
                     "  fragmentCount: %s\n" +
                     "  output_depIds: %s\n" +
                     "  input_depIds:  %s",
                     ts.getTransactionId(),
                     Arrays.toString(plan.getFragmentIds()), plan.getFragmentCount(), Arrays.toString(plan.getOutputDependencyIds()), Arrays.toString(plan.getInputDependencyIds())));
        }
        
        // NOTE: There are no dependencies that we need to pass in because the entire batch is single-partitioned
        DependencySet result = this.executePlanFragments(ts,
                                                         undoToken,
                                                         fragmentCount,
                                                         fragmentIds,
                                                         parameterSets,
                                                         output_depIds,
                                                         input_depIds,
                                                         null);
        // assert(result != null) : "Unexpected null DependencySet result for " + ts; 
        if (t) LOG.trace("Output:\n" + result);
        
        ts.fastFinishRound(this.partitionId);
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
        if (d) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%s - Executing %d fragments [lastTxnId=%d, undoToken=%d]",
                                    ts, batchSize, this.lastCommittedTxnId, undoToken));
            if (t) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Fragments", Arrays.toString(fragmentIds));
                
                Map<Integer, Object> inner = new ListOrderedMap<Integer, Object>();
                for (int i = 0; i < batchSize; i++)
                    inner.put(i, parameterSets[i].toString());
                m.put("Parameters", inner);
                
                if (batchSize > 0 && input_depIds[0] != HStoreConstants.NULL_DEPENDENCY_ID) {
                    inner = new ListOrderedMap<Integer, Object>();
                    for (int i = 0; i < batchSize; i++) {
                        List<VoltTable> deps = input_deps.get(input_depIds[i]);
                        inner.put(input_depIds[i], (deps != null ? StringUtil.join("\n", deps) : "???"));
                    } // FOR
                    m.put("Input Dependencies", inner);
                }
                m.put("Output Dependencies", Arrays.toString(output_depIds));
                sb.append("\n" + StringUtil.formatMaps(m)); 
            }
            LOG.debug(sb.toString());
        }
        // *********************************** DEBUG ***********************************

        // pass attached dependencies to the EE (for non-sysproc work).
        if (input_deps != null && input_deps.isEmpty() == false) {
            if (d) LOG.debug(String.format("%s - Stashing %d InputDependencies at partition %d",
                                           ts, input_deps.size(), this.partitionId));
            ee.stashWorkUnitDependencies(input_deps);
        }
        
        // Check whether this fragments are read-only
        if (ts.isExecReadOnly(this.partitionId)) {
            boolean readonly = CatalogUtil.areFragmentsReadOnly(this.database, fragmentIds, batchSize); 
            if (readonly == false) {
                if (d) LOG.debug(String.format("%s - Marking txn as not read-only %s", ts, Arrays.toString(fragmentIds))); 
                ts.markExecNotReadOnly(this.partitionId);
            }
            
            // We can do this here because the only way that we're not read-only is if
            // we actually modify data at this partition
            ts.setSubmittedEE(this.partitionId);
        }
        
        DependencySet result = null;
        boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.isExecLocal(this.partitionId));
        if (needs_profiling) ((LocalTransaction)ts).profiler.startExecEE();
        Throwable error = null;
        try {
            if (d) LOG.debug(String.format("%s - Executing fragments %s at partition %d",
                                           ts, Arrays.toString(fragmentIds), this.partitionId));
            
            result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                            fragmentIds,
                            batchSize,
                            input_depIds,
                            output_depIds,
                            parameterSets,
                            batchSize,
                            txn_id.longValue(),
                            this.lastCommittedTxnId,
                            undoToken);
            
        } catch (SerializableException ex) {
            if (d) LOG.error(String.format("%s - Unexpected error in the ExecutionEngine on partition %d",
                                           ts, this.partitionId), ex);
            error = ex;
            throw ex;
        } catch (Throwable ex) {
            error = ex;
            new ServerFaultException(String.format("%s - Failed to execute PlanFragments: %s", ts, Arrays.toString(fragmentIds)), ex);
        } finally {
            if (needs_profiling) ((LocalTransaction)ts).profiler.stopExecEE();
            if (error == null && result == null) {
                LOG.warn(String.format("%s - Finished executing fragments but got back null results [fragmentIds=%s]",
                                       ts, Arrays.toString(fragmentIds)));
            }
        }
        
        // *********************************** DEBUG ***********************************
        if (d) {
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
     * 
     * @param txn_id
     * @param clusterName
     * @param databaseName
     * @param tableName
     * @param data
     * @param allowELT
     * @throws VoltAbortException
     */
    public void loadTable(AbstractTransaction ts, String clusterName, String databaseName, String tableName, VoltTable data, int allowELT) throws VoltAbortException {
        if (cluster == null) {
            throw new VoltProcedure.VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        if (this.database.getName().equalsIgnoreCase(databaseName) == false) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = this.database.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        ts.setSubmittedEE(this.partitionId);
        ee.loadTable(table.getRelativeIndex(), data,
                     ts.getTransactionId(),
                     lastCommittedTxnId,
                     getNextUndoToken(),
                     allowELT != 0);
    }

    /**
     * Execute a SQLStmt batch at this partition.
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
                                                   ts.getClientHandle(),
                                                   this.partitionIdObj, 
                                                   ts.getPredictTouchedPartitions(),
                                                   ts.isPredictSinglePartition(),
                                                   ts.getTouchedPartitions(),
                                                   batchParams);
        
        assert(plan != null);
        if (d) LOG.debug("BatchPlan for " + ts + ":\n" + plan.toString());
        if (hstore_conf.site.txn_profiling) ts.profiler.stopExecPlanning();
        
        // Tell the TransactionEstimator that we're about to execute these mofos
        TransactionEstimator.State t_state = ts.getEstimatorState();
        if (t_state != null) {
            if (hstore_conf.site.txn_profiling) ts.profiler.startExecEstimation();
            this.t_estimator.executeQueries(t_state, planner.getStatements(), plan.getStatementPartitions(), true);
            if (hstore_conf.site.txn_profiling) ts.profiler.stopExecEstimation();
        }

        // Check whether our plan was caused a mispredict
        // Doing it this way allows us to update the TransactionEstimator before we abort the txn
        if (plan.getMisprediction() != null) {
            MispredictionException ex = plan.getMisprediction(); 
            ts.setPendingError(ex, false);

            MarkovGraph markov = (t_state != null ? t_state.getMarkovGraph() : null); 
            if (hstore_conf.site.markov_mispredict_recompute && markov != null) {
                if (d) LOG.debug("Recomputing MarkovGraph probabilities because " + ts + " mispredicted");
                // FIXME this.executor.helper.queueMarkovToRecompute(markov);
            }
            
            // Print Misprediction Debug
            if (d || hstore_conf.site.exec_mispredict_crash) {
                // FIXME LOG.warn("\n" + mispredictDebug(batchStmts, batchParams, markov, t_state, ex, batchSize));
            }
            
            // Crash on Misprediction!
            if (hstore_conf.site.exec_mispredict_crash) {
                LOG.fatal(String.format("Crashing because site.exec_mispredict_crash is true [txn=%s]", ts));
                this.crash(ex);
            } else if (d) {
                LOG.debug(ts + " mispredicted! Aborting and restarting!");
            }
            throw ex;
        }
        
        VoltTable results[] = null;
        if (plan.isReadOnly() == false) ts.markExecNotReadOnlyAllPartitions();
        
        // If the BatchPlan only has WorkFragments that are for this partition, then
        // we can use the fast-path executeLocalPlan() method
        if (plan.isSingledPartitionedAndLocal()) {
            if  (d) LOG.debug("Executing BatchPlan directly with ExecutionSite");
            results = this.executeLocalPlan(ts, plan, batchParams);
        }
        // Otherwise, we need to generate WorkFragments and then send the messages out 
        // to our remote partitions using the HStoreCoordinator
        else {
            this.partitionFragments.clear();
            plan.getWorkFragments(ts.getTransactionId(), this.partitionFragments);
            if (t) LOG.trace("Got back a set of tasks for " + this.partitionFragments.size() + " partitions for " + ts);

            // Block until we get all of our responses.
            results = this.dispatchWorkFragments(ts, batchSize, this.partitionFragments, batchParams);
        }
        if (d && results == null)
            LOG.warn("Got back a null results array for " + ts + "\n" + plan.toString());

        if (hstore_conf.site.txn_profiling) ts.profiler.startExecJava();
        
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
            error.serializeToBuffer(bc.b);
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
                if (t) LOG.trace(String.format("%s - Serialized Output Dependency %d\n%s",
                                               ts, result.depIds[i], result.dependencies[i]));  
            } // FOR
            this.fs.getBBContainer().discard();
        }
        
        return (builder.build());
    }
    
    /**
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    private void requestWork(LocalTransaction ts, Collection<WorkFragment> tasks, List<ByteString> parameterSets) {
        assert(!tasks.isEmpty());
        assert(ts != null);
        Long txn_id = ts.getTransactionId();

        if (t) LOG.trace(String.format("Wrapping %d WorkFragments into a TransactionWorkRequest for %s", tasks.size(), ts));
        
        // If our transaction was originally designated as a single-partitioned, then we need to make
        // sure that we don't touch any partition other than our local one. If we do, then we need abort
        // it and restart it as multi-partitioned
        boolean need_restart = false;
        boolean predict_singlepartition = ts.isPredictSinglePartition(); 
        BitSet done_partitions = ts.getDonePartitions();
        
        boolean new_done = false;
        if (hstore_conf.site.exec_speculative_execution) {
            new_done = ts.calculateDonePartitions(this.thresholds);
        }
        
        // Now we can go back through and start running all of the FragmentTaskMessages that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each FragmentTaskMessage in its own message
        for (WorkFragment ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            int target_partition = ftask.getPartitionId();
            int target_site = hstore_site.getSiteIdForPartitionId(target_partition);
            
            // Make sure that this isn't a single-partition txn trying to access a remote partition
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (d) LOG.debug(String.format("%s on partition %d is suppose to be single-partitioned, but it wants to execute a fragment on partition %d",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure that this txn isn't trying ot access a partition that we said we were
            // done with earlier
            else if (done_partitions.get(target_partition)) {
                if (d) LOG.debug(String.format("%s on partition %d was marked as done on partition %d but now it wants to go back for more!",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            // Make sure we at least have something to do!
            else if (ftask.getFragmentIdCount() == 0) {
                LOG.warn(String.format("%s - Trying to send a WorkFragment request with 0 fragments", ts));
                continue;
            }
           
            // Get the TransactionWorkRequest.Builder for the remote HStoreSite
            // We will use this store our serialized input dependencies
            TransactionWorkRequestBuilder requestBuilder = tmp_transactionRequestBuilders[target_site];
            if (requestBuilder == null) {
                requestBuilder = tmp_transactionRequestBuilders[target_site] = new TransactionWorkRequestBuilder();
            }
            TransactionWorkRequest.Builder builder = requestBuilder.getBuilder(ts);
            
            // Also keep track of what Statements they are executing so that we know
            // we need to send over the wire to them.
            requestBuilder.addParamIndexes(ftask.getParamIndexList());
            
            // Input Dependencies
            if (ftask.getNeedsInput()) {
                if (d) LOG.debug("Retrieving input dependencies for " + ts);
                
                tmp_removeDependenciesMap.clear();
                this.getFragmentInputs(ts, ftask, tmp_removeDependenciesMap);

                for (Entry<Integer, List<VoltTable>> e : tmp_removeDependenciesMap.entrySet()) {
                    if (requestBuilder.hasInputDependencyId(e.getKey())) continue;

                    if (d) LOG.debug(String.format("%s - Attaching %d input dependencies to be sent to %s",
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
                        if (d) LOG.debug(String.format("%s - Storing %d rows for InputDependency %d to send to partition %d [bytes=%d]",
                                                       ts, vt.getRowCount(), e.getKey(), ftask.getPartitionId(),
                                                       CollectionUtil.last(builder.getAttachedDataList()).size()));
                    } // FOR
                    requestBuilder.addInputDependencyId(e.getKey());
                } // FOR
                this.fs.getBBContainer().discard();
            }
            builder.addFragments(ftask);
        } // FOR (tasks)
        
        // Bad mojo! We need to throw a MispredictionException so that the VoltProcedure
        // will catch it and we can propagate the error message all the way back to the HStoreSite
        if (need_restart) {
            if (t) LOG.trace(String.format("Aborting %s because it was mispredicted", ts));
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
            if (d) LOG.debug(String.format("%s - Sent Work request to remote HStoreSites for %s",
                                           ts, target_site));

        } // FOR

        // TODO: We need to check whether we need to notify other HStoreSites that we didn't send
        // a new FragmentTaskMessage to that we are done with their partitions
        if (new_done) {
            
        }
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do...
     * This is the slowest way to execute a bunch of WorkFragments and therefore should only be invoked
     * for batches that need to access non-local Partitions
     * @param ts
     * @param fragments
     * @param parameters
     * @return
     */
    public VoltTable[] dispatchWorkFragments(final LocalTransaction ts,
                                             final int batchSize,
                                             Collection<WorkFragment> fragments,
                                             final ParameterSet parameters[]) {
        assert(fragments.isEmpty() == false) :
            "Unexpected empty WorkFragment list for " + ts;
        
        // *********************************** DEBUG ***********************************
        if (d) {
            LOG.debug(String.format("%s - Preparing to dispatch %d messages and wait for the results",
                                             ts, fragments.size()));
            if (t) {
                StringBuilder sb = new StringBuilder();
                sb.append(ts + " - WorkFragments:\n");
                for (WorkFragment fragment : fragments) {
                    sb.append(StringUtil.box(fragment.toString()) + "\n");
                } // FOR
                sb.append(ts + " - ParameterSets:\n");
                for (ParameterSet ps : parameters) {
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
            for (WorkFragment frag : fragments) {
                if (frag.getPartitionId() != this.partitionId) {
                    has_remote = true;
                }
                for (int frag_id : frag.getFragmentIdList()) {
                    PlanFragment catalog_frag = CatalogUtil.getPlanFragment(database, frag_id);
                    Statement catalog_stmt = catalog_frag.getParent();
                    assert(catalog_stmt != null);
                    Procedure catalog_proc = catalog_stmt.getParent();
                    if (catalog_proc.equals(ts.getProcedure()) == false) {
                        LOG.warn(ts.debug() + "\n" + fragments + "\n---- INVALID ----\n" + frag);
                        String msg = String.format("%s - Unexpected %s", ts, catalog_frag.fullName());
                        throw new ServerFaultException(msg, ts.getTransactionId());
                    }
                }
            } // FOR
            if (has_remote == false) {
                LOG.warn(ts.debug() + "\n" + fragments);
                String msg = String.format("%s - Trying to execute all local single-partition queries using the slow-path!", ts);
                throw new ServerFaultException(msg, ts.getTransactionId());
            }
        }
        
        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.partitionId, this.getNextUndoToken());
        ts.setBatchSize(batchSize);
        
        final boolean prefetch = ts.hasPrefetchQueries();
        final boolean predict_singlePartition = ts.isPredictSinglePartition();
        
        // Attach the ParameterSets to our transaction handle so that anybody on this HStoreSite
        // can access them directly without needing to deserialize them from the WorkFragments
        ts.attachParameterSets(parameters);
        
        // Now if we have some work sent out to other partitions, we need to wait until they come back
        // In the first part, we wait until all of our blocked FragmentTaskMessages become unblocked
        LinkedBlockingDeque<Collection<WorkFragment>> queue = ts.getUnblockedWorkFragmentsQueue();

        boolean first = true;
        boolean serializedParams = false;
        CountDownLatch latch = null;
        boolean all_local = true;
        boolean is_localSite;
        boolean is_localPartition;
        int num_localPartition = 0;
        int num_localSite = 0;
        int num_remote = 0;
        int num_skipped = 0;
        int total = 0;
        
        // Run through this loop if:
        //  (1) We have no pending errors
        //  (2) This is our first time in the loop (first == true)
        //  (3) If we know that there are still messages being blocked
        //  (4) If we know that there are still unblocked messages that we need to process
        //  (5) The latch for this round is still greater than zero
        while (ts.hasPendingError() == false && 
               (first == true || ts.stillHasWorkFragments() || (latch != null && latch.getCount() > 0))) {
            if (t) LOG.trace(String.format("%s - [first=%s, stillHasWorkFragments=%s, latch=%s]",
                                           ts, first, ts.stillHasWorkFragments(), queue.size(), latch));
            
            // If this is the not first time through the loop, then poll the queue to get our list of fragments
            if (first == false) {
                all_local = true;
                is_localSite = false;
                is_localPartition = false;
                num_localPartition = 0;
                num_localSite = 0;
                num_remote = 0;
                num_skipped = 0;
                total = 0;
                
                if (t) LOG.trace(String.format("%s - Waiting for unblocked tasks on partition %d", ts, this.partitionId));
                if (hstore_conf.site.txn_profiling) ts.profiler.startExecDtxnWork();
                try {
                    fragments = queue.takeFirst(); // BLOCKING
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) {
                        LOG.error(String.format("%s - We were interrupted while waiting for blocked tasks", ts), ex);
                    }
                    return (null);
                } finally {
                    if (hstore_conf.site.txn_profiling) ts.profiler.stopExecDtxnWork();
                }
            }
            assert(fragments != null);
            
            // If the list to fragments unblock is empty, then we 
            // know that we have dispatched all of the WorkFragments for the
            // transaction's current SQLStmt batch. That means we can just wait 
            // until all the results return to us.
            if (fragments.isEmpty()) {
                if (t) LOG.trace(ts + " - Got an empty list of WorkFragments. Blocking until dependencies arrive");
                break;
            }

            this.tmp_localWorkFragmentList.clear();
            if (predict_singlePartition == false) {
                this.tmp_remoteFragmentList.clear();
                this.tmp_localSiteFragmentList.clear();
            }
            
            // -------------------------------
            // FAST PATH: Assume everything is local
            // -------------------------------
            if (predict_singlePartition) {
                for (WorkFragment ftask : fragments) {
                    if (first == false || ts.addWorkFragment(ftask) == false) {
                        this.tmp_localWorkFragmentList.add(ftask);
                        total++;
                        num_localPartition++;
                    }
                } // FOR
                
                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = ts.getDependencyLatch();
                }
                
                // Execute all of our WorkFragments quickly at our local ExecutionEngine
                for (WorkFragment fragment : this.tmp_localWorkFragmentList) {
                    if (d) LOG.debug(String.format("Got unblocked FragmentTaskMessage for %s. Executing locally...", ts));
                    assert(fragment.getPartitionId() == this.partitionId) :
                        String.format("Trying to process FragmentTaskMessage for %s on partition %d but it should have been sent to partition %d [singlePartition=%s]\n%s",
                                      ts, this.partitionId, fragment.getPartitionId(), predict_singlePartition, fragment);
                    ParameterSet fragmentParams[] = this.getFragmentParameters(ts, fragment, parameters);
                    this.processWorkFragment(ts, fragment, fragmentParams);
                } // FOR
            }
            // -------------------------------
            // SLOW PATH: Mixed local and remote messages
            // -------------------------------
            else {
                // Look at each task and figure out whether it needs to be executed at a remote
                // HStoreSite or whether we can execute it at one of our local PartitionExecutors.
                for (WorkFragment fragment : fragments) {
                    int partition = fragment.getPartitionId();
                    is_localSite = hstore_site.isLocalPartition(partition);
                    is_localPartition = (partition == this.partitionId);
                    all_local = all_local && is_localPartition;
                    if (first == false || ts.addWorkFragment(fragment) == false) {
                        total++;
                        
                        // At this point we know that all the WorkFragment has been registered
                        // in the LocalTransaction, so then it's safe for us to look to see
                        // whether we already have a prefetched result that we need
                        if (prefetch && is_localPartition == false) {
                            boolean skip_queue = true;
                            for (int i = 0, cnt = fragment.getFragmentIdCount(); i < cnt; i++) {
                                int fragId = fragment.getFragmentId(i);
                                int paramIdx = fragment.getParamIndex(i);
                                
                                VoltTable vt = this.queryCache.getTransactionCachedResult(ts.getTransactionId(),
                                                                                          fragId,
                                                                                          partition,
                                                                                          parameters[paramIdx]);
                                if (vt != null) {
                                    ts.addResult(partition, fragment.getOutputDepId(i), vt);
                                } else {
                                    skip_queue = false;
                                }
                            } // FOR
                            // If we were able to get cached results for all of the fragmentIds in
                            // this WorkFragment, then there is no need for us to send the message
                            // So we'll just skip queuing it up! How nice!
                            if (skip_queue) {
                                if (d) LOG.debug(String.format("%s - Using prefetch result for all fragments from partition %d",
                                                               ts, partition));
                                num_skipped++;
                                continue;
                            }
                        }
                        
                        // Otherwise add it to our list of WorkFragments that we want
                        // queue up right now
                        if (is_localPartition) {
                            this.tmp_localWorkFragmentList.add(fragment);
                            num_localPartition++;
                        } else if (is_localSite) {
                            this.tmp_localSiteFragmentList.add(fragment);
                            num_localSite++;
                        } else {
                            this.tmp_remoteFragmentList.add(fragment);
                            num_remote++;
                        }
                    }
                } // FOR
                assert(total == (num_remote + num_localSite + num_localPartition + num_skipped)) :
                    String.format("Total:%d / Remote:%d / LocalSite:%d / LocalPartition:%d / Skipped:%d",
                                  total, num_remote, num_localSite, num_localPartition, num_skipped);
                if (num_localPartition == 0 && num_localSite == 0 && num_remote == 0 && num_skipped == 0) {
                    String msg = String.format("Deadlock! All tasks for %s are blocked waiting on input!", ts);
                    throw new ServerFaultException(msg, ts.getTransactionId());
                }

                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = ts.getDependencyLatch();
                }
        
                // Now request the fragments that aren't local
                // We want to push these out as soon as possible
                if (num_remote > 0) {
                    // We only need to serialize the ParameterSets once
                    if (serializedParams == false) {
                        if (hstore_conf.site.txn_profiling) ts.profiler.startSerialization();
                        tmp_serializedParams.clear();
                        for (int i = 0; i < parameters.length; i++) {
                            if (parameters[i] == null) {
                                tmp_serializedParams.add(ByteString.EMPTY);
                            } else {
                                this.fs.clear();
                                try {
                                    parameters[i].writeExternal(this.fs);
                                    ByteString bs = ByteString.copyFrom(this.fs.getBBContainer().b);
                                    tmp_serializedParams.add(bs);
                                } catch (Exception ex) {
                                    throw new ServerFaultException("Failed to serialize ParameterSet " + i + " for " + ts, ex);
                                }
                            }
                        } // FOR
                        if (hstore_conf.site.txn_profiling) ts.profiler.stopSerialization();
                    }
                    if (d) LOG.debug(String.format("%s - Requesting %d FragmentTaskMessages to be executed on remote partitions", ts, num_remote));
                    this.requestWork(ts, tmp_remoteFragmentList, tmp_serializedParams);
                }
                
                // Then dispatch the task that are needed at the same HStoreSite but 
                // at a different partition than this one
                if (num_localSite > 0) {
                    if (d) LOG.debug(String.format("%s - Executing %d FragmentTaskMessages on local site's partitions",
                                                   ts, num_localSite));
                    for (WorkFragment fragment : this.tmp_localSiteFragmentList) {
                        FragmentTaskMessage ftask = ts.getFragmentTaskMessage(fragment);
                        hstore_site.getPartitionExecutor(fragment.getPartitionId()).queueWork(ts, ftask);
                    } // FOR
                }
        
                // Then execute all of the tasks need to access the partitions at this HStoreSite
                // We'll dispatch the remote-partition-local-site fragments first because they're going
                // to need to get queued up by at the other PartitionExecutors
                if (num_localPartition > 0) {
                    if (d) LOG.debug(String.format("%s - Executing %d FragmentTaskMessages on local partition",
                                                   ts, num_localPartition));
                    for (WorkFragment fragment : this.tmp_localWorkFragmentList) {
                        ParameterSet fragmentParams[] = this.getFragmentParameters(ts, fragment, parameters);
                        this.processWorkFragment(ts, fragment, fragmentParams);
                    } // FOR
                }
            }
            if (t) LOG.trace(String.format("%s - Dispatched %d WorkFragments [remoteSite=%d, localSite=%d, localPartition=%d]",
                                           ts, total, num_remote, num_localSite, num_localPartition));
            first = false;
        } // WHILE
        this.fs.getBBContainer().discard();
        
        if (t) LOG.trace(String.format("%s - BREAK OUT [first=%s, stillHasWorkFragments=%s, latch=%s]",
                                       ts, first, ts.stillHasWorkFragments(), latch));
//        assert(ts.stillHasWorkFragments() == false) :
//            String.format("Trying to block %s before all of its WorkFragments have been dispatched!\n%s\n%s",
//                          ts,
//                          StringUtil.join("** ", "\n", tempDebug),
//                          this.getVoltProcedure(ts.getProcedureName()).getLastBatchPlan());
                
        // Now that we know all of our FragmentTaskMessages have been dispatched, we can then
        // wait for all of the results to come back in.
        if (latch == null) latch = ts.getDependencyLatch();
        if (latch.getCount() > 0) {
            if (d) {
                LOG.debug(String.format("%s - All blocked messages dispatched. Waiting for %d dependencies", ts, latch.getCount()));
                if (t) LOG.trace(ts.toString());
            }
            if (hstore_conf.site.txn_profiling) ts.profiler.startExecDtxnWork();
            boolean done = false;
            // XXX this.utilityWork(latch);
            try {
                done = latch.await(hstore_conf.site.exec_response_timeout, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ex) {
                if (this.hstore_site.isShuttingDown() == false) {
                    LOG.error(String.format("%s - We were interrupted while waiting for results", ts), ex);
                }
                done = true;
            } catch (Throwable ex) {
                new ServerFaultException(String.format("Fatal error for %s while waiting for results", ts), ex);
            } finally {
                if (hstore_conf.site.txn_profiling) ts.profiler.stopExecDtxnWork();
            }
            if (done == false && this.isShuttingDown() == false) {
                LOG.warn(String.format("Still waiting for responses for %s after %d ms [latch=%d]\n%s",
                                                ts, hstore_conf.site.exec_response_timeout, latch.getCount(), ts.debug()));
                LOG.warn("Procedure Parameters:\n" + ts.getInvocation().getParams());
                hstore_conf.site.exec_profiling = true;
                LOG.warn(hstore_site.statusSnapshot());
                
                String msg = "PartitionResponses for " + ts + " never arrived!";
                throw new ServerFaultException(msg, ts.getTransactionId());
            }
        }
        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (d) LOG.warn(String.format("%s was hit with a %s",
                                          ts, ts.getPendingError().getClass().getSimpleName()));
            throw ts.getPendingError();
        }
        
        // IMPORTANT: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        VoltTable results[] = ts.getResults();
        ts.finishRound(this.partitionId);
         if (d) {
            if (t) LOG.trace(ts + " is now running and looking for love in all the wrong places...");
            LOG.debug(ts + " is returning back " + results.length + " tables to VoltProcedure");
        }
        return (results);
    }

    // ---------------------------------------------------------------
    // COMMIT + ABORT METHODS
    // ---------------------------------------------------------------

    /**
     * Queue a speculatively executed transaction to send its ClientResponseImpl message
     */
    private void queueClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        if (d) LOG.debug(String.format("Queuing ClientResponse for %s [handle=%s, status=%s]",
                                       ts, ts.getClientHandle(), cresponse.getStatus()));
        assert(ts.isPredictSinglePartition() == true) :
            String.format("Specutatively executed multi-partition %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(ts.isSpeculative() == true) :
            String.format("Queuing ClientResponse for non-specutative %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(cresponse.getStatus() != Status.ABORT_MISPREDICT) : 
            String.format("Trying to queue ClientResponse for mispredicted %s [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());
        assert(this.currentExecMode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.currentExecMode, cresponse.getStatus());

        // The ClientResponse is already going to be in the LocalTransaction handle
        // ts.setClientResponse(cresponse);
        this.queued_responses.add(Pair.of(ts, cresponse));

        if (d) LOG.debug("Total # of Queued Responses: " + this.queued_responses.size());
    }
    
    /**
     * For the given transaction's ClientResponse, figure out whether we can send it back to the client
     * right now or whether we need to initiate two-phase commit.
     * @param ts
     * @param cresponse
     */
    public void processClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        boolean is_singlepartitioned = ts.isPredictSinglePartition();
        Status status = cresponse.getStatus();

        if (d) {
            LOG.debug(String.format("%s - Processing ClientResponse at partition %d [handle=%d, status=%s, singlePartition=%s, local=%s]",
                                    ts, this.partitionId, cresponse.getClientHandle(), status,
                                    ts.isPredictSinglePartition(), ts.isExecLocal(this.partitionId)));
            if (t) {
                LOG.trace(ts + " Touched Partitions: " + ts.getTouchedPartitions().values());
                LOG.trace(ts + " Done Partitions: " + ts.getDonePartitions());
            }
        }
        
        // -------------------------------
        // ALL: Single-Partition Transactions
        // -------------------------------
        if (is_singlepartitioned) {
            // Commit or abort the transaction
            this.finishWork(ts, (status == Status.OK));
            
            // If the txn was mispredicted, then we will pass the information over to the HStoreSite
            // so that it can re-execute the transaction. We want to do this first so that the txn gets re-executed
            // as soon as possible...
            if (status == Status.ABORT_MISPREDICT) {
                if (d) LOG.debug(String.format("%s - Restarting because transaction is mispredicted", ts));
                // We don't want to delete the transaction here because whoever is going to requeue it for
                // us will need to know what partitions that the transaction touched when it executed before
                this.hstore_site.transactionRequeue(ts, status);
            }
            // Use the separate post-processor thread to send back the result
            else if (hstore_conf.site.exec_postprocessing_thread) {
                if (t) LOG.trace(String.format("%s - Sending ClientResponse to post-processing thread [status=%s]",
                                               ts, cresponse.getStatus()));
                this.hstore_site.queueClientResponse(ts, cresponse);
            }
            // Send back the result right now!
            else {
                if (hstore_conf.site.exec_command_logging) ts.markLogEnabled();
                this.hstore_site.sendClientResponse(ts, cresponse);
                ts.markAsDeletable();
                this.hstore_site.deleteTransaction(ts.getTransactionId(), status);
            }
        } 
        // -------------------------------
        // COMMIT: Distributed Transaction
        // -------------------------------
        else if (status == Status.OK) {
            // We have to send a prepare message to all of our remote HStoreSites
            // We want to make sure that we don't go back to ones that we've already told
            BitSet donePartitions = ts.getDonePartitions();
            tmp_preparePartitions.clear();
            for (Integer p : ts.getPredictTouchedPartitions()) {
                if (donePartitions.get(p.intValue()) == false) {
                    tmp_preparePartitions.add(p);
                }
            } // FOR

            // We need to set the new ExecutionMode before we invoke transactionPrepare
            // because the LocalTransaction handle might get cleaned up immediately
            ExecutionMode newMode = null;
            if (hstore_conf.site.exec_speculative_execution) {
                newMode = (ts.isExecReadOnly(this.partitionId) ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
            } else {
                newMode = ExecutionMode.DISABLED;
            }
            this.setExecutionMode(ts, newMode);
            
            if (hstore_conf.site.txn_profiling) ts.profiler.startPostPrepare();
            TransactionPrepareCallback callback = ts.initTransactionPrepareCallback(cresponse);
            assert(callback != null) : 
                "Missing TransactionPrepareCallback for " + ts + " [initialized=" + ts.isInitialized() + "]";
            this.hstore_coordinator.transactionPrepare(ts, callback, tmp_preparePartitions);
        }
        // -------------------------------
        // ABORT: Distributed Transaction
        // -------------------------------
        else {
            // Send back the result to the client right now, since there's no way 
            // that we're magically going to be able to recover this and get them a result
            // This has to come before the network messages above because this will clean-up the 
            // LocalTransaction state information
            this.hstore_site.sendClientResponse(ts, cresponse);
            
            // Then send a message all the partitions involved that the party is over
            // and that they need to abort the transaction. We don't actually care when we get the
            // results back because we'll start working on new txns right away.
            if (hstore_conf.site.txn_profiling) ts.profiler.startPostFinish();
            TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(status);
            this.hstore_coordinator.transactionFinish(ts, status, finish_callback);
        }
    }
        
    /**
     * Internal call to abort/commit the transaction
     * @param ts
     * @param commit
     */
    private void finishWork(AbstractTransaction ts, boolean commit) {
        assert(ts.isFinishedEE(this.partitionId) == false) :
            String.format("Trying to commit %s twice at partition %d", ts, this.partitionId);
        
        // This can be null if they haven't submitted anything
        long undoToken = ts.getLastUndoToken(this.partitionId);
        
        // Only commit/abort this transaction if:
        //  (1) We have an ExecutionEngine handle
        //  (2) We have the last undo token used by this transaction
        //  (3) The transaction was executed with undo buffers
        //  (4) The transaction actually submitted work to the EE
        //  (5) The transaction modified data at this partition
        if (this.ee != null && ts.hasSubmittedEE(this.partitionId) && undoToken != HStoreConstants.NULL_UNDO_LOGGING_TOKEN) {
            if (ts.isExecReadOnly(this.partitionId) == false && undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
                if (commit == false) {
                    LOG.fatal(ts.debug());
                    String msg = "TRYING TO ABORT TRANSACTION WITHOUT UNDO LOGGING";
                    this.crash(new ServerFaultException(msg, ts.getTransactionId()));
                }
                if (d) LOG.debug(String.format("%s - undoToken == DISABLE_UNDO_LOGGING_TOKEN", ts));
            } else {
                boolean needs_profiling = (hstore_conf.site.txn_profiling && ts.isExecLocal(this.partitionId) && ts.isPredictSinglePartition());
                if (needs_profiling) ((LocalTransaction)ts).profiler.startPostEE();
                if (commit) {
                    if (d) LOG.debug(String.format("%s - Committing on partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                   ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                    this.ee.releaseUndoToken(undoToken);
    
                // Evan says that txns will be aborted LIFO. This means the first txn that
                // we get in abortWork() will have a the greatest undoToken, which means that 
                // it will automagically rollback all other outstanding txns.
                // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
                // we should be able to check whether our undoToken has already been rolled back
                } else {
                    if (d) LOG.debug(String.format("%s - Aborting on partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                   ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                    this.ee.undoUndoToken(undoToken);
                }
                if (needs_profiling) ((LocalTransaction)ts).profiler.stopPostEE();
            }
        }
        
        // We always need to do the following things regardless if we hit up the EE or not
        if (commit) this.lastCommittedTxnId = ts.getTransactionId();
        ts.setFinishedEE(this.partitionId);
    }
    
    /**
     * Somebody told us that our partition needs to abort/commit the given transaction id.
     * This method should only be used for distributed transactions, because
     * it will do some extra work for speculative execution
     * @param txn_id
     * @param commit If true, the work performed by this txn will be commited. Otherwise it will be aborted
     */
    private void finishTransaction(AbstractTransaction ts, boolean commit) {
        if (this.currentDtxn != ts) {  
            if (d) LOG.debug(String.format("%s - Skipping finishWork request at partition %d because it is not the current Dtxn [%s/undoToken=%d]",
                                   ts, this.partitionId, this.currentDtxn, ts.getLastUndoToken(partitionId)));
            return;
        }
        if (d) LOG.debug(String.format("%s - Processing finishWork request at partition %d",
                                       ts, this.partitionId));

        assert(this.currentDtxn == ts) : "Expected current DTXN to be " + ts + " but it was " + this.currentDtxn;
        
        this.finishWork(ts, commit);
        
        // Clear our cached query results that are specific for this transaction
        this.queryCache.purgeTransaction(ts.getTransactionId());
        
        // Check whether this is the response that the speculatively executed txns have been waiting for
        // We could have turned off speculative execution mode beforehand 
        if (d) LOG.debug(String.format("Attempting to unmark %s as the current DTXN at partition %d and setting execution mode to %s",
                                       this.currentDtxn, this.partitionId, ExecutionMode.COMMIT_ALL));
        exec_lock.lock();
        try {
            // Resetting the current_dtxn variable has to come *before* we change the execution mode
            this.resetCurrentDtxn();
            this.setExecutionMode(ts, ExecutionMode.COMMIT_ALL);
            
            // We can always commit our boys no matter what if we know that this multi-partition txn 
            // was read-only at the given partition
            if (hstore_conf.site.exec_speculative_execution) {
                if (d) LOG.debug(String.format("Turning off speculative execution mode at partition %d because %s is finished",
                                               this.partitionId, ts));
                this.releaseQueuedResponses(ts.isExecReadOnly(this.partitionId) ? true : commit);
            }
            // Release blocked transactions
            this.releaseBlockedTransactions(ts, false);
        } catch (Throwable ex) {
            throw new ServerFaultException(String.format("Failed to finish %s at partition %d", ts, this.partitionId), ex);
        } finally {
            exec_lock.unlock();
        } // SYNCH
        
        // If we have a cleanup callback, then invoke that
        if (ts.getCleanupCallback() != null) {
            if (t) LOG.trace(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                           ts, ts.getCleanupCallback().getClass().getSimpleName(), this.partitionId));
            ts.getCleanupCallback().run(this.partitionId);
        }
        // If it's a LocalTransaction, then we'll want to invoke their TransactionFinishCallback 
        else if (ts instanceof LocalTransaction) {
            TransactionFinishCallback callback = ((LocalTransaction)ts).getTransactionFinishCallback();
            if (t) LOG.trace(String.format("%s - Notifying %s that the txn is finished at partition %d",
                                           ts, callback.getClass().getSimpleName(), this.partitionId));
            callback.decrementCounter(1);
        }
        
    }    
    /**
     * 
     * @param txn_id
     * @param p
     */
    private void releaseBlockedTransactions(AbstractTransaction ts, boolean speculative) {
        if (this.currentBlockedTxns.isEmpty() == false) {
            if (d) LOG.debug(String.format("Attempting to release %d blocked transactions at partition %d because of %s",
                                           this.currentBlockedTxns.size(), this.partitionId, ts));
            int released = 0;
            for (VoltMessage msg : this.currentBlockedTxns) {
                this.work_queue.add(msg);
                released++;
            } // FOR
            this.currentBlockedTxns.clear();
            if (d) LOG.debug(String.format("Released %d blocked transactions at partition %d because of %s",
                                         released, this.partitionId, ts));
        }
        assert(this.currentBlockedTxns.isEmpty());
    }
    
    /**
     * Commit/abort all of the queue transactions that were specutatively executed and waiting for
     * their responses to be sent back to the client
     * @param commit
     */
    private void releaseQueuedResponses(boolean commit) {
        // First thing we need to do is get the latch that will be set by any transaction
        // that was in the middle of being executed when we were called
        if (d) LOG.debug(String.format("Checking waiting/blocked transactions at partition %d [currentMode=%s]",
                                       this.partitionId, this.currentExecMode));
        
        if (this.queued_responses.isEmpty()) {
            if (d) LOG.debug(String.format("No speculative transactions to commit at partition %d. Ignoring...", this.partitionId));
            return;
        }
        
        // Ok now at this point we can access our queue send back all of our responses
        if (d) LOG.debug(String.format("%s %d speculatively executed transactions on partition %d",
                                       (commit ? "Commiting" : "Aborting"), this.queued_responses.size(), this.partitionId));

        // Loop backwards through our queued responses and find the latest txn that 
        // we need to tell the EE to commit. All ones that completed before that won't
        // have to hit up the EE.
        Pair<LocalTransaction, ClientResponseImpl> pair = null;
        LocalTransaction ts = null;
        ClientResponseImpl cr = null;
        boolean ee_commit = true;
        int skip_commit = 0;
        int aborted = 0;
        while ((pair = (hstore_conf.site.exec_queued_response_ee_bypass ? this.queued_responses.pollLast() :
                                                                        this.queued_responses.pollFirst())) != null) {
            ts = pair.getFirst();
            cr = pair.getSecond();
            
            // 2011-07-02: I have no idea how this could not be stopped here, but for some reason
            // I am getting a random error.
            // FIXME if (hstore_conf.site.txn_profiling && ts.profiler.finish_time.isStopped()) ts.profiler.finish_time.start();
            
            // If the multi-p txn aborted, then we need to abort everything in our queue
            // Change the status to be a MISPREDICT so that they get executed again
            if (commit == false) {
                // We're going to assume that any transaction that didn't mispredict
                // was single-partitioned. We'll use their TouchedPartitions histogram
                if (cr.getStatus() != Status.ABORT_MISPREDICT) {
                    ts.setPendingError(new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions()), false);
                    cr.setStatus(Status.ABORT_MISPREDICT);
                }
                aborted++;
                
            // Optimization: Check whether the last element in the list is a commit
            // If it is, then we know that we don't need to tell the EE about all the ones that executed before it
            } else if (hstore_conf.site.exec_queued_response_ee_bypass) {
                // Don't tell the EE that we committed
                if (ee_commit == false) {
                    if (t) LOG.trace(String.format("Bypassing EE commit for %s [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ts.unsetSubmittedEE(this.partitionId);
                    skip_commit++;
                    
                } else if (ee_commit && cr.getStatus() == Status.OK) {
                    if (t) LOG.trace(String.format("Committing %s but will bypass all other successful transactions [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ee_commit = false;
                }
            }
            
            try {
                if (hstore_conf.site.exec_postprocessing_thread) {
                    if (t) LOG.trace(String.format("Passing queued ClientResponse for %s to post-processing thread [status=%s]", ts, cr.getStatus()));
                    hstore_site.queueClientResponse(ts, cr);
                } else {
                    if (t) LOG.trace(String.format("Sending queued ClientResponse for %s back directly [status=%s]", ts, cr.getStatus()));
                    this.processClientResponse(ts, cr);
                }
            } catch (Throwable ex) {
                throw new ServerFaultException("Failed to complete queued " + ts, ex);
            }
        } // WHILE
        if (d && skip_commit > 0 && hstore_conf.site.exec_queued_response_ee_bypass) {
            LOG.debug(String.format("Fast Commit EE Bypass Optimization [skipped=%d, aborted=%d]", skip_commit, aborted));
        }
        return;
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
        LOG.warn(String.format("PartitionExecutor for Partition #%d is crashing", this.partitionId), ex);
        assert(this.hstore_coordinator != null);
        this.hstore_coordinator.shutdownClusterBlocking(ex);
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.hstore_site.isShuttingDown()); // shutdown_state == State.PREPARE_SHUTDOWN || this.shutdown_state == State.SHUTDOWN);
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        this.shutdown_state = Shutdownable.ShutdownState.PREPARE_SHUTDOWN;
    }
    
    /**
     * Somebody from the outside wants us to shutdown
     */
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (d) LOG.debug(String.format("Partition #%d told to shutdown again. Ignoring...", this.partitionId));
            return;
        }
        this.shutdown_state = ShutdownState.SHUTDOWN;
        
        if (d) LOG.debug(String.format("Shutting down PartitionExecutor for Partition #%d", this.partitionId));
        
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
}
