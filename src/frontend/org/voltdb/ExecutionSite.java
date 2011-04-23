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

package org.voltdb;

import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.catalog.*;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.*;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.TransactionInfoBaseMessage;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.Pair;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.VoltProcedure.*;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.HStoreMessenger;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.RemoteTransactionState;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite implements Runnable {
    public static final Logger LOG = Logger.getLogger(ExecutionSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private boolean t = trace.get();
    private boolean d = debug.get();

    // ----------------------------------------------------------------------------
    // SPECULATIVE EXECUTION TYPES
    // ----------------------------------------------------------------------------

    protected enum SpeculateType {
        /** No speculative execution **/
        DISABLED,
        /** Allow read-only txns to return results **/
        COMMIT_READONLY,
        /** All txn responses must wait until the multi-p txn is commited **/ 
        COMMIT_NONE,
    };
    
    // ----------------------------------------------------------------------------
    // GLOBAL CONSTANTS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    public static final int NULL_DEPENDENCY_ID = -1;

    public static final int NODE_THREAD_POOL_SIZE = 1;

    public static final int PRELOAD_LOCAL_TXN_STATES = 500;
    
    public static final int PRELOAD_REMOTE_TXN_STATES = 500;
    
    public static final int PRELOAD_DEPENDENCY_INFOS = 10000;
    
    
    // ----------------------------------------------------------------------------
    // OBJECT POOLS
    // ----------------------------------------------------------------------------

    /**
     * LocalTransactionState Object Pool
     */
    public final ObjectPool localTxnPool;

    /**
     * RemoteTransactionState Object Pool
     */
    public final ObjectPool remoteTxnPool;
    
    /**
     * Create a new instance of the corresponding VoltProcedure for the given Procedure catalog object
     */
    public class VoltProcedureFactory extends CountingPoolableObjectFactory<VoltProcedure> {
        private final Procedure catalog_proc;
        private final boolean has_java;
        private final Class<? extends VoltProcedure> proc_class;
        
        @SuppressWarnings("unchecked")
        public VoltProcedureFactory(Procedure catalog_proc) {
            super(hstore_conf.pool_enable_tracking);
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
                volt_proc.globalInit(ExecutionSite.this,
                               this.catalog_proc,
                               ExecutionSite.this.backend_target,
                               ExecutionSite.this.hsql,
                               ExecutionSite.this.p_estimator,
                               ExecutionSite.this.getPartitionId());
            } catch (Exception e) {
                if (d) LOG.warn("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                throw e;
            }
            return (volt_proc);
        }
    };

    /**
     * Procedure Name -> VoltProcedure Object Pool
     */
    public final Map<String, ObjectPool> procPool = new HashMap<String, ObjectPool>();
    
    /**
     * VoltProcedure.Executor Thread Pool 
     */
//    protected final ExecutorService thread_pool;
    
    /**
     * Mapping from SQLStmt batch hash codes (computed by VoltProcedure.getBatchHashCode()) to BatchPlanners
     * The idea is that we can quickly derived the partitions for each unique set of SQLStmt list
     */
    public static final Map<Integer, BatchPlanner> batch_planners = new ConcurrentHashMap<Integer, BatchPlanner>();
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Thread self;
    
    protected int siteId;
    
    /**
     * The unique Partition Id of this ExecutionSite
     * No other ExecutionSite will have this id in the cluster
     */
    protected int partitionId;

    /**
     * If this flag is enabled, then we need to shut ourselves down and stop running txns
     */
    private boolean shutdown = false;
    private CountDownLatch shutdown_latch;
    
    /**
     * Counter for the number of errors that we've hit
     */
    private final AtomicInteger error_counter = new AtomicInteger(0);

    /**
     * Catalog objects
     */
    protected Catalog catalog;
    protected Cluster cluster;
    protected Database database;
    protected Site site;
    protected Partition partition;

    private final BackendTarget backend_target;
    private final ExecutionEngine ee;
    private final HsqlBackend hsql;
    public static final DBBPool buffer_pool = new DBBPool(true, false);

    /**
     * Runtime Estimators
     */
    protected final PartitionEstimator p_estimator;
    protected final TransactionEstimator t_estimator;
    protected EstimationThresholds thresholds;
    
    protected WorkloadTrace workload_trace;
    
    // ----------------------------------------------------------------------------
    // H-Store Transaction Stuff
    // ----------------------------------------------------------------------------

    protected HStoreSite hstore_site;
    protected HStoreMessenger hstore_messenger;
    protected HStoreConf hstore_conf;
    
    protected final Set<Integer> done_partitions = new HashSet<Integer>();

    // ----------------------------------------------------------------------------
    // Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * TransactionId -> TransactionState
     */
    protected final Map<Long, TransactionState> txn_states = new ConcurrentHashMap<Long, TransactionState>(); 

    /**
     * List of Transactions that have been marked as finished
     */
    protected final Queue<TransactionState> finished_txn_states = new ConcurrentLinkedQueue<TransactionState>();

    /**
     * Mapping from specutatively executed transactions to their ClientResponses
     */
    protected final Queue<Pair<LocalTransactionState, ClientResponseImpl>> queued_responses = new ConcurrentLinkedQueue<Pair<LocalTransactionState,ClientResponseImpl>>(); 
    private SpeculateType speculative_execution = SpeculateType.DISABLED;
    private CountDownLatch speculative_latch;
    
    /**
     * The time in ms since epoch of the last call to ExecutionEngine.tick(...)
     */
    private long lastTickTime = 0;

    /**
     * The last txn id that we committed
     */
    private long lastCommittedTxnId = -1;

    /**
     * The last undoToken that we handed out
     */
    private long lastUndoToken = 0;

    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * FragmentTaskMessage (i.e., execute some fragments on behalf of another transaction)
     */
    private final LinkedBlockingDeque<TransactionInfoBaseMessage> work_queue = new LinkedBlockingDeque<TransactionInfoBaseMessage>();


    // ----------------------------------------------------------------------------
    // Coordinator Callback
    // ----------------------------------------------------------------------------

    /**
     * Coordinator -> ExecutionSite Callback
     */
    private final RpcCallback<Dtxn.CoordinatorResponse> request_work_callback = new RpcCallback<Dtxn.CoordinatorResponse>() {
        /**
         * Convert the CoordinatorResponse into a VoltTable for the given dependency id
         * @param parameter
         */
        @Override
        public void run(Dtxn.CoordinatorResponse parameter) {
            assert(parameter.getResponseCount() > 0) : "Got a CoordinatorResponse with no FragmentResponseMessages!";
            
            // Ignore (I think this is ok...)
            if (t) LOG.trace("Processing Dtxn.CoordinatorResponse in RPC callback with " + parameter.getResponseCount() + " embedded responses");
            for (int i = 0, cnt = parameter.getResponseCount(); i < cnt; i++) {
                ByteString serialized = parameter.getResponse(i).getOutput();
                
                FragmentResponseMessage response = null;
                try {
                    response = (FragmentResponseMessage)VoltMessage.createMessageFromBuffer(serialized.asReadOnlyByteBuffer(), false);
                } catch (Exception ex) {
                    LOG.fatal("Failed to deserialize embedded Dtxn.CoordinatorResponse message\n" + Arrays.toString(serialized.toByteArray()), ex);
                    System.exit(1);
                }
                assert(response != null);
                long txn_id = response.getTxnId();
                
                // Since there is no data for us to store, we will want to just let the TransactionState know
                // that we got a response. This ensures that the txn isn't unblocked just because the data arrives
                TransactionState ts = ExecutionSite.this.txn_states.get(txn_id);
                assert(ts != null) : "No transaction state exists for txn #" + txn_id + " " + txn_states;
                ExecutionSite.this.processFragmentResponseMessage(ts, response);
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
                LOG.trace("Registered " + proc.getClass().getSimpleName() + " sysproc handle for FragmentId #" + pfId);
            }
        }
    }

    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     * access to internal state via m_systemProcedureContext.
     */
    public interface SystemProcedureExecutionContext {
        public Catalog getCatalog();
        public Database getDatabase();
        public Cluster getCluster();
        public Site getSite();
        public ExecutionEngine getExecutionEngine();
        public long getLastCommittedTxnId();
//        public long getNextUndo();
//        public long getTxnId();
//        public Object getOperStatus();
    }

    protected class SystemProcedureContext implements SystemProcedureExecutionContext {
        public Catalog getCatalog()                 { return catalog; }
        public Database getDatabase()               { return cluster.getDatabases().get("database"); }
        public Cluster getCluster()                 { return cluster; }
        public Site getSite()                       { return site; }
        public ExecutionEngine getExecutionEngine() { return ee; }
        public long getLastCommittedTxnId()         { return ExecutionSite.this.getLastCommittedTxnId(); }
//        public long getNextUndo()                   { return getNextUndoToken(); }
//        public long getTxnId()                      { return getCurrentTxnId(); }
//        public String getOperStatus()               { return VoltDB.getOperStatus(); }
    }

    private final SystemProcedureContext m_systemProcedureContext = new SystemProcedureContext();


    // ----------------------------------------------------------------------------
    // METHODS
    // ----------------------------------------------------------------------------

    /**
     * Dummy constructor...
     */
    protected ExecutionSite() {
        this.ee = null;
        this.hsql = null;
        this.p_estimator = null;
        this.t_estimator = null;
        this.thresholds = null;
        this.catalog = null;
        this.cluster = null;
        this.site = null;
        this.database = null;
        this.backend_target = BackendTarget.HSQLDB_BACKEND;
//        this.thread_pool = null;
        this.siteId = 0;
        this.partitionId = 0;
        
        this.localTxnPool = new StackObjectPool(new LocalTransactionState.Factory(this, false));
        this.remoteTxnPool = new StackObjectPool(new RemoteTransactionState.Factory(this, false));
        DependencyInfo.initializePool(HStoreConf.singleton());
    }

    /**
     * Initialize the StoredProcedure runner and EE for this Site.
     * @param partitionId
     * @param t_estimator TODO
     * @param coordinator TODO
     * @param siteManager
     * @param serializedCatalog A list of catalog commands, separated by
     * newlines that, when executed, reconstruct the complete m_catalog.
     */
    public ExecutionSite(final int partitionId, final Catalog catalog, final BackendTarget target, PartitionEstimator p_estimator, TransactionEstimator t_estimator) {
        this.catalog = catalog;
        this.partition = CatalogUtil.getPartitionById(this.catalog, partitionId);
        assert(this.partition != null) : "Invalid Partition #" + partitionId;
        this.partitionId = this.partition.getId();
        this.site = this.partition.getParent();
        assert(site != null) : "Unable to get Site for Partition #" + partitionId;
        this.siteId = this.site.getId();
        
        this.hstore_conf = HStoreConf.singleton();
        this.localTxnPool = new StackObjectPool(new LocalTransactionState.Factory(this, hstore_conf.pool_enable_tracking), hstore_conf.pool_localtxnstate_idle);
        this.remoteTxnPool = new StackObjectPool(new RemoteTransactionState.Factory(this, hstore_conf.pool_enable_tracking), hstore_conf.pool_remotetxnstate_idle);
        DependencyInfo.initializePool(hstore_conf);
        
        this.backend_target = target;
        this.cluster = CatalogUtil.getCluster(catalog);
        this.database = CatalogUtil.getDatabase(cluster);

        // Setup Thread Pool
//        int pool_size = NODE_THREAD_POOL_SIZE;
//        this.thread_pool = Executors.newFixedThreadPool(pool_size, new ThreadFactory() {
//            final AtomicInteger cnt = new AtomicInteger(0);
//            @Override
//            public Thread newThread(Runnable r) {
//                Thread t = new Thread(r);
//                t.setDaemon(true);
//                t.setName(String.format("%s-%02d", ExecutionSite.this.getThreadName(), this.cnt.getAndIncrement()));
//                if (d) LOG.debug("Creating new executor thread: " + t.getName());
//                return (t);
//            }
//        });
//        if (d) LOG.debug("Created ExecutionSite thread pool with " + pool_size + " threads");
        
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
        
        // Don't bother with creating the EE if we're on the coordinator
        if (true) { //  || !this.coordinator) {
            // An execution site can be backed by HSQLDB, by volt's EE accessed
            // via JNI or by volt's EE accessed via IPC.  When backed by HSQLDB,
            // the VoltProcedure interface invokes HSQLDB directly through its
            // hsql Backend member variable.  The real volt backend is encapsulated
            // by the ExecutionEngine class. This class has implementations for both
            // JNI and IPC - and selects the desired implementation based on the
            // value of this.eeBackend.
        HsqlBackend hsqlTemp = null;
        ExecutionEngine eeTemp = null;
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
            LOG.fatal("Failed to initialize ExecutionSite", ex);
            VoltDB.crashVoltDB();
        }
        this.ee = eeTemp;
        this.hsql = hsqlTemp;
        assert(this.ee != null);
        assert(!(this.ee == null && this.hsql == null)) : "Both execution engine objects are empty. This should never happen";
//        } else {
//            this.hsql = null;
//            this.ee = null;
        }
    }
    
    protected void initializeVoltProcedurePools() {
        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = database.getProcedures();
        for (final Procedure catalog_proc : catalogProcedures) {
            String proc_name = catalog_proc.getName();
            this.procPool.put(proc_name, new StackObjectPool(new VoltProcedureFactory(catalog_proc), hstore_conf.pool_voltprocedure_idle));
            
            // Important: If this is a sysproc, then we need to get borrow/return 
            // one of them so that init() is at least called.
            // This is because of some legacy Volt code garbage blah blah...
            if (catalog_proc.getSystemproc()) {
                VoltProcedure volt_proc = this.getVoltProcedure(proc_name);
                try {
                    this.procPool.get(proc_name).returnObject(volt_proc);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        } // FOR
    }

    
    /**
     * Preload a bunch of stuff that we'll need later on
     */
    protected void preload() {
        this.initializeVoltProcedurePools();

        double scaleFactor = HStoreSite.getPreloadScaleFactor();
        
        // Then preload a bunch of TransactionStates
        for (boolean local : new boolean[]{ true, false }) {
            List<TransactionState> states = new ArrayList<TransactionState>();
            ObjectPool pool = (local ? this.localTxnPool : this.remoteTxnPool);
            int count = (int)Math.round((local ? PRELOAD_LOCAL_TXN_STATES : PRELOAD_REMOTE_TXN_STATES) / scaleFactor);
            try {
                for (int i = 0; i < count; i++) {
                    TransactionState ts = (TransactionState)pool.borrowObject();
                    ts.init(-1l, -1l, this.partitionId);
                    states.add(ts);
                } // FOR
                
                for (TransactionState ts : states) {
                    pool.returnObject(ts);
                } // FOR
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        } // FOR
        
        // And some DependencyInfos
//        try {
//            List<DependencyInfo> infos = new ArrayList<DependencyInfo>();
//            int count = (int)Math.round(PRELOAD_DEPENDENCY_INFOS / scaleFactor);
//            for (int i = 0; i < count; i++) {
//                DependencyInfo di = (DependencyInfo)DependencyInfo.INFO_POOL.borrowObject();
//                di.init(null, 1, 1);
//                infos.add(di);
//            } // FOR
//            for (DependencyInfo di : infos) {
//                DependencyInfo.INFO_POOL.returnObject(di);
//            } // FOR
//        } catch (Exception ex) {
//            throw new RuntimeException(ex);
//        } 
    }

    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        assert(this.hstore_site != null);
        assert(this.hstore_messenger != null);
        assert(this.self == null);
        this.self = Thread.currentThread();
        this.self.setName(this.getThreadName());
        this.preload();
        
        /*
        NDC.push("ExecutionSite - " + siteId + " index " + siteIndex);
        if (VoltDB.getUseThreadAffinity()) {
            final boolean startingAffinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
            for (int ii = 0; ii < startingAffinity.length; ii++) {
                log.l7dlog( Level.INFO, LogKeys.org_voltdb_ExecutionSite_StartingThreadAffinity.name(), new Object[] { startingAffinity[ii] }, null);
                startingAffinity[ii] = false;
            }
            startingAffinity[ siteIndex % startingAffinity.length] = true;
            org.voltdb.utils.ThreadUtils.setThreadAffinity(startingAffinity);
            final boolean endingAffinity[] = org.voltdb.utils.ThreadUtils.getThreadAffinity();
            for (int ii = 0; ii < endingAffinity.length; ii++) {
                log.l7dlog( Level.INFO, LogKeys.org_voltdb_ExecutionSite_EndingThreadAffinity.name(), new Object[] { endingAffinity[ii] }, null);
                startingAffinity[ii] = false;
            }
        }
        */
        
        // Setup the shutdown latch
        assert(this.shutdown_latch == null);
        this.shutdown_latch = new CountDownLatch(1);

        // Things that we will need in the loop below
        TransactionInfoBaseMessage work = null;
        boolean stop = false;
//        int poll_ctr = 0;
        try {
            if (d) LOG.debug("Starting ExecutionSite run loop...");
            while (stop == false && this.shutdown == false) {
                work = null;
//                if (poll_ctr++ > 10000000) {
//                    d = debug.get();
//                    t = trace.get();
//
//                    if (t) LOG.trace("Polling work queue [" + poll_ctr + "]: " + this.work_queue + "");
//                    if (this.error_counter.get() > 0) {
//                        LOG.warn("There were " + error_counter.get() + " errors since the last time we checked. You might want to enable debugging");
//                        this.error_counter.set(0);
//                    }
//                    poll_ctr = 0;
//                }
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                try {
                    work = this.work_queue.takeFirst();
                } catch (InterruptedException ex) {
                    if (d) LOG.debug("Interupted while polling work queue. Halting ExecutionSite...", ex);
                    stop = true;
                    break;
                }

                // -------------------------------
                // Execute Query Plan Fragments
                // -------------------------------
                if (work instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftask = (FragmentTaskMessage)work;
                    this.processFragmentTaskMessage(ftask);

                // -------------------------------
                // Invoke Stored Procedure
                // -------------------------------
                } else if (work instanceof InitiateTaskMessage) {
                    InitiateTaskMessage itask = (InitiateTaskMessage)work;
                    this.processInitiateTaskMessage(itask);
                    
                // -------------------------------
                // BAD MOJO!
                // -------------------------------
                } else if (work != null) {
                    throw new RuntimeException("Unexpected work message in queue: " + work);
                }

            } // WHILE
        } catch (final RuntimeException ex) {
            LOG.fatal("Unexpected error for ExecutionSite Partition #" + this.partitionId, ex);
            this.hstore_messenger.shutdownCluster(new Exception(ex));
        } catch (AssertionError ex) {
            LOG.fatal("Unexpected error for ExecutionSite Partition #" + this.partitionId, ex);
            this.hstore_messenger.shutdownCluster(new Exception(ex));
        } catch (Exception ex) {
            LOG.fatal("Unexpected error for ExecutionSite Partition #" + this.partitionId, ex);
            this.hstore_messenger.shutdownCluster(new Exception(ex));
//            throw new RuntimeException(ex);
        }
        
        // Release the shutdown latch in case anybody waiting for us
        this.shutdown_latch.countDown();
        
        // Stop HStoreMessenger (because we're nice)
        if (this.shutdown == false) {
            if (this.hstore_messenger != null) this.hstore_messenger.stop();
        }
        if (d) LOG.debug("ExecutionSite thread is stopping");
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
    }

    /**
     * Enable speculative execution mode for this partition
     */
    public synchronized boolean enableSpeculativeExecution(long txn_id) {
        // assert(this.speculative_execution == SpeculateType.DISABLED) : "Trying to enable spec exec twice because of txn #" + txn_id;
        
        // Check whether the txn that we're waiting for is read-only.
        // If it is, then that means all read-only transactions can commit right away
        Boolean readonly = this.isReadOnly(txn_id);
        if (readonly != null) {
            this.speculative_execution = (readonly ? SpeculateType.COMMIT_READONLY : SpeculateType.COMMIT_NONE);
            if (d) LOG.debug(String.format("Enabled %s speculative execution at partition %d [txn=#%d]", this.speculative_execution, partitionId, txn_id));
            return (true);
        }
        return (false);
    }

    public void setHStoreMessenger(HStoreMessenger hstore_messenger) {
        this.hstore_messenger = hstore_messenger;
    }
    
    public void setHStoreSite(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.thresholds = (hstore_site != null ? hstore_site.getThresholds() : null);
    }
    
    public BackendTarget getBackendTarget() {
        return (this.backend_target);
    }
    
    public ExecutionEngine getExecutionEngine() {
        return (this.ee);
    }
    public PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public TransactionEstimator getTransactionEstimator() {
        return (this.t_estimator);
    }
    
    public Site getCatalogSite() {
        return site;
    }
    
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    
    public HStoreSite getHStoreSite() {
        return hstore_site;
    }
    
    public int getHostId() {
        return getCatalogSite().getHost().getRelativeIndex();
        // return Integer.valueOf(getCatalogSite().getHost().getTypeName());
    }
    
    public int getSiteId() {
        return (this.siteId);
    }
    
    /**
     * Return the local partition id for this ExecutionSite
     * @return
     */
    public int getPartitionId() {
        return (this.partitionId);
    }
    
    public int getQueueSize() {
        return (this.work_queue.size());
    }
    
    public VoltProcedure getRunningVoltProcedure(long txn_id) {
        // assert(this.running_xacts.containsKey(txn_id)) : "No running VoltProcedure exists for txn #" + txn_id;
        LocalTransactionState ts = (LocalTransactionState)this.txn_states.get(txn_id);
        return (ts != null ? ts.getVoltProcedure() : null);
    }
    
    /**
     * Returns true if the txn is read-only at this partition
     * @param txn_id
     * @return
     */
    public Boolean isReadOnly(long txn_id) {
        TransactionState ts = this.txn_states.get(txn_id);
        return (ts == null ? null : ts.isExecReadOnly());
    }
    
    /**
     * Returns the VoltProcedure instance for a given stored procedure name
     * @param proc_name
     * @return
     */
    public VoltProcedure getVoltProcedure(String proc_name) {
        VoltProcedure volt_proc = null;
        try {
            volt_proc = (VoltProcedure)this.procPool.get(proc_name).borrowObject();
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get new " + proc_name + " VoltProcedure", ex);
        }
        assert(volt_proc != null);
        return (volt_proc);
    }

    public String getThreadName() {
        return (this.hstore_site.getThreadName(String.format("%03d", this.getPartitionId())));
    }
    
    // ---------------------------------------------------------------
    // VOLTPROCEDURE EXECUTION METHODS
    // ---------------------------------------------------------------

    
    /**
     * Execute a FragmentResponseMessage and update the TransactionState accordingly
     * @param ts
     * @param fresponse
     */
    protected void processFragmentResponseMessage(TransactionState ts, FragmentResponseMessage fresponse) {
        final long txn_id = ts.getTransactionId();
        if (t) 
            LOG.trace(String.format("FragmentResponseMessage [txn_id=%d, srcPartition=%d, deps=%d]",
                                    txn_id, fresponse.getSourcePartitionId(), fresponse.getTableCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (fresponse.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            if (t) LOG.trace("Received non-success response " + fresponse.getStatusCodeName() + " for txn #" + txn_id);
            ts.setPendingError(fresponse.getException());
        }
        for (int ii = 0, num_tables = fresponse.getTableCount(); ii < num_tables; ii++) {
            ts.addResponse(fresponse.getSourcePartitionId(), fresponse.getTableDependencyIdAtIndex(ii));
        } // FOR
        
        //int num_tables = response.getTableCount();
        //if (num_tables > 0) {
        //if (t) LOG.trace("CoordinatorResponse contains data for txn #" + txn_id);
        //for (int ii = 0; ii < num_tables; ii++) {
        //int dependency_id = response.getTableDependencyIdAtIndex(ii);
        //int partition = (int)response.getExecutorSiteId();
        //VoltTable table = response.getTableAtIndex(ii);
        //if (t) LOG.trace("[Response#" + i + " - Table#" + ii + "] Txn#=" + txn_id + ", Partition=" + partition + ", DependencyId=" + dependency_id + ", Table=" + table.getRowCount() + " tuples");
        //ts.addResult(partition, dependency_id, table);
        //} // FOR
        //} // FOR
    }
    
    /**
     * Execute a new transaction based on an InitiateTaskMessage
     * @param itask
     */
    protected void processInitiateTaskMessage(InitiateTaskMessage itask) {
        long txn_id = itask.getTxnId();
        String proc_name = itask.getStoredProcedureName();
        
        LocalTransactionState local_ts = (LocalTransactionState)this.txn_states.get(txn_id);
        assert(local_ts != null) : "The TransactionState is somehow null for txn #" + txn_id;
        if (hstore_conf.enable_profiling) local_ts.queue_time.stop();
        
        // We want to set this here, because it may be the case that we queue a bunch of transactions
        // when speculative execution was enabled, but now the transaction that was ahead of this 
        // one is finished, so now we're just executing them regularly
        SpeculateType spec_exec = SpeculateType.DISABLED;
        if (hstore_conf.enable_speculative_execution) {
            synchronized (this.speculative_execution) {
                if (this.speculative_execution != SpeculateType.DISABLED) { 
                    if (d) LOG.debug(String.format("Marking txn #%d as speculatively executed", txn_id));
                    spec_exec = this.speculative_execution;
                    local_ts.setSpeculative(true);
                    assert(this.speculative_latch == null || this.speculative_latch.getCount() == 0);
                    if (t) LOG.trace(String.format("%s txn #%d is setting spec exec latch for partition %d", local_ts.getProcedureName(),  txn_id, this.partitionId));
                    this.speculative_latch = new CountDownLatch(1);
                }
            } // SYNCH
        }
        
        VoltProcedure volt_proc = null;
        try {
            volt_proc = this.getVoltProcedure(proc_name);
        } catch (AssertionError ex) {
            LOG.error("Unrecoverable error for txn #" + txn_id);
            LOG.error("InitiateTaskMessage= " + itask.getDumpContents().toString());
            throw ex;
        }
        assert(volt_proc != null);
        local_ts.setVoltProcedure(volt_proc);
        
        if (t) LOG.trace(String.format("Starting execution of txn #%d [proc=%s]", txn_id, proc_name));
        ClientResponseImpl cresponse = (ClientResponseImpl)volt_proc.call(local_ts, itask.getParameters()); // Blocking...
        assert(cresponse != null && this.isShuttingDown() == false) : String.format("No ClientResponse for %s txn #%d???", local_ts.getProcedureName(), txn_id);
        
        // If this transaction was speculatively executed, then see whether we are still waiting for a response 
        // from the remote transaction. We can send back the response if we know that the txn was read-only.
        // How nice!
        if (spec_exec == SpeculateType.COMMIT_NONE) {
            // Always queue our response, since we know that whatever thread is out there
            // is waiting for us to finish before it drains the queued responses
            if (t) LOG.trace(String.format("Queuing ClientResponse for txn #%d [status=%s]", txn_id, cresponse.getStatusName()));
            this.queueClientResponse(local_ts, cresponse);
            
        // Otherwise send the response back immediately! 
        } else {
            if (t) LOG.trace(String.format("Sending ClientResponse back for txn #%d [status=%s]", txn_id, cresponse.getStatusName()));
            this.sendClientResponse(local_ts, cresponse);
        }
        
        // Release the latch in case anybody is waiting for us to finish
        if (hstore_conf.enable_speculative_execution && spec_exec != SpeculateType.DISABLED) {
            if (t) LOG.trace(String.format("Txn #%d is releasing spec exec latch for partition %d", txn_id, this.partitionId));
            this.speculative_latch.countDown();
        }
    }

    /**
     * 
     * @param ftask
     * @throws Exception
     */
    protected void processFragmentTaskMessage(FragmentTaskMessage ftask) {
        long txn_id = ftask.getTxnId();
        int txn_partition_id = ftask.getSourcePartitionId();
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        // A txn is "local" if the Java is executing at the same site as we are
        boolean is_local = ts.isExecLocal();
        boolean is_dtxn = ftask.isUsingDtxnCoordinator();
        if (t) LOG.trace(String.format("Executing FragmentTaskMessage txn #%d [partition=%d, is_local=%s, is_dtxn=%s, fragments=%s]",
                         txn_id, ftask.getSourcePartitionId(), is_local, is_dtxn, Arrays.toString(ftask.getFragmentIds())));

        // If this txn isn't local, then we have to update our undoToken
        if (is_local == false) {
            ts.initRound(this.getNextUndoToken());
            ts.startRound();
        }

        FragmentResponseMessage fresponse = new FragmentResponseMessage(ftask);
        fresponse.setStatus(FragmentResponseMessage.NULL, null);
        assert(fresponse.getSourcePartitionId() == this.getPartitionId()) : "Unexpected source partition #" + fresponse.getSourcePartitionId() + "\n" + fresponse;
        
        DependencySet result = null;
        try {
            result = this.executeFragmentTaskMessage(ftask, ts.getLastUndoToken());
            fresponse.setStatus(FragmentResponseMessage.SUCCESS, null);
        } catch (EEException ex) {
            LOG.fatal(String.format("Hit an EE Error for txn #%d", txn_id));
            this.crash(ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
        } catch (SQLException ex) {
            String extra = "";
            if (ts instanceof LocalTransactionState) {
                extra = " [" + ((LocalTransactionState)ts).getProcedure().getName() + "]";
            }
            LOG.warn("Hit a SQL Error for txn #" + txn_id + extra, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
        } catch (RuntimeException ex) {
            LOG.warn("Something unexpected and bad happended for txn #" + txn_id, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
        } catch (Exception ex) {
            LOG.warn("Something unexpected and bad happended for txn #" + txn_id, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
        } finally {
            // Success, but without any results???
            if (result == null && fresponse.getStatusCode() == FragmentResponseMessage.SUCCESS) {
                Exception ex = new Exception("The Fragment executed successfully but result is null!");
                if (d) LOG.warn(ex);
                fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
            }
        }
        
        // -------------------------------
        // ERROR
        // -------------------------------
        if (fresponse.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            this.error_counter.getAndIncrement();
            if (is_local && is_dtxn == false) {
                this.processFragmentResponseMessage(ts, fresponse);
            } else {
                this.sendFragmentResponseMessage(ftask, fresponse);
            }
            
        // -------------------------------
        // SUCCESS!
        // ------------------------------- 
        } else {
            // XXX: For single-sited INSERT/UPDATE/DELETE queries, we don't directly
            // execute the SendPlanNode in order to get back the number of tuples that
            // were modified. So we have to rely on the output dependency ids set in the task
            assert(result.size() == ftask.getOutputDependencyIds().length) :
                "Got back " + result.size() + " results but was expecting " + ftask.getOutputDependencyIds().length;
            
            // If the transaction is local, store the result in the local TransactionState
            // Unless we were sent this FragmentTaskMessage through the Dtxn.Coordinator
            if (is_local && is_dtxn == false) {
                if (d) LOG.debug("Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                assert(ts != null);
                LocalTransactionState local_ts = (LocalTransactionState)ts;
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    int dep_id = ftask.getOutputDependencyIds()[i];
                    // ts.addResult(result.depIds[i], result.dependencies[i]);
                    if (t) LOG.trace("Storing DependencyId #" + dep_id  + " for txn #" + txn_id);
                    local_ts.addResultWithResponse(this.partitionId, dep_id, result.dependencies[i]);
                } // FOR
                
            // Otherwise push dependencies back to the remote partition that needs it
            } else {
                if (d) LOG.debug(String.format("Constructing FragmentResponseMessage %s with %d bytes from partition %d to send back to initial partition %d for txn #%d",
                                               Arrays.toString(ftask.getFragmentIds()), result.size(), this.partitionId, txn_partition_id, txn_id));
                
                // We need to include the DependencyIds in our response, but we will send the actual
                // data through the HStoreMessenger
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    fresponse.addDependency(result.depIds[i]);
                } // FOR
                this.sendFragmentResponseMessage(ftask, fresponse);
                
                // Bombs away!
                this.hstore_messenger.sendDependencySet(txn_id, this.partitionId, txn_partition_id, result); 
            }
        }
        
        // Again, if we're not local, just clean up our TransactionState
        if (is_local == false && is_dtxn == true) {
            if (d) LOG.debug("Executed non-local FragmentTask " + Arrays.toString(ftask.getFragmentIds()) + ". Notifying TransactionState for txn #" + txn_id + " to finish round");
            ts.finishRound();
        }
    }
    
    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the resulting DependencySet
     * @param ftask
     * @return
     * @throws Exception
     */
    protected DependencySet executeFragmentTaskMessage(FragmentTaskMessage ftask, long undoToken) throws Exception {
        DependencySet result = null;
        long txn_id = ftask.getTxnId();
        int fragmentIdIndex = ftask.getFragmentCount();
        long fragmentIds[] = ftask.getFragmentIds();
        int output_depIds[] = ftask.getOutputDependencyIds();
        int input_depIds[] = ftask.getAllUnorderedInputDepIds(); // Is this ok?
        
        if (fragmentIdIndex == 0) {
            LOG.warn("Got a FragmentTask for txn #" + txn_id + " that does not have any fragments?!?");
            return (result);
        }
        
        if (d) LOG.debug("Getting ready to kick " + ftask.getFragmentCount() + " fragments to EE for txn #" + txn_id); 
        if (t) LOG.trace("FragmentTaskIds: " + Arrays.toString(ftask.getFragmentIds()));
        ParameterSet parameterSets[] = new ParameterSet[fragmentIdIndex];
        for (int i = 0; i < fragmentIdIndex; i++) {
            ByteBuffer paramData = ftask.getParameterDataForFragment(i);
            if (paramData != null) {
                paramData.rewind();
                final FastDeserializer fds = new FastDeserializer(paramData);
                if (t) LOG.trace("Txn #" + txn_id + "->paramData[" + i + "] => " + fds.buffer());
                try {
                    parameterSets[i] = fds.readObject(ParameterSet.class);
                } catch (final IOException e) {
                    LOG.fatal(e);
                    VoltDB.crashVoltDB();
                } catch (Exception ex) {
                    LOG.fatal("Failed to deserialize ParameterSet[" + i + "] for FragmentTaskMessage " + fragmentIds[i] + " in txn #" + txn_id, ex);
                    throw ex;
                }
                // LOG.info("PARAMETER[" + i + "]: " + parameterSets[i]);
            } else {
                parameterSets[i] = new ParameterSet();
            }
        } // FOR
        
        // TODO(pavlo): Can this always be empty?
        TransactionState ts = this.txn_states.get(txn_id);
        assert(ts != null) : "Missing TransactionState for txn #" + txn_id;
        LocalTransactionState local_ts = (ts instanceof LocalTransactionState ? (LocalTransactionState)ts : null);
        ts.ee_dependencies.clear();
        if (ftask.hasAttachedResults()) {
            if (t) LOG.trace("Retrieving internal dependency results attached to FragmentTaskMessage for txn #" + txn_id);
            ts.ee_dependencies.putAll(ftask.getAttachedResults());
        }
        if (ftask.hasInputDependencies() && ts != null && ts.isExecLocal() == true) {
            local_ts = (LocalTransactionState)ts; 
            if (local_ts.getInternalDependencyIds().isEmpty() == false) {
                if (t) LOG.trace("Retrieving internal dependency results from TransactionState for txn #" + txn_id);
                ts.ee_dependencies.putAll(local_ts.removeInternalDependencies(ftask));
            }
        }

        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ftask.isSysProcTask()) {
            assert(fragmentIds.length == 1);
            long fragment_id = (long)fragmentIds[0];

            VoltSystemProcedure volt_proc = null;
            synchronized (this.m_registeredSysProcPlanFragments) {
                volt_proc = this.m_registeredSysProcPlanFragments.get(fragment_id);
            }
            if (volt_proc == null) throw new RuntimeException("No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments);
            
            // HACK: We have to set the TransactionState for sysprocs manually
            volt_proc.setTransactionState(ts);
            ts.setExecReadOnly(false);
            result = volt_proc.executePlanFragment(txn_id, ts.ee_dependencies, (int)fragmentIds[0], parameterSets[0], this.m_systemProcedureContext);
            if (t) LOG.trace("Finished executing sysproc fragments for " + volt_proc.getClass().getSimpleName());
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            if (local_ts != null && hstore_conf.enable_profiling) local_ts.ee_time.start();
            result = this.executePlanFragments(ts, undoToken, fragmentIdIndex, fragmentIds, parameterSets, output_depIds, input_depIds);
            if (local_ts != null && hstore_conf.enable_profiling) local_ts.ee_time.stop();
        }
        return (result);
    }
    
    /**
     * Execute a BatcPlan directly on this ExecutionSite without having to covert it
     * to FragmentTaskMessages first. This is big speed improvement over having to queue things up
     * @param local_ts
     * @param plan
     * @return
     */
    protected VoltTable[] executeLocalPlan(LocalTransactionState local_ts, BatchPlanner.BatchPlan plan) {
        long undoToken;
        if (local_ts.isPredictAbortable()) {
            undoToken = this.getNextUndoToken();
        } else {
            if (t) LOG.trace(String.format("Bold! Not using undo buffers for %s txn #%d", local_ts.getProcedureName(), local_ts.getTransactionId()));
            undoToken = Long.MAX_VALUE;
        }
        local_ts.fastInitRound(undoToken);
      
        long fragmentIds[] = plan.getFragmentIds();
        int fragmentIdIndex = plan.getFragmentCount();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        ParameterSet parameterSets[] = plan.getParameterSets();
        
        // Mark that we touched the local partition once for each query in the batch
        local_ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not a single-partition transaction
        if (hstore_conf.enable_speculative_execution && local_ts.isPredictSinglePartition() == false) {
            this.notifyDonePartitions(local_ts);
        }

        if (t) {
            StringBuilder sb = new StringBuilder();
            sb.append("Parameters:");
            for (int i = 0; i < parameterSets.length; i++) {
                sb.append(String.format("\n [%02d] %s", i, parameterSets[i].toString()));
            }
            LOG.trace(sb.toString());

//        FragmentTaskMessage ftask = plan.getFragmentTaskMessages().get(0);
//        LOG.info(String.format("Txn #%d - FTASK:\n" +
//                "  fragmentIds:     %s\n" + 
//                "  fragmentIdIndex: %s\n" +
//                "  output_depIds:   %s\n" +
//                "  input_depIds:    %s",
//                local_ts.getTransactionId(),
//                Arrays.toString(ftask.getFragmentIds()), ftask.getFragmentCount(), Arrays.toString(ftask.getOutputDependencyIds()), Arrays.toString(ftask.getAllUnorderedInputDepIds())));

            LOG.trace(String.format("Txn #%d - BATCHPLAN:\n" +
                     "  fragmentIds:     %s\n" + 
                     "  fragmentIdIndex: %s\n" +
                     "  output_depIds:   %s\n" +
                     "  input_depIds:    %s",
                     local_ts.getTransactionId(),
                     Arrays.toString(plan.getFragmentIds()), plan.getFragmentCount(), Arrays.toString(plan.getOutputDependencyIds()), Arrays.toString(plan.getInputDependencyIds())));
        }

        if (local_ts != null && hstore_conf.enable_profiling) local_ts.ee_time.start();
        DependencySet result = this.executePlanFragments(local_ts, undoToken, fragmentIdIndex, fragmentIds, parameterSets, output_depIds, input_depIds);
        if (local_ts != null && hstore_conf.enable_profiling) local_ts.ee_time.stop();
      
        if (t) LOG.trace("Output:\n" + StringUtil.join("\n", result.dependencies));
        
        local_ts.fastFinishRound();
        return (result.dependencies);
    }
    
    /**
     * Execute the given fragment tasks on this site's underlying EE
     * @param ts
     * @param undoToken
     * @param fragmentIdIndex
     * @param fragmentIds
     * @param parameterSets
     * @param output_depIds
     * @param input_depIds
     * @return
     */
    private DependencySet executePlanFragments(TransactionState ts, long undoToken, int fragmentIdIndex, long fragmentIds[], ParameterSet parameterSets[], int output_depIds[], int input_depIds[]) {
        assert(this.ee != null) : "The EE object is null. This is bad!";
        long txn_id = ts.getTransactionId();
        
        if (t) {
            LOG.trace("Executing " + fragmentIdIndex + " fragments for txn #" + txn_id + " [lastCommittedTxnId=" + this.lastCommittedTxnId + ", undoToken=" + undoToken + "]");
            LOG.trace("Fragments:           " + Arrays.toString(fragmentIds));
            LOG.trace("Parameters:          " + Arrays.toString(parameterSets));
            LOG.trace("Input Dependencies:  " + Arrays.toString(input_depIds));
            LOG.trace("Output Dependencies: " + Arrays.toString(output_depIds));
        }

        // pass attached dependencies to the EE (for non-sysproc work).
        if (ts.ee_dependencies.isEmpty() == false) {
            if (t) LOG.trace("Stashing Dependencies: " + ts.ee_dependencies.keySet());
//            assert(dependencies.size() == input_depIds.length) : "Expected " + input_depIds.length + " dependencies but we have " + dependencies.size();
            ee.stashWorkUnitDependencies(ts.ee_dependencies);
        }
        ts.setSubmittedEE();
        
        // Check whether this fragments are read-only
        if (ts.isExecReadOnly()) {
            boolean readonly = CatalogUtil.areFragmentsReadOnly(this.database, fragmentIds, fragmentIdIndex); 
            if (readonly == false) {
                if (t) LOG.trace(String.format("Marking txn #%d as not read-only %s", txn_id, Arrays.toString(fragmentIds))); 
                ts.setExecReadOnly(false);
            }
        }
        
        DependencySet result = null;
        try {
            if (t) LOG.trace(String.format("Executing fragments at partition %d for txn #%d", this.partitionId, txn_id));
            result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                            fragmentIds,
                            fragmentIdIndex,
                            input_depIds,
                            output_depIds,
                            parameterSets,
                            fragmentIdIndex,
                            txn_id,
                            this.lastCommittedTxnId,
                            undoToken);
        } catch (RuntimeException ex) {
            LOG.fatal(String.format("Failed to execute PlanFragments for txn #%d: %s", txn_id, Arrays.toString(fragmentIds)));
            throw ex;
        }
        
        if (t) LOG.trace("Executed fragments " + Arrays.toString(fragmentIds) + " and got back results: " + Arrays.toString(result.depIds)); //  + "\n" + Arrays.toString(result.dependencies));
        return (result);
    }
    
    /**
     * Store the given VoltTable as an input dependency for the given txn
     * @param txn_id
     * @param sender_partition_id
     * @param dependency_id
     * @param data
     */
    public void storeDependency(long txn_id, int sender_partition_id, int dependency_id, VoltTable data) {
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = String.format("No transaction state for txn #%d at partition %s", txn_id, this.partitionId);
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        ts.addResult(sender_partition_id, dependency_id, data);
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
    public void loadTable(long txn_id, String clusterName, String databaseName, String tableName, VoltTable data, int allowELT) throws VoltAbortException {
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

        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        ts.setSubmittedEE();
        ee.loadTable(table.getRelativeIndex(), data,
                     txn_id,
                     lastCommittedTxnId,
                     getNextUndoToken(),
                     allowELT != 0);
    }
    
    // ---------------------------------------------------------------
    // ExecutionSite API
    // ---------------------------------------------------------------

    /**
     * 
     */
    protected synchronized void cleanupTransaction(TransactionState ts) {
        long txn_id = ts.getTransactionId();
        if (t) LOG.trace("Cleaning up internal state information for Txn #" + txn_id);
        assert(ts.isMarkedFinished());
        this.txn_states.remove(txn_id);
        
        try {
            if (ts.isExecLocal()) {
                VoltProcedure volt_proc = ((LocalTransactionState)ts).getVoltProcedure();
                this.procPool.get(volt_proc.getProcedureName()).returnObject(volt_proc);
                this.localTxnPool.returnObject(ts);
            } else {
                this.remoteTxnPool.returnObject(ts);
            }
        } catch (Exception ex) {
            LOG.fatal("Failed to return TransactionState for txn #" + txn_id, ex);
            throw new RuntimeException(ex);
        }
    }
    
    public Long getLastCommittedTxnId() {
        return (this.lastCommittedTxnId);
    }

    /**
     * Returns the next undo token to use when hitting up the EE with work
     * MAX_VALUE = no undo
     * catProc.getReadonly() controls this in the original implementation
     * @param txn_id
     * @return
     */
    public synchronized long getNextUndoToken() {
        return (++this.lastUndoToken);
    }


    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void doWork(TransactionInfoBaseMessage task, RpcCallback<Dtxn.FragmentResponse> callback) {
        long txn_id = task.getTxnId();
        long client_handle = task.getClientHandle();
        boolean start_txn = (task instanceof InitiateTaskMessage);
        
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            try {
                if (start_txn) {
                    boolean single_partitioned = true;
                    
                    ts = (LocalTransactionState)localTxnPool.borrowObject();
                    assert(callback != null) : "Missing coordinator callback for txn #" + txn_id;
                    LocalTransactionState local_ts = (LocalTransactionState)ts;
                    local_ts.setCoordinatorCallback(callback);
                    local_ts.setPredictSinglePartitioned(single_partitioned);
                    local_ts.setEstimatorState(this.t_estimator.getState(txn_id));
                    if (d) LOG.debug(String.format("Starting new VoltProcedure invocation for txn #%d [partition=%d, singlepartitioned=%s]", txn_id, this.partitionId, single_partitioned));
                } else {
                    // Remote Transaction
                    ts = (RemoteTransactionState)remoteTxnPool.borrowObject();
                    if (d) LOG.debug(String.format("Creating new RemoteTransactionState for txn #%d running at partition %d [local_partition=%d, singlepartitioned=%s]", txn_id, task.getSourcePartitionId(), this.partitionId, false));
                }
            } catch (Exception ex) {
                LOG.fatal("Failed to construct TransactionState for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
            
            // Initialize the internal data structures
            ts.init(txn_id, client_handle, task.getSourcePartitionId());
            
            this.txn_states.put(txn_id, ts);
            if (t) LOG.trace(String.format("Creating transaction state for txn #%d [partition=%d]", txn_id, this.getPartitionId()));
        }

        // Remote Work
        if (start_txn == false) {
            FragmentTaskMessage ftask = (FragmentTaskMessage)task;
            if (callback != null) {
                if (t) LOG.trace("Storing FragmentTask callback in TransactionState for txn #" + txn_id);
                ts.setFragmentTaskCallback(ftask, callback);
            } else {
                assert(ftask.isUsingDtxnCoordinator() == false) : "No callback for remote execution request for txn #" + txn_id;
            }
        }
        this.work_queue.add(task);
    }

    /**
     * New work for a local transaction
     * @param task
     * @param callback
     * @param ts
     */
    public void doWork(TransactionInfoBaseMessage task, RpcCallback<Dtxn.FragmentResponse> callback, LocalTransactionState ts) {
        long txn_id = task.getTxnId();
        assert(ts != null) : "The TransactionState is somehow null for txn #" + txn_id;
        if (d) LOG.debug(String.format("Adding work request for txn #%d on partition %d with%s a callback", txn_id, this.partitionId, (callback == null ? "out" : "")));
        this.txn_states.put(txn_id, ts);
        this.work_queue.add(task);
    }

    /**
     * 
     * @param fresponse
     */
    public void sendFragmentResponseMessage(FragmentTaskMessage ftask, FragmentResponseMessage fresponse) {
        long txn_id = fresponse.getTxnId();
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        RpcCallback<Dtxn.FragmentResponse> callback = ts.getFragmentTaskCallback(ftask);
        if (callback == null) {
            LOG.fatal("Unable to send FragmentResponseMessage:\n" + fresponse.toString());
            LOG.fatal("Orignal FragmentTaskMessage:\n" + ftask);
            LOG.fatal(ts.toString());
            throw new RuntimeException("No RPC callback to HStoreCoordinator for txn #" + txn_id);
        }

        BBContainer bc = fresponse.getBufferForMessaging(buffer_pool);
        assert(bc.b.hasArray());
        ByteString bs = ByteString.copyFrom(bc.b.array());
//        ByteBuffer serialized = bc.b.asReadOnlyBuffer();
//        LOG.info("Serialized FragmentResponseMessage [size=" + serialized.capacity() + ",id=" + serialized.get(VoltMessage.HEADER_SIZE) + "]");
//        assert(serialized.get(VoltMessage.HEADER_SIZE) == VoltMessage.FRAGMENT_RESPONSE_ID);
        
        if (d) LOG.debug(String.format("Sending FragmentResponseMessage for txn #%d [partition=%d, size%d]", txn_id, this.partitionId, bs.size()));
        Dtxn.FragmentResponse.Builder builder = Dtxn.FragmentResponse.newBuilder().setOutput(bs);
        
        switch (fresponse.getStatusCode()) {
            case FragmentResponseMessage.SUCCESS:
                builder.setStatus(Dtxn.FragmentResponse.Status.OK);
                break;
            case FragmentResponseMessage.UNEXPECTED_ERROR:
            case FragmentResponseMessage.USER_ERROR:
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_USER);
                break;
            default:
                assert(false) : "Unexpected FragmentResponseMessage status '" + fresponse.getStatusCode() + "'";
        } // SWITCH
        callback.run(builder.build());
        bc.discard();
    }
    
    /**
     * Figure out what partitions this transaction is done with and notify those partitions
     * that they are done
     * @param ts
     */
    private void notifyDonePartitions(LocalTransactionState ts) {
        long txn_id = ts.getTransactionId();
        Set<Integer> ts_done_partitions = ts.getDonePartitions();
        Set<Integer> new_done = null;

        TransactionEstimator.State t_state = ts.getEstimatorState();
        if (t_state == null) {
            return;
        }
        
        if (d) LOG.debug(String.format("Checking MarkovEstimate for txn #%d to see whether we can notify any partitions that we're done with them [round=%d]", txn_id, ts.getCurrentRound()));
        
        MarkovEstimate estimate = t_state.getLastEstimate();
        assert(estimate != null) : "Got back null MarkovEstimate for txn #" + txn_id;
        new_done = estimate.getFinishedPartitions(this.thresholds);
        
        if (new_done.isEmpty() == false) { 
            // Note that we can actually be done with ourself, if this txn is only going to execute queries
            // at remote partitions. But we can't actually execute anything because this partition's only 
            // execution thread is going to be blocked. So we always do this so that we're not sending a 
            // useless message
            new_done.remove(this.partitionId);
            
            // Make sure that we only tell partitions that we actually touched, otherwise they will
            // be stuck waiting for a finish request that will never come!
            Set<Integer> ts_touched = ts.getTouchedPartitions().values();

            // Mark the txn done at this partition if the MarkovEstimate said we were done
            this.done_partitions.clear();
            for (Integer p : new_done) {
                if (ts_done_partitions.contains(p) == false && ts_touched.contains(p)) {
                    if (t) LOG.trace(String.format("Marking partition %d as done for txn #%d", p, txn_id));
                    ts_done_partitions.add(p);
                    this.done_partitions.add(p);
                }
            } // FOR
            
            if (this.done_partitions.size() > 0) {
                if (d) LOG.debug(String.format("Marking %d partitions as done after this fragment for txn #%d %s\n%s",
                                 this.done_partitions.size(), txn_id, this.done_partitions, estimate));
                this.hstore_messenger.sendDoneAtPartitions(txn_id, this.done_partitions);
            }
        }
    }

    /**
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    private void requestWork(LocalTransactionState ts, List<FragmentTaskMessage> tasks) {
        assert(!tasks.isEmpty());
        assert(ts != null);
        long txn_id = ts.getTransactionId();

        if (t) LOG.trace("Combining " + tasks.size() + " FragmentTaskMessages into a single Dtxn.CoordinatorFragment.");
        
        // Now we can go back through and start running all of the FragmentTaskMessages that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each FragmentTaskMessage in its own message
        Dtxn.CoordinatorFragment.Builder requestBuilder = Dtxn.CoordinatorFragment
                                                                .newBuilder()
                                                                .setTransactionId(txn_id);
        
        // If our transaction was originally designated as a single-partitioned, then we need to make
        // sure that we don't touch any partition other than our local one. If we do, then we need abort
        // it and restart it as multi-partitioned
        boolean need_restart = false;
        boolean predict_singlepartition = ts.isPredictSinglePartition(); 
        Set<Integer> done_partitions = ts.getDonePartitions();
        
        if (hstore_conf.enable_speculative_execution) this.notifyDonePartitions(ts);
        
        for (FragmentTaskMessage ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            // Important! Set the UsingDtxn flag for each FragmentTaskMessage so that we know
            // how to respond on the other side of the request
            ftask.setUsingDtxnCoordinator(true);
            
            int target_partition = ftask.getDestinationPartitionId();
            // Make sure things are still legit for our single-partition transaction
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (d) LOG.debug(String.format("Txn #%d on partition %d is suppose to be single-partitioned, but it wants to execute a fragment on partition %d", txn_id, this.partitionId, target_partition));
                need_restart = true;
                break;
            } else if (done_partitions.contains(target_partition)) {
                if (d) LOG.debug(String.format("Txn #%d on partition %d was marked as done on partition %d but now it wants to go back for more!", txn_id, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            
            int dependency_ids[] = ftask.getOutputDependencyIds();
            if (t) LOG.trace(String.format("Preparing to request fragments %s on partition %d to generate %d output dependencies for txn #%d",
                                           Arrays.toString(ftask.getFragmentIds()), target_partition, dependency_ids.length, txn_id));
            if (ftask.getFragmentCount() == 0) {
                LOG.warn("Trying to send a FragmentTask request with 0 fragments for txn #" + txn_id);
                continue;
            }

            // Since we know that we have to send these messages everywhere, then any internal dependencies
            // that we have stored locally here need to go out with them
            if (ftask.hasInputDependencies()) {
                ts.remove_dependencies_map.clear();
                ts.removeInternalDependencies(ftask, ts.remove_dependencies_map);
                if (t) LOG.trace("Attaching " + ts.remove_dependencies_map.size() + " dependencies to " + ftask);
                for (Entry<Integer, List<VoltTable>> e : ts.remove_dependencies_map.entrySet()) {
                    ftask.attachResults(e.getKey(), e.getValue());
                } // FOR
            }
            
            // Nasty...
            ByteString bs = ByteString.copyFrom(ftask.getBufferForMessaging(buffer_pool).b.array());
            requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                    .setPartitionId(target_partition)
                    .setWork(bs)
            );
            
        } // FOR (tasks)

        // Bad mojo! We need to throw a MispredictionException so that the VoltProcedure
        // will catch it and we can propagate the error message all the way back to the HStoreSite
        if (need_restart) {
            if (t) LOG.trace(String.format("Aborting txn #%d because it was mispredicted", txn_id));
            // XXX: This is kind of screwy because we don't actually want to send the touched partitions
            // histogram because VoltProcedure will just do it for us...
            throw new MispredictionException(txn_id, null);
        }
        
        // Bombs away!
        Dtxn.CoordinatorFragment dtxn_request = requestBuilder.build(); 
        this.hstore_site.requestWork(ts, dtxn_request, this.request_work_callback);
        if (d) LOG.debug(String.format("Work request is sent for txn #%d [bytes=%d, #fragments=%d]", txn_id, dtxn_request.getSerializedSize(), tasks.size()));
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do... 
     * @param txn_id
     * @param dependency_ids
     * @return
     */
    protected VoltTable[] waitForResponses(long txn_id, List<FragmentTaskMessage> tasks, int batch_size) {
        LocalTransactionState ts = (LocalTransactionState)this.txn_states.get(txn_id);
        if (ts == null) {
            throw new RuntimeException("No transaction state for txn #" + txn_id + " at " + this.getThreadName());
        }
        return (this.waitForResponses(ts, tasks, batch_size));
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do... 
     * @param ts
     * @param dependency_ids
     * @return
     */
    protected VoltTable[] waitForResponses(LocalTransactionState ts, List<FragmentTaskMessage> tasks, int batch_size) {
        final long txn_id = ts.getTransactionId();

        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.getNextUndoToken());
        ts.setBatchSize(batch_size);
        boolean all_local = true;
        boolean is_local;
        for (FragmentTaskMessage ftask : tasks) {
            is_local = (ftask.getDestinationPartitionId() == this.partitionId);
            all_local = all_local && is_local;
            if (ts.addFragmentTaskMessage(ftask) == false) {
                if (is_local) ts.local_fragment_list.add(ftask);
                else ts.remote_fragment_list.add(ftask);
                
            }
        } // FOR
        int num_local = ts.local_fragment_list.size();
        int num_remote = ts.remote_fragment_list.size();
        if (num_local == 0 && num_remote == 0) {
            throw new RuntimeException("Deadlock! All tasks for txn #" + txn_id + " are blocked waiting on input!");
        }
        
        // We have to tell the TransactinState to start the round before we send off the
        // FragmentTasks for execution, since they might start executing locally!
        ts.startRound();
        
        // First request the fragments that aren't local
        // We want to push these out as soon as possible
        if (num_remote > 0) {
            if (t) LOG.trace(String.format("Requesting %d FragmentTaskMessages to be executed on remote partitions", num_remote));
            this.requestWork(ts, ts.remote_fragment_list);
        }
        
        // Then execute all of the tasks are meant for the local partition directly
        if (num_local > 0) {
            if (t) LOG.trace(String.format("Executing %d FragmentTaskMessages directly in local partition", num_local));
            try {
                for (FragmentTaskMessage ftask : ts.local_fragment_list) {
                    this.processFragmentTaskMessage(ftask);
                } // FOR
            } catch (Exception ex) {
                LOG.error("Unexpected error when executing fragments for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
        }
        
        // Now if we have some work sent out to other partitions, we need to wait until they come back
        // In the first part, we wait until all of our blocked FragmentTaskMessages become unblocked
        LinkedBlockingDeque<FragmentTaskMessage> queue = ts.getUnblockedFragmentTaskMessageQueue();
        FragmentTaskMessage ftask = null;
        while (ts.getBlockedFragmentTaskMessageCount() > 0 || queue.size() > 0) {
            try {
                ftask = queue.takeFirst(); // BLOCKING
            } catch (InterruptedException ex) {
                if (this.hstore_site.isShuttingDown() == false) LOG.error("We were interrupted while waiting for blocked tasks for txn #" + txn_id, ex);
                return (null);
            }
            assert(ftask != null);
            assert(ftask.getTxnId() == txn_id);
            
            // Local
            if (ftask.getDestinationPartitionId() == this.partitionId) {
                if (t) LOG.trace(String.format("Got unblocked FragmentTaskMessage for txn #%d. Executing locally...", txn_id));
                this.processFragmentTaskMessage(ftask);
            // Remote
            } else {
                if (t) LOG.trace(String.format("Got unblocked FragmentTaskMessage for txn #%d. Requesting that it be executed remotely...", txn_id));
                ts.remote_fragment_list.clear();
                ts.remote_fragment_list.add(ftask);
                this.requestWork(ts, ts.remote_fragment_list);
            }
        } // WHILE

        // Now that we know all of our FragmentTaskMessages have been dispatched, we can then
        // wait for all of the results to come back in.
        CountDownLatch latch = ts.getDependencyLatch();
        if (d) LOG.debug(String.format("All blocked messages dispatched for txn #%d. Waiting for %d dependencies", txn_id, latch.getCount()));
        if (t) LOG.trace(ts.toString());
        if (latch.getCount() > 0) {
            try {
                latch.await();
            } catch (InterruptedException ex) {
                if (this.hstore_site.isShuttingDown() == false) LOG.error("We were interrupted while waiting for results for txn #" + txn_id, ex);
                return (null);
            } catch (Exception ex) {
                LOG.fatal("Fatal error for txn #" + txn_id + " while waiting for results", ex);
                System.exit(1);
            }
        }

        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (d) LOG.warn("Txn #" + txn_id + " was hit with a " + ts.getPendingError().getClass().getSimpleName());
            throw ts.getPendingError();
        }
        
        // Important: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        if (t) LOG.trace("Txn #" + txn_id + " is now running and looking for love in all the wrong places...");
        final VoltTable results[] = ts.getResults();
        ts.finishRound();
        if (t) LOG.trace("Txn #" + txn_id + " is returning back " + results.length + " tables to VoltProcedure");
        return (results);
    }

    // ---------------------------------------------------------------
    // COMMIT + ABORT METHODS
    // ---------------------------------------------------------------

    
    /**
     * Queue a specutatively executed transaction to send its ClientResponseImpl message
     */
    protected void queueClientResponse(LocalTransactionState ts, ClientResponseImpl cresponse) {
//        if (d) 
            LOG.info(String.format("Queuing ClientResponseImpl for %s txn #%d [clientHandle=%s, status=%s]",
                                ts.getProcedureName(), ts.getTransactionId(), ts.getClientHandle(), cresponse.getStatusName()));
        assert(ts.isExecSinglePartition()) : "Specutatively executed multi-partition txn #" + ts.getTransactionId();
        assert(ts.isSpeculative()) : "Queueing non-specutative txn #" + ts.getTransactionId();
        assert(this.speculative_execution != SpeculateType.DISABLED) : "Queueing when not in speculative execution mode for txn #" + ts.getTransactionId();
        this.queued_responses.add(Pair.of(ts, cresponse));
//        if (d) 
            LOG.info("Total # of Queued Responses: " + this.queued_responses.size());
    }

    /**
     * Send a ClientResponseImpl message back to the coordinator
     */
    protected void sendClientResponse(LocalTransactionState ts, ClientResponseImpl cresponse) {
        long txn_id = cresponse.getTransactionId();
        RpcCallback<Dtxn.FragmentResponse> callback = ts.getCoordinatorCallback();
        if (callback == null) {
            throw new RuntimeException("No RPC callback to HStoreCoordinator for txn #" + txn_id);
        }
        long client_handle = cresponse.getClientHandle();
        assert(client_handle != -1) : "The client handle for txn #" + txn_id + " was not set properly";
        byte status = cresponse.getStatus();
        
        if (d) LOG.debug(String.format("Sending ClientResponseImpl back for %s txn #%d [clientHandle=%s, status=%s]",
                                       ts.getProcedureName(), txn_id, cresponse.getClientHandle(), cresponse.getStatusName()));
        Dtxn.FragmentResponse.Builder builder = Dtxn.FragmentResponse.newBuilder();

        // Always mark the throttling flag so that the clients know whether they are allowed to keep
        // coming at us and make requests
        cresponse.setThrottleFlag(this.hstore_site.isThrottlingEnabled());
        
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        boolean is_local_singlepartitioned = ts.isExecSinglePartition() && ts.isExecLocal();
        if (d) LOG.debug(String.format("Txn #%d [single_partitioned=%s, local=%s]", txn_id, ts.isExecSinglePartition(), ts.isExecLocal()));
        switch (status) {
            case ClientResponseImpl.SUCCESS:
                if (t) LOG.trace("Marking txn #" + txn_id + " as success. If only Evan was still alive to see this!");
                builder.setStatus(Dtxn.FragmentResponse.Status.OK);
                if (is_local_singlepartitioned) this.commitWork(ts);
                break;
            case ClientResponseImpl.MISPREDICTION:
                if (d) LOG.debug("Txn #" + txn_id + " was mispredicted! Aborting work and restarting! [is_local=" + is_local_singlepartitioned + "]");
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_MISPREDICT);
                // We should always abort on a misprediction... is that true??
                this.abortWork(ts);
                break;
            case ClientResponseImpl.USER_ABORT:
                if (t) LOG.trace("Marking txn #" + txn_id + " as user aborted. Are you sure Mr.Pavlo?");
            default:
                if (status != ClientResponseImpl.USER_ABORT) {
                    this.error_counter.incrementAndGet();
                    if (d) LOG.warn("Unexpected server error for txn #" + txn_id + ": " + cresponse.getStatusString());
                }
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_USER);
                if (is_local_singlepartitioned) this.abortWork(ts);
                break;
        } // SWITCH
        
        // Don't send anything back if it's a mispredict because it's as waste of time...
        if (status != ClientResponseImpl.MISPREDICTION) {
            FastSerializer out = new FastSerializer(ExecutionSite.buffer_pool);
            try {
                out.writeObject(cresponse);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            builder.setOutput(ByteString.copyFrom(out.getBytes()));
            if (t) LOG.trace("RESULTS:\n" + Arrays.toString(cresponse.getResults()));
        } else {
            builder.setOutput(ByteString.EMPTY);
        }
        
        if (d) LOG.debug("Invoking Dtxn.FragmentResponse callback for txn #" + txn_id);
        callback.run(builder.build());
    }

    /**
     * The coordinator is telling our site to commit the xact with the
     * provided transaction id
     * @param txn_id
     */
    public void commitWork(long txn_id) {
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            if (t) LOG.trace(msg + ". Ignoring for now...");
            return;   
        }
        this.commitWork(ts);
    }
        
    /**
     * Internal call to commit the transaction
     * @param ts
     */
    private synchronized void commitWork(TransactionState ts) {
        long txn_id = ts.getTransactionId();
        
        // This is ok because the Dtxn.Coordinator can't send us a single message for
        // all of the partitions managed by our HStoreSite
        if (ts.isMarkedFinished()) {
            assert(this.finished_txn_states.contains(ts)) : "Txn #" + txn_id + " was marked as finished but it was not in our finished states!";
            return;
        }
        
        Long undoToken = ts.getLastUndoToken();
        // Blah blah blah...
        if (this.ee != null && undoToken != null && undoToken != Long.MAX_VALUE && ts.hasSubmittedEE() && ts.isExecReadOnly() == false) {
            if (d) LOG.debug(String.format("Committing txn #%d [partition=%d, lastCommittedTxnId=%d, undoToken=%d, submittedEE=%s]", txn_id, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE()));
            this.ee.releaseUndoToken(undoToken); 
        }

        this.lastCommittedTxnId = txn_id;
        ts.markAsFinished();
        this.finished_txn_states.add(ts);
    }
    
    /**
     * The coordinator is telling our site to abort the xact with the
     * provided transaction id
     * @param txn_id
     */
    public void abortWork(long txn_id) {
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            if (t) LOG.trace(msg + ". Ignoring for now...");
            return;
        }
        this.abortWork(ts);
    }
    
    /**
     * Internal call to abort a transaction
     * @param ts
     */
    private synchronized void abortWork(TransactionState ts) {
        long txn_id = ts.getTransactionId();
        
        // This is ok because the Dtxn.Coordinator can't send us a single message for
        // all of the partitions managed by our HStoreSite
        if (ts.isMarkedFinished()) {
            return;
        }
        
        // This can be null if they haven't submitted anything
        Long undoToken = ts.getLastUndoToken();
            
        // Evan says that txns will be aborted LIFO. This means the first txn that
        // we get in abortWork() will have a the greatest undoToken, which means that 
        // it will automagically rollback all other outstanding txns.
        // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
        // we should be able to check whether our undoToken has already been rolled back
        if (this.ee != null && undoToken != null && undoToken != Long.MAX_VALUE && ts.hasSubmittedEE() && ts.isExecReadOnly() == false) {
            if (d) LOG.debug(String.format("Aborting txn #%d [partition=%d, lastCommittedTxnId=%d, undoToken=%d, submittedEE=%s]", txn_id, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE()));
            this.ee.undoUndoToken(undoToken);
        }
        
        ts.markAsFinished();
        this.finished_txn_states.add(ts);
    }

    /**
     * Commit/abort all of the queue transactions that were specutatively executed and waiting for
     * their responses to be sent back to the client
     * @param commit
     */
    public void drainQueueResponses(boolean commit) {
        // First thing we need to do is get the latch that will be set by any transaction
        // that was in the middle of being executed when we were called
        synchronized (this.speculative_execution) {
            if (this.speculative_latch != null && this.speculative_latch.getCount() > 0) {
                try {
                    if (t) LOG.trace(String.format("Partition %d is waiting for spec exec latch to be unblocked", this.partitionId));
                    this.speculative_latch.await();
                } catch (InterruptedException ex) {
                    if (this.isShuttingDown() == false) {
                        LOG.fatal("Unexpected interuption while trying to drain queued responses", ex); 
                    }
                    return;
                }
                if (t) LOG.trace(String.format("Partition %d is now ready to clean up queue responses", this.partitionId));
            }
            this.speculative_execution = SpeculateType.DISABLED;
            this.speculative_latch = null;
        } // SYNCH
        
        if (this.queued_responses.isEmpty()) return;
        
        // Ok now at this point we can access our queue send back all of our responses
        if (d) LOG.debug(String.format("%s %d speculatively executed transactions on partition %d",
                                       (commit ? "Commiting" : "Aborting"), this.queued_responses.size(), this.partitionId));
        
        // XXX: Can we just commit/abort the last txn with the EE?
        Pair<LocalTransactionState, ClientResponseImpl> p = null;
        while ((p = this.queued_responses.poll()) != null) {
            ClientResponseImpl cresponse = p.getSecond();
            
            // If the multi-p txn aborted, then we need to abort everything in our queue
            // Change the status to be a MISPREDICT so that they get executed again
            if (commit == false) cresponse.setStatus(ClientResponse.MISPREDICTION);

            this.sendClientResponse(p.getFirst(), cresponse);
        } // WHILE

    }
    
    // ---------------------------------------------------------------
    // SHUTDOWN METHODS
    // ---------------------------------------------------------------
    
    /**
     * Cause this ExecutionSite to make the entire HStore cluster shutdown
     * This won't return!
     */
    public synchronized void crash(Throwable ex) {
        LOG.info(String.format("ExecutionSite for Partition #%d is crashing: %s", this.partitionId, ex.getMessage()));
        assert(this.hstore_messenger != null);
        this.hstore_messenger.shutdownCluster(ex); // This won't return
    }
    
    public boolean isShuttingDown() {
        return (this.shutdown);
    }
    
    /**
     * Somebody from the outside wants us to shutdown
     */
    public synchronized void shutdown() {
        if (this.shutdown) {
            if (d) LOG.debug(String.format("Partition #%d told to shutdown again. Ignoring...", this.partitionId));
            return;
        }
        this.shutdown = true;
        
        if (d) LOG.debug(String.format("Shutting down ExecutionSite for Partition #%d", this.partitionId));
        
        // Clear the queue
        this.work_queue.clear();
        
        // Make sure we shutdown our threadpool
        // this.thread_pool.shutdownNow();
        if (this.self != null) this.self.interrupt();
        
        if (this.shutdown_latch != null) {
            try {
                this.shutdown_latch.await();
            } catch (InterruptedException ex) {
                // Ignore
            } catch (Exception ex) {
                LOG.fatal("Unexpected error while shutting down", ex);
            }
        }
    }
}
