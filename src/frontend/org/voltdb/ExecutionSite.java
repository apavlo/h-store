/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
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

package org.voltdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.StackObjectPool;
import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SQLException;
import org.voltdb.exceptions.SerializableException;
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
import org.voltdb.utils.Encoder;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.Pair;
import org.voltdb.utils.DBBPool.BBContainer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.CountingPoolableObjectFactory;
import edu.brown.utils.EventObservable;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreMessenger;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.dtxn.DependencyInfo;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.RemoteTransactionState;
import edu.mit.hstore.dtxn.TransactionState;
import edu.mit.hstore.interfaces.Loggable;
import edu.mit.hstore.interfaces.Shutdownable;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite implements Runnable, Shutdownable, Loggable {
    public static final Logger LOG = Logger.getLogger(ExecutionSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
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

    protected enum ExecutionMode {
        /** Disable processing all transactions until... **/
        DISABLED,
        /** No speculative execution. All transactions are committed immediately **/
        COMMIT_ALL,
        /** Allow read-only txns to return results **/
        COMMIT_READONLY,
        /** All txn responses must wait until the multi-p txn is commited **/ 
        COMMIT_NONE,
    };
    
    // ----------------------------------------------------------------------------
    // GLOBAL CONSTANTS
    // ----------------------------------------------------------------------------
    
    /**
     * Represents a null dependency id
     */
    public static final int NULL_DEPENDENCY_ID = -1;

    public static final int NODE_THREAD_POOL_SIZE = 1;

    
    public static final long DISABLE_UNDO_LOGGING_TOKEN = Long.MAX_VALUE;
    
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
     * Procedure Name -> VoltProcedure
     */
    private final Map<String, VoltProcedure> procedures = new HashMap<String, VoltProcedure>();
    
    /**
     * Mapping from SQLStmt batch hash codes (computed by VoltProcedure.getBatchHashCode()) to BatchPlanners
     * The idea is that we can quickly derived the partitions for each unique set of SQLStmt list
     */
    public static final Map<Integer, BatchPlanner> POOL_BATCH_PLANNERS = new ConcurrentHashMap<Integer, BatchPlanner>();
    
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
    private Shutdownable.ShutdownState shutdown_state = Shutdownable.ShutdownState.INITIALIZED;
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
    protected ExecutionSiteHelper helper = null;
    
    protected final transient Set<Integer> done_partitions = new HashSet<Integer>();

    // ----------------------------------------------------------------------------
    // Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * The multi-partition TransactionState that is currently executing at this partition
     * When we get the response for these txn, we know we can commit/abort the speculatively executed transactions
     */
    private TransactionState current_dtxn = null;
    
    /**
     * Sets of InitiateTaskMessages that are blocked waiting for the outstanding dtxn to commit
     */
    private Set<TransactionInfoBaseMessage> current_dtxn_blocked = new HashSet<TransactionInfoBaseMessage>();

    /**
     * ClientResponses from speculatively executed transactions that are waiting to be committed 
     */
    private final LinkedBlockingDeque<Pair<LocalTransactionState, ClientResponseImpl>> queued_responses = new LinkedBlockingDeque<Pair<LocalTransactionState,ClientResponseImpl>>();
    private ExecutionMode exec_mode = ExecutionMode.COMMIT_ALL;
    private final Semaphore exec_latch = new Semaphore(1);

    /**
     * TransactionId -> TransactionState
     */
    protected final Map<Long, TransactionState> txn_states = new ConcurrentHashMap<Long, TransactionState>(); 

    /**
     * List of Transactions that have been marked as finished
     */
    protected final Queue<TransactionState> finished_txn_states = new ConcurrentLinkedQueue<TransactionState>();

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
    private final AtomicLong lastUndoToken = new AtomicLong(0l);

    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * FragmentTaskMessage (i.e., execute some fragments on behalf of another transaction)
     */
    private final LinkedBlockingDeque<TransactionInfoBaseMessage> work_queue = new LinkedBlockingDeque<TransactionInfoBaseMessage>();

    /**
     * How much time the ExecutionSite was idle waiting for work to do in its queue
     */
    private final ProfileMeasurement work_idle_time = new ProfileMeasurement("EE_IDLE");
    /**
     * How much time it takes for this ExecutionSite to execute a transaction
     */
    private final ProfileMeasurement work_exec_time = new ProfileMeasurement("EE_EXEC");
    
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
    // INITIALIZATION
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
        this.localTxnPool = new StackObjectPool(new LocalTransactionState.Factory(this, hstore_conf.site.pool_profiling),
                                                hstore_conf.site.pool_localtxnstate_idle);
        this.remoteTxnPool = new StackObjectPool(new RemoteTransactionState.Factory(this, hstore_conf.site.pool_profiling),
                                                 hstore_conf.site.pool_remotetxnstate_idle);
        DependencyInfo.initializePool(hstore_conf);
        
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
                } catch (final InstantiationException e) {
                    LOG.fatal("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final IllegalAccessException e) {
                    LOG.fatal("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final ClassNotFoundException e) {
                    LOG.fatal("Failed to load procedure class '" + className + "'", e);
                    System.exit(1);
                }
                
            } else {
                volt_proc = new VoltProcedure.StmtProcedure();
            }
            volt_proc.globalInit(ExecutionSite.this,
                                 catalog_proc,
                                 this.backend_target,
                                 this.hsql,
                                 this.p_estimator,
                                 this.getPartitionId());
            this.procedures.put(catalog_proc.getName(), volt_proc);
        } // FOR
    }

    /**
     * Preload a bunch of stuff that we'll need later on
     */
    protected void preload() {
        this.initializeVoltProcedures();

        // Then preload a bunch of TransactionStates
        for (boolean local : new boolean[]{ true, false }) {
            List<TransactionState> states = new ArrayList<TransactionState>();
            ObjectPool pool = (local ? this.localTxnPool : this.remoteTxnPool);
            int count = (int)Math.round((local ? hstore_conf.site.pool_localtxnstate_preload : hstore_conf.site.pool_remotetxnstate_preload) / hstore_conf.site.pool_scale_factor);
            try {
                for (int i = 0; i < count; i++) {
                    TransactionState ts = (TransactionState)pool.borrowObject();
                    ts.init(-1l, -1l, this.partitionId, false, false, true);
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
     * Link this ExecutionSite with its parent HStoreSite
     * This will initialize the references the various components shared among the ExecutionSites 
     * @param hstore_site
     */
    public void initHStoreSite(HStoreSite hstore_site) {
        if (t) LOG.trace(String.format("Initializing HStoreSite components at partition %d", this.partitionId));
        assert(this.hstore_site == null);
        this.hstore_site = hstore_site;
        this.hstore_messenger = hstore_site.getMessenger();
//        assert(this.hstore_messenger != null) : "Missing HStoreMessenger";
        this.helper = hstore_site.getExecutionSiteHelper();
//        assert(this.helper != null) : "Missing ExecutionSiteHelper";
        this.thresholds = (hstore_site != null ? hstore_site.getThresholds() : null);
        if (hstore_conf.site.exec_profiling) {
            EventObservable eo = this.hstore_site.getWorkloadObservable();
            this.work_idle_time.resetOnEvent(eo);
            this.work_exec_time.resetOnEvent(eo);
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
    public void run() {
        assert(this.hstore_site != null);
        assert(this.hstore_messenger != null);
        assert(this.self == null);
        this.self = Thread.currentThread();
        this.self.setName(this.getThreadName());
        this.preload();
        
        if (hstore_conf.site.cpu_affinity) {
            this.hstore_site.getThreadManager().registerEEThread(partition);
        }
        
        // Setup the shutdown latch
        assert(this.shutdown_latch == null);
        this.shutdown_latch = new CountDownLatch(1);

        // Things that we will need in the loop below
        TransactionInfoBaseMessage work = null;
        boolean stop = false;
        TransactionState ts = null;
        
        try {
            if (d) LOG.debug("Starting ExecutionSite run loop...");
            while (stop == false && this.isShuttingDown() == false) {
                work = null;
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                try {
                    work = this.work_queue.poll();
                    if (work == null) {
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.start();
                        work = this.work_queue.takeFirst();
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.stop();
                    }
                } catch (InterruptedException ex) {
                    if (d && this.isShuttingDown() == false) LOG.debug("Unexpected interuption while polling work queue. Halting ExecutionSite...", ex);
                    stop = true;
                    break;
                }

                ts = this.txn_states.get(work.getTxnId());
                if (ts == null) {
                    String msg = "No transaction state for txn #" + work.getTxnId();
                    LOG.error(msg);
                    throw new RuntimeException(msg);
                }
                
                // -------------------------------
                // Execute Query Plan Fragments
                // -------------------------------
                if (work instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftask = (FragmentTaskMessage)work;
                    this.processFragmentTaskMessage(ts, ftask);

                // -------------------------------
                // Invoke Stored Procedure
                // -------------------------------
                } else if (work instanceof InitiateTaskMessage) {
                    if (hstore_conf.site.exec_profiling) this.work_exec_time.start();
                    InitiateTaskMessage itask = (InitiateTaskMessage)work;
                    this.processInitiateTaskMessage((LocalTransactionState)ts, itask);
                    if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                    
                // -------------------------------
                // BAD MOJO!
                // -------------------------------
                } else if (work != null) {
                    throw new RuntimeException("Unexpected work message in queue: " + work);
                }

            } // WHILE
        } catch (final Throwable ex) {
            if (this.isShuttingDown() == false) {
                LOG.fatal(String.format("Unexpected error for ExecutionSite partition #%d%s",
                                        this.partitionId, (ts != null ? " - " + ts.toString() : "")), ex);
                if (ts != null) LOG.fatal("TransactionState Dump:\n" + ts.debug());
            }
            this.hstore_messenger.shutdownCluster(new Exception(ex));
        } finally {
//            if (d) 
                LOG.info("ExecutionSite is stopping. In-Flight Txn: " + ts);
            
            // Release the shutdown latch in case anybody waiting for us
            this.shutdown_latch.countDown();
            
            // Stop HStoreMessenger (because we're nice)
            if (this.isShuttingDown() == false) {
                if (this.hstore_messenger != null) this.hstore_messenger.shutdown();
            }
        }
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
    public PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    public TransactionEstimator getTransactionEstimator() {
        return (this.t_estimator);
    }
    
    public HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public HStoreMessenger getHStoreMessenger() {
        return (this.hstore_messenger);
    }

    public Site getCatalogSite() {
        return (this.site);
    }
    public int getHostId() {
        return (this.site.getHost().getRelativeIndex());
    }
    public int getSiteId() {
        return (this.siteId);
    }
    public int getPartitionId() {
        return (this.partitionId);
    }
    public Long getLastCommittedTxnId() {
        return (this.lastCommittedTxnId);
    }

    /**
     * Returns the next undo token to use when hitting up the EE with work
     * MAX_VALUE = no undo
     * @param txn_id
     * @return
     */
    public long getNextUndoToken() {
        return (this.lastUndoToken.incrementAndGet());
    }
    
    /**
     * Set the current ExecutionMode for this executor
     * @param mode
     * @param txn_id
     */
    private void setExecutionMode(ExecutionMode mode, long txn_id) {
        if (d && this.exec_mode != mode) {
            LOG.debug(String.format("Setting ExecutionMode for partition %d to %s [txn=%d, orig=%s]",
                                    this.partitionId, mode, txn_id, this.exec_mode));
        }
        assert(mode != ExecutionMode.COMMIT_READONLY || (mode == ExecutionMode.COMMIT_READONLY && this.current_dtxn != null)) :
            String.format("Txn #%d is trying to set partition %d to %s when the current DTXN is null?", txn_id, this.partitionId, mode);
        this.exec_mode = mode;
    }
    public ExecutionMode getExecutionMode() {
        return (this.exec_mode);
    }
    public TransactionState getCurrentDtxn() {
        return (this.current_dtxn);
    }
    
    public int getBlockedQueueSize() {
        return (this.current_dtxn_blocked.size());
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
    public long getTransactionCounter() {
        return (this.work_exec_time.getInvocations());
    }
    
    /**
     * Returns true if the txn is read-only at this partition
     * Returns null if this ExecutionSite has never seen this txn before
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
        return (this.procedures.get(proc_name));
    }

    public String getThreadName() {
        return (this.hstore_site.getThreadName(String.format("%03d", this.getPartitionId())));
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
    public boolean enableSpeculativeExecution(long txn_id, boolean force) {
        // assert(this.speculative_execution == SpeculateType.DISABLED) : "Trying to enable spec exec twice because of txn #" + txn_id;
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            return (false);
        }
        
        // Check whether the txn that we're waiting for is read-only.
        // If it is, then that means all read-only transactions can commit right away
        synchronized (this.exec_mode) {
            if (this.current_dtxn == ts && this.exec_mode != ExecutionMode.DISABLED && ts.isExecReadOnly()) {
                this.setExecutionMode(ExecutionMode.COMMIT_READONLY, txn_id);
                this.releaseBlockedTransactions(txn_id, true);
                if (d) LOG.debug(String.format("Enabled %s speculative execution at partition %d [txn=#%d]", this.exec_mode, partitionId, txn_id));
                return (true);
            }
        } // SYNCH
        return (false);
    }
    
    /**
     * Process a FragmentResponseMessage and update the TransactionState accordingly
     * @param ts
     * @param fresponse
     */
    protected void processFragmentResponseMessage(TransactionState ts, FragmentResponseMessage fresponse) {
        if (t) LOG.trace(String.format("Processing FragmentResponseMessage for %s on partition %d [srcPartition=%d, deps=%d]",
                                       ts, this.partitionId, fresponse.getSourcePartitionId(), fresponse.getTableCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (fresponse.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            if (t) LOG.trace(String.format("Received non-success response %s from partition %d for %s",
                                            fresponse.getStatusCodeName(), fresponse.getSourcePartitionId(), ts));
            ts.setPendingError(fresponse.getException());
        }
        for (int ii = 0, num_tables = fresponse.getTableCount(); ii < num_tables; ii++) {
            ts.addResponse(fresponse.getSourcePartitionId(), fresponse.getTableDependencyIdAtIndex(ii));
        } // FOR
    }
    
    /**
     * Execute a new transaction based on an InitiateTaskMessage
     * @param itask
     */
    protected void processInitiateTaskMessage(LocalTransactionState ts, InitiateTaskMessage itask) {
        final long txn_id = ts.getTransactionId();
        final boolean predict_singlePartition = ts.isPredictSinglePartition();
        if (hstore_conf.site.txn_profiling) ts.profiler.startExec();
        
        ExecutionMode spec_exec = ExecutionMode.COMMIT_ALL;
        boolean release_latch = false;
        synchronized (this.exec_mode) {
            // If this is going to be a multi-partition transaction, then we will mark it as the current dtxn
            // for this ExecutionSite. There should be no other dtxn running right now that this partition
            // knows about.
            if (predict_singlePartition == false) {
                assert(this.current_dtxn_blocked.isEmpty()) :
                    String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s]",
                                  this.partitionId, this.current_dtxn, ts);
                assert(this.current_dtxn == null || this.current_dtxn.isInitialized() == false) :
                    String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s]",
                                   this.partitionId, this.current_dtxn, ts);
                this.current_dtxn = ts;
                if (hstore_conf.site.exec_speculative_execution) {
                    this.setExecutionMode(ts.getProcedure().getReadonly() ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE, txn_id);
                } else {
                    this.setExecutionMode(ExecutionMode.DISABLED, txn_id);                  
                }
                if (d) LOG.debug(String.format("Marking %s as current DTXN on Partition %d [isLocal=%s, execMode=%s]",
                                               ts, this.partitionId, true, this.exec_mode));                    
                spec_exec = this.exec_mode;

            // If this is a single-partition transaction, then we need to check whether we are being executed
            // under speculative execution mode. We have to check this here because it may be the case that we queued a
            // bunch of transactions when speculative execution was enabled, but now the transaction that was ahead of this 
            // one is finished, so now we're just executing them regularly
            } else if (this.exec_mode != ExecutionMode.COMMIT_ALL) {
                assert(this.current_dtxn != null) : String.format("Invalid execution mode %s without a dtxn at partition %d", this.exec_mode, this.partitionId);
                
                // HACK: If we are currently under DISABLED mode when we get this, then we just need to block the transaction
                // and return back to the queue. This is easier than having to set all sorts of crazy locks
                if (this.exec_mode == ExecutionMode.DISABLED) {
                    if (t) LOG.trace(String.format("Blocking single-partition %s until dtxn #%d finishes [mode=%s]", ts, this.current_dtxn, this.exec_mode));
                    this.current_dtxn_blocked.add(itask);
                    return;
                }
                
                spec_exec = this.exec_mode;
                if (hstore_conf.site.exec_speculative_execution) {
                    ts.setSpeculative(true);
                    if (d) LOG.debug(String.format("Marking %s as speculatively executed on partition %d [txnMode=%s, dtxn=%s]", ts, this.partitionId, spec_exec, this.current_dtxn));
                    
                    // This lock is used to block somebody from draining the queued responses until this txn finishes
                    if (d) LOG.debug(String.format("%s is trying to acquire exec latch for partition %d", ts, this.partitionId));
                    this.exec_latch.acquireUninterruptibly();
                    if (d) LOG.debug(String.format("%s got exec latch for partition %d", ts, this.partitionId));
                    release_latch = true;
                }
            }
        } // SYNCH
        
        VoltProcedure volt_proc = this.procedures.get(itask.getStoredProcedureName());
        assert(volt_proc != null) : "No VoltProcedure for " + ts;
        
        if (d) {
            LOG.debug(String.format("Starting execution of %s [txnMode=%s, mode=%s]", ts, spec_exec, this.exec_mode));
            if (t) LOG.trace(ts.debug());
        }
            
        ClientResponseImpl cresponse = (ClientResponseImpl)volt_proc.call(ts, itask.getParameters()); // Blocking...
        assert(cresponse != null && this.isShuttingDown() == false) : String.format("No ClientResponse for %s???", ts);
        byte status = cresponse.getStatus();
        if (d) LOG.debug(String.format("Finished execution of %s [status=%s, txnMode=%s, mode=%s]", ts, cresponse.getStatusName(), spec_exec, this.exec_mode));

        // Process the ClientResponse immediately if:
        //  (1) This is the multi-partition transaction that everyone is waiting for
        //  (2) The transaction was not executed under speculative execution mode 
        //  (3) The transaction does not need to wait for the multi-partition transaction to finish first
        if (predict_singlePartition == false || this.canProcessClientResponseNow(ts, status, spec_exec)) {
            if (hstore_conf.site.exec_postprocessing_thread) {
                if (t) LOG.trace(String.format("Passing ClientResponse for %s to post-processing thread [status=%s]", ts, cresponse.getStatusName()));
                hstore_site.queueClientResponse(this, ts, cresponse);
            } else {
                if (t) LOG.trace(String.format("Sending ClientResponse for %s back directly [status=%s]", ts, cresponse.getStatusName()));
                this.processClientResponse(ts, cresponse);
            }

        // Otherwise always queue our response, since we know that whatever thread is out there
        // is waiting for us to finish before it drains the queued responses
        } else {
            // If the transaction aborted, then we can't execute any transaction that touch the tables that this guy touches
            // But since we can't just undo this transaction without undoing everything that came before it, we'll just
            // disable executing all transactions until the multi-partition transaction commits
            // NOTE: We don't need acquire the 'exec_mode' lock here, because we know that we either executed in non-spec mode, or 
            // that there already was a multi-partition transaction hanging around.
            if (status != ClientResponse.SUCCESS) {
                this.setExecutionMode(ExecutionMode.DISABLED, txn_id);
                synchronized (this.work_queue) {
                    FragmentTaskMessage ftask = null;
                    if (this.work_queue.peekFirst() instanceof FragmentTaskMessage) {
                        ftask = (FragmentTaskMessage)this.work_queue.pollFirst();
                        assert(ftask != null);
                    }
                    if (t) LOG.trace(String.format("Blocking %d transactions at partition %d because ExecutionMode is now %s [hasFTask=%s]",
                                                   this.work_queue.size(), this.partitionId, this.exec_mode, (ftask != null)));
                    this.work_queue.drainTo(this.current_dtxn_blocked);
                    if (ftask != null) this.work_queue.add(ftask);
                } // SYNCH
                if (d) LOG.debug(String.format("Disabling execution on partition %d because speculative %s aborted", this.partitionId, ts));
            }
            if (t) LOG.trace(String.format("Queuing ClientResponse for %s [status=%s, origMode=%s, newMode=%s, hasLatch=%s, latchQueue=%s, dtxn=%s]",
                                           ts, cresponse.getStatusName(), spec_exec, this.exec_mode,
                                           release_latch, this.exec_latch.getQueueLength(), this.current_dtxn));
            this.queueClientResponse(ts, cresponse);
        }
        
        // Release the latch in case anybody is waiting for us to finish
        if (hstore_conf.site.exec_speculative_execution && (spec_exec != ExecutionMode.COMMIT_ALL || predict_singlePartition == false) && release_latch == true) {
            if (d) LOG.debug(String.format("%s is releasing exec latch for partition %d", ts, this.partitionId));
            this.exec_latch.release();
            assert(this.exec_latch.availablePermits() <= 1) : String.format("Invalid exec latch state [permits=%d, mode=%s]", this.exec_latch.availablePermits(), this.exec_mode);
        } else if (t) {
            LOG.trace(String.format("%s did not unlock exec latch at partition %d [mode=%s, dtxn=%s]",
                                    ts, this.partitionId, spec_exec, this.current_dtxn));
        }
        
        volt_proc.finish();
    }

    /**
     * Determines whether a finished transaction that executed locally can have their ClientResponse processed immediately
     * or if it needs to wait for the response from the outstanding multi-partition transaction for this partition 
     * @param ts
     * @param status
     * @param spec_exec
     * @return
     */
    private boolean canProcessClientResponseNow(LocalTransactionState ts, byte status, ExecutionMode spec_exec) {
        if (d) LOG.debug(String.format("Checking whether to process response for %s now [status=%s, singlePartition=%s, readOnly=%s, mode=%s]",
                                       ts, ClientResponseImpl.getStatusName(status), ts.isExecSinglePartition(), ts.isExecReadOnly(), spec_exec));
        // Commit All
        if (spec_exec == ExecutionMode.COMMIT_ALL) {
            return (true);
            
        // Process successful txns based on the mode that it was executed under
        } else if (status == ClientResponse.SUCCESS) {
            switch (spec_exec) {
                case COMMIT_ALL:
                    return (true);
                case COMMIT_READONLY:
                    return (ts.isExecReadOnly());
                case COMMIT_NONE:
                    return (false);
                default:
                    throw new RuntimeException("Unexpectd execution mode: " + spec_exec); 
            } // SWITCH
        // Anything mispredicted should be processed right away
        } else if (status == ClientResponse.MISPREDICTION) {
            return (true);
            
        // If the transaction aborted and it was read-only thus far, then we want to process it immediately
        } else if (status != ClientResponse.SUCCESS && ts.isExecReadOnly()) {
            return (true);
        }
        
        return (false);
    }
    
    /**
     * 
     * @param ftask
     * @throws Exception
     */
    private void processFragmentTaskMessage(TransactionState ts, FragmentTaskMessage ftask) {
        // A txn is "local" if the Java is executing at the same site as we are
        boolean is_local = ts.isExecLocal();
        boolean is_dtxn = ftask.isUsingDtxnCoordinator();
        if (t) LOG.trace(String.format("Executing FragmentTaskMessage %s [partition=%d, is_local=%s, is_dtxn=%s, fragments=%s]",
                                       ts, ftask.getSourcePartitionId(), is_local, is_dtxn, Arrays.toString(ftask.getFragmentIds())));

        // If this txn isn't local, then we have to update our undoToken
        if (is_local == false) {
            ts.initRound(this.getNextUndoToken());
            ts.startRound();
        }

        FragmentResponseMessage fresponse = new FragmentResponseMessage(ftask);
        fresponse.setStatus(FragmentResponseMessage.NULL, null);
        assert(fresponse.getSourcePartitionId() == this.partitionId) : "Unexpected source partition #" + fresponse.getSourcePartitionId() + "\n" + fresponse;
        
        DependencySet result = null;
        try {
            result = this.executeFragmentTaskMessage(ts, ftask);
            fresponse.setStatus(FragmentResponseMessage.SUCCESS, null);
        } catch (ConstraintFailureException ex) {
            LOG.fatal("Hit an ConstraintFailureException for " + ts, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
        } catch (EEException ex) {
            LOG.fatal("Hit an EE Error for " + ts, ex);
            this.crash(ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
        } catch (SQLException ex) {
            LOG.warn("Hit a SQL Error for " + ts, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
        } catch (Throwable ex) {
            LOG.warn("Something unexpected and bad happended for " + ts, ex);
            fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
        } finally {
            // Success, but without any results???
            if (result == null && fresponse.getStatusCode() == FragmentResponseMessage.SUCCESS) {
                Exception ex = new Exception("The Fragment executed successfully but result is null for " + ts);
                if (d) LOG.warn(ex);
                fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
            }
        }
        
        // -------------------------------
        // SUCCESS
        // -------------------------------
        if (fresponse.getStatusCode() == FragmentResponseMessage.SUCCESS) {
            // XXX: For single-sited INSERT/UPDATE/DELETE queries, we don't directly
            // execute the SendPlanNode in order to get back the number of tuples that
            // were modified. So we have to rely on the output dependency ids set in the task
            assert(result.size() == ftask.getOutputDependencyIds().length) :
                "Got back " + result.size() + " results but was expecting " + ftask.getOutputDependencyIds().length;
            
            // If the transaction is local, store the result in the local TransactionState
            // Unless we were sent this FragmentTaskMessage through the Dtxn.Coordinator
            if (is_local && is_dtxn == false) {
                if (t) LOG.trace("Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                assert(ts != null);
                LocalTransactionState local_ts = (LocalTransactionState)ts;
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    int dep_id = ftask.getOutputDependencyIds()[i];
                    // ts.addResult(result.depIds[i], result.dependencies[i]);
                    if (t) LOG.trace("Storing DependencyId #" + dep_id  + " for " + ts);
                    local_ts.addResultWithResponse(this.partitionId, dep_id, result.dependencies[i]);
                } // FOR
                
            // Otherwise push dependencies back to the remote partition that needs it
            } else {
                if (d) LOG.debug(String.format("Constructing FragmentResponseMessage %s with %d bytes from partition %d to send back to initial partition %d for %s",
                                               Arrays.toString(ftask.getFragmentIds()), result.size(), this.partitionId, ftask.getSourcePartitionId(), ts));
                
                // We need to include the DependencyIds in our response, but we will send the actual
                // data through the HStoreMessenger
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    fresponse.addDependency(result.depIds[i]);
                } // FOR
                this.sendFragmentResponseMessage(ts, ftask, fresponse);
                
                // Bombs away!
                this.hstore_messenger.sendDependencySet(ts.getTransactionId(), this.partitionId, ftask.getSourcePartitionId(), result); 
            }

        // -------------------------------
        // ERRROR
        // ------------------------------- 
        } else {
            this.error_counter.getAndIncrement();
            if (is_local && is_dtxn == false) {
                this.processFragmentResponseMessage(ts, fresponse);
            } else {
                this.sendFragmentResponseMessage(ts, ftask, fresponse);
            }
        }
        
        // Again, if we're not local, just clean up our TransactionState
        if (is_local == false && is_dtxn == true) {
            if (d) LOG.debug(String.format("Executed non-local FragmentTask %s. Notifying TransactionState for %s to finish round",
                                           Arrays.toString(ftask.getFragmentIds()), ts));
            ts.finishRound();
        }
    }
    
    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the resulting DependencySet
     * @param ftask
     * @return
     * @throws Exception
     */
    private DependencySet executeFragmentTaskMessage(TransactionState ts, FragmentTaskMessage ftask) throws Exception {
        DependencySet result = null;
        final long undoToken = ts.getLastUndoToken();
        int fragmentIdIndex = ftask.getFragmentCount();
        long fragmentIds[] = ftask.getFragmentIds();
        int output_depIds[] = ftask.getOutputDependencyIds();
        int input_depIds[] = ftask.getAllUnorderedInputDepIds(); // Is this ok?
        
        if (fragmentIdIndex == 0) {
            LOG.warn(String.format("Got a FragmentTask for %s that does not have any fragments?!?", ts));
            return (result);
        }
        
        if (d) {
            LOG.debug(String.format("Getting ready to kick %d fragments to EE for %s", ftask.getFragmentCount(), ts));
            if (t) LOG.trace("FragmentTaskIds: " + Arrays.toString(ftask.getFragmentIds()));
        }
        ParameterSet parameterSets[] = new ParameterSet[fragmentIdIndex];
        for (int i = 0; i < fragmentIdIndex; i++) {
            ByteBuffer paramData = ftask.getParameterDataForFragment(i);
            if (paramData != null) {
                paramData.rewind();
                final FastDeserializer fds = new FastDeserializer(paramData);
                if (t) LOG.trace(ts + " ->paramData[" + i + "] => " + fds.buffer());
                try {
                    parameterSets[i] = fds.readObject(ParameterSet.class);
                } catch (Exception ex) {
                    LOG.fatal("Failed to deserialize ParameterSet[" + i + "] for FragmentTaskMessage " + fragmentIds[i] + " in " + ts, ex);
                    throw ex;
                }
                // LOG.info("PARAMETER[" + i + "]: " + parameterSets[i]);
            } else {
                parameterSets[i] = new ParameterSet();
            }
        } // FOR
        
        ts.ee_dependencies.clear();
        if (ftask.hasAttachedResults()) {
            if (t) LOG.trace("Retrieving internal dependency results attached to FragmentTaskMessage for " + ts);
            ts.ee_dependencies.putAll(ftask.getAttachedResults());
        }
        
        LocalTransactionState local_ts = null;
        if (ftask.hasInputDependencies() && ts != null && ts.isExecLocal() == true) {
            local_ts = (LocalTransactionState)ts; 
            if (local_ts.getInternalDependencyIds().isEmpty() == false) {
                if (t) LOG.trace("Retrieving internal dependency results from TransactionState for " + ts);
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
            } // SYNCH
            if (volt_proc == null) throw new RuntimeException("No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments);
            
            // HACK: We have to set the TransactionState for sysprocs manually
            volt_proc.setTransactionState(ts);
            ts.markExecNotReadOnly();
            result = volt_proc.executePlanFragment(ts.getTransactionId(), ts.ee_dependencies, (int)fragmentIds[0], parameterSets[0], this.m_systemProcedureContext);
            if (t) LOG.trace("Finished executing sysproc fragments for " + volt_proc.getClass().getSimpleName());
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            result = this.executePlanFragments(ts, undoToken, fragmentIdIndex, fragmentIds, parameterSets, output_depIds, input_depIds);
        }
        return (result);
    }
    
    /**
     * Execute a BatcPlan directly on this ExecutionSite without having to covert it
     * to FragmentTaskMessages first. This is big speed improvement over having to queue things up
     * @param ts
     * @param plan
     * @return
     */
    protected VoltTable[] executeLocalPlan(LocalTransactionState ts, BatchPlanner.BatchPlan plan) {
        long undoToken = ExecutionSite.DISABLE_UNDO_LOGGING_TOKEN;
        
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
        } else if (hstore_conf.site.exec_no_undo_logging_all == false) {
            undoToken = this.getNextUndoToken();
        }
        ts.fastInitRound(undoToken);
      
        long fragmentIds[] = plan.getFragmentIds();
        int fragmentIdIndex = plan.getFragmentCount();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        ParameterSet parameterSets[] = plan.getParameterSets();
        
        // Mark that we touched the local partition once for each query in the batch
        ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not a single-partition transaction
        if (hstore_conf.site.exec_speculative_execution && ts.isPredictSinglePartition() == false) {
            this.notifyDonePartitions(ts);
        }

        if (t) {
            StringBuilder sb = new StringBuilder();
            sb.append("Parameters:");
            for (int i = 0; i < parameterSets.length; i++) {
                sb.append(String.format("\n [%02d] %s", i, parameterSets[i].toString()));
            }
            LOG.trace(sb.toString());
            LOG.trace(String.format("Txn #%d - BATCHPLAN:\n" +
                     "  fragmentIds:     %s\n" + 
                     "  fragmentIdIndex: %s\n" +
                     "  output_depIds:   %s\n" +
                     "  input_depIds:    %s",
                     ts.getTransactionId(),
                     Arrays.toString(plan.getFragmentIds()), plan.getFragmentCount(), Arrays.toString(plan.getOutputDependencyIds()), Arrays.toString(plan.getInputDependencyIds())));
        }
        DependencySet result = this.executePlanFragments(ts, undoToken, fragmentIdIndex, fragmentIds, parameterSets, output_depIds, input_depIds);
        assert(result != null) : "Unexpected null DependencySet for " + ts; 
        if (t) LOG.trace("Output:\n" + StringUtil.join("\n", result.dependencies));
        
        ts.fastFinishRound();
        return (result.dependencies);
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
    private DependencySet executePlanFragments(TransactionState ts, long undoToken, int batchSize, long fragmentIds[], ParameterSet parameterSets[], int output_depIds[], int input_depIds[]) {
        assert(this.ee != null) : "The EE object is null. This is bad!";
        long txn_id = ts.getTransactionId();
        
        if (d) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Executing %d fragments for %s [lastTxnId=%d, undoToken=%d]",
                      batchSize, ts, this.lastCommittedTxnId, undoToken));
            if (t) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Fragments", Arrays.toString(fragmentIds));
                m.put("Parameters", Arrays.toString(parameterSets));
                m.put("Input Dependencies", Arrays.toString(input_depIds));
                m.put("Output Dependencies", Arrays.toString(output_depIds));
                sb.append("\n" + StringUtil.formatMaps(m)); 
            }
            LOG.debug(sb.toString());
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
            boolean readonly = CatalogUtil.areFragmentsReadOnly(this.database, fragmentIds, batchSize); 
            if (readonly == false) {
                if (t) LOG.trace(String.format("Marking txn #%d as not read-only %s", txn_id, Arrays.toString(fragmentIds))); 
                ts.markExecNotReadOnly();
            }
        }
        
        DependencySet result = null;
        try {
            if (t) LOG.trace(String.format("Executing %d fragments at partition %d for %s", batchSize, this.partitionId, ts));
            if (hstore_conf.site.txn_profiling && ts instanceof LocalTransactionState) {
                ((LocalTransactionState)ts).profiler.startExecEE();
            }
            synchronized (this.ee) {
                result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                                fragmentIds,
                                batchSize,
                                input_depIds,
                                output_depIds,
                                parameterSets,
                                batchSize,
                                txn_id,
                                this.lastCommittedTxnId,
                                undoToken);
            } // SYNCH
            if (hstore_conf.site.txn_profiling && ts instanceof LocalTransactionState) {
                ((LocalTransactionState)ts).profiler.stopExecEE();
            }
        } catch (EEException ex) {
            LOG.fatal("Unrecoverable error in the ExecutionEngine", ex);
            System.exit(1);
        } catch (Throwable ex) {
            new RuntimeException(String.format("Failed to execute PlanFragments for %s: %s", ts, Arrays.toString(fragmentIds)), ex);
        }
        
        if (t) LOG.trace(String.format("Executed fragments %s and got back results %s",
                                       Arrays.toString(fragmentIds), Arrays.toString(result.depIds))); //  + "\n" + Arrays.toString(result.dependencies));
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
    protected void cleanupTransaction(TransactionState ts) {
        long txn_id = ts.getTransactionId();
        assert(ts.isEE_Finished());
        if (this.txn_states.remove(txn_id) != null) {
            if (t) LOG.trace(String.format("Cleaning up internal state information for Txn #%d at partition %d", txn_id, this.partitionId));
            try {
                if (ts.isExecLocal()) {
                    this.localTxnPool.returnObject(ts);
                } else {
                    this.remoteTxnPool.returnObject(ts);
                }
            } catch (Exception ex) {
                LOG.fatal("Failed to return TransactionState for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
        }
    }
    
    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * We should not be sent an InitiateTaskMessage here!
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void doWork(FragmentTaskMessage task, RpcCallback<Dtxn.FragmentResponse> callback) {
        long txn_id = task.getTxnId();
        long client_handle = task.getClientHandle();
        
        TransactionState ts = this.txn_states.get(txn_id);
        boolean read_only = task.isReadOnly();
        if (ts == null) {
            try {
                // Remote Transaction
                ts = (RemoteTransactionState)remoteTxnPool.borrowObject();
                ts.init(txn_id, client_handle, task.getSourcePartitionId(), false, read_only, true);
                if (d) LOG.debug(String.format("Creating new RemoteTransactionState %s from remote partition %d to execute at partition %d [readOnly=%s, singlePartitioned=%s]",
                                               ts, task.getSourcePartitionId(), this.partitionId, read_only, false));
            } catch (Exception ex) {
                LOG.fatal("Failed to construct TransactionState for txn #" + txn_id, ex);
                throw new RuntimeException(ex);
            }
            this.txn_states.put(txn_id, ts);
            if (t) LOG.trace(String.format("Stored new transaction state for %s at partition %d", ts, this.partitionId));
        }
        assert(ts.isInitialized());
            
        if (ts != this.current_dtxn) {
            synchronized (this.exec_mode) {
                assert(this.current_dtxn_blocked.isEmpty()) :
                    String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s]",
                                  this.partitionId, this.current_dtxn, ts);
                assert(this.current_dtxn == null || this.current_dtxn.isInitialized() == false) :
                    String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s]",
                                  this.partitionId, this.current_dtxn, ts);
                this.current_dtxn = ts;
                if (hstore_conf.site.exec_speculative_execution) {
                    this.setExecutionMode(read_only ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE, txn_id);
                } else {
                    this.setExecutionMode(ExecutionMode.DISABLED, txn_id);
                }
                
                if (d) LOG.debug(String.format("Marking %s as current DTXN on partition %d [execMode=%s]",
                                              ts, this.partitionId, this.exec_mode));                    
            } // SYNCH

        // Check whether we should drop down to a less permissive speculative execution mode
        } else if (hstore_conf.site.exec_speculative_execution && read_only == false) {
            this.setExecutionMode(ExecutionMode.COMMIT_NONE, txn_id);
        }

        // Remote Work
        if (callback != null) {
            if (t) LOG.trace(String.format("Storing FragmentTask callback in TransactionState for %s at partition %d", ts, this.partitionId));
            ts.setFragmentTaskCallback(task, callback);
        } else {
            assert(task.isUsingDtxnCoordinator() == false) : String.format("No callback for remote execution request for %s at partition %d", ts, this.partitionId);
        }
        
        // We have to lock the work queue here to prevent a speculatively executed transaction that aborts
        // from swaping the work queue to the block task list
        if (t) LOG.trace(String.format("Adding multi-partition %s to front of work queue [size=%d]", ts, this.work_queue.size()));
        synchronized (this.work_queue) {
            this.work_queue.addFirst(task);
        } // SYNCH
    }

    /**
     * New work for a local transaction
     * @param task
     * @param callback
     * @param ts
     */
    public void doWork(InitiateTaskMessage task, RpcCallback<Dtxn.FragmentResponse> callback, LocalTransactionState ts) {
        long txn_id = task.getTxnId();
        assert(ts != null) : "The TransactionState is somehow null for txn #" + txn_id;
        if (d) LOG.debug(String.format("Queuing new transaction execution request for %s on partition %d with%s a callback",
                                       ts, this.partitionId, (callback == null ? "out" : "")));
        this.txn_states.put(txn_id, ts);
        
        // If we're a multi-partition txn then queue this mofo immediately
        if (ts.isPredictSinglePartition() == false) {
            if (t) LOG.trace(String.format("Adding %s to work queue at partition %d [size=%d]", ts, this.partitionId, this.work_queue.size()));
            // We need to acquire this lock because we need to make sure that we're always put in the front
            // of the queue. Otherwise somebody might move the entire work queue into the blocked list
            synchronized (this.work_queue) {
                this.work_queue.addFirst(task);    
             } // SYNCH

        // If we're a single-partition and speculative execution is enabled, then we can always set it up now
        } else if (hstore_conf.site.exec_speculative_execution && this.exec_mode != ExecutionMode.DISABLED) {
            if (t) LOG.trace(String.format("Adding %s to work queue at partition %d [size=%d]", ts, this.partitionId, this.work_queue.size()));
            this.work_queue.add(task);
            
        // Otherwise figure out whether this txn needs to be blocked or not
        } else {
            synchronized (this.exec_mode) {
                // No outstanding DTXN
                if (this.current_dtxn == null && this.exec_mode != ExecutionMode.DISABLED) {
                    if (t) LOG.trace(String.format("Adding single-partition %s to work queue [size=%d]", ts, this.work_queue.size()));
                    this.work_queue.add(task);
                } else {
                    if (t) LOG.trace(String.format("Blocking single-partition %s until dtxn #%d finishes", ts, this.current_dtxn));
                    this.current_dtxn_blocked.add(task);
                }
            } // SYNCH
        }
    }

    /**
     * 
     * @param fresponse
     */
    public void sendFragmentResponseMessage(TransactionState ts, FragmentTaskMessage ftask, FragmentResponseMessage fresponse) {
        RpcCallback<Dtxn.FragmentResponse> callback = ts.getFragmentTaskCallback(ftask);
        if (callback == null) {
            LOG.fatal("Unable to send FragmentResponseMessage:\n" + fresponse.toString());
            LOG.fatal("Orignal FragmentTaskMessage:\n" + ftask);
            LOG.fatal(ts.toString());
            throw new RuntimeException("No RPC callback to HStoreSite for " + ts);
        }

        BBContainer bc = fresponse.getBufferForMessaging(buffer_pool);
        assert(bc.b.hasArray());
        ByteString bs = ByteString.copyFrom(bc.b.array());
//        ByteBuffer serialized = bc.b.asReadOnlyBuffer();
//        LOG.info("Serialized FragmentResponseMessage [size=" + serialized.capacity() + ",id=" + serialized.get(VoltMessage.HEADER_SIZE) + "]");
//        assert(serialized.get(VoltMessage.HEADER_SIZE) == VoltMessage.FRAGMENT_RESPONSE_ID);
        
        if (d) LOG.debug(String.format("Sending FragmentResponseMessage for %s [partition=%d, size=%d]", ts, this.partitionId, bs.size()));
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
        
        if (d) LOG.debug(String.format("Checking MarkovEstimate for %s to see whether we can notify any partitions that we're done with them [round=%d]",
                                       ts, ts.getCurrentRound()));
        
        MarkovEstimate estimate = t_state.getLastEstimate();
        assert(estimate != null) : "Got back null MarkovEstimate for " + ts;
        new_done = estimate.getFinishedPartitions(this.thresholds);
        
        if (new_done.isEmpty() == false) { 
            // Note that we can actually be done with ourself, if this txn is only going to execute queries
            // at remote partitions. But we can't actually execute anything because this partition's only 
            // execution thread is going to be blocked. So we always do this so that we're not sending a 
            // useless message
            new_done.remove(this.partitionId);
            
            // Make sure that we only tell partitions that we actually touched, otherwise they will
            // be stuck waiting for a finish request that will never come!
            Collection<Integer> ts_touched = ts.getTouchedPartitions().values();

            // Mark the txn done at this partition if the MarkovEstimate said we were done
            this.done_partitions.clear();
            for (Integer p : new_done) {
                if (ts_done_partitions.contains(p) == false && ts_touched.contains(p)) {
                    if (t) LOG.trace(String.format("Marking partition %d as done for %s", p, ts));
                    ts_done_partitions.add(p);
                    this.done_partitions.add(p);
                }
            } // FOR
            
            if (this.done_partitions.size() > 0) {
                if (d) LOG.debug(String.format("Marking %d partitions as done after this fragment for %s: %s\n%s",
                                 this.done_partitions.size(), ts, this.done_partitions, estimate));
                this.hstore_messenger.sendDoneAtPartitions(txn_id, this.done_partitions);
            }
        }
    }

    /**
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    private void requestWork(LocalTransactionState ts, Collection<FragmentTaskMessage> tasks) {
        assert(!tasks.isEmpty());
        assert(ts != null);
        long txn_id = ts.getTransactionId();

        if (t) LOG.trace(String.format("Combining %d FragmentTaskMessages into a single Dtxn.CoordinatorFragment for %s", tasks.size(), ts));
        
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
        
        if (hstore_conf.site.exec_speculative_execution) this.notifyDonePartitions(ts);
        
        for (FragmentTaskMessage ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            // Important! Set the UsingDtxn flag for each FragmentTaskMessage so that we know
            // how to respond on the other side of the request
            ftask.setUsingDtxnCoordinator(true);
            
            int target_partition = ftask.getDestinationPartitionId();
            // Make sure things are still legit for our single-partition transaction
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (d) LOG.debug(String.format("%s on partition %d is suppose to be single-partitioned, but it wants to execute a fragment on partition %d",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            } else if (done_partitions.contains(target_partition)) {
                if (d) LOG.debug(String.format("%s on partition %d was marked as done on partition %d but now it wants to go back for more!",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            
            int dependency_ids[] = ftask.getOutputDependencyIds();
            if (t) LOG.trace(String.format("Preparing to request fragments %s on partition %d to generate %d output dependencies for %s",
                                           Arrays.toString(ftask.getFragmentIds()), target_partition, dependency_ids.length, ts));
            if (ftask.getFragmentCount() == 0) {
                LOG.warn("Trying to send a FragmentTask request with 0 fragments for " + ts);
                continue;
            }

            // Since we know that we have to send these messages everywhere, then any internal dependencies
            // that we have stored locally here need to go out with them
            if (ftask.hasInputDependencies()) {
                ts.remove_dependencies_map.clear();
                ts.removeInternalDependencies(ftask, ts.remove_dependencies_map);
                if (t) LOG.trace(String.format("%s - Attaching %d dependencies to %s", ts, ts.remove_dependencies_map.size(), ftask));
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
            if (t) LOG.trace(String.format("Aborting %s because it was mispredicted", ts));
            // XXX: This is kind of screwy because we don't actually want to send the touched partitions
            // histogram because VoltProcedure will just do it for us...
            throw new MispredictionException(txn_id, null);
        }
        
        // Bombs away!
        Dtxn.CoordinatorFragment dtxn_request = requestBuilder.build(); 
        this.hstore_site.requestWork(ts, dtxn_request, this.request_work_callback);
        if (d) LOG.debug(String.format("Work request is sent for %s [bytes=%d, #fragments=%d]",
                                       ts, dtxn_request.getSerializedSize(), tasks.size()));
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
            throw new RuntimeException("No transaction state for txn #" + txn_id + " at partition " + this.partitionId);
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
    protected VoltTable[] waitForResponses(LocalTransactionState ts, Collection<FragmentTaskMessage> ftasks, int batch_size) {
        if (d) LOG.debug(String.format("Dispatching %d messages and waiting for the results for %s", ftasks.size(), ts));
        
        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.getNextUndoToken());
        assert(batch_size > 0);
        ts.setBatchSize(batch_size);
        boolean first = true;
        boolean read_only = ts.isExecReadOnly();
        boolean predict_singlePartition = ts.isPredictSinglePartition();
        CountDownLatch latch = null;
        
        // Now if we have some work sent out to other partitions, we need to wait until they come back
        // In the first part, we wait until all of our blocked FragmentTaskMessages become unblocked
        LinkedBlockingDeque<Collection<FragmentTaskMessage>> queue = ts.getUnblockedFragmentTaskMessageQueue();
        
        // Run through this loop if:
        //  (1) This is our first time in the loop (first == true)
        //  (2) If we know that there are still messages being blocked
        //  (3) If we know that there are still unblocked messages that we need to process
        //  (4) The latch for this round is still greater than zero
//        if (hstore_conf.site.txn_profiling) ts.profiler.startExecCoordinatorBlocked();
        while (first == true || ts.getBlockedFragmentTaskMessageCount() > 0 || queue.size() > 0 || (latch != null && latch.getCount() > 0)) {
            
            // If this is the not first time through the loop, then poll the queue to get our list of fragments
            if (first == false) {
                ts.local_fragment_list.clear();
                if (!predict_singlePartition) ts.remote_fragment_list.clear();
                
                if (t) LOG.trace(String.format("Waiting for unblocked tasks for %s on partition %d", ts, this.partitionId));
                try {
                    ftasks = queue.takeFirst(); // BLOCKING
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) LOG.error("We were interrupted while waiting for blocked tasks for " + ts, ex);
                    return (null);
                }
            }
            assert(ftasks != null);
            if (ftasks.size() == 0) break;
            
            // FAST PATH: Assume everything is local
            if (predict_singlePartition) {
                for (FragmentTaskMessage ftask : ftasks) {
                    if (first == false || ts.addFragmentTaskMessage(ftask) == false) ts.local_fragment_list.add(ftask);
                } // FOR
                
                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound();
                    latch = ts.getDependencyLatch();
                }
                
                for (FragmentTaskMessage ftask : ts.local_fragment_list) {
                    if (t) LOG.trace(String.format("Got unblocked FragmentTaskMessage for %s. Executing locally...", ts));
                    assert(ftask.getDestinationPartitionId() == this.partitionId);
                    this.processFragmentTaskMessage(ts, ftask);
                    read_only = read_only && ftask.isReadOnly();
                } // FOR
                
            // SLOW PATH: Mixed local and remote messages
            } else {
                boolean all_local = true;
                boolean is_local;
                int num_local = 0;
                int num_remote = 0;
                
                // Look at each task and figure out whether it should be executed remotely or locally
                for (FragmentTaskMessage ftask : ftasks) {
                    is_local = (ftask.getDestinationPartitionId() == this.partitionId);
                    all_local = all_local && is_local;
                    if (first == false || ts.addFragmentTaskMessage(ftask) == false) {
                        if (is_local) {
                            ts.local_fragment_list.add(ftask);
                            num_local++;
                        } else {
                            ts.remote_fragment_list.add(ftask);
                            num_remote++;
                        }
                        read_only = read_only && ftask.isReadOnly();
                    }
                } // FOR
                if (num_local == 0 && num_remote == 0) {
                    throw new RuntimeException(String.format("Deadlock! All tasks for %s are blocked waiting on input!", ts));
                }

                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound();
                    latch = ts.getDependencyLatch();
                }
        
                // Now request the fragments that aren't local
                // We want to push these out as soon as possible
                if (num_remote > 0) {
                    if (t) LOG.trace(String.format("Requesting %d FragmentTaskMessages to be executed on remote partitions for %s", num_remote, ts));
                    this.requestWork(ts, ts.remote_fragment_list);
                }
        
                // Then execute all of the tasks are meant for the local partition directly
                if (num_local > 0) {
                    if (t) LOG.trace(String.format("Executing %d FragmentTaskMessages directly in local partition for %s", num_local, ts));
                    try {
                        for (FragmentTaskMessage ftask : ts.local_fragment_list) {
                            this.processFragmentTaskMessage(ts, ftask);
                        } // FOR
                    } catch (Exception ex) {
                        LOG.error("Unexpected error when executing fragments for " + ts, ex);
                        throw new RuntimeException(ex);
                    }
                }
            }
            if (read_only == false) ts.markExecNotReadOnly();
            first = false;
        } // WHILE

        // Now that we know all of our FragmentTaskMessages have been dispatched, we can then
        // wait for all of the results to come back in.
        if (latch == null) latch = ts.getDependencyLatch();
        if (latch.getCount() > 0) {
            if (d) {
                LOG.debug(String.format("All blocked messages dispatched for %s. Waiting for %d dependencies", ts, latch.getCount()));
                if (t) LOG.trace(ts.toString());
            }
            while (true) {
                boolean done = false;
                try {
                    done = latch.await(10000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) LOG.error("We were interrupted while waiting for results for " + ts, ex);
                    return (null);
                } catch (Throwable ex) {
                    new RuntimeException(String.format("Fatal error for %s while waiting for results", ts), ex);
                }
                if (done) break;
                LOG.warn("Still waiting for responses for " + ts + "\n" + ts.debug());
            } // WHILE
        }
        
//        if (hstore_conf.site.txn_profiling) ts.profiler.stopExecCoordinatorBlocked();
        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (d) LOG.warn(String.format("%s was hit with a %s", ts, ts.getPendingError().getClass().getSimpleName()));
            throw ts.getPendingError();
        }
        
        // Important: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        final VoltTable results[] = ts.getResults();
        ts.finishRound();
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
    protected void queueClientResponse(LocalTransactionState ts, ClientResponseImpl cresponse) {
        if (d) LOG.debug(String.format("Queuing ClientResponse for %s [handle=%s, status=%s]",
                                       ts, ts.getClientHandle(), cresponse.getStatusName()));
        assert(ts.isPredictSinglePartition() == true) :
            String.format("Specutatively executed multi-partition %s [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatusName());
        assert(ts.isSpeculative() == true) :
            String.format("Queuing non-specutative %s [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatusName());
        assert(this.exec_mode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatusName());
        assert(cresponse.getStatus() != ClientResponse.MISPREDICTION) : "Trying to queue mispredicted " + ts;
        // FIXME if (hstore_conf.site.txn_profiling) ts.profiler.finish_time.stop();
        this.queued_responses.add(Pair.of(ts, cresponse));
        if (d) LOG.debug("Total # of Queued Responses: " + this.queued_responses.size());
    }

    /**
     * Send a ClientResponseImpl message back to the coordinator
     */
    protected void processClientResponse(LocalTransactionState ts, ClientResponseImpl cresponse) {
        RpcCallback<Dtxn.FragmentResponse> callback = ts.getCoordinatorCallback();
        if (callback == null) {
            throw new RuntimeException("No RPC callback to HStoreSite for " + ts);
        }
        assert(cresponse.getClientHandle() != -1) : "The client handle for " + ts + " was not set properly";
        byte status = cresponse.getStatus();

        // Always mark the throttling flag so that the clients know whether they are allowed to keep
        // coming at us and make requests
        Dtxn.FragmentResponse.Builder builder = Dtxn.FragmentResponse.newBuilder();
        cresponse.setThrottleFlag(this.hstore_site.isIncomingThrottled(ts.getBasePartition()));
        
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        boolean is_local_singlepartitioned = ts.isExecSinglePartition() && ts.isExecLocal();
        if (d) LOG.debug(String.format("Processing ClientResponse for %s at partition %d [handle=%d, status=%s, singlePartition=%s, local=%s]",
                                       ts, this.partitionId, cresponse.getClientHandle(), cresponse.getStatusName(),
                                       ts.isExecSinglePartition(), ts.isExecLocal()));
        switch (status) {
            case ClientResponseImpl.SUCCESS:
                if (t) LOG.trace("Marking " + ts + " as success.");
                builder.setStatus(Dtxn.FragmentResponse.Status.OK);
                if (is_local_singlepartitioned) this.finishWork(ts, true);
                break;
            case ClientResponseImpl.MISPREDICTION:
                if (d) LOG.debug(String.format("%s mispredicted while executing at partition %d! Aborting work and restarting [isLocal=%s, singlePartition=%s, hasError=%s]",
                                               ts, this.partitionId, ts.isExecLocal(), ts.isExecSinglePartition(), ts.hasPendingError()));
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_MISPREDICT);
                this.finishWork(ts, false);
                break;
            case ClientResponseImpl.USER_ABORT:
                if (t) LOG.trace("Marking " + ts + " as user aborted.");
            default:
                if (status != ClientResponseImpl.USER_ABORT) {
                    this.error_counter.incrementAndGet();
                    if (d) LOG.warn("Unexpected server error for " + ts + ": " + cresponse.getStatusString());
                }
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_USER);
                if (is_local_singlepartitioned) this.finishWork(ts, false);
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
        
        if (d) LOG.debug("Invoking Dtxn.FragmentResponse callback for " + ts);
        callback.run(builder.build());
    }


        
    /**
     * Internal call to abort/commit the transaction
     * @param ts
     * @param commit
     */
    private void finishWork(TransactionState ts, boolean commit) {
        // Don't do anything if the TransactionState has already been marked as finished
        // This is ok because the Dtxn.Coordinator can't send us a single message for all of the partitions managed by our HStoreSite
        if (ts.isEE_Finished()) {
            assert(this.finished_txn_states.contains(ts)) : ts + " was marked as finished but it was not in our finished states!";
            return;
        }
        
        // This can be null if they haven't submitted anything
        Long undoToken = ts.getLastUndoToken();
        
        // Only commit/abort this transaction if:
        //  (1) We have an ExecutionEngine handle
        //  (2) We have the last undo token used by this transaction
        //  (3) The transaction was executed with undo buffers
        //  (4) The transaction actually submitted work to the EE
        //  (5) The transaction modified data at this partition
        if (this.ee != null && ts.hasSubmittedEE() && ts.isExecReadOnly() == false && undoToken != null) {
            if (undoToken == ExecutionSite.DISABLE_UNDO_LOGGING_TOKEN) {
                if (commit == false) {
                    LOG.fatal(ts.debug());
                    this.crash(new RuntimeException("TRYING TO ABORT TRANSACTION WITHOUT UNDO LOGGING: "+ ts));
                }
            } else {
                synchronized (this.ee) {
                    if (commit) {
                        if (d) LOG.debug(String.format("Committing %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                       ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE()));
                        this.ee.releaseUndoToken(undoToken);
        
                    // Evan says that txns will be aborted LIFO. This means the first txn that
                    // we get in abortWork() will have a the greatest undoToken, which means that 
                    // it will automagically rollback all other outstanding txns.
                    // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
                    // we should be able to check whether our undoToken has already been rolled back
                    } else {
                        if (d) LOG.debug(String.format("Aborting %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                       ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE()));
                        this.ee.undoUndoToken(undoToken);
                    }
                } // SYNCH
            }
        }
        
        // We always need to do the following things regardless if we hit up the EE or not
        if (commit) this.lastCommittedTxnId = ts.getTransactionId();
        ts.setEE_Finished();
        this.finished_txn_states.add(ts);
    }
    
    /**
     * The coordinator is telling our site to abort/commit the txn with the
     * provided transaction id. This method should only be used for multi-partition transactions, because
     * it will do some extra work for speculative execution
     * @param txn_id
     * @param commit If true, the work performed by this txn will be commited. Otherwise it will be aborted
     */
    public void finishWork(long txn_id, boolean commit) {
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            if (t) LOG.trace(msg + ". Ignoring for now...");
            return;   
        }
        boolean cleanup_dtxn = (this.current_dtxn == ts);
        if (d) LOG.debug(String.format("Procesing finishWork request for %s at partition %d [currentDtxn=%s, cleanup=%s]", ts, this.partitionId, this.current_dtxn, cleanup_dtxn));
        
        this.finishWork(ts, commit);
        
        // Check whether this is the response that the speculatively executed txns have been waiting for
        // We could have turned off speculative execution mode beforehand 
        if (cleanup_dtxn) {
            synchronized (this.exec_mode) {
                // We can always commit our boys no matter what if we know that this multi-partition txn was read-only 
                // at the given partition
                if (hstore_conf.site.exec_speculative_execution) {
                    if (d) LOG.debug(String.format("Turning off speculative execution mode at partition %d because %s is finished", this.partitionId, ts));
                    Boolean readonly = this.isReadOnly(txn_id);
                    this.releaseQueuedResponses(readonly != null && readonly == true ? true : commit);
                    
                    this.exec_latch.release();
                    assert(this.exec_latch.availablePermits() <= 1) : String.format("Invalid exec latch state [permits=%d, mode=%s]", this.exec_latch.availablePermits(), this.exec_mode);
                }

                // Setting the current_dtxn variable has to come *before* we change the execution mode
                if (t) LOG.trace(String.format("Unmarking %s as the current DTXN at partition %d and setting execution mode to %s",
                                               this.current_dtxn, this.partitionId, ExecutionMode.COMMIT_ALL));
                this.current_dtxn = null;
                this.setExecutionMode(ExecutionMode.COMMIT_ALL, txn_id);
                
                // Release blocked transactions
                this.releaseBlockedTransactions(txn_id, false);
            } // SYNCH
        }
    }    
    /**
     * 
     * @param txn_id
     * @param p
     */
    private void releaseBlockedTransactions(long txn_id, boolean speculative) {
        if (this.current_dtxn_blocked.isEmpty() == false) {
            if (d) LOG.debug(String.format("Releasing %d transactions at partition %d because of txn #%d",
                                           this.current_dtxn_blocked.size(), this.partitionId, txn_id));
            this.work_queue.addAll(this.current_dtxn_blocked);
//            for (TransactionInfoBaseMessage task : this.current_dtxn_blocked) {
//                if (speculative) release_ts.setSpeculative(true);
//                this.work_queue.add(task);
//            } // FOR
            this.current_dtxn_blocked.clear();
        }
    }
    
    /**
     * Commit/abort all of the queue transactions that were specutatively executed and waiting for
     * their responses to be sent back to the client
     * @param commit
     */
    private void releaseQueuedResponses(boolean commit) {
        // First thing we need to do is get the latch that will be set by any transaction
        // that was in the middle of being executed when we were called
        if (d) LOG.debug(String.format("Checking waiting/blocked transactions at partition %d [mode=%s, isLocked=%s]",
                                       this.partitionId, this.exec_mode, (this.exec_latch.availablePermits() == 0)));
        
        if (this.exec_latch.tryAcquire() == false) {
            try {
                if (d) LOG.debug(String.format("Partition %d is waiting for spec exec latch to be unblocked", this.partitionId));
                this.exec_latch.acquire();
            } catch (InterruptedException ex) {
                if (this.isShuttingDown() == false) {
                    LOG.fatal("Unexpected interuption while trying to drain queued responses", ex); 
                }
                return;
            }
            if (d) LOG.debug(String.format("Partition %d is now ready to clean up queue responses", this.partitionId));
        }
        assert(this.exec_latch.availablePermits() == 0) : String.format("Invalid exec latch state [permits=%d, mode=%s]", this.exec_latch.availablePermits(), this.exec_mode);
        
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
        Pair<LocalTransactionState, ClientResponseImpl> p = null;
        boolean ee_commit = true;
        int skip_commit = 0;
        int aborted = 0;
        while ((p = (hstore_conf.site.exec_queued_response_ee_bypass ? this.queued_responses.pollLast() : this.queued_responses.pollFirst())) != null) {
            LocalTransactionState ts = p.getFirst();
            ClientResponseImpl cr = p.getSecond();
            // 2011-07-02: I have no idea how this could not be stopped here, but for some reason
            // I am getting a random error.
            // FIXME if (hstore_conf.site.txn_profiling && ts.profiler.finish_time.isStopped()) ts.profiler.finish_time.start();
            
            // If the multi-p txn aborted, then we need to abort everything in our queue
            // Change the status to be a MISPREDICT so that they get executed again
            if (commit == false) {
                cr.setStatus(ClientResponse.MISPREDICTION);
                ts.setPendingError(new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions()), false);
                aborted++;
                
            // Optimization: Check whether the last element in the list is a commit
            // If it is, then we know that we don't need to tell the EE about all the ones that executed before it
            } else if (hstore_conf.site.exec_queued_response_ee_bypass) {
                // Don't tell the EE that we committed
                if (ee_commit == false) {
                    if (t) LOG.trace(String.format("Bypassing EE commit for %s [undoToken=%d]", ts, ts.getLastUndoToken()));
                    ts.unsetSubmittedEE();
                    skip_commit++;
                    
                } else if (ee_commit && cr.getStatus() == ClientResponse.SUCCESS) {
                    if (t) LOG.trace(String.format("Committing %s but will bypass all other successful transactions [undoToken=%d]", ts, ts.getLastUndoToken()));
                    ee_commit = false;
                }
            }
            
            if (hstore_conf.site.exec_postprocessing_thread) {
                if (t) LOG.trace(String.format("Passing queued ClientResponse for %s to post-processing thread [status=%s]", ts, cr.getStatusName()));
                hstore_site.queueClientResponse(this, ts, cr);
            } else {
                if (t) LOG.trace(String.format("Sending queued ClientResponse for %s back directly [status=%s]", ts, cr.getStatusName()));
                this.processClientResponse(ts, cr);
            }
        } // WHILE
        if (d && skip_commit > 0 && hstore_conf.site.exec_queued_response_ee_bypass) {
            LOG.debug(String.format("Fast Commit EE Bypass Optimization [skipped=%d, aborted=%d]", skip_commit, aborted));
        }
        return;
    }
    
    // ---------------------------------------------------------------
    // SHUTDOWN METHODS
    // ---------------------------------------------------------------
    
    /**
     * Cause this ExecutionSite to make the entire HStore cluster shutdown
     * This won't return!
     */
    public synchronized void crash(Throwable ex) {
        LOG.warn(String.format("ExecutionSite for Partition #%d is crashing", this.partitionId), ex);
        assert(this.hstore_messenger != null);
        this.hstore_messenger.shutdownCluster(ex); // This won't return
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.hstore_site.isShuttingDown()); // shutdown_state == State.PREPARE_SHUTDOWN || this.shutdown_state == State.SHUTDOWN);
    }
    
    @Override
    public void prepareShutdown() {
        shutdown_state = Shutdownable.ShutdownState.PREPARE_SHUTDOWN;
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
