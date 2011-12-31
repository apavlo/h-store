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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
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
import org.voltdb.messaging.FinishTaskMessage;
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
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.TransactionWorkResponse;
import edu.brown.hstore.Hstore.TransactionWorkResponse.PartitionResult;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.EstimationThresholds;
import edu.brown.markov.MarkovEstimate;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.EventObservable;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TypedPoolableObjectFactory;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreConstants;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionFinishCallback;
import edu.mit.hstore.callbacks.TransactionPrepareCallback;
import edu.mit.hstore.dtxn.AbstractTransaction;
import edu.mit.hstore.dtxn.ExecutionState;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.dtxn.MapReduceTransaction;
import edu.mit.hstore.dtxn.RemoteTransaction;
import edu.mit.hstore.interfaces.Loggable;
import edu.mit.hstore.interfaces.Shutdownable;
import edu.mit.hstore.util.ThrottlingQueue;

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
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to load procedure class '" + className + "'", e);
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
                               ExecutionSite.this.p_estimator);
            } catch (Exception e) {
                if (d) LOG.warn("__FILE__:__LINE__ " + "Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
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
    public final Map<Integer, BatchPlanner> POOL_BATCH_PLANNERS = new HashMap<Integer, BatchPlanner>(100);
    
    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    private Thread self;
    
    protected int siteId;
    protected int partitionId;
    protected Collection<Integer> localPartitionIds;
    
    /**
     * This is the execution state for the current transaction.
     * There is only one of these per partition, so it must be cleared out for each new txn
     */
    private final ExecutionState execState;

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
    protected HStoreCoordinator hstore_coordinator;
    protected HStoreConf hstore_conf;
    
    // ----------------------------------------------------------------------------
    // Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * We can only have one active distributed transactions at a time.  
     * The multi-partition TransactionState that is currently executing at this partition
     * When we get the response for these txn, we know we can commit/abort the speculatively executed transactions
     */
    private AbstractTransaction current_dtxn = null;
    
    
    private final ReentrantLock exec_lock = new ReentrantLock();
    
//    private final Semaphore dtxn_lock = new Semaphore(1);

    /**
     * Sets of InitiateTaskMessages that are blocked waiting for the outstanding dtxn to commit
     */
    private Set<TransactionInfoBaseMessage> current_dtxn_blocked = new HashSet<TransactionInfoBaseMessage>();

    /**
     * ClientResponses from speculatively executed transactions that are waiting to be committed 
     */
    private final LinkedBlockingDeque<Pair<LocalTransaction, ClientResponseImpl>> queued_responses = new LinkedBlockingDeque<Pair<LocalTransaction,ClientResponseImpl>>();
    private ExecutionMode exec_mode = ExecutionMode.COMMIT_ALL;
    
    private Long currentTxnId = null;
    
    /** The time in ms since epoch of the last call to ExecutionEngine.tick(...) */
    private long lastTickTime = 0;
    /** The last txn id that we executed (either local or remote) */
    private volatile Long lastExecutedTxnId = null;
    /** The last txn id that we committed */
    private volatile long lastCommittedTxnId = -1;
    /** The last undoToken that we handed out */
    private final AtomicLong lastUndoToken = new AtomicLong(0l);

    /**
     * This is the queue of the list of things that we need to execute.
     * The entries may be either InitiateTaskMessages (i.e., start a stored procedure) or
     * FragmentTaskMessage (i.e., execute some fragments on behalf of another transaction)
     */
    private final PriorityBlockingQueue<TransactionInfoBaseMessage> work_queue = new PriorityBlockingQueue<TransactionInfoBaseMessage>(10000, work_comparator) {
        private static final long serialVersionUID = 1L;
        private final List<TransactionInfoBaseMessage> swap = new ArrayList<TransactionInfoBaseMessage>();
        
        @Override
        public int drainTo(Collection<? super TransactionInfoBaseMessage> c) {
            assert(c != null);
            TransactionInfoBaseMessage msg = null;
            int ctr = 0;
            this.swap.clear();
            while ((msg = this.poll()) != null) {
                // All new transaction requests must be put in the new collection
                if (msg instanceof InitiateTaskMessage) {
                    c.add(msg);
                    ctr++;
                // Everything else will get added back in afterwards 
                } else {
                    this.swap.add(msg);
                }
            } // WHILE
            if (this.swap.isEmpty() == false) this.addAll(this.swap);
            return (ctr);
        }
    };
    private final ThrottlingQueue<TransactionInfoBaseMessage> work_throttler;
    
    private static final Comparator<TransactionInfoBaseMessage> work_comparator = new Comparator<TransactionInfoBaseMessage>() {
        @Override
        public int compare(TransactionInfoBaseMessage msg0, TransactionInfoBaseMessage msg1) {
            assert(msg0 != null);
            assert(msg1 != null);
            
            Class<? extends TransactionInfoBaseMessage> class0 = msg0.getClass();
            Class<? extends TransactionInfoBaseMessage> class1 = msg1.getClass();
            
            if (class0.equals(class1)) return (int)(msg0.getTxnId() - msg1.getTxnId());

            boolean isFinish0 = class0.equals(FinishTaskMessage.class);
            boolean isFinish1 = class1.equals(FinishTaskMessage.class);
            if (isFinish0 && !isFinish1) return (-1);
            else if (!isFinish0 && isFinish1) return (1);
            
            boolean isWork0 = class0.equals(FragmentTaskMessage.class);
            boolean isWork1 = class1.equals(FragmentTaskMessage.class);
            if (isWork0 && !isWork1) return (-1);
            else if (!isWork0 && isWork1) return (1);
            
            assert(false) : String.format("%s <-> %s", class0, class1);
            return 0;
        }
    };

    // ----------------------------------------------------------------------------
    // TEMPORARY DATA COLLECTIONS
    // ----------------------------------------------------------------------------
    
    /**
     * Temporary space used in ExecutionSite.waitForResponses
     */
    private final List<FragmentTaskMessage> tmp_remoteFragmentList = new ArrayList<FragmentTaskMessage>();
    private final List<FragmentTaskMessage> tmp_localPartitionFragmentList = new ArrayList<FragmentTaskMessage>();
    private final List<FragmentTaskMessage> tmp_localSiteFragmentList = new ArrayList<FragmentTaskMessage>();
    
    /**
     * Temporary space used when calling removeInternalDependencies()
     */
    private final HashMap<Integer, List<VoltTable>> tmp_removeDependenciesMap = new HashMap<Integer, List<VoltTable>>();

    /**
     * Remote SiteId -> TransactionWorkRequest.Builder
     */
    private final Map<Integer, Hstore.TransactionWorkRequest.Builder> tmp_transactionRequestBuildersMap = new HashMap<Integer, Hstore.TransactionWorkRequest.Builder>();
    
    /**
     * PartitionId -> List<VoltTable>
     */
    private final Map<Integer, List<VoltTable>> tmp_EEdependencies = new HashMap<Integer, List<VoltTable>>();
    
    // ----------------------------------------------------------------------------
    // PROFILING OBJECTS
    // ----------------------------------------------------------------------------
    
    /**
     * How much time the ExecutionSite was idle waiting for work to do in its queue
     */
    private final ProfileMeasurement work_idle_time = new ProfileMeasurement("EE_IDLE");
    /**
     * How much time it takes for this ExecutionSite to execute a transaction
     */
    private final ProfileMeasurement work_exec_time = new ProfileMeasurement("EE_EXEC");
    
    // ----------------------------------------------------------------------------
    // Callbacks
    // ----------------------------------------------------------------------------

    /**
     * This will be invoked for each TransactionWorkResponse that comes back from
     * the remote HStoreSites. Note that we don't need to do any counting as to whether
     * a transaction has gotten back all of the responses that it expected. That logic is down
     * below in waitForResponses()
     */
    private final RpcCallback<Hstore.TransactionWorkResponse> request_work_callback = new RpcCallback<Hstore.TransactionWorkResponse>() {
        @Override
        public void run(Hstore.TransactionWorkResponse msg) {
            long txn_id = msg.getTransactionId();
            AbstractTransaction ts = hstore_site.getTransaction(txn_id);
            assert(ts != null) : "No transaction state exists for txn #" + txn_id;
            
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Processing Hstore.TransactionWorkResponse for %s with %d results",
                                        ts, msg.getResultsCount()));
            for (int i = 0, cnt = msg.getResultsCount(); i < cnt; i++) {
                PartitionResult result = msg.getResults(i); 
                if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Got %s from partition %d for %s [bytes=%d]",
                                               result.getClass().getSimpleName(), result.getPartitionId(), ts, result.getOutput().size()));
                
                ByteString serialized = result.getOutput();
                FragmentResponseMessage response = null;
                try {
                    response = (FragmentResponseMessage)VoltMessage.createMessageFromBuffer(serialized.asReadOnlyByteBuffer(), false);
                } catch (Exception ex) {
                    LOG.fatal("__FILE__:__LINE__ " + String.format("Failed to deserialize embedded %s message\n%s",
                                            msg.getClass().getSimpleName(), Arrays.toString(serialized.toByteArray())), ex);
                    System.exit(1);
                }
                assert(response != null);
                ExecutionSite.this.processFragmentResponseMessage((LocalTransaction)ts, response);
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
                LOG.trace("__FILE__:__LINE__ " + "Registered @" + proc.getClass().getSimpleName() + " sysproc handle for FragmentId #" + pfId);
            }
        } // SYNCH
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
        this.work_throttler = null;
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
        this.localPartitionIds = null;
        this.execState = null;
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
    public ExecutionSite(final int partitionId, final Catalog catalog, final BackendTarget target, PartitionEstimator p_estimator, TransactionEstimator t_estimator) {
        this.hstore_conf = HStoreConf.singleton();
        
        this.work_throttler = new ThrottlingQueue<TransactionInfoBaseMessage>(
                this.work_queue,
                hstore_conf.site.queue_incoming_max_per_partition,
                hstore_conf.site.queue_incoming_release_factor,
                hstore_conf.site.queue_incoming_increase
        );
        
        this.catalog = catalog;
        this.partition = CatalogUtil.getPartitionById(this.catalog, partitionId);
        assert(this.partition != null) : "Invalid Partition #" + partitionId;
        this.partitionId = this.partition.getId();
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
            if (d) LOG.debug("__FILE__:__LINE__ " + "Creating EE wrapper with target type '" + target + "'");
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
            LOG.fatal("__FILE__:__LINE__ " + "Failed to initialize ExecutionSite", ex);
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
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final IllegalAccessException e) {
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
                    System.exit(1);
                } catch (final ClassNotFoundException e) {
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to load procedure class '" + className + "'", e);
                    System.exit(1);
                }
                
            } else {
                volt_proc = new VoltProcedure.StmtProcedure();
            }
            volt_proc.globalInit(ExecutionSite.this,
                                 catalog_proc,
                                 this.backend_target,
                                 this.hsql,
                                 this.p_estimator);
            this.procedures.put(catalog_proc.getName(), volt_proc);
        } // FOR
    }

    /**
     * Link this ExecutionSite with its parent HStoreSite
     * This will initialize the references the various components shared among the ExecutionSites 
     * @param hstore_site
     */
    public void initHStoreSite(HStoreSite hstore_site) {
        if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Initializing HStoreSite components at partition %d", this.partitionId));
        assert(this.hstore_site == null);
        this.hstore_site = hstore_site;
        this.hstore_coordinator = hstore_site.getCoordinator();
//        this.helper = hstore_site.getExecutionSiteHelper();
        this.thresholds = (hstore_site != null ? hstore_site.getThresholds() : null);
        this.localPartitionIds = hstore_site.getLocalPartitionIds();
        
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
        this.self.setName(HStoreSite.getThreadName(this.hstore_site, this.partitionId));
        
        if (hstore_conf.site.cpu_affinity) {
            this.hstore_site.getThreadManager().registerEEThread(partition);
        }
        
        // Things that we will need in the loop below
        AbstractTransaction current_txn = null;
        TransactionInfoBaseMessage work = null;
        boolean stop = false;
        long txn_id = -1;
        
        try {
            // Setup shutdown lock
            this.shutdown_latch = new Semaphore(0);
            
            if (d) LOG.debug("__FILE__:__LINE__ " + "Starting ExecutionSite run loop...");
            while (stop == false && this.isShuttingDown() == false) {
                txn_id = -1;
                work = null;
                
                // -------------------------------
                // Poll Work Queue
                // -------------------------------
                try {
                    work = this.work_queue.poll();
                    if (work == null) {
                        if (t) LOG.trace("__FILE__:__LINE__ " + "Partition " + this.partitionId + " queue is empty. Waiting...");
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.start();
                        work = this.work_queue.take();
                        if (hstore_conf.site.exec_profiling) this.work_idle_time.stop();
                    }
                } catch (InterruptedException ex) {
                    if (d && this.isShuttingDown() == false) LOG.debug("__FILE__:__LINE__ " + "Unexpected interuption while polling work queue. Halting ExecutionSite...", ex);
                    stop = true;
                    break;
                }
                
                txn_id = work.getTxnId();
                current_txn = hstore_site.getTransaction(txn_id);
                if (current_txn == null) {
                    String msg = "No transaction state for txn #" + txn_id;
                    LOG.error("__FILE__:__LINE__ " + msg + "\n" + work.toString());
                    throw new RuntimeException(msg);
                }
                if (hstore_conf.site.exec_profiling) {
                    this.currentTxnId = txn_id;
                }
                
                // -------------------------------
                // Execute Query Plan Fragments
                // -------------------------------
                if (work instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftask = (FragmentTaskMessage)work;
                    
                    // If there is already a DTXN for this partition and it's not us, then we 
                    // we have to block.
                    // TODO: We should have a HStoreConf option that determines whether we should
                    // block or insert ourselves back into the queue so that we can maybe execute
                    // some other transactions.
//                    if (this.current_dtxn == null || current_txn != this.current_dtxn) {
//                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Blocking %s on DTXN lock [currentDtxn=%s]", current_txn, current_dtxn));
//                        this.dtxn_lock.acquire();
//                        if (d) LOG.debug("__FILE__:__LINE__ " + current_txn + " has acquired DTXN lock");
//                    }
                    
                    // At this point we know that we are either the current dtxn or the current dtxn is null
                    exec_lock.lock();
                    try {
                        if (this.current_dtxn == null) {
                            this.setCurrentDtxn(current_txn);
                            if (hstore_conf.site.exec_speculative_execution) {
                                this.setExecutionMode(current_txn, ftask.isReadOnly() ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
                            } else {
                                this.setExecutionMode(current_txn, ExecutionMode.DISABLED);
                            }
                                
                            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Marking %s as current DTXN on partition %d [execMode=%s]",
                                                           current_txn, this.partitionId, this.exec_mode));                    
    
                        // Check whether we should drop down to a less permissive speculative execution mode
                        } else if (hstore_conf.site.exec_speculative_execution && ftask.isReadOnly() == false) {
                            this.setExecutionMode(current_txn, ExecutionMode.COMMIT_NONE);
                        }
                    } finally {
                        exec_lock.unlock();
                    } // SYNCH
                    
                    this.processFragmentTaskMessage(current_txn, ftask);

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

                    this.processInitiateTaskMessage((LocalTransaction)current_txn, itask);
                    if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                    
                // -------------------------------
                // Finish Transaction
                // -------------------------------
                } else if (work instanceof FinishTaskMessage) {
//                    if (hstore_conf.site.exec_profiling) this.work_exec_time.start();
                    if(d) LOG.debug("<FinishTaskMessage>for txn: " + current_txn);
//                    if (current_txn instanceof MapReduceTransaction) {
//                        MapReduceTransaction orig_ts = (MapReduceTransaction)current_txn; 
//                        current_txn = orig_ts.getLocalTransaction(this.partitionId);
//                        //this.setCurrentDtxn(current_txn);
//                        if(d) LOG.debug("<FinishTaskMessage> I am a MapReduceTransaction: " + current_txn);
//                        assert(current_txn != null) : "Unexpected null LocalTransaction handle from " + orig_ts;
//                    }
                    
                    FinishTaskMessage ftask = (FinishTaskMessage)work;
                    this.finishTransaction(current_txn, (ftask.getStatus() == Hstore.Status.OK));
//                    if (hstore_conf.site.exec_profiling) this.work_exec_time.stop();
                    
                // -------------------------------
                // BAD MOJO!
                // -------------------------------
                } else if (work != null) {
                    throw new RuntimeException("Unexpected work message in queue: " + work);
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
                LOG.fatal("__FILE__:__LINE__ " + String.format("Unexpected error for ExecutionSite partition #%d [%s]%s",
                                        this.partitionId, (current_txn != null ? " - " + current_txn : ""), ex), ex);
                if (current_txn != null) LOG.fatal("__FILE__:__LINE__ " + "TransactionState Dump:\n" + current_txn.debug());
                
            }
            this.hstore_coordinator.shutdownCluster(new Exception(ex));
        } finally {
            String txnDebug = "";
            if (debug.get() && current_txn != null && current_txn.getBasePartition() == this.partitionId) {
                txnDebug = "\n" + current_txn.debug();
            }
            LOG.warn("__FILE__:__LINE__ " + String.format("Partition %d ExecutionSite is stopping.%s%s",
                                   this.partitionId, (txn_id > 0 ? " In-Flight Txn: #" + txn_id : ""), txnDebug));
            
            // Release the shutdown latch in case anybody waiting for us
            this.shutdown_latch.release();
            
            // Stop HStoreMessenger (because we're nice)
            if (this.isShuttingDown() == false) {
                if (this.hstore_coordinator != null) this.hstore_coordinator.shutdown();
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
    public ThrottlingQueue<TransactionInfoBaseMessage> getThrottlingQueue() {
        return (this.work_throttler);
    }
    
    public HStoreSite getHStoreSite() {
        return (this.hstore_site);
    }
    public HStoreConf getHStoreConf() {
        return (this.hstore_conf);
    }
    public HStoreCoordinator getHStoreMessenger() {
        return (this.hstore_coordinator);
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
    public Partition getPartition() {
        return (this.partition);
    }
    public int getPartitionId() {
        return (this.partitionId);
    }
    public Collection<Integer> getLocalPartitionIds() {
        return (this.localPartitionIds);
    }
    
    public Long getLastExecutedTxnId() {
        return (this.lastExecutedTxnId);
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
    private void setExecutionMode(AbstractTransaction ts, ExecutionMode mode) {
        if (d && this.exec_mode != mode) {
//        if (this.exec_mode != mode) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Setting ExecutionMode for partition %d to %s because of %s [currentDtxn=%s, origMode=%s]",
                                    this.partitionId, mode, ts, this.current_dtxn, this.exec_mode));
        }
        assert(mode != ExecutionMode.COMMIT_READONLY || (mode == ExecutionMode.COMMIT_READONLY && this.current_dtxn != null)) :
            String.format("%s is trying to set partition %d to %s when the current DTXN is null?", ts, this.partitionId, mode);
        this.exec_mode = mode;
    }
    public ExecutionMode getExecutionMode() {
        return (this.exec_mode);
    }
    public AbstractTransaction getCurrentDtxn() {
        return (this.current_dtxn);
    }
    public Long getCurrentTxnId() {
        return (this.currentTxnId);
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
     * Returns the VoltProcedure instance for a given stored procedure name
     * @param proc_name
     * @return
     */
    public VoltProcedure getVoltProcedure(String proc_name) {
        return (this.procedures.get(proc_name));
    }
    
    
    /**
     * 
     * @param ts
     */
    private void setCurrentDtxn(AbstractTransaction ts) {
        // There can never be another current dtxn still unfinished at this partition!
        assert(ts == null || this.current_dtxn_blocked.isEmpty()) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.current_dtxn, ts, this.current_dtxn_blocked.size());
        assert(ts == null || this.current_dtxn == null || this.current_dtxn.isInitialized() == false) :
            String.format("Concurrent multi-partition transactions at partition %d: Orig[%s] <=> New[%s] / BlockedQueue:%d",
                          this.partitionId, this.current_dtxn, ts, this.current_dtxn_blocked.size());
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Setting %s as the current DTXN for partition #%d [previous=%s]",
                                       ts, this.partitionId, this.current_dtxn));
        this.current_dtxn = ts;
    }
    
    // ---------------------------------------------------------------
    // ExecutionSite API
    // ---------------------------------------------------------------
    
    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * We should not be sent an InitiateTaskMessage here!
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void queueWork(AbstractTransaction ts, FragmentTaskMessage task) {
        assert(ts.isInitialized());
        
        this.work_queue.add(task);
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Added multi-partition %s for %s to front of partition %d work queue [size=%d]",
                                       task.getClass().getSimpleName(), ts, this.partitionId, this.work_queue.size()));
    }
    
    /**
     * 
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void queueFinish(AbstractTransaction ts, FinishTaskMessage task) {
        assert(ts.isInitialized());
        
        this.work_queue.add(task);
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Added multi-partition %s for %s to front of partition %d work queue [size=%d]",
                                       task.getClass().getSimpleName(), ts, this.partitionId, this.work_queue.size()));
    }

    /**
     * New work for a local transaction
     * @param ts
     * @param task
     * @param callback
     */
    public boolean queueNewTransaction(LocalTransaction ts, InitiateTaskMessage task) {
        assert(ts != null) : "The TransactionState is somehow null for txn #" + task.getTxnId();
        final boolean singlePartitioned = ts.isPredictSinglePartition();
        boolean success = true;
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Queuing new transaction execution request for %s on partition %d [currentDtxn=%s, mode=%s, taskHash=%d]",
                                       ts, this.partitionId, this.current_dtxn, this.exec_mode, task.hashCode()));
        
        // If we're a single-partition and speculative execution is enabled, then we can always set it up now
        if (hstore_conf.site.exec_speculative_execution && singlePartitioned && this.exec_mode != ExecutionMode.DISABLED) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Adding %s to work queue at partition %d [size=%d]", ts, this.partitionId, this.work_queue.size()));
            success = this.work_throttler.offer(task, false);
            
        // Otherwise figure out whether this txn needs to be blocked or not
        } else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Attempting to add %s for %s to partition %d queue [currentTxn=%s]",
                                           task.getClass().getSimpleName(), ts, this.partitionId, this.currentTxnId));
            exec_lock.lock();
            try {
                // No outstanding DTXN
                if (this.current_dtxn == null && this.exec_mode != ExecutionMode.DISABLED) {
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Adding %s for %s to work queue [size=%d]",
                                                   task.getClass().getSimpleName(), ts, this.work_queue.size()));
                    // Only use the throttler for single-partition txns
                    if (singlePartitioned) {
                        success = this.work_throttler.offer(task, false);
                    } else {
                        // this.work_queue.addFirst(task);
                        this.work_queue.add(task);
                    }
                } else {
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Blocking %s until dtxn %s finishes", ts, this.current_dtxn));
                    this.current_dtxn_blocked.add(task);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        if (success == false) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s got throttled by partition %d [currentTxn=%s, throttled=%s, queueSize=%d]",
                                           ts, this.partitionId, this.currentTxnId, this.work_throttler.isThrottled(), this.work_throttler.size()));
            if (singlePartitioned == false) {
                TransactionFinishCallback finish_callback = ts.initTransactionFinishCallback(Hstore.Status.ABORT_THROTTLED);
                hstore_coordinator.transactionFinish(ts, Hstore.Status.ABORT_THROTTLED, finish_callback);
            }
            hstore_site.transactionReject(ts, Hstore.Status.ABORT_THROTTLED);
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
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Checking whether %s is read-only at partition %d [readOnly=%s]",
                        ts, this.partitionId, ts.isExecReadOnly(this.partitionId)));
        
        // Check whether the txn that we're waiting for is read-only.
        // If it is, then that means all read-only transactions can commit right away
        if (ts.isExecReadOnly(this.partitionId)) {
            ExecutionMode newMode = ExecutionMode.COMMIT_READONLY;
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Attempting to enable %s speculative execution at partition %d [currentMode=%s, txn=%s]",
                                           newMode, partitionId, this.exec_mode, ts));
            exec_lock.lock();
            try {
                if (this.current_dtxn == ts && this.exec_mode != ExecutionMode.DISABLED) {
                    this.setExecutionMode(ts, newMode);
                    this.releaseBlockedTransactions(ts, true);
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Enabled %s speculative execution at partition %d [txn=%s]",
                                                   this.exec_mode, partitionId, ts));
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
     * @param fresponse
     */
    protected void processFragmentResponseMessage(LocalTransaction ts, FragmentResponseMessage fresponse) {
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Processing FragmentResponseMessage for %s on partition %d [srcPartition=%d, deps=%d]",
                                       ts, this.partitionId, fresponse.getSourcePartitionId(), fresponse.getTableCount()));
        
        // If the Fragment failed to execute, then we need to abort the Transaction
        // Note that we have to do this before we add the responses to the TransactionState so that
        // we can be sure that the VoltProcedure knows about the problem when it wakes the stored 
        // procedure back up
        if (fresponse.getStatusCode() != FragmentResponseMessage.SUCCESS) {
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Received non-success response %s from partition %d for %s",
                                            fresponse.getStatusCodeName(), fresponse.getSourcePartitionId(), ts));
            ts.setPendingError(fresponse.getException());
        }
        for (int i = 0, cnt = fresponse.getTableCount(); i < cnt; i++) {
            assert(fresponse.hasTableAtIndex(i)) : 
                String.format("Missing result table from FragmentResponseMessage for %s\n%s", ts, fresponse);
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Storing intermediate result from partition %d for %s",
                                           fresponse.getSourcePartitionId(), ts));
            ts.addResult(fresponse.getSourcePartitionId(),
                         fresponse.getTableDependencyIdAtIndex(i),
                         fresponse.getTableAtIndex(i));
        } // FOR
    }
    
    /**
     * Execute a new transaction based on an InitiateTaskMessage
     * @param itask
     */
    protected void processInitiateTaskMessage(LocalTransaction ts, InitiateTaskMessage itask) throws InterruptedException {
        if (hstore_conf.site.txn_profiling) ts.profiler.startExec();
        
        ExecutionMode before_mode = ExecutionMode.COMMIT_ALL;
        boolean predict_singlePartition = ts.isPredictSinglePartition();
        
        if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Attempting to begin processing %s for %s on partition %d [taskHash=%d]",
                                       itask.getClass().getSimpleName(), ts, this.partitionId, itask.hashCode()));
        // If this is going to be a multi-partition transaction, then we will mark it as the current dtxn
        // for this ExecutionSite.
        if (predict_singlePartition == false) {
            this.exec_lock.lock();
            try {
                if (this.current_dtxn != null) {
                    this.current_dtxn_blocked.add(itask);
                    return;
                }
                this.setCurrentDtxn(ts);
                // 2011-11-14: We don't want to set the execution mode here, because we know that we
                //             can check whether we were read-only after the txn finishes
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Marking %s as current DTXN on Partition %d [isLocal=%s, execMode=%s]",
                                               ts, this.partitionId, true, this.exec_mode));                    
                before_mode = this.exec_mode;
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
                if (this.exec_mode != ExecutionMode.COMMIT_ALL) {
                    assert(this.current_dtxn != null) : String.format("Invalid execution mode %s without a dtxn at partition %d", this.exec_mode, this.partitionId);
                    
                    // HACK: If we are currently under DISABLED mode when we get this, then we just need to block the transaction
                    // and return back to the queue. This is easier than having to set all sorts of crazy locks
                    if (this.exec_mode == ExecutionMode.DISABLED) {
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Blocking single-partition %s until dtxn %s finishes [mode=%s]", ts, this.current_dtxn, this.exec_mode));
                        this.current_dtxn_blocked.add(itask);
                        return;
                    }
                    
                    before_mode = this.exec_mode;
                    if (hstore_conf.site.exec_speculative_execution) {
                        ts.setSpeculative(true);
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Marking %s as speculatively executed on partition %d [txnMode=%s, dtxn=%s]", ts, this.partitionId, before_mode, this.current_dtxn));
                    }
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        // Always reset the ExecutionState
        this.execState.clear();
        ts.setExecutionState(this.execState);
        
        VoltProcedure volt_proc = this.procedures.get(itask.getStoredProcedureName());
        assert(volt_proc != null) : "No VoltProcedure for " + ts;
        
        if (d) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Starting execution of %s [txnMode=%s, mode=%s]", ts, before_mode, this.exec_mode));
            if (t) LOG.trace("__FILE__:__LINE__ " + "Current Transaction at partition #" + this.partitionId + "\n" + ts.debug());
        }
            
        ClientResponseImpl cresponse = null;
        try {
            cresponse = (ClientResponseImpl)volt_proc.call(ts, itask.getParameters()); // Blocking...
        // VoltProcedure.call() should handle any exceptions thrown by the transaction
        // If we get anything out here then that's bad news
        } catch (Throwable ex) {
            if (this.isShuttingDown() == false) {
                SQLStmt last[] = volt_proc.voltLastQueriesExecuted();
                
                LOG.fatal("__FILE__:__LINE__ " + "Unexpected error while executing " + ts, ex);
                if (last.length > 0) {
                    LOG.fatal("__FILE__:__LINE__ " + "Last Queries Executed: " + Arrays.toString(last));
                }
                LOG.fatal("__FILE__:__LINE__ " + "LocalTransactionState Dump:\n" + ts.debug());
                this.crash(ex);
            }
        }
        // If this is a MapReduce job, then we can just ignore the ClientResponse
        // and return immediately
        if (ts.isMapReduce()) {
            return;
        } else if (cresponse == null) {
            assert(this.isShuttingDown()) : String.format("No ClientResponse for %s???", ts);
            return;
        }
        
        Hstore.Status status = cresponse.getStatus();
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Finished execution of %s [status=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, before_mode, this.exec_mode));

        // We assume that most transactions are not speculatively executed and are successful
        // Therefore we don't want to grab the exec_mode lock here.
        if (this.canProcessClientResponseNow(ts, status, before_mode)) {
            this.processClientResponse(ts, cresponse);
        } else {
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
                    if (status != Hstore.Status.OK) {
                        this.setExecutionMode(ts, ExecutionMode.DISABLED);
                        int blocked = this.work_queue.drainTo(this.current_dtxn_blocked);
                        if (t && blocked > 0)
                            LOG.trace("__FILE__:__LINE__ " + String.format("Blocking %d transactions at partition %d because ExecutionMode is now %s",
                                                    blocked, this.partitionId, this.exec_mode));
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Disabling execution on partition %d because speculative %s aborted", this.partitionId, ts));
                    }
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Queuing ClientResponse for %s [status=%s, origMode=%s, newMode=%s, dtxn=%s]",
                                                   ts, cresponse.getStatus(), before_mode, this.exec_mode, this.current_dtxn));
                    this.queueClientResponse(ts, cresponse);
                }
            } finally {
                exec_lock.unlock();
            } // SYNCH
        }
        
        volt_proc.finish();
    }
    
    private void processClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        if (hstore_conf.site.exec_postprocessing_thread) {
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Passing ClientResponse for %s to post-processing thread [status=%s]", ts, cresponse.getStatus()));
            hstore_site.queueClientResponse(this, ts, cresponse);
        } else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Sending ClientResponse for %s back directly [status=%s]", ts, cresponse.getStatus()));
            this.sendClientResponse(ts, cresponse);
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
    private boolean canProcessClientResponseNow(LocalTransaction ts, Hstore.Status status, ExecutionMode before_mode) {
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Checking whether to process response for %s now [status=%s, singlePartition=%s, readOnly=%s, beforeMode=%s, currentMode=%s]",
                                       ts, status, ts.isExecSinglePartition(), ts.isExecReadOnly(this.partitionId), before_mode, this.exec_mode));
        // Commit All
        if (ts.isPredictSinglePartition() == false || this.exec_mode == ExecutionMode.COMMIT_ALL) {
            return (true);
            
        // Process successful txns based on the mode that it was executed under
        } else if (status == Hstore.Status.OK) {
            switch (before_mode) {
                case COMMIT_ALL:
                    return (true);
                case COMMIT_READONLY:
                    return (ts.isExecReadOnly(this.partitionId));
                case COMMIT_NONE: {
                    return (false);
                }
                default:
                    throw new RuntimeException("Unexpectd execution mode: " + before_mode); 
            } // SWITCH
        }
        // Anything mispredicted should be processed right away
        else if (status == Hstore.Status.ABORT_MISPREDICT) {
            return (true);
        }    
        // If the transaction aborted and it was read-only thus far, then we want to process it immediately
        else if (status != Hstore.Status.OK && ts.isExecReadOnly(this.partitionId)) {
            return (true);
        }
        // If this txn threw a user abort, and the current outstanding dtxn is read-only
        // then it's safe for us to rollback
        else if (status == Hstore.Status.ABORT_USER && (this.current_dtxn != null && this.current_dtxn.isExecReadOnly(this.partitionId))) {
            return (true);
        }
        
        assert(this.exec_mode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.exec_mode, status);
        return (false);
    }
    
    /**
     * 
     * @param ftask
     * @throws Exception
     */
    private void processFragmentTaskMessage(AbstractTransaction ts, FragmentTaskMessage ftask) {
        
        // A txn is "local" if the Java is executing at the same site as we are
        boolean is_local = ts.isExecLocal(this.partitionId);
        boolean is_dtxn = (ts instanceof LocalTransaction == false);
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Executing FragmentTaskMessage %s [basePartition=%d, isLocal=%s, isDtxn=%s, fragments=%s]",
                                       ts, ftask.getSourcePartitionId(), is_local, is_dtxn, Arrays.toString(ftask.getFragmentIds())));

        // If this txn isn't local, then we have to update our undoToken
        if (is_local == false) {
            ts.initRound(this.partitionId, this.getNextUndoToken());
            ts.startRound(this.partitionId);
            
            // Copy the input dependencies from the txn's base partition into our "thread memory"
            // TODO: This should be optimized using something down in the EE
            if (is_dtxn == false && ftask.hasInputDependencies()) {
                this.tmp_removeDependenciesMap.clear();
                ((LocalTransaction)ts).removeInternalDependencies(ftask, this.tmp_removeDependenciesMap);
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s - Attaching %d dependencies to %s", ts, this.tmp_removeDependenciesMap.size(), ftask));
                for (Entry<Integer, List<VoltTable>> e : this.tmp_removeDependenciesMap.entrySet()) {
                    ftask.attachResults(e.getKey(), e.getValue());
                } // FOR
            }
        }
        
        DependencySet result = null;
        byte status = FragmentResponseMessage.SUCCESS;
        SerializableException error = null;
        
        try {
            result = this.executeFragmentTaskMessage(ts, ftask);
        } catch (ConstraintFailureException ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Hit an ConstraintFailureException for " + ts, ex);
            status = FragmentResponseMessage.UNEXPECTED_ERROR;
            error = ex;
        } catch (EEException ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Hit an EE Error for " + ts, ex);
            this.crash(ex);
            status = FragmentResponseMessage.UNEXPECTED_ERROR;
            error = ex;
        } catch (SQLException ex) {
            LOG.warn("__FILE__:__LINE__ " + "Hit a SQL Error for " + ts, ex);
            status = FragmentResponseMessage.UNEXPECTED_ERROR;
            error = ex;
        } catch (Throwable ex) {
            LOG.warn("__FILE__:__LINE__ " + "Something unexpected and bad happended for " + ts, ex);
            status = FragmentResponseMessage.UNEXPECTED_ERROR;
            error = new SerializableException(ex);
        } finally {
            // Success, but without any results???
            if (result == null && status == FragmentResponseMessage.SUCCESS) {
                Exception ex = new Exception("The Fragment executed successfully but result is null for " + ts);
                if (d) LOG.warn("__FILE__:__LINE__ " + ex);
                status = FragmentResponseMessage.UNEXPECTED_ERROR;
                error = new SerializableException(ex);
            }
        }
        
        // For single-partition INSERT/UPDATE/DELETE queries, we don't directly
        // execute the SendPlanNode in order to get back the number of tuples that
        // were modified. So we have to rely on the output dependency ids set in the task
        assert(status != FragmentResponseMessage.SUCCESS ||
               (status == FragmentResponseMessage.SUCCESS && result.size() == ftask.getOutputDependencyIds().length)) :
           "Got back " + result.size() + " results but was expecting " + ftask.getOutputDependencyIds().length;
        
        // Make sure that we mark the round as finished before we start sending results
        if (is_local == false) {
            ts.finishRound(this.partitionId);
        }
        
        // -------------------------------
        // LOCAL TRANSACTION
        // -------------------------------
        if (is_dtxn == false) {
            // If the transaction is local, store the result directly in the local TransactionState
            if (status == FragmentResponseMessage.SUCCESS) {
                if (t) LOG.trace("__FILE__:__LINE__ " + "Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                LocalTransaction local_ts = (LocalTransaction)ts;
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    int dep_id = ftask.getOutputDependencyIds()[i];
                    // ts.addResult(result.depIds[i], result.dependencies[i]);
                    if (t) LOG.trace("__FILE__:__LINE__ " + "Storing DependencyId #" + dep_id  + " for " + ts);
                    try {
                        local_ts.addResult(this.partitionId, dep_id, result.dependencies[i]);
                    } catch (Throwable ex) {
                        String msg = String.format("Failed to stored Dependency #%d for %s [idx=%d, fragmentIds=%s]",
                                                   dep_id, ts, i, Arrays.toString(ftask.getFragmentIds()));
                        LOG.error("__FILE__:__LINE__ " + msg + "\n" + ftask.toString());
                        throw new RuntimeException(msg, ex);
                    }
                } // FOR
            } else {
                ts.setPendingError(error);
            }
        }
            
        // -------------------------------
        // REMOTE TRANSACTION
        // -------------------------------
        else {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Constructing FragmentResponseMessage %s with %d bytes from partition %d to send back to initial partition %d for %s",
                                           Arrays.toString(ftask.getFragmentIds()),
                                           result.size(), this.partitionId, ftask.getSourcePartitionId(), ts));
            FragmentResponseMessage fresponse = new FragmentResponseMessage(ftask);
            fresponse.setStatus(status, error);
            assert(fresponse.getSourcePartitionId() == this.partitionId) : "Unexpected source partition #" + fresponse.getSourcePartitionId() + "\n" + fresponse;
            
            // Push dependencies back to the remote partition that needs it
            if (status == FragmentResponseMessage.SUCCESS) {
                for (int i = 0, cnt = result.size(); i < cnt; i++) {
                    fresponse.addDependency(result.depIds[i], result.dependencies[i]);
                } // FOR
            }
        
            this.sendFragmentResponseMessage((RemoteTransaction)ts, ftask, fresponse);
        }
    }
    
    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the resulting DependencySet
     * @param ftask
     * @return
     * @throws Exception
     */
    private DependencySet executeFragmentTaskMessage(AbstractTransaction ts, FragmentTaskMessage ftask) throws Exception {
        DependencySet result = null;
        final long undoToken = ts.getLastUndoToken(this.partitionId);
        int fragmentIdIndex = ftask.getFragmentCount();
        long fragmentIds[] = ftask.getFragmentIds();
        int output_depIds[] = ftask.getOutputDependencyIds();
        int input_depIds[] = ftask.getAllUnorderedInputDepIds(); // Is this ok?
        
        if (fragmentIdIndex == 0) {
            LOG.warn("__FILE__:__LINE__ " + String.format("Got a FragmentTask for %s that does not have any fragments?!?", ts));
            return (result);
        }
        
        if (d) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Getting ready to kick %d fragments to EE for %s", ftask.getFragmentCount(), ts));
            if (t) LOG.trace("__FILE__:__LINE__ " + "FragmentTaskIds: " + Arrays.toString(ftask.getFragmentIds()));
        }
        ParameterSet parameterSets[] = new ParameterSet[fragmentIdIndex];
        for (int i = 0; i < fragmentIdIndex; i++) {
            ByteBuffer paramData = ftask.getParameterDataForFragment(i);
            if (paramData != null) {
                paramData.rewind();
                final FastDeserializer fds = new FastDeserializer(paramData);
                if (t) LOG.trace("__FILE__:__LINE__ " + ts + " ->paramData[" + i + "] => " + fds.buffer());
                try {
                    parameterSets[i] = fds.readObject(ParameterSet.class);
                } catch (Exception ex) {
                    LOG.fatal("__FILE__:__LINE__ " + "Failed to deserialize ParameterSet[" + i + "] for FragmentTaskMessage " + fragmentIds[i] + " in " + ts, ex);
                    throw ex;
                }
                // LOG.info("PARAMETER[" + i + "]: " + parameterSets[i]);
            } else {
                parameterSets[i] = new ParameterSet();
            }
        } // FOR
        
        this.tmp_EEdependencies.clear();
        if (ftask.hasAttachedResults()) {
            if (d) LOG.debug("__FILE__:__LINE__ " + "Retrieving internal dependency results attached to FragmentTaskMessage for " + ts);
            this.tmp_EEdependencies.putAll(ftask.getAttachedResults());
        }
        
        LocalTransaction local_ts = null;
        if (ftask.hasInputDependencies() && ts != null && ts.isExecLocal(this.partitionId) == true) {
            local_ts = (LocalTransaction)ts; 
            if (local_ts.getInternalDependencyIds().isEmpty() == false) {
                if (d) LOG.debug("__FILE__:__LINE__ " + "Retrieving internal dependency results from TransactionState for " + ts);
                local_ts.removeInternalDependencies(ftask, this.tmp_EEdependencies);
            }
        }

        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ftask.isSysProcTask()) {
            assert(fragmentIds.length == 1);
            long fragment_id = (long)fragmentIds[0];

            VoltSystemProcedure volt_proc = this.m_registeredSysProcPlanFragments.get(fragment_id);
            if (volt_proc == null) throw new RuntimeException("No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments);
            
            // HACK: We have to set the TransactionState for sysprocs manually
            volt_proc.setTransactionState(ts);
            ts.markExecNotReadOnly(this.partitionId);
            result = volt_proc.executePlanFragment(ts.getTransactionId(), this.tmp_EEdependencies, (int)fragmentIds[0], parameterSets[0], this.m_systemProcedureContext);
            if (t) LOG.trace("__FILE__:__LINE__ " + "Finished executing sysproc fragments for " + volt_proc.getClass().getSimpleName());
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
    protected VoltTable[] executeLocalPlan(LocalTransaction ts, BatchPlanner.BatchPlan plan, ParameterSet parameterSets[]) {
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
                LOG.debug("__FILE__:__LINE__ " + String.format("Bold! Disabling undo buffers for inflight %s [prob=%f]\n%s\n%s",
                                        ts, est.getAbortProbability(), est, plan.toString()));
            }
        } else if (hstore_conf.site.exec_no_undo_logging_all == false) {
            undoToken = this.getNextUndoToken();
        }
        ts.fastInitRound(this.partitionId, undoToken);
      
        long fragmentIds[] = plan.getFragmentIds();
        int fragmentIdIndex = plan.getFragmentCount();
        int output_depIds[] = plan.getOutputDependencyIds();
        int input_depIds[] = plan.getInputDependencyIds();
        
        // Mark that we touched the local partition once for each query in the batch
        ts.getTouchedPartitions().put(this.partitionId, plan.getBatchSize());
        
        // Only notify other partitions that we're done with them if we're not a single-partition transaction
        if (hstore_conf.site.exec_speculative_execution && ts.isPredictSinglePartition() == false) {
            // TODO: We need to notify the remote HStoreSites that we are done with their partitions
            this.calculateDonePartitions(ts);
        }

        if (t) {
            StringBuilder sb = new StringBuilder();
            sb.append("Parameters:");
            for (int i = 0; i < parameterSets.length; i++) {
                sb.append(String.format("\n [%02d] %s", i, parameterSets[i].toString()));
            }
            LOG.trace("__FILE__:__LINE__ " + sb.toString());
            LOG.trace("__FILE__:__LINE__ " + String.format("Txn #%d - BATCHPLAN:\n" +
                     "  fragmentIds:     %s\n" + 
                     "  fragmentIdIndex: %s\n" +
                     "  output_depIds:   %s\n" +
                     "  input_depIds:    %s",
                     ts.getTransactionId(),
                     Arrays.toString(plan.getFragmentIds()), plan.getFragmentCount(), Arrays.toString(plan.getOutputDependencyIds()), Arrays.toString(plan.getInputDependencyIds())));
        }
        DependencySet result = this.executePlanFragments(ts, undoToken, fragmentIdIndex, fragmentIds, parameterSets, output_depIds, input_depIds);
        assert(result != null) : "Unexpected null DependencySet for " + ts; 
        if (t) LOG.trace("__FILE__:__LINE__ " + "Output:\n" + StringUtil.join("\n", result.dependencies));
        
        ts.fastFinishRound(this.partitionId);
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
    private DependencySet executePlanFragments(AbstractTransaction ts, long undoToken, int batchSize, long fragmentIds[], ParameterSet parameterSets[], int output_depIds[], int input_depIds[]) {
        assert(this.ee != null) : "The EE object is null. This is bad!";
        long txn_id = ts.getTransactionId();
        
        if (d) {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Executing %d fragments for %s [lastTxnId=%d, undoToken=%d]",
                      batchSize, ts, this.lastCommittedTxnId, undoToken));
            if (t) {
                Map<String, Object> m = new ListOrderedMap<String, Object>();
                m.put("Fragments", Arrays.toString(fragmentIds));
                
                Map<Integer, Object> inner = new ListOrderedMap<Integer, Object>();
                for (int i = 0; i < parameterSets.length; i++)
                    inner.put(i, parameterSets[i].toString());
                m.put("Parameters", inner);
                
                if (input_depIds.length > 0 && input_depIds[0] != HStoreConstants.NULL_DEPENDENCY_ID) {
                    inner = new ListOrderedMap<Integer, Object>();
                    for (int i = 0; i < input_depIds.length; i++) {
                        List<VoltTable> deps = this.tmp_EEdependencies.get(input_depIds[i]);
                        if (deps != null) {
                            inner.put(input_depIds[i], StringUtil.join("\n", deps));
                        } else {
                            inner.put(input_depIds[i], "???");
                        }
                    }
                    m.put("Input Dependencies", inner);
                }
                
                m.put("Output Dependencies", Arrays.toString(output_depIds));
                
                sb.append("\n" + StringUtil.formatMaps(m)); 
            }
            LOG.debug("__FILE__:__LINE__ " + sb.toString());
        }

        // pass attached dependencies to the EE (for non-sysproc work).
        if (this.tmp_EEdependencies.isEmpty() == false) {
            if (t) LOG.trace("__FILE__:__LINE__ " + "Stashing Dependencies: " + this.tmp_EEdependencies.keySet());
//            assert(dependencies.size() == input_depIds.length) : "Expected " + input_depIds.length + " dependencies but we have " + dependencies.size();
            ee.stashWorkUnitDependencies(this.tmp_EEdependencies);
        }
        
        // Check whether this fragments are read-only
        if (ts.isExecReadOnly(this.partitionId)) {
            boolean readonly = CatalogUtil.areFragmentsReadOnly(this.database, fragmentIds, batchSize); 
            if (readonly == false) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Marking txn #%d as not read-only %s", txn_id, Arrays.toString(fragmentIds))); 
                ts.markExecNotReadOnly(this.partitionId);
            }
            
            // We can do this here because the only way that we're not read-only is if
            // we actually modify data at this partition
            ts.setSubmittedEE(this.partitionId);
        }
        
        DependencySet result = null;
        try {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Executing %d fragments at partition %d for %s", batchSize, this.partitionId, ts));
            if (hstore_conf.site.txn_profiling && ts instanceof LocalTransaction) {
                ((LocalTransaction)ts).profiler.startExecEE();
            }
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
            if (hstore_conf.site.txn_profiling && ts instanceof LocalTransaction) {
                ((LocalTransaction)ts).profiler.stopExecEE();
            }
        } catch (EEException ex) {
            LOG.fatal("__FILE__:__LINE__ " + "Unrecoverable error in the ExecutionEngine", ex);
            System.exit(1);
        } catch (Throwable ex) {
            new RuntimeException(String.format("Failed to execute PlanFragments for %s: %s", ts, Arrays.toString(fragmentIds)), ex);
        }
        
        if (d) {
            if (result != null) {
                LOG.debug("__FILE__:__LINE__ " + String.format("Finished executing fragments for %s and got back %d results", ts, result.depIds.length));
                if (t) 
                    LOG.trace("__FILE__:__LINE__ " + String.format("FRAGMENTS: %s\nRESULTS: %s",
                                               Arrays.toString(fragmentIds), Arrays.toString(result.depIds)));
            } else {
                LOG.debug("__FILE__:__LINE__ " + String.format("Finished executing fragments for %s but got back null results? That seems bad...", ts));
            }
        }
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
     * 
     * @param fresponse
     */
    public void sendFragmentResponseMessage(RemoteTransaction ts, FragmentTaskMessage ftask, FragmentResponseMessage fresponse) {
        RpcCallback<TransactionWorkResponse.PartitionResult> callback = ts.getFragmentTaskCallback();
        if (callback == null) {
            LOG.fatal("__FILE__:__LINE__ " + "Unable to send FragmentResponseMessage:\n" + fresponse.toString());
            LOG.fatal("__FILE__:__LINE__ " + "Orignal FragmentTaskMessage:\n" + ftask);
            LOG.fatal("__FILE__:__LINE__ " + ts.toString());
            throw new RuntimeException("No RPC callback to HStoreSite for " + ts);
        }

        BBContainer bc = fresponse.getBufferForMessaging(buffer_pool);
        assert(bc.b.hasArray());
        ByteString bs = ByteString.copyFrom(bc.b.array()); // XXX
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Sending FragmentResponseMessage for %s [partition=%d, bytes=%d, error=%s]",
                                       ts, this.partitionId, bs.size(), fresponse.getException()));            
        TransactionWorkResponse.PartitionResult.Builder builder = TransactionWorkResponse.PartitionResult.newBuilder()
                                                                      .setOutput(bs)
                                                                      .setPartitionId(this.partitionId);
        if (fresponse.getException() != null) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Marking PartitionResult from partition %d for %s with an error because of FragmentResponse exception -> %s",
                                           this.partitionId, ts, fresponse.getException().getMessage()));
            builder.setError(true);
        }
        callback.run(builder.build());
        bc.discard();

    }
    
    /**
     * Figure out what partitions this transaction is done with and notify those partitions
     * that they are done
     * @param ts
     */
    private boolean calculateDonePartitions(LocalTransaction ts) {
        final Collection<Integer> ts_done_partitions = ts.getDonePartitions();
        final int ts_done_partitions_size = ts_done_partitions.size();
        Set<Integer> new_done = null;

        TransactionEstimator.State t_state = ts.getEstimatorState();
        if (t_state == null) {
            return (false);
        }
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Checking MarkovEstimate for %s to see whether we can notify any partitions that we're done with them [round=%d]",
                                       ts, ts.getCurrentRound(this.partitionId)));
        
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
            for (Integer p : new_done) {
                if (ts_done_partitions.contains(p) == false && ts_touched.contains(p)) {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Marking partition %d as done for %s", p, ts));
                    ts_done_partitions.add(p);
                }
            } // FOR
        }
        return (ts_done_partitions.size() != ts_done_partitions_size);
    }

    /**
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    private void requestWork(LocalTransaction ts, Collection<FragmentTaskMessage> tasks) {
        assert(!tasks.isEmpty());
        assert(ts != null);
        long txn_id = ts.getTransactionId();

        if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Wrapping %d FragmentTaskMessages into a Hstore.TransactionWorkRequest for %s", tasks.size(), ts));
        
        // If our transaction was originally designated as a single-partitioned, then we need to make
        // sure that we don't touch any partition other than our local one. If we do, then we need abort
        // it and restart it as multi-partitioned
        boolean need_restart = false;
        boolean predict_singlepartition = ts.isPredictSinglePartition(); 
        Collection<Integer> done_partitions = ts.getDonePartitions();
        
        boolean new_done = false;
        if (hstore_conf.site.exec_speculative_execution) {
            new_done = this.calculateDonePartitions(ts);
        }

        // Now we can go back through and start running all of the FragmentTaskMessages that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each FragmentTaskMessage in its own message
        assert(tmp_transactionRequestBuildersMap.isEmpty());
        for (FragmentTaskMessage ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            int target_partition = ftask.getDestinationPartitionId();
            int target_site = hstore_site.getSiteIdForPartitionId(target_partition);
            
            // Make sure things are still legit for our single-partition transaction
            if (predict_singlepartition && target_partition != this.partitionId) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s on partition %d is suppose to be single-partitioned, but it wants to execute a fragment on partition %d",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            } else if (done_partitions.contains(target_partition)) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s on partition %d was marked as done on partition %d but now it wants to go back for more!",
                                               ts, this.partitionId, target_partition));
                need_restart = true;
                break;
            }
            
            int dependency_ids[] = ftask.getOutputDependencyIds();
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Preparing to request fragments %s on partition %d to generate %d output dependencies for %s",
                                           Arrays.toString(ftask.getFragmentIds()), target_partition, dependency_ids.length, ts));
            if (ftask.getFragmentCount() == 0) {
                LOG.warn("__FILE__:__LINE__ " + "Trying to send a FragmentTask request with 0 fragments for " + ts);
                continue;
            }

            // Since we know that we have to send these messages everywhere, then any internal dependencies
            // that we have stored locally here need to go out with them
            if (ftask.hasInputDependencies()) {
                this.tmp_removeDependenciesMap.clear();
                ts.removeInternalDependencies(ftask, this.tmp_removeDependenciesMap);
                if (t) LOG.trace("__FILE__:__LINE__ " + String.format("%s - Attaching %d dependencies to %s", ts, this.tmp_removeDependenciesMap.size(), ftask));
                for (Entry<Integer, List<VoltTable>> e : this.tmp_removeDependenciesMap.entrySet()) {
                    ftask.attachResults(e.getKey(), e.getValue());
                } // FOR
            }

            Hstore.TransactionWorkRequest.Builder request = tmp_transactionRequestBuildersMap.get(target_site);
            if (request == null) {
                request = Hstore.TransactionWorkRequest.newBuilder()
                                        .setTransactionId(txn_id)
                                        .addAllDonePartition(done_partitions);
                tmp_transactionRequestBuildersMap.put(target_site, request);
            }
            
            // The ByteString API forces us to copy from our buffer... 
            ByteString bs = ByteString.copyFrom(ftask.getBufferForMessaging(buffer_pool).b.array());
            request.addFragments(Hstore.TransactionWorkRequest.PartitionFragment.newBuilder()
                                                                .setPartitionId(target_partition)
                                                                .setWork(bs));
        } // FOR (tasks)
        
        // Bad mojo! We need to throw a MispredictionException so that the VoltProcedure
        // will catch it and we can propagate the error message all the way back to the HStoreSite
        if (need_restart) {
            if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Aborting %s because it was mispredicted", ts));
            // This is kind of screwy because we don't actually want to send the touched partitions
            // histogram because VoltProcedure will just do it for us...
            throw new MispredictionException(txn_id, null);
        }
        
        // Bombs away!
        this.hstore_coordinator.transactionWork(ts, tmp_transactionRequestBuildersMap, this.request_work_callback);
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Work request for %d fragments was sent to %d remote HStoreSites for %s",
                                       tasks.size(), tmp_transactionRequestBuildersMap.size(), ts));

        // TODO: We need to check whether we need to notify other HStoreSites that we didn't send
        // a new FragmentTaskMessage to that we are done with their partitions
        if (new_done) {
            
        }
        
        // We want to clear out our temporary map here so that we don't have to do it
        // the next time we need to use this
        tmp_transactionRequestBuildersMap.clear();
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do... 
     * @param ts
     * @param dependency_ids
     * @return
     */
    public VoltTable[] dispatchFragmentTasks(LocalTransaction ts, Collection<FragmentTaskMessage> ftasks, int batchSize) {
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Dispatching %d messages and waiting for the results for %s", ftasks.size(), ts));
        
        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.partitionId, this.getNextUndoToken());
        ts.setBatchSize(batchSize);
        boolean first = true;
//        boolean read_only = ts.isExecReadOnly(this.partitionId);
        final boolean predict_singlePartition = ts.isPredictSinglePartition();
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
                if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Waiting for unblocked tasks for %s on partition %d", ts, this.partitionId));
                try {
                    ftasks = queue.takeFirst(); // BLOCKING
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) LOG.error("__FILE__:__LINE__ " + "We were interrupted while waiting for blocked tasks for " + ts, ex);
                    return (null);
                }
            }
            assert(ftasks != null);
            if (ftasks.size() == 0) break;

            this.tmp_localPartitionFragmentList.clear();
            if (predict_singlePartition == false) {
                this.tmp_remoteFragmentList.clear();
                this.tmp_localSiteFragmentList.clear();
            }
            
            // FAST PATH: Assume everything is local
            if (predict_singlePartition) {
                for (FragmentTaskMessage ftask : ftasks) {
                    if (first == false || ts.addFragmentTaskMessage(ftask) == false) 
                        this.tmp_localPartitionFragmentList.add(ftask);
                } // FOR
                
                // We have to tell the TransactinState to start the round before we send off the
                // FragmentTasks for execution, since they might start executing locally!
                if (first) {
                    ts.startRound(this.partitionId);
                    latch = ts.getDependencyLatch();
                }
                
                for (FragmentTaskMessage ftask : this.tmp_localPartitionFragmentList) {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Got unblocked FragmentTaskMessage for %s. Executing locally...", ts));
                    assert(ftask.getDestinationPartitionId() == this.partitionId) :
                        String.format("Trying to process FragmentTaskMessage for %s on partition %d but it should have been sent to partition %d [singlePartition=%s]\n%s",
                                      ts, this.partitionId, ftask.getDestinationPartitionId(), predict_singlePartition, ftask);
                    this.processFragmentTaskMessage(ts, ftask);
//                    read_only = read_only && ftask.isReadOnly();
                } // FOR
                
            // SLOW PATH: Mixed local and remote messages
            } else {
                boolean all_local = true;
                boolean is_localSite;
                boolean is_localPartition;
                int num_localPartition = 0;
                int num_localSite = 0;
                int num_remote = 0;
                
                // Look at each task and figure out whether it should be executed remotely or locally
                for (FragmentTaskMessage ftask : ftasks) {
                    int partition = ftask.getDestinationPartitionId();
                    is_localSite = localPartitionIds.contains(partition);
                    is_localPartition = (is_localSite && partition == this.partitionId);
                    all_local = all_local && is_localPartition;
                    if (first == false || ts.addFragmentTaskMessage(ftask) == false) {
                        if (is_localPartition) {
                            this.tmp_localPartitionFragmentList.add(ftask);
                            num_localPartition++;
                        } else if (is_localSite) {
                            this.tmp_localSiteFragmentList.add(ftask);
                            num_localSite++;
                        } else {
                            this.tmp_remoteFragmentList.add(ftask);
                            num_remote++;
                        }
                    }
                } // FOR
                if (num_localPartition == 0 && num_localSite == 0 && num_remote == 0) {
                    throw new RuntimeException(String.format("Deadlock! All tasks for %s are blocked waiting on input!", ts));
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
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Requesting %d FragmentTaskMessages to be executed on remote partitions for %s", num_remote, ts));
                    this.requestWork(ts, this.tmp_remoteFragmentList);
                }
                
                // Then dispatch the task that are needed at the same HStoreSite but 
                // at a different partition than this one
                if (num_localSite > 0) {
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Executing %d FragmentTaskMessages on local site's partitions for %s",
                                                   num_localSite, ts));
                    for (FragmentTaskMessage ftask : this.tmp_localSiteFragmentList) {
                        try {
                            hstore_site.getExecutionSite(ftask.getDestinationPartitionId()).queueWork(ts, ftask);
                        } catch (Throwable ex) {
                            throw new RuntimeException(String.format("Unexpected error when executing local site fragments for %s on partition %d",
                                                                     ts, ftask.getDestinationPartitionId()), ex);
                        }
                    } // FOR
                }
        
                // Then execute all of the tasks need to access the partitions at this HStoreSite
                // We'll dispatch the remote-partition-local-site fragments first because they're going
                // to need to get queued up by at the other ExecutionSites
                if (num_localPartition > 0) {
                    if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Executing %d FragmentTaskMessages on local partition for %s",
                                                   num_localPartition, ts));
                    for (FragmentTaskMessage ftask : this.tmp_localPartitionFragmentList) {
                        try {
                            this.processFragmentTaskMessage(ts, ftask);
                        } catch (Throwable ex) {
                            throw new RuntimeException(String.format("Unexpected error when executing local partition fragments for %s on partition %d",
                                                                     ts, ftask.getDestinationPartitionId()), ex);
                        }
                    } // FOR
                }
            }
//            if (read_only == false) ts.markExecNotReadOnly(this.partitionId);
            first = false;
        } // WHILE

        // Now that we know all of our FragmentTaskMessages have been dispatched, we can then
        // wait for all of the results to come back in.
        if (latch == null) latch = ts.getDependencyLatch();
        if (latch.getCount() > 0) {
            if (d) {
                LOG.debug("__FILE__:__LINE__ " + String.format("All blocked messages dispatched for %s. Waiting for %d dependencies", ts, latch.getCount()));
                if (t) LOG.trace("__FILE__:__LINE__ " + ts.toString());
            }
            while (true) {
                boolean done = false;
                try {
                    done = latch.await(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    if (this.hstore_site.isShuttingDown() == false) LOG.error("__FILE__:__LINE__ " + "We were interrupted while waiting for results for " + ts, ex);
                    return (null);
                } catch (Throwable ex) {
                    new RuntimeException(String.format("Fatal error for %s while waiting for results", ts), ex);
                }
                if (done) break;
                if (this.isShuttingDown() == false) {
                    LOG.warn("__FILE__:__LINE__ " + "Still waiting for responses for " + ts + "\n" + ts.debug());
                    throw new VoltAbortException("Responses for " + ts + "never arrived!");
                }
            } // WHILE
        }
        
//        if (hstore_conf.site.txn_profiling) ts.profiler.stopExecCoordinatorBlocked();
        
        // IMPORTANT: Check whether the fragments failed somewhere and we got a response with an error
        // We will rethrow this so that it pops the stack all the way back to VoltProcedure.call()
        // where we can generate a message to the client 
        if (ts.hasPendingError()) {
            if (d) LOG.warn("__FILE__:__LINE__ " + String.format("%s was hit with a %s", ts, ts.getPendingError().getClass().getSimpleName()));
            throw ts.getPendingError();
        }
        
        // Important: Don't try to check whether we got back the right number of tables because the batch
        // may have hit an error and we didn't execute all of them.
        VoltTable results[] = ts.getResults();
        ts.finishRound(this.partitionId);
         if (d) {
            if (t) LOG.trace("__FILE__:__LINE__ " + ts + " is now running and looking for love in all the wrong places...");
            LOG.debug("__FILE__:__LINE__ " + ts + " is returning back " + results.length + " tables to VoltProcedure");
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
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Queuing ClientResponse for %s [handle=%s, status=%s]",
                                       ts, ts.getClientHandle(), cresponse.getStatus()));
        assert(ts.isPredictSinglePartition() == true) :
            String.format("Specutatively executed multi-partition %s [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatus());
        assert(ts.isSpeculative() == true) :
            String.format("Queuing ClientResponse for non-specutative %s [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatus());
        assert(cresponse.getStatus() != Hstore.Status.ABORT_MISPREDICT) : 
            String.format("Trying to queue ClientResponse for mispredicted %s [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatus());
        assert(this.exec_mode != ExecutionMode.COMMIT_ALL) :
            String.format("Queuing ClientResponse for %s when in non-specutative mode [mode=%s, status=%s]",
                          ts, this.exec_mode, cresponse.getStatus());

        this.queued_responses.add(Pair.of(ts, cresponse));

        if (d) LOG.debug("__FILE__:__LINE__ " + "Total # of Queued Responses: " + this.queued_responses.size());
    }
    
    protected void sendClientResponse(LocalTransaction ts, ClientResponseImpl cresponse) {
        // IMPORTANT: If we executed this locally and only touched our partition, then we need to commit/abort right here
        // 2010-11-14: The reason why we can do this is because we will just ignore the commit
        // message when it shows from the Dtxn.Coordinator. We should probably double check with Evan on this...
        boolean is_singlepartitioned = ts.isPredictSinglePartition();
        Hstore.Status status = cresponse.getStatus();

        if (d) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Processing ClientResponse for %s at partition %d [handle=%d, status=%s, singlePartition=%s, local=%s]",
                                    ts, this.partitionId, cresponse.getClientHandle(), status,
                                    ts.isPredictSinglePartition(), ts.isExecLocal(this.partitionId)));
            if (t) {
                LOG.trace("__FILE__:__LINE__ " + ts + " Touched Partitions: " + ts.getTouchedPartitions().values());
                LOG.trace("__FILE__:__LINE__ " + ts + " Done Partitions: " + ts.getDonePartitions());
            }
        }
        
        // ALL: Single-Partition Transactions
        if (is_singlepartitioned) {
            // Commit or abort the transaction
            this.finishWork(ts, (status == Hstore.Status.OK));
            
            // Then send the result back to the client!
            this.hstore_site.sendClientResponse(ts, cresponse);
            this.hstore_site.completeTransaction(ts.getTransactionId(), status);
        } 
        // COMMIT: Distributed Transaction
        else if (status == Hstore.Status.OK) {
            // Store the ClientResponse in the TransactionPrepareCallback so that
            // when we get all of our 
            TransactionPrepareCallback callback = ts.getTransactionPrepareCallback();
            assert(callback != null) : "Missing TransactionPrepareCallback for " + ts + " [initialized=" + ts.isInitialized() + "]";
            callback.setClientResponse(cresponse);
            
            // We have to send a prepare message to all of our remote HStoreSites
            // We want to make sure that we don't go back to ones that we've already told
            Collection<Integer> predictPartitions = ts.getPredictTouchedPartitions();
            Collection<Integer> donePartitions = ts.getDonePartitions();
            Collection<Integer> partitions = CollectionUtils.subtract(predictPartitions, donePartitions);
            this.hstore_coordinator.transactionPrepare(ts, ts.getTransactionPrepareCallback(), partitions);
            
            if (hstore_conf.site.exec_speculative_execution) {
                this.setExecutionMode(ts, ts.isExecReadOnly(this.partitionId) ? ExecutionMode.COMMIT_READONLY : ExecutionMode.COMMIT_NONE);
            } else {
                this.setExecutionMode(ts, ExecutionMode.DISABLED);                  
            }

        }
        // ABORT: Distributed Transaction
        else {
            // Send back the result to the client right now, since there's no way 
            // that we're magically going to be able to recover this and get them a result
            // This has to come before the network messages above because this will clean-up the 
            // LocalTransaction state information
            this.hstore_site.sendClientResponse(ts, cresponse);
            
            // Then send a message all the partitions involved that the party is over
            // and that they need to abort the transaction. We don't actually care when we get the
            // results back because we'll start working on new txns right away.
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
        Long undoToken = ts.getLastUndoToken(this.partitionId);
        
        // Only commit/abort this transaction if:
        //  (1) We have an ExecutionEngine handle
        //  (2) We have the last undo token used by this transaction
        //  (3) The transaction was executed with undo buffers
        //  (4) The transaction actually submitted work to the EE
        //  (5) The transaction modified data at this partition
        if (this.ee != null && ts.hasSubmittedEE(this.partitionId) && ts.isExecReadOnly(this.partitionId) == false && undoToken != null) {
            if (undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN) {
                if (commit == false) {
                    LOG.fatal("__FILE__:__LINE__ " + ts.debug());
                    this.crash(new RuntimeException("TRYING TO ABORT TRANSACTION WITHOUT UNDO LOGGING: "+ ts));
                }
                if(d) LOG.debug("<FinishWork> undoToken == HStoreConstants.DISABLE_UNDO_LOGGING_TOKEN");
            } else {
//                synchronized (this.ee) {
                    if (commit) {
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Committing %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                       ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                        if(d) LOG.debug("<FinishWork> this.ee.releaseUndoToken(undoToken)");
                        this.ee.releaseUndoToken(undoToken);
        
                    // Evan says that txns will be aborted LIFO. This means the first txn that
                    // we get in abortWork() will have a the greatest undoToken, which means that 
                    // it will automagically rollback all other outstanding txns.
                    // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
                    // we should be able to check whether our undoToken has already been rolled back
                    } else {
                        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Aborting %s at partition=%d [lastTxnId=%d, undoToken=%d, submittedEE=%s]",
                                                       ts, this.partitionId, this.lastCommittedTxnId, undoToken, ts.hasSubmittedEE(this.partitionId)));
                        this.ee.undoUndoToken(undoToken);
                    }
//                } // SYNCH
            }
        }
        
        // We always need to do the following things regardless if we hit up the EE or not
        if (commit) this.lastCommittedTxnId = ts.getTransactionId();
        ts.setFinishedEE(this.partitionId);
    }
    
    /**
     * The coordinator is telling our site to abort/commit the txn with the
     * provided transaction id. This method should only be used for multi-partition transactions, because
     * it will do some extra work for speculative execution
     * @param txn_id
     * @param commit If true, the work performed by this txn will be commited. Otherwise it will be aborted
     */
    public void finishTransaction(AbstractTransaction ts, boolean commit) {
        if (d) LOG.debug("<finishTransaction> for  multi-partition transactions: "+ ts + "\n this.current_dtxn:"+this.current_dtxn + "  commit="+commit);
        //if(ts instanceof MapReduceTransaction) this.current_dtxn = ts;
        if (this.current_dtxn != ts) {  
            return;
        }
        assert(this.current_dtxn == ts) : "Expected current DTXN to be " + ts + " but it was " + this.current_dtxn;
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Processing finishWork request for %s at partition %d", ts, this.partitionId));
        
        this.finishWork(ts, commit);
        
        // Check whether this is the response that the speculatively executed txns have been waiting for
        // We could have turned off speculative execution mode beforehand 
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Attempting to unmark %s as the current DTXN at partition %d and setting execution mode to %s",
                                       this.current_dtxn, this.partitionId, ExecutionMode.COMMIT_ALL));
        exec_lock.lock();
        try {
            // Resetting the current_dtxn variable has to come *before* we change the execution mode
            this.setCurrentDtxn(null);
            this.setExecutionMode(ts, ExecutionMode.COMMIT_ALL);
            
            // We can always commit our boys no matter what if we know that this multi-partition txn 
            // was read-only at the given partition
            if (hstore_conf.site.exec_speculative_execution) {
                if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Turning off speculative execution mode at partition %d because %s is finished",
                                               this.partitionId, ts));
                Boolean readonly = ts.isExecReadOnly(this.partitionId);
                this.releaseQueuedResponses(readonly != null && readonly == true ? true : commit);
            }
            if(d) LOG.debug("I am trying to releaseBlocked Transaction");
            // Release blocked transactions
            this.releaseBlockedTransactions(ts, false);
        } catch (Throwable ex) {
            throw new RuntimeException(String.format("Failed to finish %s at partition %d", ts, this.partitionId), ex);
        } finally {
            exec_lock.unlock();
        } // SYNCH
//        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s is releasing DTXN lock [queueSize=%d, waitingLock=%d]",
//                                       ts, this.work_queue.size(), this.dtxn_lock.getQueueLength()));
//         this.dtxn_lock.release();
        
        // HACK: If this is a RemoteTransaction, invoke the cleanup callback
        if (ts instanceof RemoteTransaction) {
            ((RemoteTransaction)ts).getCleanupCallback().run(this.partitionId);
        } else {
            TransactionFinishCallback finish_callback = ((LocalTransaction)ts).getTransactionFinishCallback();
            if (trace.get())
                LOG.trace("__FILE__:__LINE__ " + String.format("Notifying %s that %s is finished at partition %d",
                                        finish_callback.getClass().getSimpleName(), ts, this.partitionId));
            finish_callback.decrementCounter(1);
        }
        
    }    
    /**
     * 
     * @param txn_id
     * @param p
     */
    private void releaseBlockedTransactions(AbstractTransaction ts, boolean speculative) {
        if (this.current_dtxn_blocked.isEmpty() == false) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Attempting to release %d blocked transactions at partition %d because of %s",
                                           this.current_dtxn_blocked.size(), this.partitionId, ts));
            int released = 0;
            for (TransactionInfoBaseMessage msg : this.current_dtxn_blocked) {
                this.work_queue.add(msg);
                released++;
            } // FOR
            this.current_dtxn_blocked.clear();
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Released %d blocked transactions at partition %d because of %s",
                                         released, this.partitionId, ts));
        }
        assert(this.current_dtxn_blocked.isEmpty());
    }
    
    /**
     * Commit/abort all of the queue transactions that were specutatively executed and waiting for
     * their responses to be sent back to the client
     * @param commit
     */
    private void releaseQueuedResponses(boolean commit) {
        // First thing we need to do is get the latch that will be set by any transaction
        // that was in the middle of being executed when we were called
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Checking waiting/blocked transactions at partition %d [currentMode=%s]",
                                       this.partitionId, this.exec_mode));
        
        if (this.queued_responses.isEmpty()) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("No speculative transactions to commit at partition %d. Ignoring...", this.partitionId));
            return;
        }
        
        // Ok now at this point we can access our queue send back all of our responses
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("%s %d speculatively executed transactions on partition %d",
                                       (commit ? "Commiting" : "Aborting"), this.queued_responses.size(), this.partitionId));

        // Loop backwards through our queued responses and find the latest txn that 
        // we need to tell the EE to commit. All ones that completed before that won't
        // have to hit up the EE.
        Pair<LocalTransaction, ClientResponseImpl> p = null;
        boolean ee_commit = true;
        int skip_commit = 0;
        int aborted = 0;
        while ((p = (hstore_conf.site.exec_queued_response_ee_bypass ? this.queued_responses.pollLast() : this.queued_responses.pollFirst())) != null) {
            LocalTransaction ts = p.getFirst();
            ClientResponseImpl cr = p.getSecond();
            // 2011-07-02: I have no idea how this could not be stopped here, but for some reason
            // I am getting a random error.
            // FIXME if (hstore_conf.site.txn_profiling && ts.profiler.finish_time.isStopped()) ts.profiler.finish_time.start();
            
            // If the multi-p txn aborted, then we need to abort everything in our queue
            // Change the status to be a MISPREDICT so that they get executed again
            if (commit == false) {
                cr.setStatus(Hstore.Status.ABORT_MISPREDICT);
                ts.setPendingError(new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions()), false);
                aborted++;
                
            // Optimization: Check whether the last element in the list is a commit
            // If it is, then we know that we don't need to tell the EE about all the ones that executed before it
            } else if (hstore_conf.site.exec_queued_response_ee_bypass) {
                // Don't tell the EE that we committed
                if (ee_commit == false) {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Bypassing EE commit for %s [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ts.unsetSubmittedEE(this.partitionId);
                    skip_commit++;
                    
                } else if (ee_commit && cr.getStatus() == Hstore.Status.OK) {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Committing %s but will bypass all other successful transactions [undoToken=%d]", ts, ts.getLastUndoToken(this.partitionId)));
                    ee_commit = false;
                }
            }
            
            try {
                if (hstore_conf.site.exec_postprocessing_thread) {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Passing queued ClientResponse for %s to post-processing thread [status=%s]", ts, cr.getStatus()));
                    hstore_site.queueClientResponse(this, ts, cr);
                } else {
                    if (t) LOG.trace("__FILE__:__LINE__ " + String.format("Sending queued ClientResponse for %s back directly [status=%s]", ts, cr.getStatus()));
                    this.sendClientResponse(ts, cr);
                }
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to complete queued " + ts, ex);
            }
        } // WHILE
        if (d && skip_commit > 0 && hstore_conf.site.exec_queued_response_ee_bypass) {
            LOG.debug("__FILE__:__LINE__ " + String.format("Fast Commit EE Bypass Optimization [skipped=%d, aborted=%d]", skip_commit, aborted));
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
        LOG.warn("__FILE__:__LINE__ " + String.format("ExecutionSite for Partition #%d is crashing", this.partitionId), ex);
        assert(this.hstore_coordinator != null);
        this.hstore_coordinator.shutdownCluster(ex); // This won't return
    }
    
    @Override
    public boolean isShuttingDown() {
        return (this.hstore_site.isShuttingDown()); // shutdown_state == State.PREPARE_SHUTDOWN || this.shutdown_state == State.SHUTDOWN);
    }
    
    @Override
    public void prepareShutdown(boolean error) {
        shutdown_state = Shutdownable.ShutdownState.PREPARE_SHUTDOWN;
    }
    
    /**
     * Somebody from the outside wants us to shutdown
     */
    public synchronized void shutdown() {
        if (this.shutdown_state == ShutdownState.SHUTDOWN) {
            if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Partition #%d told to shutdown again. Ignoring...", this.partitionId));
            return;
        }
        this.shutdown_state = ShutdownState.SHUTDOWN;
        
        if (d) LOG.debug("__FILE__:__LINE__ " + String.format("Shutting down ExecutionSite for Partition #%d", this.partitionId));
        
        // Clear the queue
        this.work_queue.clear();
        
        // Make sure we shutdown our threadpool
        // this.thread_pool.shutdownNow();
        if (this.self != null) this.self.interrupt();
        
        if (this.shutdown_latch != null) {
            try {
                this.shutdown_latch.acquire();
            } catch (InterruptedException ex) {
                // Ignore
            } catch (Exception ex) {
                LOG.fatal("__FILE__:__LINE__ " + "Unexpected error while shutting down", ex);
            }
        }
    }
}
