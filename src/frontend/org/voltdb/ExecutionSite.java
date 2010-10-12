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

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import org.voltdb.catalog.*;
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
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.VoltProcedure.*;

import ca.evanjones.protorpc.ProtoRpcController;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.PartitionEstimator;
import edu.mit.dtxn.Dtxn;
import edu.mit.hstore.HStoreCoordinatorNode;
import edu.mit.hstore.HStoreMessenger;

/**
 * The main executor of transactional work in the system. Controls running
 * stored procedures and manages the execution engine's running of plan
 * fragments. Interacts with the DTXN system to get work to do. The thread might
 * do other things, but this is where the good stuff happens.
 */
public class ExecutionSite implements Runnable {
    public static final Logger LOG = Logger.getLogger(ExecutionSite.class.getName());

    // ----------------------------------------------------------------------------
    // GLOBAL CONSTANTS
    // ----------------------------------------------------------------------------

    /**
     * 
     */
    public static final int NULL_DEPENDENCY_ID = -1;

    private static final int COORD_THREAD_POOL_SIZE = 1;
    private static final int NODE_THREAD_POOL_SIZE = 5;

    /**
     * Default number of VoltProcedure threads to keep around
     */
    private static final int COORD_VOLTPROCEDURE_POOL_SIZE = 1;
    private static final int NODE_VOLTPROCEDURE_POOL_SIZE = 5;
    

    // ----------------------------------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------------------------------

    public int siteId;

    /**
     * Catalog objects
     */
    protected Catalog catalog;
    protected Cluster cluster;
    protected Database database;
    protected Site site;
    protected Partition partition;
    
    // Quick lookup for Procedure Name -> Procedure Catalog Object
    private final Map<String, Procedure> proc_lookup = new HashMap<String, Procedure>();

    protected final BackendTarget backend_target;
    protected final ExecutionEngine ee;
    protected final HsqlBackend hsql;
    protected final DBBPool buffer_pool = new DBBPool(true, true);

    /**
     * Runtime Estimators
     */
    protected final PartitionEstimator p_estimator;
    protected final TransactionEstimator t_estimator;
    
    protected WorkloadTrace workload_trace;
    
    // ----------------------------------------------------------------------------
    // H-Store Transaction Stuff
    // ----------------------------------------------------------------------------

    protected HStoreCoordinatorNode hstore_coordinator;
    protected HStoreMessenger hstore_messenger;

    // ----------------------------------------------------------------------------
    // Execution State
    // ----------------------------------------------------------------------------
    
    /**
     * TransactionId -> TransactionState
     */
    protected final ConcurrentHashMap<Long, TransactionState> txn_states = new ConcurrentHashMap<Long, TransactionState>(); 

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

    /**
     * These are our executing VoltProcedure threads for transactions initiated in this site. 
     */
    protected final ConcurrentHashMap<Long, VoltProcedure> running_xacts = new ConcurrentHashMap<Long, VoltProcedure>();

    /**
     * Thread Pool 
     */
    protected final ExecutorService pool;
    
    /**
     * VoltProcedure pool
     * Since we can invoke multi stored procedures at a time, we will want to keep a pool of reusable
     * instances so that we don't have to make allocate new memory each time.
     */
    protected final HashMap<String, ConcurrentLinkedQueue<VoltProcedure>> proc_pool = new HashMap<String, ConcurrentLinkedQueue<VoltProcedure>>();

    /**
     * List of all procedures from createVoltProcedure()
     */
    protected final HashMap<String, ConcurrentLinkedQueue<VoltProcedure>> all_procs = new HashMap<String, ConcurrentLinkedQueue<VoltProcedure>>();

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
            final boolean trace = LOG.isTraceEnabled();
            final boolean debug = LOG.isDebugEnabled();
            
            // Ignore (I think this is ok...)
            if (trace) LOG.trace("Processing Dtxn.CoordinatorResponse in RPC callback with " + parameter.getResponseCount() + " embedded responses");
            for (int i = 0, cnt = parameter.getResponseCount(); i < cnt; i++) {
                ByteString serialized = parameter.getResponse(i).getOutput();
                long txn_id = parameter.getTransactionId(); // response.getTxnId();
                if (trace) LOG.trace("Got FragmentResponseMessage for Txn #"+ txn_id + " [size=" + serialized.size() + "]");
                
                FragmentResponseMessage response = null;
                try {
                    response = (FragmentResponseMessage)VoltMessage.createMessageFromBuffer(serialized.asReadOnlyByteBuffer(), false);
                } catch (Exception ex) {
                    LOG.fatal("Failed to deserialize embedded Dtxn.CoordinatorResponse message\n" + Arrays.toString(serialized.toByteArray()), ex);
                    System.exit(1);
                }
                assert(response != null);
                assert(txn_id == response.getTxnId());
                if (trace) LOG.trace("FragmentResponseMessage [txn_id=" + txn_id + ", " +
                                                              "size=" + serialized.size() + ", " +
//                                                              "id=" + serialized.byteAt(VoltMessage.HEADER_SIZE) + ", " +
                                                              "src_partition=" + response.getSourcePartitionId() + ", " +
                                                              "deps=" + response.getTableCount() + "]");
                
                // Since there is no data for us to store, we will want to just let the TransactionState know
                // that we got a response. This ensures that the txn isn't unblocked just because the data arrives
                TransactionState ts = ExecutionSite.this.txn_states.get(txn_id);
                assert(ts != null) : "No transaction state exists for txn #" + txn_id + " " + txn_states;
                for (int ii = 0, num_tables = response.getTableCount(); ii < num_tables; ii++) {
                    ts.addResponse(response.getSourcePartitionId(), response.getTableDependencyIdAtIndex(ii));
                } // FOR
                
//                int num_tables = response.getTableCount();
//                if (num_tables > 0) {
//                    if (trace) LOG.trace("CoordinatorResponse contains data for txn #" + txn_id);
//                    for (int ii = 0; ii < num_tables; ii++) {
//                        int dependency_id = response.getTableDependencyIdAtIndex(ii);
//                        int partition = (int)response.getExecutorSiteId();
//                        VoltTable table = response.getTableAtIndex(ii);
//                        if (trace) LOG.trace("[Response#" + i + " - Table#" + ii + "] Txn#=" + txn_id + ", Partition=" + partition + ", DependencyId=" + dependency_id + ", Table=" + table.getRowCount() + " tuples");
//                        ts.addResult(partition, dependency_id, table);
//                    } // FOR
//                } // FOR
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
        this.catalog = null;
        this.cluster = null;
        this.site = null;
        this.database = null;
        this.backend_target = BackendTarget.HSQLDB_BACKEND;
        this.pool = null;
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
        this.site = this.partition.getParent();
        assert(site != null) : "Unable to get Site for Partition #" + partitionId;
        this.siteId = this.site.getId();
        
        this.backend_target = target;
        this.cluster = CatalogUtil.getCluster(catalog);
        this.database = CatalogUtil.getDatabase(cluster);

        // Setup Thread Pool
        int pool_size = NODE_THREAD_POOL_SIZE;
        this.pool = Executors.newFixedThreadPool(pool_size);
        LOG.debug("Created ExecutionSite thread pool with " + pool_size + " threads");
        
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
            LOG.debug("Creating EE wrapper with target type '" + target + "'");
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

        // load up all the stored procedures
        final CatalogMap<Procedure> catalogProcedures = database.getProcedures();
        for (final Procedure catalog_proc : catalogProcedures) {
            this.all_procs.put(catalog_proc.getName(), new ConcurrentLinkedQueue<VoltProcedure>());
            this.proc_pool.put(catalog_proc.getName(), new ConcurrentLinkedQueue<VoltProcedure>());
            this.proc_lookup.put(catalog_proc.getName(), catalog_proc);
            
            if (catalog_proc.getSystemproc()) {
                pool_size = 1;
            } else {
                pool_size = NODE_VOLTPROCEDURE_POOL_SIZE;
            }
            this.createVoltProcedures(catalog_proc, pool_size);
        } // FOR
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
        // doSnapshotWork();
    }
    
    /**
     * Create a bunch of instances of the corresponding VoltProcedure for the given Procedure catalog object
     * @param catalog_proc
     * @param count
     * @return
     */
    protected void createVoltProcedures(final Procedure catalog_proc, int count) {
        assert(count > 0);
        Class<?> procClass = null;

        // Only try to load the Java class file for the SP if it has one
        if (catalog_proc.getHasjava()) {
            final String className = catalog_proc.getClassname();
            try {
                procClass = Class.forName(className);
            } catch (final ClassNotFoundException e) {
                LOG.fatal("Failed to load procedure class '" + className + "'", e);
                System.exit(1);
            }
        }
        try {
            for (int i = 0; i < count; i++) {
                VoltProcedure volt_proc = null;
                if (catalog_proc.getHasjava()) {
                    volt_proc = (VoltProcedure) procClass.newInstance();
                } else {
                    volt_proc = new VoltProcedure.StmtProcedure();
                }
                // volt_proc.registerCallback(this.callback);
                volt_proc.init(this, catalog_proc, this.backend_target, hsql, cluster, this.p_estimator, this.getPartitionId());
                this.all_procs.get(catalog_proc.getName()).add(volt_proc);
                this.proc_pool.get(catalog_proc.getName()).add(volt_proc);
            } // FOR
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) LOG.warn("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
        }

    }

    public void setHStoreMessenger(HStoreMessenger hstore_messenger) {
        this.hstore_messenger = hstore_messenger;
    }
    
    public void setHStoreCoordinatorNode(HStoreCoordinatorNode hstore_coordinator) {
        this.hstore_coordinator = hstore_coordinator;
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
    
    public int getHostId() {
        return getCatalogSite().getHost().getRelativeIndex();
        // return Integer.valueOf(getCatalogSite().getHost().getTypeName());
    }
    
    public int getSiteId() {
        return (this.site.getId());
    }
    
    /**
     * Return the local partition id for this ExecutionSite
     * @return
     */
    public int getPartitionId() {
        return (this.partition.getId());
    }

    public VoltProcedure getRunningVoltProcedure(long txn_id) {
        // assert(this.running_xacts.containsKey(txn_id)) : "No running VoltProcedure exists for txn #" + txn_id;
        return (this.running_xacts.get(txn_id));
    }
    
    /**
     * Returns the VoltProcedure instance for a given stored procedure name
     * @param proc_name
     * @return
     */
    public VoltProcedure getProcedure(String proc_name) {
        Procedure catalog_proc = this.proc_lookup.get(proc_name);
        assert(catalog_proc != null) : "Invalid stored procedure name '" + proc_name + "'"; 
        
        // If our pool is empty, then we need to make a new one
        VoltProcedure volt_proc = this.proc_pool.get(proc_name).poll();
        if (volt_proc == null) {
            this.createVoltProcedures(catalog_proc, 1);
            volt_proc = this.proc_pool.get(proc_name).poll();
        }
        assert(volt_proc != null);
        return (volt_proc);
    }

    public String getThreadName() {
        String name = String.format("E%03d-%03d", this.getSiteId(), this.getPartitionId());
        return (name);
    }

    /**
     * Primary run method that is invoked a single time when the thread is started.
     * Has the opportunity to do startup config.
     */
    @Override
    public void run() {
        // pick a name with four places for siteid so it can be sorted later
        // bit hackish, sorry
        Thread thread = Thread.currentThread();
        thread.setName(this.getThreadName());

        assert(this.hstore_coordinator != null);
        assert(this.hstore_messenger != null);

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

        try {
            if (LOG.isDebugEnabled()) LOG.debug("Starting ExecutionSite run loop...");
            boolean stop = false;
            while (!stop) {
                final boolean trace = LOG.isTraceEnabled();
                final boolean debug = LOG.isDebugEnabled();
                TransactionInfoBaseMessage work = null;
                
                // Check if there is any work that we need to execute
                try {
                    // LOG.info("Polling work queue [" + this.work_queue + "]");
                    work = this.work_queue.poll(250, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    if (debug) LOG.debug("Interupted while polling work queue. Halting ExecutionSite...", ex);
                    stop = true;
                }

                // -------------------------------
                // Execute Query Plan Fragments
                // -------------------------------
                if (work instanceof FragmentTaskMessage) {
                    FragmentTaskMessage ftask = (FragmentTaskMessage)work;
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
                    if (trace) LOG.trace("Executing FragmentTaskMessage txn #" + txn_id + " [partition=" + ftask.getSourcePartitionId() + ", " +
                    		                                                                "is_local=" + is_local + ", " +
                                                                                            "is_dtxn=" + is_dtxn + ", " +
                                                                                            "fragments=" + Arrays.toString(ftask.getFragmentIds()) + "]");

                    // If this txn isn't local, then we have to update our undoToken
                    if (is_local == false) ts.initRound(this.getNextUndoToken());

                    FragmentResponseMessage fresponse = new FragmentResponseMessage(ftask);
                    fresponse.setStatus(FragmentResponseMessage.NULL, null);
                    DependencySet result = null;
                    try {
                        result = this.processFragmentTaskMessage(ftask, ts.getLastUndoToken());
                    } catch (EEException ex) {
                        LOG.error("Hit an EE Error for txn #" + txn_id, ex);
                        fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
                    } catch (SQLException ex) {
                        LOG.error("Hit a SQL Error for txn #" + txn_id, ex);
                        fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, ex);
                    } catch (Exception ex) {
                        LOG.error("Something unexpected and bad happended for txn #" + txn_id, ex);
                        fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
                    }
                    
                    
                    if (result == null && fresponse.getStatusCode() == FragmentResponseMessage.SUCCESS) {
                        Exception ex = new Exception("The Fragment executed successfully but result is null!");
                        LOG.error(ex);
                        fresponse.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, new SerializableException(ex));
                        
                    // If the txn is running locally, should we propagate something back to the
                    // client here??
                    } else if (result != null) {
                        fresponse.setStatus(FragmentResponseMessage.SUCCESS, null);
                        
                        // XXX: For single-sited INSERT/UPDATE/DELETE queries, we don't directly
                        // execute the SendPlanNode in order to get back the number of tuples that
                        // were modified. So we have to rely on the output dependency ids set in the task
                        assert(result.size() == ftask.getOutputDependencyIds().length) :
                            "Got back " + result.size() + " results but was expecting " + ftask.getOutputDependencyIds().length;
                        
                        // If the transaction is local, store the result in the local TransactionState
                        // Unless we were sent this FragmentTaskMessage through the Dtxn.Coordinator
                        if (is_local && is_dtxn == false) {
                            if (debug) LOG.debug("Storing " + result.size() + " dependency results locally for successful FragmentTaskMessage");
                            assert(ts != null);
                            for (int i = 0, cnt = result.size(); i < cnt; i++) {
                                // ts.addResult(result.depIds[i], result.dependencies[i]);
                                if (trace) LOG.trace("Storing DependencyId #" + ftask.getOutputDependencyIds()[i] + " for txn #" + txn_id);
                                ts.addResult(this.getPartitionId(), ftask.getOutputDependencyIds()[i], result.dependencies[i]);
                                ts.addResponse(this.getPartitionId(), ftask.getOutputDependencyIds()[i]);
                            } // FOR
                            
                        // Otherwise push dependencies back to the remote partition that needs it
                        } else {
                            if (debug) LOG.debug("Constructing FragmentResponseMessage with " + result.size() + " results to send back to initial site for txn #" + txn_id);
                            
                            // We need to include the DependencyIds in our response, but we will send the actual
                            // data through the HStoreMessenger
                            for (int i = 0, cnt = result.size(); i < cnt; i++) {
                                fresponse.addDependency(result.depIds[i]);
                            } // FOR
                            this.sendFragmentResponseMessage(ftask, fresponse);
                            
                            // Bombs away!
                            this.hstore_messenger.sendDependencySet(txn_id, this.getPartitionId(), txn_partition_id, result); 
                        }
                    }
                    
                    // Again, if we're not local, just clean up our TransactionState
                    if (!is_local) {
                        if (debug) LOG.debug("Executed non-local FragmentTask. Notifying TransactionState for txn #" + txn_id + " to finish round");
                        ts.finishRound();
                    }

                // -------------------------------
                // Invoke Stored Procedure
                // -------------------------------
                } else if (work instanceof InitiateTaskMessage) {
                    InitiateTaskMessage init_work = (InitiateTaskMessage)work;
                    VoltProcedure volt_proc = null;
                    assert(!this.txn_states.contains(init_work.getTxnId())) : "Duplicate InitiateTaskMessage message for txn #" + init_work.getTxnId();

                    try {
                        volt_proc = this.getProcedure(init_work.getStoredProcedureName());
                    } catch (AssertionError ex) {
                        LOG.error("Unrecoverable error for txn #" + init_work.getTxnId());
                        LOG.error("InitiateTaskMessage= " + init_work.getDumpContents().toString());
                        throw ex;
                    }
                    long txn_id = init_work.getTxnId();
                    TransactionState ts = this.txn_states.get(txn_id);
                    assert(ts != null) : "The TransactionState is somehow null for txn #" + txn_id;
                    if (trace) LOG.trace("Initiating new " + init_work.getStoredProcedureName() + " invocation for txn #" + txn_id);
                    
                    this.startTransaction(ts, volt_proc, init_work);

                //
                // Spare Cycles
                //
                } else if (work == null) {
                    // Nothing to do right now, but we may want to take this opportunity to do
                    // some utility work (e.g., recompute Markov model probabilities)
                    // TODO(pavlo)
                    
                } else {
                    throw new RuntimeException("Unexpected work message in queue: " + work);
                }

                this.tick();
                stop = stop || thread.isInterrupted();
            } // WHILE
        } catch (final RuntimeException e) {
            LOG.fatal(e);
            throw e;
        } catch (Exception ex) {
            LOG.fatal("Something bad happended", ex);
            throw new RuntimeException(ex);
        }
        
        // Stop HStoreMessenger (because we're nice)
        if (this.hstore_messenger != null) this.hstore_messenger.stop();
        
        LOG.debug("ExecutionSite thread is stopping");
    }

    // ---------------------------------------------------------------
    // VOLTPROCEDURE EXECUTION METHODS
    // ---------------------------------------------------------------

    /**
     * This method tells the VoltProcedure to start executing  
     * @param init_work
     * @param volt_proc
     */
    protected void startTransaction(TransactionState ts, VoltProcedure volt_proc, InitiateTaskMessage init_work) {
        long txn_id = init_work.getTxnId();
        LOG.debug("Initializing the execution of " + volt_proc.procedure_name + " for txn #" + txn_id + " on partition " + this.getPartitionId());
        
        // Tell the TransactionEstimator to begin following this transaction
        // TODO(pavlo+svelgap): We need figure out what is going to do the query estimations at the beginning
        //                      of the transaction in order to get what partitions this thing will touch.
        // TODO(pavlo+evanj): Once we get back these estimations, I need to know who to tell about them.
        
//        TransactionEstimator.Estimate estimate = this.t_estimator.startTransaction(txn_id, volt_proc.getProcedure(), init_work.getParameters());
//        if (estimate != null) {
//            // Do something!!
//        }
        
        // Invoke the VoltProcedure thread to start the transaction
        assert(!this.running_xacts.values().contains(volt_proc));
        this.running_xacts.put(txn_id, volt_proc);
        volt_proc.call(ts, init_work.getParameters());
    }

    /**
     * Executes a FragmentTaskMessage on behalf of some remote site and returns the resulting DependencySet
     * @param ftask
     * @return
     * @throws Exception
     */
    protected DependencySet processFragmentTaskMessage(FragmentTaskMessage ftask, long undoToken) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        DependencySet result = null;
        long txn_id = ftask.getTxnId();
        int fragmentIdIndex = ftask.getFragmentCount();
        long fragmentIds[] = ftask.getFragmentIds();
        int output_depIds[] = ftask.getOutputDependencyIds();
        int input_depIds[] = ftask.getAllUnorderedInputDepIds(); // Is this ok?
        
        if (ftask.getFragmentCount() == 0) {
            LOG.warn("Got a FragmentTask for txn #" + txn_id + " that does not have any fragments?!?");
            return (result);
        }
        
        if (debug) LOG.debug("Getting ready to kick " + ftask.getFragmentCount() + " fragments to EE for txn #" + txn_id); 
        if (trace) LOG.trace("FragmentTaskIds: " + Arrays.toString(ftask.getFragmentIds()));
        int parameterSetIndex = ftask.getFragmentCount();
        ParameterSet parameterSets[] = new ParameterSet[parameterSetIndex];
        for (int i = 0; i < parameterSetIndex; i++) {
            ByteBuffer paramData = ftask.getParameterDataForFragment(i);
            if (paramData != null) {
                // HACK: Copy the ByteBuffer because I think it's getting released somewhere else
                // not by us whenever we have large parameters (like when we try to insert tuples)
                paramData = paramData.duplicate();
                

                final FastDeserializer fds = new FastDeserializer(paramData);
                if (trace) LOG.trace("Txn #" + txn_id + "->paramData[" + i + "] => " + fds.buffer());
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
        HashMap<Integer, List<VoltTable>> dependencies = new HashMap<Integer, List<VoltTable>>();
        TransactionState ts = this.txn_states.get(txn_id);
//        if (ftask.hasAttachedResults()) {
//            if (trace) LOG.trace("Retreiving internal dependency results attached to FragmentTaskMessage for txn #" + txn_id);
//            dependencies.putAll(ftask.getAttachedResults());
//        }
        if (ftask.hasInputDependencies()) {
            if (ts != null && !ts.getInternalDependencyIds().isEmpty()) {
                if (trace) LOG.trace("Retreiving internal dependency results from TransactionState for txn #" + txn_id);
                dependencies.putAll(ts.removeInternalDependencies(ftask));
            }
        }

        // -------------------------------
        // SYSPROC FRAGMENTS
        // -------------------------------
        if (ftask.isSysProcTask()) {
            assert(fragmentIds.length == 1);
            long fragment_id = (long)fragmentIds[0];

            VoltSystemProcedure proc = null;
            synchronized (this.m_registeredSysProcPlanFragments) {
                proc = this.m_registeredSysProcPlanFragments.get(fragment_id);
            }
            if (proc == null) throw new RuntimeException("No sysproc handle exists for FragmentID #" + fragment_id + " :: " + this.m_registeredSysProcPlanFragments);
            
            // HACK: We have to set the TransactionState for sysprocs manually
            proc.setTransactionState(ts);
            result = proc.executePlanFragment(txn_id, dependencies, (int)fragmentIds[0], parameterSets[0], this.m_systemProcedureContext);
            if (trace) LOG.trace("Finished executing sysproc fragments for " + proc.getClass().getSimpleName());
        // -------------------------------
        // REGULAR FRAGMENTS
        // -------------------------------
        } else {
            assert(this.ee != null) : "The EE object is null. This is bad!";
            if (trace) LOG.trace("Executing " + fragmentIdIndex + " fragments for txn #" + txn_id + " [lastCommittedTxnId=" + this.lastCommittedTxnId + ", undoToken=" + undoToken + "]");
            
            assert(fragmentIdIndex == parameterSetIndex);
            if (trace) {
                LOG.trace("Fragments:           " + Arrays.toString(fragmentIds));
                LOG.trace("Parameters:          " + Arrays.toString(parameterSets));
                LOG.trace("Input Dependencies:  " + Arrays.toString(input_depIds));
                LOG.trace("Output Dependencies: " + Arrays.toString(output_depIds));
            }

            // pass attached dependencies to the EE (for non-sysproc work).
            if (dependencies != null) {
                if (trace) LOG.trace("Stashing Dependencies: " + dependencies.keySet());
//                assert(dependencies.size() == input_depIds.length) : "Expected " + input_depIds.length + " dependencies but we have " + dependencies.size();
                ee.stashWorkUnitDependencies(dependencies);
            }
            result = this.ee.executeQueryPlanFragmentsAndGetDependencySet(
                        fragmentIds,
                        fragmentIdIndex,
                        input_depIds,
                        output_depIds,
                        parameterSets,
                        parameterSetIndex,
                        txn_id,
                        this.lastCommittedTxnId,
                        undoToken);
            if (trace) LOG.trace("Executed fragments " + Arrays.toString(fragmentIds) + " and got back results: " + Arrays.toString(result.depIds)); //  + "\n" + Arrays.toString(result.dependencies));
            assert(result != null) : "The resulting DependencySet for FragmentTaskMessage " + ftask + " is null!";
        }
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
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        ts.addResult(sender_partition_id, dependency_id, data);
    }
    
    public void loadTable(
            long txnId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data,
            int allowELT)
    throws VoltAbortException
    {
        if (cluster == null) {
            throw new VoltProcedure.VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        Database db = cluster.getDatabases().get(databaseName);
        if (db == null) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        ee.loadTable(table.getRelativeIndex(), data,
                     txnId,
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
    public void cleanupTransaction(long txn_id) {
        if (LOG.isDebugEnabled()) LOG.debug("Cleaning up internal state information for Txn #" + txn_id);
        VoltProcedure volt_proc = this.running_xacts.remove(txn_id);
        assert(volt_proc != null);
        assert(this.all_procs.get(volt_proc.getProcedureName()).contains(volt_proc));
        this.proc_pool.get(volt_proc.getProcedureName()).add(volt_proc);
        
//        if (LOG.isTraceEnabled()) LOG.trace("Removing TransactionState for Txn #" + txn_id);
//        assert(this.txn_states.contains(txn_id));
//        this.txn_states.remove(txn_id);
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
     * New work from an internal mechanism  
     * @param task
     */
    public void doWork(TransactionInfoBaseMessage task) {
        this.doWork(task, null);
    }

    /**
     * New work from the coordinator that this local site needs to execute (non-blocking)
     * This method will simply chuck the task into the work queue.
     * @param task
     * @param callback the RPC handle to send the response to
     */
    public void doWork(TransactionInfoBaseMessage task, RpcCallback<Dtxn.FragmentResponse> callback) {
        final boolean debug = LOG.isDebugEnabled(); 
        final boolean trace = LOG.isTraceEnabled();

        long txn_id = task.getTxnId();
        long client_handle = task.getClientHandle();
        boolean start_txn = (task instanceof InitiateTaskMessage);
        
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            ts = new TransactionState(this, txn_id, task.getSourcePartitionId(), client_handle, start_txn);
            this.txn_states.put(txn_id, ts);
            if (trace) LOG.trace("Creating transaction state for txn #" + txn_id);
        }
        
        // Again, we could be getting an InitiateTaskMessage or some other stuff
        if (start_txn) {
            if (trace) LOG.trace("Starting new VoltProcedure invocation for txn #" + txn_id);
            if (start_txn) assert(callback != null) : "Missing callback for txn #" + txn_id;
            ts.setCoordinatorCallback(callback);
        } else {
            FragmentTaskMessage ftask = (FragmentTaskMessage)task;
            if (callback != null) {
                if (trace) LOG.trace("Storing FragmentTask callback in TransactionState for txn #" + txn_id);
                ts.setFragmentTaskCallback(ftask, callback);
            } else {
                assert(ftask.isUsingDtxnCoordinator() == false) : "No callback for remote execution request for txn #" + txn_id;
            }
        }
        assert(ts != null) : "The TransactionState is somehow null for txn #" + txn_id;
        
        if (debug) LOG.debug("Adding work request for txn #" + txn_id + " with" + (callback == null ? "out" : "") + " a callback");
        this.work_queue.add(task);
    }

    /**
     * Send a ClientResponseImpl message back to the coordinator
     */
    public void sendClientResponse(ClientResponseImpl cresponse) {
        final boolean trace = LOG.isTraceEnabled(); 
        long txn_id = cresponse.getTransactionId();
        // Don't remove the TransactionState here. We do that later in commit/abort
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        RpcCallback<Dtxn.FragmentResponse> callback = ts.getCoordinatorCallback();
        if (callback == null) {
            throw new RuntimeException("No RPC callback to HStoreCoordinator for txn #" + txn_id);
        }
        long client_handle = cresponse.getClientHandle();
        assert(client_handle != -1) : "The client handle for txn #" + txn_id + " was not set properly";

        FastSerializer out = new FastSerializer(); // TODO: Should I be using this.buffer_pool ???
        try {
            out.writeObject(cresponse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Dtxn.FragmentResponse.Builder builder = Dtxn.FragmentResponse.newBuilder().setOutput(ByteString.copyFrom(out.getBytes()));
        if (trace) {
            LOG.trace("Sending ClientResponseImpl back for txn #" + txn_id + " [status=" + cresponse.getStatusName() + ", size=" + builder.getOutput().size() + "]");
            LOG.trace("RESULTS:\n" + Arrays.toString(cresponse.getResults()));
        }

        // IMPORTANT: If we executed this locally, then we need to commit/abort right here 
        boolean is_local = ts.isSinglePartition() && ts.isExecLocal();
        switch (cresponse.getStatus()) {
            case ClientResponseImpl.SUCCESS:
                if (trace) LOG.trace("Marking txn #" + txn_id + " as success. If only Evan was still alive to see this!");
                builder.setStatus(Dtxn.FragmentResponse.Status.OK);
                if (is_local) this.commitWork(txn_id);
                break;
            case ClientResponseImpl.USER_ABORT:
                if (trace) LOG.trace("Marking txn #" + txn_id + " as user aborted. Are you sure Mr.Pavlo?");
            default:
                if (cresponse.getStatus() != ClientResponseImpl.USER_ABORT) {
                    LOG.warn("Server error! Throwing an abort! Rabble Rabble!");
                }
                builder.setStatus(Dtxn.FragmentResponse.Status.ABORT_USER);
                if (is_local) this.abortWork(txn_id);
                break;
        } // SWITCH
        if (trace) LOG.trace("Invoking finished callback for txn #" + txn_id);
        callback.run(builder.build());
    }

    /**
     * 
     * @param fresponse
     */
    public void sendFragmentResponseMessage(FragmentTaskMessage ftask, FragmentResponseMessage fresponse) {
        final boolean trace = LOG.isTraceEnabled();
        long txn_id = fresponse.getTxnId();
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        RpcCallback<Dtxn.FragmentResponse> callback = ts.getFragmentTaskCallback(ftask);
        if (callback == null) {
            throw new RuntimeException("No RPC callback to HStoreCoordinator for txn #" + txn_id);
        }

        BBContainer bc = fresponse.getBufferForMessaging(this.buffer_pool);
        assert(bc.b.hasArray());
        ByteString bs = ByteString.copyFrom(bc.b.array());
//        ByteBuffer serialized = bc.b.asReadOnlyBuffer();
//        LOG.info("Serialized FragmentResponseMessage [size=" + serialized.capacity() + ",id=" + serialized.get(VoltMessage.HEADER_SIZE) + "]");
//        assert(serialized.get(VoltMessage.HEADER_SIZE) == VoltMessage.FRAGMENT_RESPONSE_ID);
        
        if (trace) LOG.trace("Sending FragmentResponseMessage for txn #" + txn_id + " [size=" + bs.size() + "]");
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
     * This site is requesting that the coordinator execute work on its behalf
     * at remote sites in the cluster 
     * @param ftasks
     */
    protected void requestWork(TransactionState ts, List<FragmentTaskMessage> tasks) {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        assert(!tasks.isEmpty());
        assert(ts != null);
        long txn_id = ts.getTransactionId();

        if (debug) LOG.debug("Combining " + tasks.size() + " FragmentTaskMessages into a single Dtxn.CoordinatorFragment.");
        
        // Now we can go back through and start running all of the FragmentTaskMessages that were not blocked
        // waiting for an input dependency. Note that we pack all the fragments into a single
        // CoordinatorFragment rather than sending each FragmentTaskMessage in its own message
        Dtxn.CoordinatorFragment.Builder requestBuilder = Dtxn.CoordinatorFragment.newBuilder();
        requestBuilder.setTransactionId((int)txn_id);
        for (FragmentTaskMessage ftask : tasks) {
            assert(!ts.isBlocked(ftask));
            
            // Important! Set the UsingDtxn flag for each FragmentTaskMessage so that we know
            // how to respond on the other side of the request
            ftask.setUsingDtxnCoordinator(true);
            
            int target_partition = ftask.getDestinationPartitionId();
            int dependency_ids[] = ftask.getOutputDependencyIds();
            if (debug) LOG.debug("Preparing to request fragments " + Arrays.toString(ftask.getFragmentIds()) + " on partition " + target_partition + " to generate " + dependency_ids.length + " output dependencies for txn #" + txn_id);
            if (ftask.getFragmentCount() == 0) {
                LOG.warn("Trying to send a FragmentTask request with 0 fragments for txn #" + ts.getTransactionId());
                continue;
            }
            
            // Nasty...
            ByteString bs = null;
            synchronized (this) {
                bs = ByteString.copyFrom(ftask.getBufferForMessaging(this.buffer_pool).b.array());
            }
            requestBuilder.addFragment(Dtxn.CoordinatorFragment.PartitionFragment.newBuilder()
                    .setPartitionId(target_partition)
                    .setWork(bs));
            // requestBuilder.setLastFragment(false); // Why doesn't this work right? ftask.isFinalTask());
        } // FOR (tasks)

        // Bombs away!
        this.hstore_coordinator.getDtxnCoordinator().execute(new ProtoRpcController(),
                                                             requestBuilder.build(),
                                                             this.request_work_callback);
        if (debug) LOG.debug("Work request is sent for txn #" + txn_id);
    }

    /**
     * Execute the given tasks and then block the current thread waiting for the list of dependency_ids to come
     * back from whatever it was we were suppose to do... 
     * @param txn_id
     * @param dependency_ids
     * @return
     */
    public VoltTable[] waitForResponses(long txn_id, List<FragmentTaskMessage> tasks) {
        final boolean trace = LOG.isTraceEnabled(); 
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            throw new RuntimeException("No transaction state for txn #" + txn_id + " at " + this.getThreadName());
        }

        // We have to store all of the tasks in the TransactionState before we start executing, otherwise
        // there is a race condition that a task with input dependencies will start running as soon as we
        // get one response back from another executor
        ts.initRound(this.getNextUndoToken());
        List<FragmentTaskMessage> runnable = new ArrayList<FragmentTaskMessage>();
        boolean all_local = true;
        for (FragmentTaskMessage ftask : tasks) {
            all_local = all_local && (ftask.getDestinationPartitionId() == this.getPartitionId());
            if (!ts.addFragmentTaskMessage(ftask)) runnable.add(ftask);
        } // FOR
        if (runnable.isEmpty()) {
            throw new RuntimeException("Deadlock! All tasks for txn #" + txn_id + " are blocked waiting on input!");
        }
        
        // TODO: Need to figure out when we should be executing things locally versus
        // executing things at remote partitions
        if (all_local) {
            if (trace) LOG.trace("Adding " + runnable.size() + " FragmentTasks to local work queue");
            System.err.println(ts);
            this.work_queue.addAll(runnable);
        } else {
            if (trace) LOG.trace("Requesting " + runnable.size() + " FragmentTasks to be executed on remote partitions");
            this.requestWork(ts, runnable);
        }

        CountDownLatch latch = ts.startRound();
        if (trace) LOG.trace("Txn #" + txn_id + " is blocked waiting for " + latch.getCount() + " dependencies");
        try {
            latch.await();
        } catch (InterruptedException ex) {
            LOG.warn(ex);
            return (null);
        } catch (Exception ex) {
            LOG.fatal("Fatal error for txn #" + txn_id + " while waiting for results", ex);
            System.exit(1);
        }

        if (trace) LOG.trace("Txn #" + txn_id + " is now running and looking for love in all the wrong places...");
        final VoltTable results[] = ts.getResults();
        ts.finishRound();
        if (trace) LOG.trace("Txn #" + txn_id + " is returning back " + results.length + " tables to VoltProcedure");
        return (results);
    }

    /**
     * The coordinator is telling our site to commit the xact with the
     * provided transaction id
     * @param txn_id
             */
    public void commitWork(long txn_id) {
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled(); 
        
        // Important: Unless this txn is running locally at ourselves, we always want to remove 
        // it from our txn map
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            if (trace) LOG.trace(msg + ". Ignoring for now...");
            return;
            // throw new RuntimeException(msg);
        }
        
        Long undoToken = ts.getLastUndoToken();
        if (debug) LOG.debug("Committing txn #" + txn_id + " [lastCommittedTxnId=" + lastCommittedTxnId + ", undoToken=" + undoToken + "]");

        // Blah blah blah...
        if (this.ee != null && undoToken != null) {
            if (trace) LOG.trace("Releasing undoToken '" + undoToken + "' for txn #" + txn_id);
            this.ee.releaseUndoToken(undoToken); 
        }

        this.lastCommittedTxnId = txn_id;
//        if (ts.isExecLocal() == false) {
            if (trace) LOG.trace("Removing TransactionState for Txn #" + txn_id);
            this.txn_states.remove(txn_id); 
//        }
    }

    /**
     * The coordinator is telling our site to abort the xact with the
     * provided transaction id
     * @param txn_id
     */
    public void abortWork(long txn_id) {
        final boolean debug = LOG.isDebugEnabled();
        final boolean trace = LOG.isTraceEnabled(); 
        TransactionState ts = this.txn_states.get(txn_id);
        if (ts == null) {
            String msg = "No transaction state for txn #" + txn_id;
            if (trace) LOG.trace(msg + ". Ignoring for now...");
            return;
            // throw new RuntimeException(msg);
        }
        Long undoToken = ts.getLastUndoToken();
        if (debug) LOG.debug("Aborting txn #" + txn_id + " [lastCommittedTxnId=" + lastCommittedTxnId + ", undoToken=" + undoToken + "]");

        // Evan says that txns will be aborted LIFO. This means the first txn that
        // we get in abortWork() will have a the greatest undoToken, which means that 
        // it will automagically rollback all other outstanding txns.
        // I'm lazy/tired, so for now I'll just rollback everything I get, but in theory
        // we should be able to check whether our undoToken has already been rolled back
        if (this.ee != null && undoToken != null) {
            if (trace) LOG.trace("Rolling back work for txn #" + txn_id + " starting at undoToken " + undoToken);
            this.ee.undoUndoToken(undoToken);
        }
//        if (ts.isExecLocal() == false) {
            if (trace) LOG.trace("Removing TransactionState for Txn #" + txn_id);
            this.txn_states.remove(txn_id);
//        }
    }
}
