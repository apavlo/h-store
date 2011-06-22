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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.types.TimestampType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.hashing.AbstractHasher;
import edu.brown.markov.Edge;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.TransactionEstimator;
import edu.brown.markov.Vertex;
import edu.brown.markov.TransactionEstimator.State;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.Poolable;
import edu.brown.utils.ProfileMeasurement;
import edu.brown.utils.StringUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.dtxn.LocalTransactionState;
import edu.mit.hstore.dtxn.TransactionState;

/**
 * Wraps the stored procedure object created by the user
 * with metadata available at runtime. This is used to call
 * the procedure.
 *
 * VoltProcedure is extended by all running stored procedures.
 * Consider this when specifying access privileges.
 *
 */
public abstract class VoltProcedure implements Poolable {
    private static final Logger LOG = Logger.getLogger(VoltProcedure.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private static boolean t = trace.get();
    private static boolean d = debug.get();
    
    // ----------------------------------------------------------------------------
    // STATIC CONSTANTS
    // ----------------------------------------------------------------------------
    
    private static final VoltTable[] EMPTY_RESULT = new VoltTable[0];
    
    // Used to get around the "abstract" for StmtProcedures.
    // Path of least resistance?
    protected static class StmtProcedure extends VoltProcedure {}

    private final static Double DOUBLE_NULL = new Double(-1.7976931348623157E+308);
    
    public static final String ANON_STMT_NAME = "sql";

    // ----------------------------------------------------------------------------
    // GLOBAL MEMBERS
    // ----------------------------------------------------------------------------
    
    protected HsqlBackend hsql;

    // package scoped members used by VoltSystemProcedure
    protected ExecutionSite executor;
     
    private boolean m_initialized;

    // private members reserved exclusively to VoltProcedure
    private Method procMethod;
    private Class<?>[] paramTypes;
    private boolean paramTypeIsPrimitive[];
    private boolean paramTypeIsArray[];
    private Class<?> paramTypeComponentType[];
    private int paramTypesLength;
    private boolean isNative = true;
    protected Object procParams[];
    //final HashMap<Object, Statement> stmts = new HashMap<Object, Statement>( 16, (float).1);

    // cached fake SQLStmt array for single statement non-java procs
    SQLStmt[] m_cachedSingleStmt = { null };

    // Used to figure out what partitions a query needs to go to
    protected Catalog catalog;
    protected Procedure catProc;
    protected String procedure_name;
    protected AbstractHasher hasher;
    protected PartitionEstimator p_estimator;
    protected TransactionEstimator t_estimator;
    protected HStoreConf hstore_conf;
    
    protected int base_partition = -1;

    /** Callback for when the VoltProcedure finishes and we need to send a ClientResponse somewhere **/
    private final EventObservable observable = new EventObservable();

    // data from hsql wrapper
    private final ArrayList<VoltTable> queryResults = new ArrayList<VoltTable>();

    // cached txnid-seeded RNG so all calls to getSeededRandomNumberGenerator() for
    // a given call don't re-seed and generate the same number over and over
    private Random m_cachedRNG = null;
    
    // ----------------------------------------------------------------------------
    // WORKLOAD TRACE HANDLES
    // ----------------------------------------------------------------------------
    
    private boolean enable_tracing = false;
    private Object m_workloadXactHandle = null;
    private Integer m_workloadBatchId = null;
    private final Set<Object> m_workloadQueryHandles = new HashSet<Object>();

    // ----------------------------------------------------------------------------
    // INVOCATION MEMBERS
    // ----------------------------------------------------------------------------
    
    private long txn_id = -1;
    private long client_handle;
    private boolean predict_singlepartition;
    private TransactionState m_currentTxnState;  // assigned in call()
    private LocalTransactionState m_localTxnState;  // assigned in call()
    private final SQLStmt batchQueryStmts[] = new SQLStmt[1000];
    private int batchQueryStmtIndex = 0;
    private final Object[] batchQueryArgs[] = new Object[1000][];
    private int batchQueryArgsIndex = 0;
    private VoltTable[] results = EMPTY_RESULT;
    private byte status = ClientResponseImpl.SUCCESS;
    private SerializableException error = null;
    private String status_msg = "";

    /**
     * Status code that can be set by stored procedure upon invocation that will be returned with the response.
     */
    private byte m_statusCode = Byte.MIN_VALUE;
    private String m_statusString = null;
    
    // ----------------------------------------------------------------------------
    // BATCH EXECUTION MEMBERS
    // ----------------------------------------------------------------------------
    private BatchPlanner planner = null;     
    private BatchPlanner.BatchPlan plan = null;
    
    /**
     * End users should not instantiate VoltProcedure instances.
     * Constructor does nothing. All actual initialization is done in the
     * {@link VoltProcedure init} method.
     */
    public VoltProcedure() {}

    /**
     * Allow VoltProcedures access to their transaction id.
     * @return transaction id
     */
    public Long getTransactionId() {
        return this.txn_id; // m_currentTxnState.txnId;
    }

    /**
     * Allow VoltProcedures access to their transaction id.
     * @return transaction id
     */
    public void setTransactionId(long txn_id) {
        this.txn_id = txn_id;
    }

    /**
     * Allow sysprocs to update m_currentTxnState manually. User procedures are
     * passed this state in call(); sysprocs have other entry points on
     * non-coordinator sites.
     */
    public void setTransactionState(TransactionState txnState) {
        m_currentTxnState = txnState;
    }

    public TransactionState getTransactionState() {
        return m_currentTxnState;
    }
    
    /**
     * Main initialization method
     * @param site
     * @param catalog_proc
     * @param eeType
     * @param hsql
     * @param p_estimator
     * @param local_partition
     */
    public void globalInit(ExecutionSite site, Procedure catalog_proc, BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator, Integer local_partition) {
        if (m_initialized) {
            throw new IllegalStateException("VoltProcedure has already been initialized");
        } else {
            m_initialized = true;
        }
        assert(site != null);

        this.executor = site;
        this.hstore_conf = HStoreConf.singleton();
        this.catProc = catalog_proc;
        this.procedure_name = this.catProc.getName();
        this.catalog = this.catProc.getCatalog();
        this.isNative = (eeType != BackendTarget.HSQLDB_BACKEND);
        this.hsql = hsql;
        this.base_partition = this.executor.getPartitionId();
        
        this.enable_tracing = (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) &&
                              (ProcedureProfiler.workloadTrace != null);
        
        this.t_estimator = this.executor.getTransactionEstimator();
        this.p_estimator = p_estimator;
        this.hasher = this.p_estimator.getHasher();
        
        if (d) LOG.debug(String.format("Initialized VoltProcedure for %s [partition=%d]", this.procedure_name, this.base_partition));
        
        if (catalog_proc.getHasjava()) {
            int tempParamTypesLength = 0;
            Method tempProcMethod = null;
            Method[] methods = getClass().getMethods();
            Class<?> tempParamTypes[] = null;
            boolean tempParamTypeIsPrimitive[] = null;
            boolean tempParamTypeIsArray[] = null;
            Class<?> tempParamTypeComponentType[] = null;
            for (final Method m : methods) {
                String name = m.getName();
                if (name.equals("run")) {
                    //inspect(m);
                    tempProcMethod = m;
                    tempParamTypes = tempProcMethod.getParameterTypes();
                    tempParamTypesLength = tempParamTypes.length;
                    tempParamTypeIsPrimitive = new boolean[tempParamTypesLength];
                    tempParamTypeIsArray = new boolean[tempParamTypesLength];
                    tempParamTypeComponentType = new Class<?>[tempParamTypesLength];
                    for (int ii = 0; ii < tempParamTypesLength; ii++) {
                        tempParamTypeIsPrimitive[ii] = tempParamTypes[ii].isPrimitive();
                        tempParamTypeIsArray[ii] = tempParamTypes[ii].isArray();
                        tempParamTypeComponentType[ii] = tempParamTypes[ii].getComponentType();
                    }
                }
            }
            paramTypesLength = tempParamTypesLength;
            procMethod = tempProcMethod;
            paramTypes = tempParamTypes;
            paramTypeIsPrimitive = tempParamTypeIsPrimitive;
            paramTypeIsArray = tempParamTypeIsArray;
            paramTypeComponentType = tempParamTypeComponentType;

            if (procMethod == null) {
                LOG.fatal("No good method found in: " + getClass().getName());
            }

            Field[] fields = getClass().getFields();
            for (final Field f : fields) {
                if (f.getType() == SQLStmt.class) {
                    String name = f.getName();
                    Statement s = catalog_proc.getStatements().get(name);
                    if (s != null) {
                        try {
                            /*
                             * Cache all the information we need about the statements in this stored
                             * procedure locally instead of pulling them from the catalog on
                             * a regular basis.
                             */
                            SQLStmt stmt = (SQLStmt) f.get(this);

                            stmt.catStmt = s;

                            stmt.numFragGUIDs = s.getFragments().size();
                            PlanFragment fragments[] = s.getFragments().values();
                            stmt.fragGUIDs = new long[stmt.numFragGUIDs];
                            for (int ii = 0; ii < stmt.numFragGUIDs; ii++) {
                                stmt.fragGUIDs[ii] = Long.parseLong(fragments[ii].getName());
                            }
    
                            stmt.numStatementParamJavaTypes = s.getParameters().size();
                            stmt.statementParamJavaTypes = new byte[stmt.numStatementParamJavaTypes];
                            StmtParameter parameters[] = s.getParameters().values();
                            for (int ii = 0; ii < stmt.numStatementParamJavaTypes; ii++) {
                                stmt.statementParamJavaTypes[ii] = (byte)parameters[ii].getJavatype();
                            }
                            stmt.computeHashCode();
                        //stmts.put((Object) (f.get(null)), s);
                        } catch (IllegalArgumentException e) {
                            e.printStackTrace();
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    //LOG.debug("Found statement " + name);
                    }
                }
            }
        }
        // has no java
        else {
            Statement catStmt = catalog_proc.getStatements().get(ANON_STMT_NAME);
            SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
            stmt.catStmt = catStmt;
            initSQLStmt(stmt);
            m_cachedSingleStmt[0] = stmt;

            procMethod = null;

            paramTypesLength = catalog_proc.getParameters().size();

            paramTypes = new Class<?>[paramTypesLength];
            paramTypeIsPrimitive = new boolean[paramTypesLength];
            paramTypeIsArray = new boolean[paramTypesLength];
            paramTypeComponentType = new Class<?>[paramTypesLength];
            for (ProcParameter param : catalog_proc.getParameters()) {
                VoltType type = VoltType.get((byte) param.getType());
                if (type == VoltType.INTEGER) type = VoltType.BIGINT;
                if (type == VoltType.SMALLINT) type = VoltType.BIGINT;
                if (type == VoltType.TINYINT) type = VoltType.BIGINT;
                paramTypes[param.getIndex()] = type.classFromType();
                paramTypeIsPrimitive[param.getIndex()] = true;
                paramTypeIsArray[param.getIndex()] = param.getIsarray();
                assert(paramTypeIsArray[param.getIndex()] == false);
                paramTypeComponentType[param.getIndex()] = null;
            }
        }
    }
    
    public boolean isInitialized() {
        return (this.m_initialized);
    }
    
    @Override
    public void finish() {
        t = trace.get();
        d = debug.get();
        
        this.m_currentTxnState = null;
        this.m_localTxnState = null;
        this.txn_id = -1;
        this.client_handle = -1;
        this.results = EMPTY_RESULT;
        this.status = ClientResponseImpl.SUCCESS;
        this.error = null;
        this.status_msg = "";
    }
        
    final void initSQLStmt(SQLStmt stmt) {
        stmt.numFragGUIDs = stmt.catStmt.getFragments().size();
        PlanFragment fragments[] = new PlanFragment[stmt.numFragGUIDs];
        stmt.fragGUIDs = new long[stmt.numFragGUIDs];
        int i = 0;
        for (PlanFragment frag : stmt.catStmt.getFragments()) {
            fragments[i] = frag;
            stmt.fragGUIDs[i] = CatalogUtil.getUniqueIdForFragment(frag);
            i++;
        }
    
        stmt.numStatementParamJavaTypes = stmt.catStmt.getParameters().size();
        //StmtParameter parameters[] = new StmtParameter[stmt.numStatementParamJavaTypes];
        stmt.statementParamJavaTypes = new byte[stmt.numStatementParamJavaTypes];
        for (StmtParameter param : stmt.catStmt.getParameters()) {
            //parameters[i] = param;
            stmt.statementParamJavaTypes[param.getIndex()] = (byte)param.getJavatype();
            i++;
        }
    }
    
    /**
     * Get a Java RNG seeded with the current transaction id. This will ensure that
     * two procedures for the same transaction, but running on different replicas,
     * can generate an identical stream of random numbers. This is required to endure
     * procedures have deterministic behavior.
     *
     * @return A deterministically-seeded java.util.Random instance.
     */
    public Random getSeededRandomNumberGenerator() {
        // this value is memoized here and reset at the beginning of call(...).
        if (m_cachedRNG == null) {
            m_cachedRNG = new Random(this.getTransactionId());
        }
        return m_cachedRNG;
    }
    
    /**
     * End users should not call this method.
     * Used by the VoltDB runtime to initialize stored procedures for execution.
     */
//    public void init(ExecutionEngine engine, Procedure catProc, Cluster cluster, PartitionEstimator p_estimator, int local_partition) {
//        assert this.engine == null;
//        assert engine != null;
//        this.engine = engine;
//        init(null, catProc, BackendTarget.NATIVE_EE_JNI, null, cluster, p_estimator, local_partition);
//    }
    
    public String getProcedureName() {
        return (this.procedure_name);
    }
    
    /**
     * Return a hash code that uniquely identifies this list of SQLStmts
     * Derived from AbstractList.hashCode()
     * @param batchStmts
     * @param numBatchStmts
     * @return
     */
    public static int getBatchHashCode(SQLStmt[] batchStmts, int numBatchStmts) {
        int hashCode = 1;
        for (int i = 0; i < numBatchStmts; i++) {
            hashCode = 31*hashCode + (batchStmts[i] == null ? 0 : batchStmts[i].hashCode());
        } // FOR
        return hashCode;
    }

    public void registerCallback(EventObserver observer) {
        this.observable.addObserver(observer);
    }

    public void unregisterCallback(EventObserver observer) {
        this.observable.deleteObserver(observer);
    }
    
    /**
     * Convenience method to call and block a VoltProcedure
     * @param paramList
     * @return
     */
    public final ClientResponse callAndBlock(TransactionState txnState, Object... paramList) {
        final LinkedBlockingDeque<ClientResponse> lock = new LinkedBlockingDeque<ClientResponse>(1);
        EventObserver observer = new EventObserver() {
            @Override
            public void update(Observable o, Object arg) {
                assert(arg != null);
                lock.offer((ClientResponse)arg);
            }
        };
        this.registerCallback(observer);
        this.call(txnState, paramList);

        ClientResponse response = null;
        try {
            response = lock.take();
        } catch (Exception ex) {
            LOG.error("Failed to retrieve response from VoltProcedure blocking callback", ex);
        }
        this.unregisterCallback(observer);
        assert(response != null);
        return (response);
    }

    /**
     * Non-Blocking call.
     * @param paramList
     * @return
     */
    public final ClientResponse call(TransactionState txnState, Object... paramList) {
        if (t) LOG.trace("Setting up internal state for txn #" + txnState.getTransactionId());
        if (d) Thread.currentThread().setName(this.executor.getThreadName() + "-" + this.procedure_name);

        ClientResponse response = null;
        
        long current_txn_id = txnState.getTransactionId();
        assert(this.txn_id == -1) : "Old Transaction Id: " + this.txn_id + " -> New Transaction Id: " + current_txn_id;
        this.m_currentTxnState = txnState;
        this.m_localTxnState = (LocalTransactionState)txnState;
        this.txn_id = current_txn_id;
        this.client_handle = txnState.getClientHandle();
        this.procParams = paramList;
        this.predict_singlepartition = this.m_localTxnState.isPredictSinglePartition();
        
        if (d) LOG.debug("Starting execution of txn #" + current_txn_id);
        
        try {
            // Execute the txn (this blocks until we return)
            if (t) LOG.trace("Invoking VoltProcedure.call for txn #" + current_txn_id);
            response = this.call(); // Bombs away!
            assert(response != null);
            if (hstore_conf.site.txn_profiling) this.m_localTxnState.finish_time.start();

            // Notify anybody who cares that we're finished (used in testing)
            if (t) LOG.trace("Notifying observers that txn #" + current_txn_id + " is finished");
            this.observable.notifyObservers(response);
            
        } catch (AssertionError ex) {
            if (this.executor.isShuttingDown() == false) {
                LOG.fatal("Unexpected error while executing txn #" + current_txn_id + " [" + this.procedure_name + "]", ex);
                LOG.fatal("LocalTransactionState Dump:\n" + m_localTxnState);
                this.executor.crash(ex);
            }
        } catch (Exception ex) {
            LOG.fatal("Unexpected error while executing txn #" + current_txn_id + " [" + this.procedure_name + "]", ex);
            this.executor.crash(ex);
        }
        return (response);
    }

    /**
     * The thing that actually executes the VoltProcedure.run() method 
     * @return
     */
    private final ClientResponse call() {
        // in case someone queues sql but never calls execute, clear the queue here.
        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;

        // Select a local_partition to use if we're on the coordinator, otherwise
        // just use the real partition. We shouldn't have to do this once Evan gets it
        // so that we can have a coordinator on each node
        assert(this.base_partition != -1);

        //lastBatchNeedsRollback = false;

        if (this.procParams.length != this.paramTypesLength) {
            String msg = "PROCEDURE " + procedure_name + " EXPECTS " + String.valueOf(paramTypesLength) +
                " PARAMS, BUT RECEIVED " + String.valueOf(this.procParams.length);
            LOG.error(msg);
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            status_msg = msg;
            return new ClientResponseImpl(this.txn_id, status, results, status_msg, this.client_handle);
        }

        for (int i = 0; i < paramTypesLength; i++) {
            try {
                this.procParams[i] = tryToMakeCompatible(i, this.procParams[i]);
            } catch (Exception e) {
                String msg = "PROCEDURE " + procedure_name + " TYPE ERROR FOR PARAMETER " + i +
                        ": " + e.getMessage();
                LOG.fatal(msg, e);
                status = ClientResponseImpl.GRACEFUL_FAILURE;
                status_msg = msg;
                return new ClientResponseImpl(this.txn_id, this.status, this.results, this.status_msg, this.client_handle);
            }
        }

        // Workload Trace
        // Create a new transaction record in the trace manager. This will give us back
        // a handle that we need to pass to the trace manager when we want to register a new query
        if (this.enable_tracing) {
            this.m_workloadQueryHandles.clear();
            this.m_workloadXactHandle = ProcedureProfiler.workloadTrace.startTransaction(this.txn_id, catProc, this.procParams);
        }

        if (hstore_conf.site.txn_profiling) this.m_localTxnState.java_time.start();
        try {
            if (t) LOG.trace(String.format("Invoking txn #%d [procMethod=%s, class=%s, partition=%d]",
                                           this.txn_id, this.procedure_name, getClass().getSimpleName(), this.base_partition));
            try {
                Object rawResult = procMethod.invoke(this, this.procParams);
                this.results = getResultsFromRawResults(rawResult);
                if (this.results == null) results = EMPTY_RESULT;
            } catch (IllegalAccessException e) {
                // If reflection fails, invoke the same error handling that other exceptions do
                throw new InvocationTargetException(e);
            } catch (RuntimeException e) {
                LOG.fatal(String.format("Unexpected error when executing %s txn #%d", this.procedure_name, this.txn_id));
                throw new InvocationTargetException(e);
            } catch (AssertionError e) {
                LOG.fatal(String.format("Unexpected error when executing %s txn #%d", this.procedure_name, this.txn_id), e);
                System.exit(1);
            }
            if (d) LOG.debug(this.catProc + " is finished for txn #" + this.txn_id);
            
        // -------------------------------
        // Exceptions that we can process+handle
        // -------------------------------
        } catch (InvocationTargetException itex) {
            Throwable ex = itex.getCause();
            Class<?> ex_class = ex.getClass();
            
            // Pass the exception back to the client if it is serializable
            if (ex instanceof SerializableException) {
                this.error = (SerializableException)ex;
            } else if (ex instanceof AssertionError) {
                throw (AssertionError)ex;
            }
            
            // -------------------------------
            // VoltAbortException
            // -------------------------------
            if (ex_class.equals(VoltAbortException.class)) {
                if (d) LOG.debug("Caught VoltAbortException for txn #" + this.txn_id, ex);
                this.status = ClientResponseImpl.USER_ABORT;
                this.status_msg = "USER ABORT: " + ex.getMessage();
                
                if (this.enable_tracing && m_workloadXactHandle != null) {
                    ProcedureProfiler.workloadTrace.abortTransaction(m_workloadXactHandle);
                }
            // -------------------------------
            // MispredictionException
            // -------------------------------
            } else if (ex_class.equals(MispredictionException.class)) {
                if (d) LOG.warn("Caught MispredictionException for txn #" + this.txn_id);
                this.status = ClientResponse.MISPREDICTION;
                this.m_localTxnState.getTouchedPartitions().putHistogram((((MispredictionException)ex).getPartitions()));

            // -------------------------------
            // ConstraintFailureException
            // -------------------------------
            } else if (ex_class.equals(org.voltdb.exceptions.ConstraintFailureException.class)) {
                this.status = ClientResponseImpl.UNEXPECTED_FAILURE;
                this.status_msg = "CONSTRAINT FAILURE: " + ex.getMessage();
                
            // -------------------------------
            // Everthing Else
            // -------------------------------
            } else {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                String msg = sw.toString();
                if (msg == null) msg = ex.toString();
                
                String currentQueries = "";
                for (int i = 0; i < batchQueryStmtIndex; i++) {
                    currentQueries += String.format("[%02d] %s\n", i, CatalogUtil.getDisplayName(batchQueryStmts[i].catStmt));
                }
                if (executor.isShuttingDown() == false) {
                    LOG.fatal(String.format("PROCEDURE %s UNEXPECTED ABORT TXN #%d: %s\n%s", procedure_name, this.txn_id, msg, currentQueries), ex);
                }
                
                status = ClientResponseImpl.UNEXPECTED_FAILURE;
                status_msg = "UNEXPECTED ABORT: " + msg;
            }
        // -------------------------------
        // Something really bad happened. Just bomb out!
        // -------------------------------
        } catch (AssertionError e) {
            LOG.fatal(e);
            System.exit(1);
        } finally {
            if (hstore_conf.site.txn_profiling) {
                long time = ProfileMeasurement.getTime();
                if (this.m_localTxnState.java_time.isStarted()) this.m_localTxnState.java_time.stop(time);
                if (this.m_localTxnState.coord_time.isStarted()) this.m_localTxnState.coord_time.stop(time);
                if (this.m_localTxnState.plan_time.isStarted()) this.m_localTxnState.plan_time.stop(time);
            }
        }

        // Workload Trace - Stop the transaction trace record.
        if (this.enable_tracing && m_workloadXactHandle != null && this.status == ClientResponseImpl.SUCCESS) {
            ProcedureProfiler.workloadTrace.stopTransaction(m_workloadXactHandle);
        }
        
        if (this.results == null) {
            LOG.fatal("We got back a null result from txn #" + this.txn_id + " [proc=" + this.procedure_name + "]");
            System.exit(1);
        }
        
        ClientResponseImpl ret = null;
        try {
            ret = new ClientResponseImpl(this.txn_id, this.status, this.results, this.status_msg, this.error);
            ret.setClientHandle(this.client_handle);
        } catch (AssertionError ex) {
            LOG.fatal("Failed to create ClientResponse for txn #" + this.txn_id + " [proc=" + this.procedure_name + "]", ex);
            System.exit(1);
        } catch (Exception ex) {
            LOG.fatal("Failed to create ClientResponse for txn #" + this.txn_id + " [proc=" + this.procedure_name + "]", ex);
            System.exit(1);
        }
        
        return (ret);
    }

    
    protected long getClientHandle() {
        return (this.client_handle);
    }

    protected int getLocalPartition() {
        return (this.base_partition);
    }

    protected Catalog getCatalog() {
        return (this.catalog);
        }

    public Procedure getProcedure() {
        return (this.catProc);
    }

    protected PartitionEstimator getPartitionEstimator() {
        return (this.p_estimator);
    }
    
    /**
     * Given the results of a procedure, convert it into a sensible array of VoltTables.
     */
    final private VoltTable[] getResultsFromRawResults(Object result) {
        if (result == null)
            return new VoltTable[0];
        if (result instanceof VoltTable[])
            return (VoltTable[]) result;
        if (result instanceof VoltTable)
            return new VoltTable[] { (VoltTable) result };
        if (result instanceof Long) {
            VoltTable t = new VoltTable(new VoltTable.ColumnInfo("", VoltType.BIGINT));
            t.addRow(result);
            return new VoltTable[] { t };
        }
        throw new RuntimeException("Procedure didn't return acceptable type.");
    }

    /** @throws Exception with a message describing why the types are incompatible. */
    final private Object tryToMakeCompatible(int paramTypeIndex, Object param) throws Exception {
        if (param == null || param == VoltType.NULL_STRING ||
            param == VoltType.NULL_DECIMAL)
        {
            // Passing a null where we expect a primitive is a Java compile time error.
            if (paramTypeIsPrimitive[paramTypeIndex]) {
                throw new Exception("Primitive type " + paramTypes[paramTypeIndex] + " cannot be null");
            }

            // Pass null reference to the procedure run() method. These null values will be
            // converted to a serialize-able NULL representation for the EE in getCleanParams()
            // when the parameters are serialized for the plan fragment.
            return null;
        }

        if (param instanceof ExecutionSite.SystemProcedureExecutionContext) {
            return param;
        }

        Class<?> pclass = param.getClass();
        boolean slotIsArray = paramTypeIsArray[paramTypeIndex];
        if (slotIsArray != pclass.isArray())
            throw new Exception("Array / Scalar parameter mismatch");

        if (slotIsArray) {
            Class<?> pSubCls = pclass.getComponentType();
            Class<?> sSubCls = paramTypeComponentType[paramTypeIndex];
            if (pSubCls == sSubCls) {
                return param;
            } else {
                /*
                 * Arrays can be quite large so it doesn't make sense to silently do the conversion
                 * and incur the performance hit. The client should serialize the correct invocation
                 * parameters
                 */
                throw new Exception(
                        "tryScalarMakeCompatible: Unable to match parameter array:"
                        + sSubCls.getName() + " to provided " + pSubCls.getName());
            }
        }

        /*
         * inline tryScalarMakeCompatible so we can save on reflection
         */
        final Class<?> slot = paramTypes[paramTypeIndex];
        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == byte.class) && (pclass == Byte.class)) return param;
        if ((slot == double.class) && (pclass == Double.class)) return param;
        if ((slot == String.class) && (pclass == String.class)) return param;
        if (slot == TimestampType.class) {
            if (pclass == Long.class) return new TimestampType((Long)param);
            if (pclass == TimestampType.class) return param;
        }
        if (slot == BigDecimal.class) {
            if (pclass == Long.class) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
        }
        if (slot == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }
        throw new Exception(
                "tryToMakeCompatible: Unable to match parameters:"
                + slot.getName() + " to provided " + pclass.getName());
    }

    /**
     * Thrown from a stored procedure to indicate to VoltDB
     * that the procedure should be aborted and rolled back.
     */
    public static class VoltAbortException extends RuntimeException {
        private static final long serialVersionUID = -1L;
        private String message = "No message specified.";

        /**
         * Constructs a new <code>AbortException</code>
         */
        public VoltAbortException() {}

        /**
         * Constructs a new <code>AbortException</code> with the specified detail message.
         */
        public VoltAbortException(String msg) {
            message = msg;
        }
        /**
         * Returns the detail message string of this <tt>AbortException</tt>
         *
         * @return The detail message.
         */
        @Override
        public String getMessage() {
            return message;
        }
    }

    
    /**
     * Currently unsupported in VoltDB.
     * Batch load method for populating a table with a large number of records.
     *
     * Faster then calling {@link #voltQueueSQL(SQLStmt, Object...)} and {@link #voltExecuteSQL()} to
     * insert one row at a time.
     * @param clusterName Name of the cluster containing the database, containing the table
     *                    that the records will be loaded in.
     * @param databaseName Name of the database containing the table to be loaded.
     * @param tableName Name of the table records should be loaded in.
     * @param data {@link org.voltdb.VoltTable VoltTable} containing the records to be loaded.
     *             {@link org.voltdb.VoltTable.ColumnInfo VoltTable.ColumnInfo} schema must match the schema of the table being
     *             loaded.
     * @throws VoltAbortException
     */
    public void voltLoadTable(String clusterName, String databaseName,
                              String tableName, VoltTable data, int allowELT) throws VoltAbortException {
        if (data == null || data.getRowCount() == 0) return;
        assert(m_currentTxnState != null);
        voltLoadTable(m_currentTxnState.getTransactionId(), clusterName, databaseName, tableName, data, allowELT);
    }
    
    public void voltLoadTable(long txn_id, String clusterName, String databaseName,
                              String tableName, VoltTable data, int allowELT) throws VoltAbortException {
        if (data == null || data.getRowCount() == 0) return;
        try {
            assert(executor != null);
            executor.loadTable(txn_id, clusterName, databaseName, tableName, data, allowELT);
        } catch (EEException e) {
            throw new VoltAbortException("Failed to load table: " + tableName);
        }
    }

    /**
     * Get the time that this procedure was accepted into the VoltDB cluster. This is the
     * effective, but not always actual, moment in time this procedure executes. Use this
     * method to get the current time instead of non-deterministic methods. Note that the
     * value will not be unique across transactions as it is only millisecond granularity.
     *
     * @return A java.util.Date instance with deterministic time for all replicas using
     * UTC (Universal Coordinated Time is like GMT).
     */
    public Date getTransactionTime() {
        long ts = TransactionIdManager.getTimestampFromTransactionId(m_currentTxnState.getTransactionId());
        return new Date(ts);
    }

    /**
     * Queue the SQL {@link org.voltdb.SQLStmt statement} for execution with the specified argument list.
     *
     * @param stmt {@link org.voltdb.SQLStmt Statement} to queue for execution.
     * @param args List of arguments to be bound as parameters for the {@link org.voltdb.SQLStmt statement}
     * @see <a href="#allowable_params">List of allowable parameter types</a>
     */
    public void voltQueueSQL(final SQLStmt stmt, Object... args) {
        if (!isNative) {
            //HSQLProcedureWrapper does nothing smart. it just implements this interface with runStatement()
            VoltTable table = hsql.runSQLWithSubstitutions(stmt, args);
            queryResults.add(table);
            return;
        }

        if (batchQueryStmtIndex == batchQueryStmts.length) {
            throw new RuntimeException("Procedure attempted to queue more than " + batchQueryStmts.length +
                    " statements in a batch.");
        } else {
            batchQueryStmts[batchQueryStmtIndex++] = stmt;
            batchQueryArgs[batchQueryArgsIndex++] = args;
        }
        if (t) LOG.trace("Batching Statement: " + stmt.getText());
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables.
     *
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL() {
        return voltExecuteSQL(false);
    }

    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables. Boolean option allows caller to indicate if this is the final
     * batch for a procedure. If it's final, then additional optimizatons can be enabled.
     *
     * @param isFinalSQL Is this the final batch for a procedure?
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL(boolean isFinalSQL) {
        if (!isNative) {
            VoltTable[] batch_results = queryResults.toArray(new VoltTable[queryResults.size()]);
            queryResults.clear();
            return batch_results;
        }
        
        assert (batchQueryStmtIndex == batchQueryArgsIndex);

        // Workload Trace - Start Query
        if (this.enable_tracing && m_workloadXactHandle != null) {
            m_workloadBatchId = ProcedureProfiler.workloadTrace.getNextBatchId(m_workloadXactHandle);
            for (int i = 0; i < batchQueryStmtIndex; i++) {
                Object queryHandle = ProcedureProfiler.workloadTrace.startQuery(m_workloadXactHandle,
                                                                                batchQueryStmts[i].catStmt,
                                                                                batchQueryArgs[i],
                                                                                m_workloadBatchId);
                assert(queryHandle != null);
                m_workloadQueryHandles.add(queryHandle);
            }
        }

        // Execute the queries and return the VoltTable results
        VoltTable[] retval = this.executeQueriesInABatch(batchQueryStmtIndex, batchQueryStmts, batchQueryArgs, isFinalSQL);

        // Workload Trace - Stop Query
        if (this.enable_tracing && m_workloadXactHandle != null) {
            for (Object handle : m_workloadQueryHandles) {
                if (handle != null) ProcedureProfiler.workloadTrace.stopQuery(handle);
            }
            // Make sure that we clear out our query handles so that the next
            // time they queue a query they will get a new batch id
            m_workloadQueryHandles.clear();
        }

        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;
        
        return retval;
    }

    /**
     * 
     * @param batchSize
     * @param batchStmts
     * @param batchArgs
     * @param finalTask
     * @return
     */
    private VoltTable[] executeQueriesInABatch(final int batchSize, SQLStmt[] batchStmts, Object[][] batchArgs, boolean finalTask) {
        assert(batchStmts != null);
        assert(batchArgs != null);
        assert(batchStmts.length > 0);
        assert(batchArgs.length > 0);
        if (batchSize == 0) return (EMPTY_RESULT);
        
        if (hstore_conf.site.txn_profiling) ProfileMeasurement.swap(this.m_localTxnState.java_time, this.m_localTxnState.plan_time);


        /*if (lastBatchNeedsRollback) {
            lastBatchNeedsRollback = false;
            m_site.ee.undoUndoToken(m_site.undoWindowEnd);
        }*/

        // Create a list of clean parameters
        final ParameterSet params[] = new ParameterSet[batchSize];
        for (int i = 0; i < batchSize; i++) {
            params[i] = getCleanParams(batchStmts[i], batchArgs[i]);
        } // FOR
        
        // Calculate the hash code for this batch to see whether we already have a planner
        final Integer batchHashCode = VoltProcedure.getBatchHashCode(batchStmts, batchSize);
        this.planner = ExecutionSite.POOL_BATCH_PLANNERS.get(batchHashCode);
        if (this.planner == null) { // Assume fast case
            synchronized (ExecutionSite.POOL_BATCH_PLANNERS) {
                this.planner = ExecutionSite.POOL_BATCH_PLANNERS.get(batchHashCode);
                if (this.planner == null) {
                    this.planner = new BatchPlanner(batchStmts, batchSize, this.catProc, this.p_estimator);
                    ExecutionSite.POOL_BATCH_PLANNERS.put(batchHashCode, planner);
                }
            } // SYNCH
        }
        assert(this.planner != null);

        // At this point we have to calculate exactly what we need to do on each partition
        // for this batch. So somehow right now we need to fire this off to either our
        // local executor or to Evan's magical distributed transaction manager
        this.plan = this.planner.plan(this.txn_id, this.client_handle, this.base_partition, params, this.predict_singlepartition);
        assert(this.plan != null);
        if (d) LOG.debug("BatchPlan for txn #" + this.txn_id + ":\n" + plan.toString());
        
        // Tell the TransactionEstimator that we're about to execute these mofos
        if (hstore_conf.site.txn_profiling) ProfileMeasurement.swap(this.m_localTxnState.plan_time, this.m_localTxnState.est_time);
        TransactionEstimator.State t_state = this.m_localTxnState.getEstimatorState();
        if (t_state != null) {
            this.t_estimator.executeQueries(t_state, this.planner.getStatements(), this.plan.getStatementPartitions());
        }
        if (hstore_conf.site.txn_profiling) this.m_localTxnState.est_time.stop();

        // Check whether our plan was caused a mispredict
        // Doing it this way allows us to update the TransactionEstimator before we abort the txn
        if (this.plan.getMisprediction() != null) {
            MispredictionException ex = this.plan.getMisprediction(); 

            State s = this.m_localTxnState.getEstimatorState();
            MarkovGraph markov = null;
            if (s != null && hstore_conf.site.markov_mispredict_recompute) {
                if (d) LOG.debug("Recomputing MarkovGraph probabilities because " + m_localTxnState + " mispredicted");
                markov = s.getMarkovGraph();
                markov.recalculateProbabilities();
                if (d && markov.isValid() == false) {
                    LOG.error("Invalid MarkovGraph after recomputing! Crashing...");
                    hstore_conf.site.exec_mispredict_crash = true;
                }
            }
            
            if (d || hstore_conf.site.exec_mispredict_crash) {
                StringBuilder sb = new StringBuilder();
                sb.append("Caught " + ex.getClass().getSimpleName() + "!\n")
                  .append(StringUtil.SINGLE_LINE);
                
                sb.append("CURRENT BATCH\n");
                for (int i = 0; i < batchSize; i++) {
                    sb.append(String.format("[%02d] %s <==> %s\n     %s\n     %s\n",
                                            i,
                                            batchStmts[i].catStmt.fullName(),
                                            planner.catalog_stmts[i].fullName(),
                                            batchStmts[i].catStmt.getSqltext(),
                                            Arrays.toString(params[i].toArray())));
                }
                
                sb.append(String.format("\nTXN #%d PARAMS:\n%s", this.txn_id, sb.toString()));
                ParameterMangler pm = new ParameterMangler(catProc);
                Object mangled[] = pm.convert(this.procParams); 
                for (int i = 0; i < mangled.length; i++) {
                    sb.append(String.format("  [%02d] ", i));
                    if (this.paramTypeIsArray[i]) {
                        sb.append(Arrays.toString((Object[])mangled[i]));
                    } else {
                        sb.append(mangled[i]);
                    }
                    sb.append("\n");
                } // FOR
                
                sb.append("\nTRANSACTION STATE\n").append(this.m_localTxnState.debug());
                
                sb.append("\nESTIMATOR STATE:\n");
                if (s != null) {
                    try {
                        GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(markov, true, markov.getPath(s.getEstimatedPath()));
                        gv.highlightPath(markov.getPath(s.getActualPath()), "blue");
                        
                        LOG.info("PARTITION: " + this.executor.partitionId);
                        LOG.info("GRAPH: " + gv.writeToTempFile(procedure_name));
                    } catch (Exception ex2) {
                        LOG.fatal("???????????????????????", ex2);
                    }
                } else {
                    sb.append("No TransactionEstimator.State! Can't dump out MarkovGraph!\n");
                }
                
                sb.append("\nPLANNER\n");
                for (int i = 0; i < batchSize; i++) {
                    Statement stmt0 = planner.catalog_stmts[i];
                    Statement stmt1 = batchStmts[i].catStmt;
                    assert(stmt0.fullName().equals(stmt1.fullName())) : stmt0.fullName() + " != " + stmt1.fullName(); 
                    sb.append(String.format("[%02d] %s\n     %s\n", i, stmt0.fullName(), stmt1.fullName()));
                } // FOR

                LOG.warn("\n" + sb.toString());
            }
            
            // Crash on Misprediction!
            if (hstore_conf.site.exec_mispredict_crash) {
                LOG.fatal(String.format("Crashing because site.exec_mispredict_crash is true [txn=%s]", this.m_localTxnState));
                this.executor.crash(ex);
            }
            throw ex;
        }

        VoltTable results[] = null;
        if (this.plan.isSingledPartitionedAndLocal()) {
            if  (d) LOG.debug("Executing BatchPlan directly with ExecutionSite");
            results = this.executor.executeLocalPlan(m_localTxnState, this.plan);
            
        } else {
            List<FragmentTaskMessage> tasks = this.plan.getFragmentTaskMessages();
            if (t) LOG.trace("Got back a set of tasks for " + tasks.size() + " partitions for txn #" + this.txn_id);
    
            // Block until we get all of our responses.
            // We can do this because our ExecutionSite is multi-threaded
            results = this.executor.waitForResponses(this.m_localTxnState, tasks, plan.getBatchSize());
        }
        assert(results != null) : "Got back a null results array for txn #" + this.txn_id + "\n" + plan.toString();

        // Tell our TransactionState that we are done with this BatchPlan
        // It will dispose of it when the transaction commits/aborts
        this.m_currentTxnState.addFinishedBatchPlan(this.plan);
        
        if (hstore_conf.site.txn_profiling) this.m_localTxnState.java_time.start();
        
        return (results);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY CRAP
    // ----------------------------------------------------------------------------

    final private Object tryScalarMakeCompatible(Class<?> slot, Object param) throws Exception {
        Class<?> pclass = param.getClass();
        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
        if ((slot == byte.class) && (pclass == Byte.class)) return param;
        if ((slot == double.class) && (pclass == Double.class)) return param;
        if ((slot == String.class) && (pclass == String.class)) return param;
        if (slot == Date.class) {
            if (pclass == Long.class) return new Date((Long)param);
            if (pclass == Date.class) return param;
        }
        if (slot == BigDecimal.class) {
            if (pclass == Long.class) {
                BigInteger bi = new BigInteger(param.toString());
                BigDecimal bd = new BigDecimal(bi);
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
            if (pclass == BigDecimal.class) {
                BigDecimal bd = (BigDecimal) param;
                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
                return bd;
            }
        }
        if (slot == VoltTable.class && pclass == VoltTable.class) {
            return param;
        }
        throw new Exception("tryScalarMakeCompatible: Unable to matoh parameters:" + slot.getName());
    }

    public static ParameterSet getCleanParams(SQLStmt stmt, Object[] args) {
        final int numParamTypes = stmt.numStatementParamJavaTypes;
        final byte stmtParamTypes[] = stmt.statementParamJavaTypes;
        if (args.length != numParamTypes) {
            throw new ExpectedProcedureException(
                    "Number of arguments provided was " + args.length  +
                    " where " + numParamTypes + " was expected for statement " + stmt.getText());
        }
        for (int ii = 0; ii < numParamTypes; ii++) {
            // this only handles null values
            if (args[ii] != null) continue;
            VoltType type = VoltType.get(stmtParamTypes[ii]);
            if (type == VoltType.TINYINT)
                args[ii] = Byte.MIN_VALUE;
            else if (type == VoltType.SMALLINT)
                args[ii] = Short.MIN_VALUE;
            else if (type == VoltType.INTEGER)
                args[ii] = Integer.MIN_VALUE;
            else if (type == VoltType.BIGINT)
                args[ii] = Long.MIN_VALUE;
            else if (type == VoltType.FLOAT)
                args[ii] = DOUBLE_NULL;
            else if (type == VoltType.TIMESTAMP)
                args[ii] = new TimestampType(Long.MIN_VALUE);
            else if (type == VoltType.STRING)
                args[ii] = VoltType.NULL_STRING;
            else if (type == VoltType.DECIMAL)
                args[ii] = VoltType.NULL_DECIMAL;
            else
                throw new ExpectedProcedureException("Unknown type " + type +
                 " can not be converted to NULL representation for arg " + ii + " for SQL stmt " + stmt.getText());
        }

        final ParameterSet params = new ParameterSet(true);
        params.setParameters(args);
        return params;
    }

    /**
     * Derivation of StatsSource to expose timing information of procedure invocations.
     *
     */
    private final class ProcedureStatsCollector extends SiteStatsSource {

        /**
         * Record procedure execution time ever N invocations
         */
        final int timeCollectionInterval = 20;

        /**
         * Number of times this procedure has been invoked.
         */
        private long m_invocations = 0;
        private long m_lastInvocations = 0;

        /**
         * Number of timed invocations
         */
        private long m_timedInvocations = 0;
        private long m_lastTimedInvocations = 0;

        /**
         * Total amount of timed execution time
         */
        private long m_totalTimedExecutionTime = 0;
        private long m_lastTotalTimedExecutionTime = 0;

        /**
         * Shortest amount of time this procedure has executed in
         */
        private long m_minExecutionTime = Long.MAX_VALUE;
        private long m_lastMinExecutionTime = Long.MAX_VALUE;

        /**
         * Longest amount of time this procedure has executed in
         */
        private long m_maxExecutionTime = Long.MIN_VALUE;
        private long m_lastMaxExecutionTime = Long.MIN_VALUE;

        /**
         * Time the procedure was last started
         */
        private long m_currentStartTime = -1;

        /**
         * Count of the number of aborts (user initiated or DB initiated)
         */
        private long m_abortCount = 0;
        private long m_lastAbortCount = 0;

        /**
         * Count of the number of errors that occured during procedure execution
         */
        private long m_failureCount = 0;
        private long m_lastFailureCount = 0;

        /**
         * Whether to return results in intervals since polling or since the beginning
         */
        private boolean m_interval = false;
        /**
         * Constructor requires no args because it has access to the enclosing classes members.
         */
        public ProcedureStatsCollector() {
            super("XXX", 1);
//            super(m_site.getCorrespondingSiteId() + " " + catProc.getClassname(),
//                  m_site.getCorrespondingSiteId());
        }

        /**
         * Called when a procedure begins executing. Caches the time the procedure starts.
         */
        public final void beginProcedure() {
            if (m_invocations % timeCollectionInterval == 0) {
                m_currentStartTime = System.nanoTime();
            }
        }

        /**
         * Called after a procedure is finished executing. Compares the start and end time and calculates
         * the statistics.
         */
        public final void endProcedure(boolean aborted, boolean failed) {
            if (m_currentStartTime > 0) {
                final long endTime = System.nanoTime();
                final int delta = (int)(endTime - m_currentStartTime);
                m_totalTimedExecutionTime += delta;
                m_timedInvocations++;
                m_minExecutionTime = Math.min( delta, m_minExecutionTime);
                m_maxExecutionTime = Math.max( delta, m_maxExecutionTime);
                m_lastMinExecutionTime = Math.min( delta, m_lastMinExecutionTime);
                m_lastMaxExecutionTime = Math.max( delta, m_lastMaxExecutionTime);
                m_currentStartTime = -1;
            }
            if (aborted) {
                m_abortCount++;
            }
            if (failed) {
                m_failureCount++;
            }
            m_invocations++;
        }

        /**
         * Update the rowValues array with the latest statistical information.
         * This method is overrides the super class version
         * which must also be called so that it can update its columns.
         * @param values Values of each column of the row of stats. Used as output.
         */
        @Override
        protected void updateStatsRow(Object rowKey, Object rowValues[]) {
            super.updateStatsRow(rowKey, rowValues);
            rowValues[columnNameToIndex.get("PARTITION_ID")] = executor.getPartitionId();
            rowValues[columnNameToIndex.get("PROCEDURE")] = catProc.getClassname();
            long invocations = m_invocations;
            long totalTimedExecutionTime = m_totalTimedExecutionTime;
            long timedInvocations = m_timedInvocations;
            long minExecutionTime = m_minExecutionTime;
            long maxExecutionTime = m_maxExecutionTime;
            long abortCount = m_abortCount;
            long failureCount = m_failureCount;

            if (m_interval) {
                invocations = m_invocations - m_lastInvocations;
                m_lastInvocations = m_invocations;

                totalTimedExecutionTime = m_totalTimedExecutionTime - m_lastTotalTimedExecutionTime;
                m_lastTotalTimedExecutionTime = m_totalTimedExecutionTime;

                timedInvocations = m_timedInvocations - m_lastTimedInvocations;
                m_lastTimedInvocations = m_timedInvocations;

                abortCount = m_abortCount - m_lastAbortCount;
                m_lastAbortCount = m_abortCount;

                failureCount = m_failureCount - m_lastFailureCount;
                m_lastFailureCount = m_failureCount;

                minExecutionTime = m_lastMinExecutionTime;
                maxExecutionTime = m_lastMaxExecutionTime;
                m_lastMinExecutionTime = Long.MAX_VALUE;
                m_lastMaxExecutionTime = Long.MIN_VALUE;
            }

            rowValues[columnNameToIndex.get("INVOCATIONS")] = invocations;
            rowValues[columnNameToIndex.get("TIMED_INVOCATIONS")] = timedInvocations;
            rowValues[columnNameToIndex.get("MIN_EXECUTION_TIME")] = minExecutionTime;
            rowValues[columnNameToIndex.get("MAX_EXECUTION_TIME")] = maxExecutionTime;
            if (timedInvocations != 0) {
                rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] =
                     (totalTimedExecutionTime / timedInvocations);
            } else {
                rowValues[columnNameToIndex.get("AVG_EXECUTION_TIME")] = 0L;
            }
            rowValues[columnNameToIndex.get("ABORTS")] = abortCount;
            rowValues[columnNameToIndex.get("FAILURES")] = failureCount;
        }

        /**
         * Specifies the columns of statistics that are added by this class to the schema of a statistical results.
         * @param columns List of columns that are in a stats row.
         */
        @Override
        protected void populateColumnSchema(ArrayList<VoltTable.ColumnInfo> columns) {
            super.populateColumnSchema(columns);
            columns.add(new VoltTable.ColumnInfo("PARTITION_ID", VoltType.INTEGER));
            columns.add(new VoltTable.ColumnInfo("PROCEDURE", VoltType.STRING));
            columns.add(new VoltTable.ColumnInfo("INVOCATIONS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("TIMED_INVOCATIONS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("MIN_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("MAX_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("AVG_EXECUTION_TIME", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("ABORTS", VoltType.BIGINT));
            columns.add(new VoltTable.ColumnInfo("FAILURES", VoltType.BIGINT));
        }

        @Override
        protected Iterator<Object> getStatsRowKeyIterator(boolean interval) {
            m_interval = interval;
            return new Iterator<Object>() {
                boolean givenNext = false;
                @Override
                public boolean hasNext() {
                    if (!m_interval) {
                        if (m_invocations == 0) {
                            return false;
                        }
                    } else if (m_invocations - m_lastInvocations == 0){
                        return false;
                    }
                    return !givenNext;
                }

                @Override
                public Object next() {
                    if (!givenNext) {
                        givenNext = true;
                        return new Object();
                    }
                    return null;
                }

                @Override
                public void remove() {}

            };
        }

        @Override
        public String toString() {
            return catProc.getTypeName();
        }
    }

    /**
     * Set the status code that will be returned to the client. This is not the same as the status
     * code returned by the server. If a procedure sets the status code and then rolls back or causes an error
     * the status code will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     * @param statusCode
     */
    public void setAppStatusCode(byte statusCode) {
        m_statusCode = statusCode;
    }

    public void setAppStatusString(String statusString) {
        m_statusString = statusString;
    }

    /**
     *
     * @param e
     * @return A ClientResponse containing error information
     */
    private ClientResponseImpl getErrorResponse(Throwable e) {
        StackTraceElement[] stack = e.getStackTrace();
        ArrayList<StackTraceElement> matches = new ArrayList<StackTraceElement>();
        for (StackTraceElement ste : stack) {
            if (ste.getClassName().equals(getClass().getName()))
                matches.add(ste);
        }

        byte status = ClientResponseImpl.UNEXPECTED_FAILURE;
        StringBuilder msg = new StringBuilder();

        if (e.getClass() == VoltAbortException.class) {
            status = ClientResponseImpl.USER_ABORT;
            msg.append("USER ABORT\n");
        }
        else if (e.getClass() == org.voltdb.exceptions.ConstraintFailureException.class) {
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            msg.append("CONSTRAINT VIOLATION\n");
        }
        else if (e.getClass() == org.voltdb.exceptions.SQLException.class) {
            status = ClientResponseImpl.GRACEFUL_FAILURE;
            msg.append("SQL ERROR\n");
        }
        else if (e.getClass() == org.voltdb.ExpectedProcedureException.class) {
            msg.append("HSQL-BACKEND ERROR\n");
            if (e.getCause() != null)
                e = e.getCause();
        }
        else {
            msg.append("UNEXPECTED FAILURE:\n");
        }

        String exMsg = e.getMessage();
        if (exMsg == null)
            if (e.getClass() == NullPointerException.class) {
                exMsg = "Null Pointer Exception";
            }
            else {
                exMsg = "Possible Null Pointer Exception (";
                exMsg += e.getClass().getSimpleName() + ")";
                e.printStackTrace();
            }

        msg.append("  ").append(exMsg);

        for (StackTraceElement ste : matches) {
            msg.append("\n    at ");
            msg.append(ste.getClassName()).append(".").append(ste.getMethodName());
            msg.append("(").append(ste.getFileName()).append(":");
            msg.append(ste.getLineNumber()).append(")");
        }

        return getErrorResponse(
                status, msg.toString(),
                e instanceof SerializableException ? (SerializableException)e : null);
    }

    private ClientResponseImpl getErrorResponse(byte status, String msg, SerializableException e) {

        StringBuilder msgOut = new StringBuilder();
        msgOut.append("\n===============================================================================\n");
        msgOut.append("VOLTDB ERROR: ");
        msgOut.append(msg);
        msgOut.append("\n===============================================================================\n");

        LOG.trace(msgOut);

        return new ClientResponseImpl(
                this.getTransactionId(),
                status,
                m_statusCode,
                m_statusString,
                new VoltTable[0],
                msgOut.toString(), e);
    }
}
