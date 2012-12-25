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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.ConstraintFailureException;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.EvictedTupleAccessException;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.types.TimestampType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.Poolable;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionEstimator;

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
    
    // ----------------------------------------------------------------------------
    // STATIC CONSTANTS
    // ----------------------------------------------------------------------------
    
    // Used to get around the "abstract" for StmtProcedures.
    // Path of least resistance?
    public static class StmtProcedure extends VoltProcedure { }

    /**
     * VoltTable Schema used for scalar return values
     */
    private static final VoltTable.ColumnInfo SCALAR_RESULT_SCHEMA[] = { 
        new VoltTable.ColumnInfo("", VoltType.BIGINT)
    };
    
    // ----------------------------------------------------------------------------
    // GLOBAL MEMBERS
    // ----------------------------------------------------------------------------
    
    protected HsqlBackend hsql;

    // package scoped members used by VoltSystemProcedure
    protected PartitionExecutor executor;
     
    private boolean m_initialized;

    // private members reserved exclusively to VoltProcedure
    private Method procMethod;
    private boolean procMethodNoJava = false;
    private boolean procIsMapReduce = false;
    private Class<?>[] paramTypes;
    private boolean paramTypeIsPrimitive[];
    private boolean paramTypeIsArray[];
    private Class<?> paramTypeComponentType[];
    private int paramTypesLength;
    private boolean isNative = true;
    protected Object procParams[];
    protected final Map<String, SQLStmt> stmts = new HashMap<String, SQLStmt>();

    // cached fake SQLStmt array for single statement non-java procs
    private SQLStmt[] m_cachedSingleStmt = { null };

    // Used to figure out what partitions a query needs to go to
    private Procedure catalog_proc;
    private String procedure_name;
    protected PartitionEstimator p_estimator;
    protected HStoreSite hstore_site;
    protected HStoreConf hstore_conf;
    
    /** The local partition id where this VoltProcedure is running */
    protected int partitionId = -1;

    /** Callback for when the VoltProcedure finishes and we need to send a ClientResponse somewhere **/
    private EventObservable<ClientResponse> observable = null;

    // data from hsql wrapper
    private final ArrayList<VoltTable> queryResults = new ArrayList<VoltTable>();

    // cached txnid-seeded RNG so all calls to getSeededRandomNumberGenerator() for
    // a given call don't re-seed and generate the same number over and over
    private Random m_cachedRNG = null;
    
    // ----------------------------------------------------------------------------
    // WORKLOAD TRACE HANDLES
    // ----------------------------------------------------------------------------
    
    /**
     * Whether to enable dumping out the transactions/queries executed by this procedure
     * These traces are not used for recovery and are slow.
     */
    private boolean workloadTraceEnable = false;
    private Object workloadTxnHandle = null;
    private List<Object> workloadQueryHandles;

    // ----------------------------------------------------------------------------
    // INVOCATION MEMBERS
    // ----------------------------------------------------------------------------
    
    protected AbstractTransaction m_currentTxnState;  // assigned in call()
    protected LocalTransaction m_localTxnState;  // assigned in call()
    private int batchId = 0;
    private SQLStmt batchQueryStmts[];
    private int batchQueryStmtIndex = 0;
    private int last_batchQueryStmtIndex = 0;
    private Object[] batchQueryArgs[];
    private int batchQueryArgsIndex = 0;
    private VoltTable[] results = HStoreConstants.EMPTY_RESULT;
    private Status status = Status.OK;
    private SerializableException error = null;
    private String status_msg = "";

    /**
     * Status code that can be set by stored procedure upon invocation that will be returned with the response.
     */
    private byte m_statusCode = Byte.MIN_VALUE;
    private String m_statusString = null;
    
    /**
     * End users should not instantiate VoltProcedure instances.
     * Constructor does nothing. All actual initialization is done in the
     * {@link VoltProcedure init} method.
     */
    public VoltProcedure() {
    }

    /**
     * Allow VoltProcedures access to their transaction id.
     * @return transaction id
     */
    public Long getTransactionId() {
        return m_currentTxnState.getTransactionId();
    }

    /**
     * Allow sysprocs to update m_currentTxnState manually. User procedures are
     * passed this state in call(); sysprocs have other entry points on
     * non-coordinator sites.
     */
    public void setTransactionState(AbstractTransaction txnState) {
        m_currentTxnState = txnState;
    }

    /**
     * Get the Transaction state handle for the current invocation
     * <B>NOTE:</B> This should only be used for debugging/testing.
     * @return
     */
    protected final AbstractTransaction getTransactionState() {
        return m_currentTxnState;
    }
    
    /**
     * Main initialization method
     * @param executor
     * @param catalog_proc
     * @param eeType
     * @param hsql
     * @param p_estimator
     */
    public void globalInit(PartitionExecutor executor,
                            Procedure catalog_proc,
                            BackendTarget eeType,
                            HsqlBackend hsql,
                            PartitionEstimator p_estimator) {
        if (m_initialized) {
            throw new IllegalStateException("VoltProcedure has already been initialized");
        } else {
            m_initialized = true;
        }
        assert(executor != null);
        
        this.executor = executor;
        this.hstore_site = executor.getHStoreSite();
        this.hstore_conf = HStoreConf.singleton();
        this.catalog_proc = catalog_proc;
        this.procedure_name = this.catalog_proc.getName();
        this.isNative = (eeType != BackendTarget.HSQLDB_BACKEND);
        this.hsql = hsql;
        this.partitionId = this.executor.getPartitionId();
        assert(this.partitionId != -1);
        
        this.batchQueryArgs = new Object[hstore_conf.site.planner_max_batch_size][];
        this.batchQueryStmts = new SQLStmt[hstore_conf.site.planner_max_batch_size];
        
        // Enable Workload Tracing
        this.workloadTraceEnable = (ProcedureProfiler.profilingLevel == ProcedureProfiler.Level.INTRUSIVE) &&
                              (ProcedureProfiler.workloadTrace != null);
        if (this.workloadTraceEnable) {
            this.workloadQueryHandles = new ArrayList<Object>();
        }
        
        this.p_estimator = p_estimator;
        
        if (catalog_proc.getHasjava()) {
            int tempParamTypesLength = 0;
            Method tempProcMethod = null;
            Method[] methods = getClass().getMethods();
            Class<?> tempParamTypes[] = null;
            boolean tempParamTypeIsPrimitive[] = null;
            boolean tempParamTypeIsArray[] = null;
            Class<?> tempParamTypeComponentType[] = null;
            
            this.procIsMapReduce = catalog_proc.getMapreduce();
            boolean hasMap = false;
            boolean hasReduce = false;
            
            for (final Method m : methods) {
                String name = m.getName();
                // TODO: Change procMethod to point to VoltMapReduceProcedure.runMap() if this is
                //       a MR stored procedure
                if (name.equals("run")) {
                    //inspect(m);
                    tempProcMethod = m;
                    
                    // We can only do this if it's not a MapReduce procedure
                    if (procIsMapReduce == false) {
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
                    // Otherwise everything must come from the catalog
                    else {
                        CatalogMap<ProcParameter> params = catalog_proc.getParameters();
                        tempParamTypesLength = params.size();
                        tempParamTypes = new Class<?>[tempParamTypesLength];
                        tempParamTypeIsPrimitive = new boolean[tempParamTypesLength];
                        tempParamTypeIsArray = new boolean[tempParamTypesLength];
                        tempParamTypeComponentType = new Class<?>[tempParamTypesLength];

                        for (int i = 0; i < tempParamTypesLength; i++) {
                            ProcParameter catalog_param = params.get(i);
                            VoltType vtype = VoltType.get(catalog_param.getType());
                            assert(vtype != null);
                            tempParamTypes[i] = vtype.classFromType(); 
                        } // FOR
                        
                        // We'll try to cast everything as a primitive
                        Arrays.fill(tempParamTypeIsPrimitive, true);
                        
                        // At this point we don't support arrays as inputs to Statements
                        Arrays.fill(tempParamTypeIsArray, false);
                    }
                } else if(name.equals("map")){
                    hasMap = true;
                } else if (name.equals("reduce")) {
                    hasReduce = true;
                } 
            }
            if (procIsMapReduce) {
                if (hasMap == false) {
                    throw new RuntimeException(String.format("%s Map/Reduce is missing MAP function"));
                } else if (hasReduce == false) {
                    throw new RuntimeException(String.format("%s Map/Reduce is missing REDUCE function"));
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
                            initSQLStmt(stmt);
                            this.stmts.put(name, stmt);
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
            Statement catStmt = catalog_proc.getStatements().get(HStoreConstants.ANON_STMT_NAME);
            SQLStmt stmt = new SQLStmt(catStmt.getSqltext());
            stmt.catStmt = catStmt;
            initSQLStmt(stmt);
            m_cachedSingleStmt[0] = stmt;
            procMethodNoJava = true;

            String executeNoJavaProcedure = "executeNoJavaProcedure";
            for (Method m : VoltProcedure.class.getDeclaredMethods()) {
                if (m.getName().equals(executeNoJavaProcedure)) {
                    procMethod = m;
                    break;
                }
            } // FOR
            if (procMethod == null) {
                throw new RuntimeException(String.format("The method %s.%s does not exist",
                                           VoltProcedure.class.getSimpleName(), executeNoJavaProcedure));
            }

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
        if (trace.val) LOG.trace(String.format("Initialized VoltProcedure for %s [partition=%d]", this.procedure_name, this.partitionId));
    }
    
    protected SQLStmt getSQLStmt(String name) {
        return (this.stmts.get(name));
    }
    
    final void initSQLStmt(SQLStmt stmt) {
        stmt.numFragGUIDs = stmt.catStmt.getFragments().size();
        PlanFragment fragments[] = stmt.catStmt.getFragments().values();
        stmt.fragGUIDs = new long[stmt.numFragGUIDs];
        for (int ii = 0; ii < stmt.numFragGUIDs; ii++) {
            stmt.fragGUIDs[ii] = Long.parseLong(fragments[ii].getName());
        } // FOR
    
        stmt.numStatementParamJavaTypes = stmt.catStmt.getParameters().size();
        stmt.statementParamJavaTypes = new byte[stmt.numStatementParamJavaTypes];
        StmtParameter parameters[] = stmt.catStmt.getParameters().values();
        for (int ii = 0; ii < stmt.numStatementParamJavaTypes; ii++) {
            stmt.statementParamJavaTypes[ii] = (byte)parameters[ii].getJavatype();
        } // FOR
        stmt.computeHashCode();
    }
    
    public boolean isInitialized() {
        return (this.m_initialized);
    }
    
    @Override
    public void finish() {
        this.m_currentTxnState = null;
        this.m_localTxnState = null;
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
            m_cachedRNG = new Random(this.m_currentTxnState.getTransactionId());
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
    
    public final String getProcedureName() {
        return (this.procedure_name);
    }
    
    /**
     * Return a hash code that uniquely identifies this list of SQLStmts
     * Derived from AbstractList.hashCode()
     * @param batchStmts
     * @param numBatchStmts
     * @return
     */
    public static Integer getBatchHashCode(SQLStmt[] batchStmts, int numBatchStmts) {
        int hashCode = 1;
        for (int i = 0; i < numBatchStmts; i++) {
            hashCode = 31*hashCode + (batchStmts[i] == null ? 0 : batchStmts[i].hashCode());
        } // FOR
        return Integer.valueOf(hashCode);
    }

    protected void registerCallback(EventObserver<ClientResponse> observer) {
        if (this.observable == null) {
            this.observable = new EventObservable<ClientResponse>();
        }
        this.observable.addObserver(observer);
    }

    protected void unregisterCallback(EventObserver<ClientResponse> observer) {
        this.observable.deleteObserver(observer);
    }
    
    /**
     * The thing that actually executes the VoltProcedure.run() method 
     * @param paramList
     * @return
     */
    public final ClientResponseImpl call(LocalTransaction txnState, Object... paramList) {
        ClientResponseImpl response = null;
        this.m_currentTxnState = txnState;
        this.m_localTxnState = txnState;
        this.procParams = paramList;
        this.results = HStoreConstants.EMPTY_RESULT;
        this.status = Status.OK;
        this.error = null;
        this.status_msg = "";
        this.m_statusCode = Byte.MIN_VALUE;
        this.m_statusString = null;
        
        
        // in case someone queues sql but never calls execute, clear the queue here.
        this.batchId = 0;
        this.batchQueryStmtIndex = 0;
        this.batchQueryArgsIndex = 0;
        this.last_batchQueryStmtIndex = -1;
        
        if (debug.val) LOG.debug("Starting execution of " + this.m_currentTxnState);
        if (this.procParams.length != this.paramTypesLength) {
            String msg = "PROCEDURE " + procedure_name + " EXPECTS " + String.valueOf(paramTypesLength) +
                " PARAMS, BUT RECEIVED " + String.valueOf(this.procParams.length);
            if (debug.val) LOG.error(msg);
            status = Status.ABORT_GRACEFUL;
            status_msg = msg;
            response = new ClientResponseImpl(txnState.getTransactionId(),
                                              txnState.getClientHandle(),
                                              this.partitionId,
                                              status,
                                              results,
                                              status_msg); 
            if (this.observable != null) this.observable.notifyObservers(response);
            return (response); 
        }

        for (int i = 0; i < paramTypesLength; i++) {
            try {
                this.procParams[i] = tryToMakeCompatible(i, this.procParams[i]);
//                if (trace.val) LOG.trace(String.format("[%02d] ORIG:%s -> NEW:%s", i, orig, this.procParams[i].getClass().getSimpleName()));
            } catch (Exception e) {
                String msg = "PROCEDURE " + procedure_name + " TYPE ERROR FOR PARAMETER " + i +
                        ": " + e.getMessage();
                LOG.error(msg, e);
                status = Status.ABORT_GRACEFUL;
                status_msg = msg;
                response = new ClientResponseImpl(txnState.getTransactionId(),
                                                  txnState.getClientHandle(),
                                                  this.partitionId,
                                                  this.status,
                                                  this.results,
                                                  this.status_msg);
                if (this.observable != null) this.observable.notifyObservers(response);
                return (response);
            }
        }

        // Workload Trace
        // Create a new transaction record in the trace manager. This will give us back
        // a handle that we need to pass to the trace manager when we want to register a new query
        if (this.workloadTraceEnable) {
            this.workloadQueryHandles.clear();
            this.workloadTxnHandle = ProcedureProfiler.workloadTrace.startTransaction(
                    this.m_currentTxnState.getTransactionId(), catalog_proc, this.procParams);
        }

        // Fix to make no-Java procedures work
        if (procMethodNoJava || procIsMapReduce) this.procParams = new Object[] { this.procParams } ;
        
        if (hstore_conf.site.txn_profiling && this.m_localTxnState.profiler != null) {
            this.m_localTxnState.profiler.startExecJava();
        }
        try {
            if (trace.val) LOG.trace(String.format("Invoking %s [params=%s, partition=%d]",
                                           this.procMethod,
                                           this.procParams + Arrays.toString(this.procParams),
                                           this.partitionId));
            try {
                // ANTI-CACHE TABLE MERGE
                if (hstore_conf.site.anticache_enable && txnState.hasAntiCacheMergeTable()) {
                    // Note that I decided to put this in here because we already
                    // have the logic down below for handling various errors from the EE
                    Table catalog_tbl = txnState.getAntiCacheMergeTable();
                    executor.getExecutionEngine().antiCacheMergeBlocks(catalog_tbl);
                }
                
                Object rawResult = procMethod.invoke(this, this.procParams);
                this.results = getResultsFromRawResults(rawResult);
                if (this.results == null) results = HStoreConstants.EMPTY_RESULT;
            } catch (IllegalAccessException e) {
                // If reflection fails, invoke the same error handling that other exceptions do
                throw new InvocationTargetException(e);
            } catch (RuntimeException e) {
                throw new InvocationTargetException(e);
            }
            if (debug.val) LOG.debug(this.m_currentTxnState + " is finished on partition " + this.partitionId);
            
        // -------------------------------
        // Exceptions that we can process+handle
        // -------------------------------
        } catch (InvocationTargetException itex) {
            Throwable ex = itex.getCause();
            Class<?> ex_class = ex.getClass();
            
            // Pass the exception back to the client if it is serializable
            if (ex instanceof SerializableException) {
                if (this.m_localTxnState.hasPendingError() == false) {
                    this.m_localTxnState.setPendingError(new SerializableException(ex), false);
                }    
                this.error = (SerializableException)ex;
            }
            
            // -------------------------------
            // VoltAbortException
            // -------------------------------
            if (ex_class.equals(VoltAbortException.class)) {
                if (debug.val) LOG.debug("Caught VoltAbortException for " + this.m_currentTxnState, ex);
                this.status = Status.ABORT_USER;
                this.status_msg = "USER ABORT: " + ex.getMessage();
                
                if (this.workloadTraceEnable && this.workloadTxnHandle != null) {
                    ProcedureProfiler.workloadTrace.abortTransaction(this.workloadTxnHandle);
                }
            // -------------------------------
            // MispredictionException
            // -------------------------------
            } else if (ex_class.equals(MispredictionException.class)) {
                if (debug.val) LOG.warn("Caught MispredictionException for " + this.m_currentTxnState);
                this.status = Status.ABORT_MISPREDICT;
                this.m_localTxnState.getTouchedPartitions().put((((MispredictionException)ex).getPartitions()));
                
            // -------------------------------
            // EvictedTupleAccessException
            // -------------------------------
            } else if (ex_class.equals(EvictedTupleAccessException.class)) {
                if (debug.val) LOG.warn("Caught EvictedTupleAccessException for " + this.m_currentTxnState);
                this.status = Status.ABORT_EVICTEDACCESS;

            // -------------------------------
            // ConstraintFailureException
            // -------------------------------
            } else if (ex_class.equals(ConstraintFailureException.class)) {
                this.status = Status.ABORT_UNEXPECTED;
                this.status_msg = "CONSTRAINT VIOLATION: " + ex.getMessage();
                
            // -------------------------------
            // ServerFaultException
            // -------------------------------
            } else if (ex_class.equals(ServerFaultException.class)) {
                // A server fault means that we definitely did something wrong
                this.status = Status.ABORT_UNEXPECTED;
                this.status_msg = "SERVER FAULT: " + ex.getMessage();
                
            // -------------------------------
            // Everything Else
            // -------------------------------
            } else {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                ex.printStackTrace(pw);
                String msg = sw.toString();
                if (msg == null) msg = ex.toString();
                String statusMsg = msg;
                
                if (batchQueryStmtIndex > 0) {
                    msg += "\nCurrently Queued Queries:\n";
                    for (int i = 0; i < batchQueryStmtIndex; i++) {
                        msg += String.format("  [%02d] %s\n", i, CatalogUtil.getDisplayName(batchQueryStmts[i].catStmt));
                    }
                } else if (last_batchQueryStmtIndex > 0) {
                    msg += "\nLast Executed Queries:\n";
                    for (int i = 0; i < last_batchQueryStmtIndex; i++) {
                        msg += String.format("  [%02d] %s\n", i, CatalogUtil.getDisplayName(batchQueryStmts[i].catStmt));
                    }
                }
                if (debug.val && executor.isShuttingDown() == false) {
                    LOG.warn(String.format("%s Unexpected Abort: %s", this.m_currentTxnState, msg), ex);
                }
                status = Status.ABORT_UNEXPECTED;
                status_msg = "UNEXPECTED ABORT: " + statusMsg;
                
                if (debug.val) LOG.error("Unpexpected error when executing " + this.m_currentTxnState, ex);
            }
        // -------------------------------
        // Something bad happened inside of the procedure that wasn't our fault
        // -------------------------------
        } catch (Throwable ex) {
            if (debug.val) LOG.error("Unpexpected error when executing " + this.m_currentTxnState, ex);
            status = Status.ABORT_UNEXPECTED;
            status_msg = "UNEXPECTED ERROR IN " + this.m_localTxnState;
        } finally {
            this.m_localTxnState.markAsExecuted();
            if (debug.val) LOG.debug(this.m_currentTxnState + " - Finished transaction [" + this.status + "]");

            // Workload Trace - Stop the transaction trace record.
            if (this.workloadTraceEnable && this.workloadTxnHandle != null && this.status == Status.OK) {
                if (hstore_conf.site.trace_txn_output) {
                    ProcedureProfiler.workloadTrace.stopTransaction(this.workloadTxnHandle, this.results);
                } else {
                    ProcedureProfiler.workloadTrace.stopTransaction(this.workloadTxnHandle);
                }
            }
        }

        // This should never happen
        if (this.results == null) {
            throw new RuntimeException("We got back a null result from " + this.m_currentTxnState);
        }
        
//        if(debug.get()) {
//            LOG.debug(String.format("Client_Response, 1:%d,2:%d,3:%d,4:%s,5:%s,6:%s,7:%s", 
//                    this.m_currentTxnState.getTransactionId().longValue(),
//                    this.client_handle,
//                    this.partitionId,
//                    this.status,
//                    this.results,
//                    this.status_msg,
//                    this.error));
//        }
        
        response = new ClientResponseImpl();
        response.init(this.m_localTxnState,
                      this.status,
                      this.m_statusCode,
                      this.m_statusString,
                      this.results,
                      this.status_msg,
                      this.error
        );
        if (hstore_conf.site.txn_client_debug) {
            ClientResponseDebug responseDebug = new ClientResponseDebug(m_localTxnState);
            response.setDebug(responseDebug);
        }

        if (this.observable != null) this.observable.notifyObservers(response);
        if (trace.val) LOG.trace(response);
        return (response);
    }

    
    protected final Procedure getProcedure() {
        return (this.catalog_proc);
    }
    
    protected final VoltTable executeNoJavaProcedure(Object...params) {
        voltQueueSQL(this.m_cachedSingleStmt[0], params);
        VoltTable result[] = voltExecuteSQL(true);
        assert(result.length == 1);
        return (result[0]);
    }
    
    /**
     * Given the results of a procedure, convert it into a sensible array of VoltTables.
     */
    final private VoltTable[] getResultsFromRawResults(Object result) {
        if (result == null)
            return HStoreConstants.EMPTY_RESULT;
        if (result instanceof VoltTable[])
            return (VoltTable[]) result;
        if (result instanceof VoltTable)
            return new VoltTable[] { (VoltTable) result };
        if (result instanceof Long) {
            VoltTable t = new VoltTable(SCALAR_RESULT_SCHEMA);
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

        if (param instanceof PartitionExecutor.SystemProcedureExecutionContext) {
            return param;
        }

        Class<?> pclass = param.getClass();
        boolean slotIsArray = paramTypeIsArray[paramTypeIndex];
        if (slotIsArray != pclass.isArray()) {
            LOG.warn(String.format("Param #%d -> %s [class=%s, isArray=%s, slotIsArray=%s]",
                                   paramTypeIndex, param, pclass.getSimpleName(), pclass.isArray(), slotIsArray));
            throw new Exception("Array / Scalar parameter mismatch");
        }

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
        if ((slot == boolean.class) && (pclass == Boolean.class)) return param;
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
                "tryToMakeCompatible: Unable to match parameters: "
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
        try {
            assert(executor != null);
            executor.loadTable(m_currentTxnState, clusterName, databaseName, tableName, data, allowELT);
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
        if (trace.val) LOG.trace("Batching Statement: " + stmt.getText());
    }

    public final void voltClearQueue() {
        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;
    }
    
    /**
     * Return the number of slots remaining in the current batch
     * for this transaction. When this reaches zero, you will not
     * be able to queue anymore SQLStmts
     * @return
     */
    public final int voltRemainingQueue() {
        return (batchQueryStmts.length - batchQueryStmtIndex);
    }
    
    /**
     * Execute the currently queued SQL {@link org.voltdb.SQLStmt statements} and return
     * the result tables.
     *
     * @return Result {@link org.voltdb.VoltTable tables} generated by executing the queued
     * query {@link org.voltdb.SQLStmt statements}
     */
    public VoltTable[] voltExecuteSQL() {
        return voltExecuteSQL(false, false);
    }

    /**
     * Execute the currently SQL as always single-partition queries 
     * @return
     */
    protected VoltTable[] voltExecuteSQLForceSinglePartition() {
        return voltExecuteSQL(false, true);
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
        return voltExecuteSQL(isFinalSQL, false);
    }
    
    private VoltTable[] voltExecuteSQL(boolean isFinalSQL, boolean forceSinglePartition) {
        if (isNative == false) {
            VoltTable[] batch_results = queryResults.toArray(new VoltTable[queryResults.size()]);
            queryResults.clear();
            return (batch_results);
        }
        assert (batchQueryStmtIndex == batchQueryArgsIndex);
        
        // Workload Trace - Start Query
        if (this.workloadTraceEnable && workloadTxnHandle != null) {
            workloadQueryHandles.clear();
            for (int i = 0; i < batchQueryStmtIndex; i++) {
                Object queryHandle = ProcedureProfiler.workloadTrace.startQuery(workloadTxnHandle,
                                                                                batchQueryStmts[i].catStmt,
                                                                                batchQueryArgs[i],
                                                                                this.batchId);
                
                assert(queryHandle != null);
                
                workloadQueryHandles.add(queryHandle);
            }
        }

        // Execute the queries and return the VoltTable results
        last_batchQueryStmtIndex = batchQueryStmtIndex;

        VoltTable[] retval = null;
        try {
            retval = this.executeQueriesInABatch(batchQueryStmtIndex,
                                                 batchQueryStmts,
                                                 batchQueryArgs,
                                                 isFinalSQL,
                                                 forceSinglePartition);
        // This should just be forwarded along
        } catch (SerializableException ex) {
            throw ex;
        // We know that any error that we get here is because of us and not their user code 
        } catch (Throwable ex) {
            String message = "Unexpected error while executing queries";
            throw new ServerFaultException(message, ex, this.getTransactionId());
        }

        // Workload Trace - Stop Query
        if (this.workloadTraceEnable && workloadTxnHandle != null) {
            for (int i = 0; i < batchQueryStmtIndex; i++) {
                Object handle = workloadQueryHandles.get(i);
                if (handle != null) {
                    if (hstore_conf.site.trace_query_output) {
                        ProcedureProfiler.workloadTrace.stopQuery(handle, retval[i]);
                    } else {
                        ProcedureProfiler.workloadTrace.stopQuery(handle, null);
                    }
                }
            } // FOR
            // Make sure that we clear out our query handles so that the next
            // time they queue a query they will get a new batch id
            workloadQueryHandles.clear();
        }

        batchQueryStmtIndex = 0;
        batchQueryArgsIndex = 0;
        
        return retval;
    }

    /**
     * Return the SQLStmt handles for the last batch of executed queries
     * @return
     */
    public SQLStmt[] voltLastQueriesExecuted() {
        if (last_batchQueryStmtIndex != -1) {
            return Arrays.copyOf(batchQueryStmts, last_batchQueryStmtIndex);
        }
        return new SQLStmt[0];
    }
    
    /**
     * 
     * @param batchSize
     * @param batchStmts
     * @param batchArgs
     * @param finalTask
     * @return
     */
    private VoltTable[] executeQueriesInABatch(final int batchSize,
                                               final SQLStmt[] batchStmts,
                                               final Object[][] batchArgs,
                                               final boolean finalTask,
                                               final boolean forceSinglePartition) {
        assert(batchStmts != null);
        assert(batchArgs != null);
        assert(batchStmts.length > 0);
        assert(batchArgs.length > 0);
        if (batchSize == 0) return (HStoreConstants.EMPTY_RESULT);
        
        // Create a list of clean parameters
        ParameterSet params[] = this.m_localTxnState.getExecutionState().
                                     procParameterSets.getParameterSet(batchSize);
        assert(params != null);
        for (int i = 0; i < batchSize; i++) {
            params[i] = getCleanParams(batchStmts[i], batchArgs[i], params[i]);
        } // FOR
        
        VoltTable results[] = null;
        try {
            results = this.executor.executeSQLStmtBatch(m_localTxnState,
                                                        batchSize,
                                                        batchStmts,
                                                        params,
                                                        finalTask,
                                                        forceSinglePartition);
        } catch (ServerFaultException ex) {
            throw ex;
        } catch (SerializableException ex) {
            throw ex;
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw new ServerFaultException("Unexpected error", ex, m_localTxnState.getTransactionId());
        } finally {
            this.batchId++;
            if (m_localTxnState.hasPendingError()) {
                throw m_localTxnState.getPendingError();
            }
        }
        return (results);
    }
    
    // ----------------------------------------------------------------------------
    // UTILITY CRAP
    // ----------------------------------------------------------------------------

//    final private Object tryScalarMakeCompatible(Class<?> slot, Object param) throws Exception {
//        Class<?> pclass = param.getClass();
//        if ((slot == long.class) && (pclass == Long.class || pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
//        if ((slot == int.class) && (pclass == Integer.class || pclass == Short.class || pclass == Byte.class)) return param;
//        if ((slot == short.class) && (pclass == Short.class || pclass == Byte.class)) return param;
//        if ((slot == byte.class) && (pclass == Byte.class)) return param;
//        if ((slot == double.class) && (pclass == Double.class)) return param;
//        if ((slot == String.class) && (pclass == String.class)) return param;
//        if (slot == Date.class) {
//            if (pclass == Long.class) return new Date((Long)param);
//            if (pclass == Date.class) return param;
//        }
//        if (slot == BigDecimal.class) {
//            if (pclass == Long.class) {
//                BigInteger bi = new BigInteger(param.toString());
//                BigDecimal bd = new BigDecimal(bi);
//                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
//                return bd;
//            }
//            if (pclass == BigDecimal.class) {
//                BigDecimal bd = (BigDecimal) param;
//                bd.setScale(4, BigDecimal.ROUND_HALF_EVEN);
//                return bd;
//            }
//        }
//        if (slot == VoltTable.class && pclass == VoltTable.class) {
//            return param;
//        }
//        throw new Exception("tryScalarMakeCompatible: Unable to matoh parameters:" + slot.getName());
//    }

    public static ParameterSet getCleanParams(SQLStmt stmt, Object[] args) {
        return getCleanParams(stmt, args, new ParameterSet(true));
    }
    
    public static ParameterSet getCleanParams(SQLStmt stmt, Object[] args, ParameterSet params) {
        assert(params != null) : "Unexpected null ParameterSet for " + stmt.catStmt.fullName();
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
                args[ii] = VoltType.NULL_DOUBLE;
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

        params.setParameters(args);
        return params;
    }

    /**
     * Derivation of StatsSource to expose timing information of procedure invocations.
     *
     */
    @SuppressWarnings("unused")
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
            super("XXX", 1, false);
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
            rowValues[columnNameToIndex.get("PROCEDURE")] = catalog_proc.getClassname();
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
            return catalog_proc.getTypeName();
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

}
