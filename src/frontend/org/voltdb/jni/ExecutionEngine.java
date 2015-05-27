/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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

package org.voltdb.jni;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.EEException;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.types.AntiCacheDBType;

import edu.brown.hstore.HStore;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.StringUtil;

/**
 * Wrapper for native Execution Engine library. There are two implementations,
 * one using JNI and one using IPC. ExecutionEngine provides a consistent interface
 * for these implementations to the ExecutionSite.
 */
public abstract class ExecutionEngine implements FastDeserializer.DeserializationMonitor {
    private static final Logger LOG = Logger.getLogger(ExecutionEngine.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

//    private static boolean voltSharedLibraryLoaded = false;
    protected PartitionExecutor executor;

    // is the execution site dirty
    protected boolean m_dirty;
    
    // Whether the anti-cache feature is enabled
    protected boolean m_anticache;
    
    /** Error codes exported for JNI methods. */
    public static final int ERRORCODE_SUCCESS = 0;
    public static final int ERRORCODE_ERROR = 1; // just error or not so far.
    public static final int ERRORCODE_WRONG_SERIALIZED_BYTES = 101;
    public static final int ERRORCODE_NO_DATA = 102;

    /** Create an ee and load the volt shared library */
    public ExecutionEngine(final PartitionExecutor executor) {
        this.executor = executor;
    }
    
    /** Make the EE clean and ready to do new transactional work. */
    public void resetDirtyStatus() {
        m_dirty = false;
    }

    /** Has the database changed any state since the last reset of dirty status? */
    public boolean getDirtyStatus() {
        return m_dirty;
    }
    
    public PartitionExecutor getPartitionExecutor(){
		return executor;
    }

    /** Utility method to verify return code and throw as required */
    final protected void checkErrorCode(final int errorCode) {
        if (errorCode != ERRORCODE_SUCCESS) {
            if (debug.val)
                LOG.error(String.format("Unexpected ExecutionEngine error [code=%d]", errorCode));
            throwExceptionForError(errorCode);
        }
    }

    /**
     * Utility method to generate an EEXception that can be overridden by
     * derived classes
     **/
    protected void throwExceptionForError(final int errorCode) {
        throw new EEException(errorCode);
    }

    @Override
    public void deserializedBytes(final int numBytes) {
    }

    /*
     * State to manage dependency tables for the current work unit.
     * The EE pulls from this state as necessary across JNI (or IPC)
     */
    DependencyTracker m_dependencyTracker = new DependencyTracker();

    /**
     * Called by Java to store dependencies for the EE. Creates
     * a private list of dependencies to be manipulated by the tracker.
     * Does not copy the table data - references WorkUnit's tables.
     * @param dependencies
     */
    public void stashWorkUnitDependencies(final Map<Integer, List<VoltTable>> dependencies) {
        if (debug.val)
            LOG.debug(String.format("Stashing %d InputDependencies:\n%s",
                      dependencies.size(), StringUtil.formatMaps(dependencies)));
        m_dependencyTracker.trackNewWorkUnit(dependencies);
    }

    /**
     * Stash a single dependency. Exists only for test cases.
     * @param depId
     * @param vt
     */
    public void stashDependency(final int depId, final VoltTable vt) {
        m_dependencyTracker.addDependency(depId, vt);
    }


    private class DependencyTracker {
        private final Map<Integer, ArrayDeque<VoltTable>> m_depsById = new ConcurrentHashMap<Integer, ArrayDeque<VoltTable>>();

        private final Logger hostLog =
            Logger.getLogger("HOST", VoltLoggerFactory.instance());

        /**
         * Add a single dependency. Exists only for test cases.
         * @param depId
         * @param vt
         */
        void addDependency(final int depId, final VoltTable vt) {
            ArrayDeque<VoltTable> deque = m_depsById.get(depId);
            if (deque == null) {
                deque = new ArrayDeque<VoltTable>();
                m_depsById.put(depId, deque);
            }
            deque.add(vt);
        }

        /**
         * Store dependency tables for later retrieval by the EE.
         * @param workunit
         */
        void trackNewWorkUnit(final Map<Integer, List<VoltTable>> dependencies) {
            for (final Entry<Integer, List<VoltTable>> e : dependencies.entrySet()) {
                // could do this optionally - debug only.
                if (debug.val) verifyDependencySanity(e.getKey(), e.getValue());
                // create a new list of references to the workunit's table
                // to avoid any changes to the WorkUnit's list. But do not
                // copy the table data.
                ArrayDeque<VoltTable> deque = m_depsById.get(e.getKey());
                if (deque == null) {
                    deque = new ArrayDeque<VoltTable>();
                // intentionally overwrite the previous dependency id.
                // would a lookup and a clear() be faster?
                m_depsById.put(e.getKey(), deque);
                } else {
                    deque.clear();
                }
                for (VoltTable vt : e.getValue()) {
                    if (vt != null) deque.add(vt);
                } // FOR
            }
            if (debug.val) LOG.debug("Current InputDepencies:\n" + StringUtil.formatMaps(m_depsById));
        }

        public VoltTable nextDependency(final int dependencyId) {
            // this formulation retains an arraydeque in the tracker that is
            // overwritten by the next transaction using this dependency id. If
            // the EE requests all dependencies (as is expected), the deque
            // will not retain any references to VoltTables (which is the goal).
            final ArrayDeque<VoltTable> vtstack = m_depsById.get(dependencyId);
            if (vtstack != null && vtstack.size() > 0) {
                // java doc. says this amortized constant time.
                return vtstack.pop();
            }
            else if (vtstack == null) {
                assert(false) : "receive without associated tracked dependency. [depId=" + dependencyId + "]";
                return null;
            }
            else {
                return null;
            }
        }

        /**
         * Log and exit if a dependency list fails an invariant.
         * @param dependencyId
         * @param dependencies
         */
        void verifyDependencySanity(final Integer dependencyId, final List<VoltTable> dependencies) {
            if (dependencies == null) {
                hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyNotFound.name(),
                               new Object[] { dependencyId }, null);
                HStore.crashDB();
            }
            for (final Object dependency : dependencies) {
                if (dependency == null) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyContainedNull.name(),
                                   new Object[] { dependencyId },
                            null);
                    HStore.crashDB();
                }
                if (!(dependency instanceof VoltTable)) {
                    hostLog.l7dlog(Level.FATAL, LogKeys.host_ExecutionSite_DependencyNotVoltTable.name(),
                                   new Object[] { dependencyId }, null);
                    HStore.crashDB();
                }
                if (trace.val) LOG.trace(String.format("Storing Dependency %d\n:%s", dependencyId, dependency));
            } // FOR

        }
    }



    /*
     * Interface backend invokes to communicate to Java frontend
     */


    /**
     * Call VoltDB.crashVoltDB on behalf of the EE
     * @param reason Reason the EE crashed
     */
    public static void crashVoltDB(String reason, String traces[], String filename, int lineno) {
        if (reason != null) {
            LOG.fatal("ExecutionEngine requested that we crash: " + reason);
            LOG.fatal("Error was in " + filename + ":" + lineno + "\n" + StringUtil.join("\n", traces));
        }
        HStore.crashDB();
    }

    /**
     * Called from the ExecutionEngine to request serialized dependencies.
     */
    public byte[] nextDependencyAsBytes(final int dependencyId) {
        final VoltTable vt =  m_dependencyTracker.nextDependency(dependencyId);
        if (vt != null) {
            ByteBuffer buffer = vt.getDirectDataReference();
            if (debug.val) LOG.debug(String.format("Passing Dependency %d to EE [rows=%d, cols=%d, bytes=%d/%d]\n%s",
                                           dependencyId,
                                           vt.getRowCount(),
                                           vt.getColumnCount(),
                                           vt.getUnderlyingBufferSize(),
                                           buffer.array().length,
                                           vt.toString()));
            assert(buffer.hasArray());
            return (buffer.array());
        }
        // Note that we will hit this after retrieving all the VoltTables for the given dependencyId
        // It does not mean that there were no VoltTables at all, it just means that 
        // we have gotten all of them
        else if (debug.val) {
            LOG.warn(String.format("Failed to find Dependency %d for EE [dep=%s, count=%d, ids=%s]",
                                    dependencyId,
                                    m_dependencyTracker.m_depsById.get(dependencyId),
                                    m_dependencyTracker.m_depsById.size(),
                                    m_dependencyTracker.m_depsById.keySet()));
        }
        return null;
    }

    /*
     * Interface frontend invokes to communicate to CPP execution engine.
     */

    abstract public boolean activateTableStream(final int tableId, TableStreamType type);

    /**
     * Serialize more tuples from the specified table that already has a stream enabled
     * @param c Buffer to serialize tuple data too
     * @param tableId Catalog ID of the table to serialize
     * @return A positive number indicating the number of bytes serialized or 0 if there is no more data.
     *        -1 is returned if there is an error (such as the table not having the specified stream type activated).
     */
    public abstract int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType type);

    public abstract void processRecoveryMessage( ByteBuffer buffer, long pointer);
    
    // ARIES
    public abstract void doAriesRecoveryPhase(long replayPointer, long replayLogSize, long replayTxnId);

    /** Releases the Engine object. */
    abstract public void release() throws EEException, InterruptedException;

    /** Pass the catalog to the engine */
    abstract public void loadCatalog(final String serializedCatalog) throws EEException;

    /** Pass diffs to apply to the EE's catalog to update it */
    abstract public void updateCatalog(final String diffCommands, int catalogVersion) throws EEException;

    /** Run a plan fragment */
    abstract public DependencySet executePlanFragment(
        long planFragmentId, int outputDepId,
        int inputDepId, ParameterSet parameterSet,
        long txnId, long lastCommittedTxnId, long undoQuantumToken)
      throws EEException;

    /** Run a plan fragment */
    abstract public VoltTable executeCustomPlanFragment(
            String plan, int outputDepId,
            int inputDepId, long txnId,
            long lastCommittedTxnId, long undoQuantumToken) throws EEException;

    /** Run multiple query plan fragments */
    abstract public DependencySet executeQueryPlanFragmentsAndGetDependencySet(long[] planFragmentIds,
                                                                       int numFragmentIds,
                                                                       int[] input_depIds,
                                                                       int[] output_depIds,
                                                                       ParameterSet[] parameterSets,
                                                                       int numParameterSets,
                                                                       long txnId, long lastCommittedTxnId,
                                                                       long undoQuantumToken) throws EEException;

//    abstract public DependencySet executeQueryPlanFragmentsAndGetDependencySet(long[] planFragmentIds,
//            int numFragmentIds,
//            int[] input_depIds,
//            int[] output_depIds,
//            ByteString serializedParameterSets[],
//            int numParameterSets,
//            long txnId, long lastCommittedTxnId,
//            long undoQuantumToken) throws EEException;
    
    /** Run multiple query plan fragments */
//    public VoltTable[] executeQueryPlanFragmentsAndGetResults(long[] planFragmentIds,
//                                                                       int numFragmentIds,
//                                                                       int[] input_depIds,
//                                                                       int[] output_depIds,
//                                                                       ParameterSet[] parameterSets,
//                                                                       int numParameterSets,
//                                                                       long txnId, long lastCommittedTxnId,
//                                                                       long undoQuantumToken) throws EEException {
//        DependencySet dset = this.executeQueryPlanFragmentsAndGetDependencySet(
//                planFragmentIds, numFragmentIds,
//                input_depIds,
//                output_depIds,
//                parameterSets, numParameterSets, txnId, lastCommittedTxnId, undoQuantumToken);
//        assert(dset != null);
//        return (dset.dependencies);
//    }

    /** Used for test code only (AFAIK jhugg) */
    abstract public VoltTable serializeTable(int tableId) throws EEException;

    abstract public void loadTable(
        int tableId, VoltTable table, long txnId,
        long lastCommittedTxnId, long undoToken, boolean allowExport) throws EEException;

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     */
    abstract public boolean setLogLevels(long logLevels) throws EEException;

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    abstract public void tick(long time, long lastCommittedTxnId);

    /**
     * Instruct EE to come to an idle state. Flush Export buffers, finish
     * any in-progress checkpoint, etc.
     */
    abstract public void quiesce(long lastCommittedTxnId);

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Array of results tables. An array of length 0 indicates there are no results. null indicates failure.
     */
    abstract public VoltTable[] getStats(
            SysProcSelector selector,
            int locators[],
            boolean interval,
            Long now);

    /**
     * Instruct the EE to start/stop its profiler.
     */
    public abstract int toggleProfiler(int toggle);

    /**
     * Release all undo actions up to and including the specified undo token
     * @param undoToken The undo token.
     */
    public abstract boolean releaseUndoToken(long undoToken);

    /**
     * Undo all undo actions back to and including the specified undo token
     * @param undoToken The undo token.
     */
    public abstract boolean undoUndoToken(long undoToken);

    /**
     * Execute an Export action against the execution engine.
     * @param ackAction true if this message instructs an ack.
     * @param pollAction true if this message instructs a poll.
     * @param syncAction TODO
     * @param ackTxnId if an ack, the transaction id being acked
     * @param tableId the table being polled or acked.
     * @param syncOffset TODO
     * @return the response ExportMessage
     */
    public abstract ExportProtoMessage exportAction(
            boolean ackAction, boolean pollAction, boolean resetAction, boolean syncAction,
            long ackOffset, long seqNo, int partitionId, long tableId);

    /**
     * Calculate a hash code for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId table to calculate a hash code for
     */
    public abstract long tableHashCode(int tableId);

    /**
     * Compute the partition to which the parameter value maps using the
     * ExecutionEngine's hashinator.  Currently only valid for int types
     * (tiny, small, integer, big) and strings.
     *
     * THIS METHOD IS CURRENTLY ONLY USED FOR TESTING
     */
    public abstract int hashinate(Object value, int partitionCount);

    // ARIES
    public abstract long getArieslogBufferLength();

    public abstract void getArieslogData(int bufferLength, byte[] arieslogDataArray);

    public abstract long readAriesLogForReplay(long[] size);

    public abstract void freePointerToReplayLog(long ariesReplayPointer);
    
    /*
     * Declare the native interface. Structurally, in Java, it would be cleaner to
     * declare this in ExecutionEngineJNI.java. However, that would necessitate multiple
     * jni_class instances in the execution engine. From the EE perspective, a single
     * JNI class is better.  So put this here with the backend->frontend api definition.
     */

    protected native byte[] nextDependencyTest(int dependencyId);

    /**
     * Just creates a new VoltDBEngine object and returns it to Java.
     * Never fail to destroy() for the VoltDBEngine* once you call this method
     * NOTE: Call initialize() separately for initialization.
     * This does strictly nothing so that this method never throws an exception.
     * @return the created VoltDBEngine pointer casted to jlong.
    */
    protected native long nativeCreate(boolean isSunJVM);
    /**
     * Releases all resources held in the execution engine.
     * @param pointer the VoltDBEngine pointer to be destroyed
     * @return error code
     */
    protected native int nativeDestroy(long pointer);

    /**
     * Initializes the execution engine with given parameter.
     * @param pointer the VoltDBEngine pointer to be initialized
     * @param cluster_id id of the cluster the execution engine belongs to
     * @param siteId this id will be set to the execution engine
     * @param partitionId id of partitioned assigned to this EE
     * @param hostId id of the host this EE is running on
     * @param hostname name of the host this EE is running on
     * @return error code
     */
    protected native int nativeInitialize(
            long pointer,
            int clusterIndex,
            int siteId,
            int partitionId,
            int hostId,
            String hostname);

    /**
     * Sets (or re-sets) all the shared direct byte buffers in the EE.
     * @param pointer
     * @param parameter_buffer
     * @param parameter_buffer_size
     * @param resultBuffer
     * @param result_buffer_size
     * @param exceptionBuffer
     * @param exception_buffer_size
     * @return error code
     */
    protected native int nativeSetBuffers(long pointer, ByteBuffer parameter_buffer, int parameter_buffer_size,
                                          ByteBuffer resultBuffer, int result_buffer_size,
                                          ByteBuffer exceptionBuffer, int exception_buffer_size,
                                          ByteBuffer ariesLogBuffer, int arieslog_buffer_size);
    
    /**
     * Load the system catalog for this engine.
     * @param pointer the VoltDBEngine pointer
     * @param serialized_catalog the root catalog object serialized as text strings.
     * this parameter is jstring, not jbytearray because Catalog is serialized into
     * human-readable text strings separated by line feeds.
     * @return error code
     */
    protected native int nativeLoadCatalog(long pointer, String serialized_catalog);

    /**
     * Update the EE's catalog.
     * @param pointer the VoltDBEngine pointer
     * @param diff_commands Commands to apply to the existing EE catalog to update it
     * @param catalogVersion
     * @return error code
     */
    protected native int nativeUpdateCatalog(long pointer, String diff_commands, int catalogVersion);

    /**
     * This method is called to initially load table data.
     * @param pointer the VoltDBEngine pointer
     * @param table_id catalog ID of the table
     * @param serialized_table the table data to be loaded
     * @param Length of the serialized table
     * @param undoToken token for undo quantum where changes should be logged.
     */
    protected native int nativeLoadTable(long pointer, int table_id, byte[] serialized_table,
            long txnId, long lastCommittedTxnId, long undoToken, boolean allowExport);

    //Execution

    /**
     * Executes a plan fragment with the given parameter set.
     * @param pointer the VoltDBEngine pointer
     * @param plan_fragment_id ID of the plan fragment to be executed.
     * @return error code
     */
    protected native int nativeExecutePlanFragment(long pointer, long planFragmentId,
            int outputDepId, int inputDepId, long txnId, long lastCommittedTxnId, long undoToken);

    protected native int nativeExecuteCustomPlanFragment(long pointer, String plan,
            int outputDepId, int inputDepId, long txnId, long lastCommittedTxnId, long undoToken);

    /**
     * Executes multiple plan fragments with the given parameter sets and gets the results.
     * @param pointer the VoltDBEngine pointer
     * @param planFragmentIds ID of the plan fragment to be executed.
     * @return error code
     */
    protected native int nativeExecuteQueryPlanFragmentsAndGetResults(long pointer,
            long[] planFragmentIds, int numFragments,
            int[] input_depIds,
            int[] outputDepIds,
            long txnId, long lastCommittedTxnId, long undoToken);

    /**
     * Serialize the result temporary table.
     * @param pointer the VoltDBEngine pointer
     * @param table_id Id of the table to be serialized
     * @param outputBuffer buffer to be filled with the table.
     * @param outputCapacity maximum number of bytes to write to buffer.
     * @return serialized temporary table
     */
    protected native int nativeSerializeTable(long pointer, int table_id,
                                              ByteBuffer outputBuffer, int outputCapacity);

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    protected native void nativeTick(long pointer, long time, long lastCommittedTxnId);

    /**
     * Native implementation of quiesce engine interface method.
     * @param pointer
     */
    protected native void nativeQuiesce(long pointer, long lastCommittedTxnId);

    /**
     * Retrieve a set of statistics using the specified selector ordinal from the StatisticsSelector enum.
     * @param stat_selector Ordinal value of a statistic selector from StatisticsSelector.
     * @param catalog_locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Number of result tables, 0 on no results, -1 on failure.
     */
    protected native int nativeGetStats(
            long pointer,
            int stat_selector,
            int catalog_locators[],
            boolean interval,
            long now);

    /**
     * Toggle profile gathering within the execution engine
     * @param mode 0 to disable. 1 to enable.
     * @return 0 on success.
     */
    protected native int nativeToggleProfiler(long pointer, int mode);

    /**
     * Use the EE's hashinator to compute the partition to which the
     * value provided in the input parameter buffer maps.  This is
     * currently a test-only method.
     * @param pointer
     * @param partitionCount
     * @return
     */
    protected native int nativeHashinate(long pointer, int partitionCount);

    /**
     * @param nextUndoToken The undo token to associate with future work
     * @return true for success false for failure
     */
    protected native boolean nativeSetUndoToken(long pointer, long nextUndoToken);

    /**
     * @param undoToken The undo token to release
     * @return true for success false for failure
     */
    protected native boolean nativeReleaseUndoToken(long pointer, long undoToken);

    /**
     * @param undoToken The undo token to undo
     * @return true for success false for failure
     */
    protected native boolean nativeUndoUndoToken(long pointer, long undoToken);

    /**
     * @param pointer Pointer to an engine instance
     * @param logLevels Levels for the various loggers
     * @return true for success false for failure
     */
    protected native boolean nativeSetLogLevels(long pointer, long logLevels);

    /**
     * Active a table stream of the specified type for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId Catalog ID of the table
     * @param streamType type of stream to activate
     * @return <code>true</code> on success and <code>false</code> on failure
     */
    protected native boolean nativeActivateTableStream(long pointer, int tableId, int streamType);

    /**
     * Serialize more tuples from the specified table that has an active stream of the specified type
     * @param pointer Pointer to an engine instance
     * @param bufferPointer Buffer to serialize data to
     * @param offset Offset into the buffer to start serializing to
     * @param length length of the buffer
     * @param tableId Catalog ID of the table to serialize
     * @param streamType type of stream to pull data from
     * @return A positive number indicating the number of bytes serialized or 0 if there is no more data.
     *         -1 is returned if there is an error (such as the table not being COW mode).
     */
    protected native int nativeTableStreamSerializeMore(long pointer, long bufferPointer, int offset, int length, int tableId, int streamType);

    /**
     * Process a recovery message and load the data it contains.
     * @param pointer Pointer to an engine instance
     * @param message Recovery message to load
     */
    protected native void nativeProcessRecoveryMessage(long pointer, long message, int offset, int length);

    /**
     * Calculate a hash code for a table.
     * @param pointer Pointer to an engine instance
     * @param tableId table to calculate a hash code for
     */
    protected native long nativeTableHashCode(long pointer, int tableId);

    /**
     * Perform an export poll or ack action. Poll data will be returned via the usual
     * results buffer. A single action may encompass both a poll and ack.
     * @param pointer Pointer to an engine instance
     * @param mAckAction True if an ack is requested
     * @param mPollAction True if a  poll is requested
     * @param mAckOffset The offset being ACKd.
     * @param mTableId The table ID being acted against.
     * @return
     */
    protected native long nativeExportAction(
            long pointer,
            boolean ackAction,
            boolean pollAction,
            boolean resetAction,
            boolean syncAction,
            long mAckOffset,
            long seqNo,
            long mTableId);

    // ----------------------------------------------------------------------------
    // READ/WRITE SET TRACKING
    // ----------------------------------------------------------------------------
    
    /**
     * Enable/disable row-level tracking for a transaction at runtime.
     * @param value
     * @throws EEException
     */
    public abstract void trackingEnable(Long txnId) throws EEException;
    
    /**
     * Enable/disable tracking the read/write sets of individual transactions at runtime.
     * @param pointer 
     * @param value
     * @return
     * @throws EEException
     */
    protected native int nativeTrackingEnable(long pointer, long txnId) throws EEException;
    
    /**
     * Mark a txn as finished in the EE. This will clean up the row tracking stuff.
     * @param txnId
     * @throws EEException
     */
    public abstract void trackingFinish(Long txnId) throws EEException;
    
    /**
     * Mark a txn as finished in the EE. This will clean up the row tracking stuff.
     * @param value
     * @param pointer
     * @return
     * @throws EEException
     */
    protected native int nativeTrackingFinish(long pointer, long txnId) throws EEException;
    
    /**
     * Get the read set for this txn.
     * @param txnId
     * @throws EEException
     */
    public abstract VoltTable trackingReadSet(Long txnId) throws EEException;
    
    /**
     * Get the read set for this txn.
     * @param value
     * @param pointer
     * @return
     * @throws EEException
     */
    protected native int nativeTrackingReadSet(long pointer, long txnId) throws EEException;
    
    /**
     * Get the write set for this txn.
     * @param txnId
     * @throws EEException
     */
    public abstract VoltTable trackingWriteSet(Long txnId) throws EEException;
    
    /**
     * Get the write set for this txn.
     * @param value
     * @param pointer
     * @return
     * @throws EEException
     */
    protected native int nativeTrackingWriteSet(long pointer, long txnId) throws EEException;

    
    // ----------------------------------------------------------------------------
    // ANTI-CACHING
    // ----------------------------------------------------------------------------
    
    /**
     * Initialize anti-caching at this partition's EE.
     * <B>NOTE:</B> This must be invoked before loadCatalog is invoked
     * @param dbDir
     * @param dbType
     * @param blocking
     * @param blockSize TODO
     * @param maxSize
     * @param blockMerge
     * @throws EEException
     */
    public abstract void antiCacheInitialize(File dbDir, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException;

    /**
     * Initialize additional levels of anticaching DBs.
     * <B>NOTE:</B> This can only be invoked after antiCacheInitialize is invoked
     * @param dbDir
     * @param AntiCacheDBType
     * @param blocking
     * @param blockSize
     * @param maxSize 
     * @param blockMerge
     * @throws EEException
     */
    public abstract void antiCacheAddDB(File dbDir, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException;
    
    /**
     * 
     * @param catalog_tbl
     * @param block_ids
     */
    public abstract void antiCacheReadBlocks(Table catalog_tbl, int block_ids[], int tuple_offsets[]);

    /**
     * Forcibly tell the EE that it needs to evict a certain number of bytes
     * for a table. This is most likely only useful for testing
     * @param catalog_tbl
     * @param block_size The number of bytes to evict from the target table
     * @return TODO
     */
    public abstract VoltTable antiCacheEvictBlock(Table catalog_tbl, long block_size, int num_blocks);
    
    /**
     * Forcibly tell the EE that it needs to evict a certain number of bytes
     * for a table in batch. 
     * @param catalog_tbl
     * @param childTable 
     * @param block_size The number of bytes to evict from the target table
     */
    public abstract VoltTable antiCacheEvictBlockInBatch(Table catalog_tbl, Table childTable, long block_size, int num_blocks);
    
    /**
     * Instruct the EE to merge in the unevicted blocks into the table's regular data.
     * This is a blocking call and should only be executed when there is no other transaction
     * running at this partition.
     * @param catalog_tbl
     */
    public abstract void antiCacheMergeBlocks(Table catalog_tbl);
        
    /**
     * Enables the anti-cache feature in the EE. The given database directory path
     * must be a unique location for this partition where the EE can store 
     * evicted blocks of tuples. The EE assumes that the parent directories 
     * for dbDir exist and are writable.  
     * @param pointer
     * @param dbDir
     * @param blockSize TODO
     * @param dbType
     * @param blocking
     * @param maxSize
     * @param blockMerge
     * @return
     */
    protected native int nativeAntiCacheInitialize(long pointer, String dbDir, long blockSize, int dbtype, boolean blocking, long maxSize, boolean blockMerge);

    /** 
     * Adds new additional AntiCacheDB instances for multilevel anticaching. The database
     *  directory path should be unique and exist. The existance of multilevel databases
     * is checked in the front end.
     * @param pointer
     * @param dbDir
     * @param blockSize
     * @param dbType
     * @param blocking
     * @param maxSize
     * @param blockMerge
     * @return
     */
    protected native int nativeAntiCacheAddDB(long pointer, String dbDir, long blockSize, int dbtype, boolean blocking, long maxSize, boolean blockMerge);
    
     /**
     * 
     * @param pointer
     * @param tableId
     * @param block_ids
     * @return
     */
    protected native int nativeAntiCacheReadBlocks(long pointer, int tableId, int block_ids[], int tuple_offsets[]);
    
    /**
     * 
     * @param pointer
     * @param tableId
     * @param blockSize
     * @return
     */
    protected native int nativeAntiCacheEvictBlock(long pointer, int tableId, long blockSize, int num_blocks);

    /**
     * 
     * @param pointer
     * @param tableId
     * @param blockSize
     * @return
     */
    protected native int nativeAntiCacheEvictBlockInBatch(long pointer, int tableId, int childTableId, long blockSize, int num_blocks);

    /**
     * 
     * @param pointer
     * @param tableId
     * @return
     */
    protected native int nativeAntiCacheMergeBlocks(long pointer, int tableId);
    
    /**
     * This code only does anything useful on MACOSX.
     * On LINUX, procfs is read to get RSS
     * @return Returns the RSS size in bytes or -1 on error (or wrong platform).
     */
    public native static long nativeGetRSS();    
    
    // ----------------------------------------------------------------------------
    // STORAGE MMAP
    // ----------------------------------------------------------------------------
    
    public abstract void MMAPInitialize(File dbDir, long mapSize, long syncFrequency) throws EEException;
    
    /**
     * Enables the mmap storage feature in the EE. The given database directory path
     * must be a unique location for this partition where the EE can store MMAP'ed files.
     */
    protected native int nativeMMAPInitialize(long pointer, String dbDir, long mapSize, long syncFrequency);

    // ----------------------------------------------------------------------------
    // ARIES
    // ----------------------------------------------------------------------------

    public abstract void ARIESInitialize(File dbDir, File logFile) throws EEException;

    /**
     * Enables the ARIES  feature in the EE. The given database directory path
     * must be a unique location for this partition where the EE can store ARIES logs
     */
    protected native int nativeARIESInitialize(long pointer, String dbDir, String logFile);
        
    protected native void nativeDoAriesRecoveryPhase(long pointer, long replayPointer, long replayLogSize, long replayTxnId);

    protected native long nativeGetArieslogBufferLength(long pointer);
    
    protected native void nativeRewindArieslogBuffer(long pointer);

    protected native long nativeReadAriesLogForReplay(long pointer, long[] size);

    protected native void nativeFreePointerToReplayLog(long pointer, long ariesReplayPointer);

   
}
