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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.EEException;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.messaging.FastSerializer.BufferGrowCallback;
import org.voltdb.types.AntiCacheDBType;
import org.voltdb.utils.DBBPool.BBContainer;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Wrapper for native Execution Engine library.
 * All native methods are private to make it simple
 * and keep Java/C++ separated as much as possible.
 * These native methods basically just receive IDs in primitive types
 * to simplify JNI calls. This is why these native methods are private and
 * we have public methods that receive objects for type-safety.
 * For each methods, see comments in hstorejni.cpp.
 * Read <a href="package-summary.html">com.horinzontica.jni</a> for
 * guidelines to add/modify JNI methods.
 */
public class ExecutionEngineJNI extends ExecutionEngine {
    private static final Logger LOG = Logger.getLogger(ExecutionEngineJNI.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /** The HStoreEngine pointer. */
    private long pointer;

    /** Create a FastSerializer for serializing arguments to C++. Use a direct
    ByteBuffer as it will be passed directly to the C++ code. */
    private final FastSerializer fsForParameterSet;

    /**
     * A deserializer backed by a direct byte buffer, for fast access from C++.
     * Since this is generally the largest shared buffer between Java and C++
     * it is also generally speaking (since there are no larger shared buffers)
     * the largest possible message size that can be sent between the IPC backend
     * and Java. Every time the size of this buffer is changed the MAX_MSG_SZ define
     * in voltdbipc.cpp must be changed to match so that tests and apps
     * that rely on being able to serialize large results sets will get the same amount of storage
     * when using the IPC backend.
     **/
    private final BBContainer deserializerBufferOrigin = org.voltdb.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private FastDeserializer deserializer = new FastDeserializer(deserializerBufferOrigin.b);

    private final BBContainer exceptionBufferOrigin = org.voltdb.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private ByteBuffer exceptionBuffer = exceptionBufferOrigin.b;

    // ARIES
    private final BBContainer ariesLogBufferOrigin = org.voltdb.utils.DBBPool.allocateDirect(1024 * 1024 * 10);
    private ByteBuffer ariesLogBuffer = ariesLogBufferOrigin.b;
    
    /**
     * Java cache for read/write tracking sets
     */
    private Map<Long, VoltTable[]> trackingCache;
    
    /**
     * initialize the native Engine object.
     */
    public ExecutionEngineJNI(
            final PartitionExecutor executor,
            final int clusterIndex,
            final int siteId,
            final int partitionId,
            final int hostId,
            final String hostname)
    {
        // base class loads the volt shared library
        super(executor);
        //exceptionBuffer.order(ByteOrder.nativeOrder());
        if (debug.val) LOG.debug("Creating Execution Engine [site#=" + siteId + ", partition#=" + partitionId + "]");
        /*
         * (Ning): The reason I'm testing if we're running in Sun's JVM is that
         * EE needs this info in order to decide whether it's safe to install
         * the signal handler or not.
         */
	 pointer = nativeCreate(System.getProperty("java.vm.vendor", "xyz").toLowerCase().contains("sun microsystems"));
        nativeSetLogLevels(this.pointer, EELoggers.getLogLevels());
        int errorCode =
            nativeInitialize(
                    pointer,
                    clusterIndex,
                    siteId,
                    partitionId,
                    hostId,
                    hostname);
        checkErrorCode(errorCode);
        fsForParameterSet = new FastSerializer(false, new BufferGrowCallback() {
            public void onBufferGrow(final FastSerializer obj) {
                if (trace.val) LOG.trace("Parameter buffer has grown. re-setting to EE..");
                final int code = nativeSetBuffers(pointer,
                        fsForParameterSet.getContainerNoFlip().b,
                        fsForParameterSet.getContainerNoFlip().b.capacity(),
                        deserializer.buffer(), deserializer.buffer().capacity(),
                        exceptionBuffer, exceptionBuffer.capacity(),
                        ariesLogBuffer, ariesLogBuffer.capacity());
                checkErrorCode(code);
            }
        }, null);

        errorCode = nativeSetBuffers(this.pointer, fsForParameterSet.getContainerNoFlip().b,
                fsForParameterSet.getContainerNoFlip().b.capacity(),
                deserializer.buffer(), deserializer.buffer().capacity(),
                exceptionBuffer, exceptionBuffer.capacity(),
                ariesLogBuffer, ariesLogBuffer.capacity());

        checkErrorCode(errorCode);
        
        //LOG.info("Initialized Execution Engine");
    }

    /** Utility method to throw a Runtime exception based on the error code and serialized exception **/
    @Override
    final protected void throwExceptionForError(final int errorCode) throws RuntimeException {
        exceptionBuffer.clear();
        final int exceptionLength = exceptionBuffer.getInt();
        if (debug.val) LOG.debug("EEException Length: " + exceptionLength);

        if (exceptionLength == 0) {
            throw new EEException(errorCode);
        } else {
            exceptionBuffer.position(0);
            exceptionBuffer.limit(4 + exceptionLength);
            SerializableException ex = null;
            try {
                ex = SerializableException.deserializeFromBuffer(exceptionBuffer);
            } catch (Throwable e) {
                ex = new SerializableException(); 
                e.printStackTrace();
            }
            throw ex;
        }
    }

    /**
     * Releases the Engine object.
     * This method is automatically called from #finalize(), but
     * it's recommended to call this method just after you finish
     * using the object.
     * @see #nativeDestroy(long)
     */
    @Override
    public void release() throws EEException {
        if (trace.val) LOG.trace("Releasing Execution Engine... " + pointer);
        if (this.pointer != 0L) {
            final int errorCode = nativeDestroy(this.pointer);
            pointer = 0L;
            checkErrorCode(errorCode);
        }
        deserializer = null;
        deserializerBufferOrigin.discard();
        exceptionBuffer = null;
        exceptionBufferOrigin.discard();
        ariesLogBuffer = null;
        ariesLogBufferOrigin.discard();

        if (trace.val) LOG.trace("Released Execution Engine.");
    }
    
    /**
     *  Provide a serialized catalog and initialize version 0 of the engine's
     *  catalog.
     */
    @Override
    public void loadCatalog(final String serializedCatalog) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeLoadCatalog(this.pointer, serializedCatalog);
        }
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * Provide a catalog diff and a new catalog version and update the
     * engine's catalog.
     */
    @Override
    public void updateCatalog(final String catalogDiffs, int catalogVersion) throws EEException {
        //C++ JSON deserializer is not thread safe, must synchronize
        if (trace.val) LOG.trace("Loading Application Catalog...");
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeUpdateCatalog(this.pointer, catalogDiffs, catalogVersion);
        }
        checkErrorCode(errorCode);
        //LOG.info("Loaded Catalog.");
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     */
    @Override
    public DependencySet executePlanFragment(final long planFragmentId,
                                              final int outputDepId,
                                              final int inputDepId,
                                              final ParameterSet parameterSet,
                                              final long txnId,
                                              final long lastCommittedTxnId,
                                              final long undoToken)
      throws EEException
    {
        if (trace.val)
            LOG.trace("Executing planfragment:" + planFragmentId + ", params=" + parameterSet.toString());
        
        if (this.trackingCache != null) {
            this.trackingResetCacheEntry(txnId);
        }

        // serialize the param set
        fsForParameterSet.clear();
        try {
            parameterSet.writeExternal(fsForParameterSet);
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }
        // checkMaxFsSize();
        // Execute the plan, passing a raw pointer to the byte buffer.
        deserializer.clear();
        final int errorCode = nativeExecutePlanFragment(this.pointer, planFragmentId, outputDepId, inputDepId,
                                                        txnId, lastCommittedTxnId, undoToken);
        checkErrorCode(errorCode);

        try {
            // read the complete size of the buffer used (ignored here)
            deserializer.readInt();
            // check if anything was changed
            final boolean dirty = deserializer.readBoolean();
            if (dirty)
                m_dirty = true;
            // read the number of tables returned by this fragment
            final int numDependencies = deserializer.readInt();
            final VoltTable dependencies[] = new VoltTable[numDependencies];
            final int depIds[] = new int[numDependencies];
            for (int i = 0; i < numDependencies; ++i) {
                depIds[i] = deserializer.readInt();
                dependencies[i] = deserializer.readObject(VoltTable.class);
            } // FOR
            assert(depIds.length == 1);
            return new DependencySet(depIds, dependencies);
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result dependencies" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }

    }

    @Override
    public VoltTable executeCustomPlanFragment(final String plan, final int outputDepId,
            final int inputDepId, final long txnId, final long lastCommittedTxnId,
            final long undoQuantumToken) throws EEException
    {
        if (this.trackingCache != null) {
            this.trackingResetCacheEntry(txnId);
        }
        
        fsForParameterSet.clear();
        deserializer.clear();
        //C++ JSON deserializer is not thread safe, must synchronize
        int errorCode = 0;
        synchronized (ExecutionEngineJNI.class) {
            errorCode = nativeExecuteCustomPlanFragment(this.pointer, plan, outputDepId, inputDepId,
                                                        txnId, lastCommittedTxnId, undoQuantumToken);
        }
        checkErrorCode(errorCode);

        try {
            deserializer.readInt(); // total size of the data
            // check if anything was changed
            final boolean dirty = deserializer.readBoolean();
            if (dirty)
                m_dirty = true;
            final int numDependencies = deserializer.readInt();
            assert(numDependencies == 1);
            final VoltTable dependencies[] = new VoltTable[numDependencies];
            for (int i = 0; i < numDependencies; ++i) {
                /*int depId =*/ deserializer.readInt();
                dependencies[i] = deserializer.readObject(VoltTable.class);
            }
            return dependencies[0];
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result dependencies" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    /**
     * @param undoToken Token identifying undo quantum for generated undo info
     * Wrapper for {@link #nativeExecuteQueryPlanFragmentsAndGetResults(long, int[], int, long, long, long)}.
     */
    @Override
    public DependencySet executeQueryPlanFragmentsAndGetDependencySet(
            long[] planFragmentIds,
            int batchSize,
            int[] input_depIds,
            int[] output_depIds,
            ParameterSet[] parameterSets,
            int numParameterSets,
            long txnId, long lastCommittedTxnId, long undoToken) throws EEException {

        assert(parameterSets != null) : "Null ParameterSets for txn #" + txnId;
        assert(planFragmentIds.length == parameterSets.length) :
            String.format("Expected %d ParameterSets but there were %d for txn #%d\n" +
            		      "PlanFragments:%s\nParameterSets:%s",
                          planFragmentIds.length, parameterSets.length, txnId,
                          Arrays.toString(planFragmentIds),
                          Arrays.toString(parameterSets));
        
        if (batchSize == 0) {
            LOG.warn("No fragments to execute. Returning empty DependencySet");
            return (new DependencySet(new int[0], HStoreConstants.EMPTY_RESULT));
        }
        
        if (this.trackingCache != null) {
            this.trackingResetCacheEntry(txnId);
        }

        // serialize the param sets
        fsForParameterSet.clear();
        try {
            for (int i = 0; i < batchSize; ++i) {
                assert(parameterSets[i] != null) :
                    String.format("Null ParameterSet at offset %d for txn #%d\n" +
                                  "PlanFragments:%s\nParameterSets:%s",
                                  i, txnId,
                                  Arrays.toString(planFragmentIds),
                                  Arrays.toString(parameterSets));
                
                parameterSets[i].writeExternal(fsForParameterSet);
                if (trace.val)
                    LOG.trace(String.format("Batch Executing planfragment:%d, params=%s",
                              planFragmentIds[i], parameterSets[i]));
            }
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }

        // Execute the plan, passing a raw pointer to the byte buffers for input and output
        deserializer.clear();
        final int errorCode = nativeExecuteQueryPlanFragmentsAndGetResults(this.pointer,
                planFragmentIds, batchSize,
                input_depIds,
                output_depIds,
                txnId, lastCommittedTxnId, undoToken);
        checkErrorCode(errorCode);

        // get a copy of the result buffers and make the tables use the copy
        ByteBuffer fullBacking = deserializer.buffer();
        try {
            // read the complete size of the buffer used
            fullBacking.getInt();
            // check if anything was changed
            m_dirty = (fullBacking.get() == 1 ? true : false);

            // get a copy of the buffer
            // Because this is a copy, that means we don't have to worry about the EE overwriting us
            // Not sure of the implications for performance.
             // deserializer.readBuffer(totalSize);
            
            // At this point we don't know how many dependencies we expect to get back from our fragments.
            // We're just going to assume that each PlanFragment generated one and only one output dependency
            VoltTable results[] = new VoltTable[batchSize];
            int dependencies[] = new int[batchSize];
            int dep_ctr = 0;
            for (int i = 0; i < batchSize; ++i) {
                int numDependencies = fullBacking.getInt(); // number of dependencies for this frag
                assert(numDependencies == 1) :
                    "Unexpected multiple output dependencies from PlanFragment #" + planFragmentIds[i];
                
                // PAVLO: Since we can't pass the dependency ids using nativeExecuteQueryPlanFragmentsAndGetResults(),
                // the results will come back without a dependency id. So we have to just assume
                // that the frags were executed in the order that we passed to the EE and that we
                // can just use the list of output_depIds that we have 
                for (int ii = 0; ii < numDependencies; ++ii) {
                    assert(dep_ctr < output_depIds.length) : 
                        "Trying to get depId #" + dep_ctr + ": " + Arrays.toString(output_depIds);
                    fullBacking.getInt(); // IGNORE 
                    int depid = output_depIds[dep_ctr];
                    assert(depid >= 0);
                    
                    int tableSize = fullBacking.getInt();
                    assert(tableSize < 10000000);
                    byte tableBytes[] = new byte[tableSize];
                    fullBacking.get(tableBytes, 0, tableSize);
                    final ByteBuffer tableBacking = ByteBuffer.wrap(tableBytes);
//                    fullBacking.position(fullBacking.position() + tableSize);

                    results[dep_ctr] = PrivateVoltTableFactory.createVoltTableFromBuffer(tableBacking, true);
                    dependencies[dep_ctr] = depid;
                    if (debug.val) LOG.debug(String.format("%d - New output VoltTable for DependencyId %d [origTableSize=%d]\n%s",
                                                   txnId, depid, tableSize, results[dep_ctr].toString())); 
                    dep_ctr++;
                } // FOR
            } // FOR
            
            return (new DependencySet(dependencies, results));
        } catch (Throwable ex) {
            LOG.error("Failed to deserialze result table" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }
    
//    @Override
//    public DependencySet executeQueryPlanFragmentsAndGetDependencySet(
//            long[] planFragmentIds,
//            int numFragmentIds,
//            int[] input_depIds,
//            int[] output_depIds,
//            ByteString[] parameterSets,
//            int numParameterSets,
//            long txnId, long lastCommittedTxnId, long undoToken) throws EEException {
//        
//        assert(parameterSets != null) : "Null ParameterSets for txn #" + txnId;
//        assert (planFragmentIds.length == parameterSets.length);
//        
//        // serialize the param sets
//        fsForParameterSet.clear();
//        for (int i = 0; i < numParameterSets; ++i) {
//            fsForParameterSet.getBBContainer().b.put(parameterSets[i].asReadOnlyByteBuffer());
//            if (trace.val) LOG.trace("Batch Executing planfragment:" + planFragmentIds[i] + ", params=" + parameterSets[i].toString());
//        }
//        
//        return _executeQueryPlanFragmentsAndGetDependencySet(planFragmentIds, numFragmentIds, input_depIds, output_depIds, txnId, lastCommittedTxnId, undoToken);
//    }
    

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Retrieving VoltTable:" + tableId);
        }
        deserializer.clear();
        final int errorCode = nativeSerializeTable(this.pointer, tableId, deserializer.buffer(),
                deserializer.buffer().capacity());
        checkErrorCode(errorCode);

        try {
            return deserializer.readObject(VoltTable.class);
        } catch (final IOException ex) {
            LOG.error("Failed to retrieve table:" + tableId + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public void loadTable(final int tableId, final VoltTable table,
        final long txnId, final long lastCommittedTxnId,
        final long undoToken, boolean allowExport) throws EEException
    {
        byte[] serialized_table = table.getTableDataReference().array();
        if (trace.val)
            LOG.trace(String.format("Passing table into EE [id=%d, bytes=%s]",
                      tableId, serialized_table.length));

        final int errorCode = nativeLoadTable(this.pointer, tableId, serialized_table,
                                              txnId, lastCommittedTxnId,
                                              undoToken, allowExport);
        checkErrorCode(errorCode);
    }

    /**
     * This method should be called roughly every second. It allows the EE
     * to do periodic non-transactional work.
     * @param time The current time in milliseconds since the epoch. See
     * System.currentTimeMillis();
     */
    @Override
    public void tick(final long time, final long lastCommittedTxnId) {
        nativeTick(this.pointer, time, lastCommittedTxnId);
    }

    @Override
    public void quiesce(long lastCommittedTxnId) {
        nativeQuiesce(this.pointer, lastCommittedTxnId);
    }

    /**
     * Retrieve a set of statistics using the specified selector from the StatisticsSelector enum.
     * @param selector Selector from StatisticsSelector specifying what statistics to retrieve
     * @param locators CatalogIds specifying what set of items the stats should come from.
     * @param interval Return counters since the beginning or since this method was last invoked
     * @param now Timestamp to return with each row
     * @return Array of results tables. An array of length 0 indicates there are no results. On error, an EEException will be thrown.
     */
    @Override
    public VoltTable[] getStats(
            final SysProcSelector selector,
            final int locators[],
            final boolean interval,
            final Long now)
    {
        deserializer.clear();
        final int numResults = nativeGetStats(this.pointer, selector.ordinal(), locators, interval, now);
        if (numResults == -1) {
            throwExceptionForError(ERRORCODE_ERROR);
        }


        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
                results[ii] = (VoltTable)deserializer.readObject(resultTable, this);
            }
            return results;
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for getStats" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
    public int toggleProfiler(final int toggle) {
        return nativeToggleProfiler(this.pointer, toggle);
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        return nativeReleaseUndoToken(this.pointer, undoToken);
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        return nativeUndoUndoToken(this.pointer, undoToken);
    }

    /**
     * Set the log levels to be used when logging in this engine
     * @param logLevels Levels to set
     * @throws EEException
     * @returns true on success false on failure
     */
    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        return nativeSetLogLevels( pointer, logLevels);
    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType streamType) {
        return nativeActivateTableStream( pointer, tableId, streamType.ordinal());
    }

    @Override
    public int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType streamType) {
        return nativeTableStreamSerializeMore(this.pointer, c.address, c.b.position(), c.b.remaining(), tableId, streamType.ordinal());
    }

    /**
     * Instruct the EE to execute an Export poll and/or ack action. Poll response
     * data is returned in the usual results buffer, length preceded as usual.
     */
    @Override
    public ExportProtoMessage exportAction(boolean ackAction, boolean pollAction,
            boolean resetAction, boolean syncAction,
            long ackTxnId, long seqNo, int partitionId, long tableId)
    {
        deserializer.clear();
        ExportProtoMessage result = null;
        try {
            long offset = nativeExportAction(this.pointer, ackAction, pollAction, resetAction,
                                             syncAction, ackTxnId, seqNo, tableId);
            if (offset < 0) {
                result = new ExportProtoMessage(partitionId, tableId);
                result.error();
            }
            else if (pollAction) {
                ByteBuffer b;
                int byteLen = deserializer.readInt();
                if (byteLen < 0) {
                    throw new IOException("Invalid length in Export poll response results.");
                }

                // need to keep the embedded length in the resulting buffer.
                // the buffer's embedded length prefix is not self-inclusive,
                // so add it back to the byteLen.
                deserializer.buffer().position(0);
                b = deserializer.readBuffer(byteLen + 4);
                result = new ExportProtoMessage(partitionId, tableId);
                result.pollResponse(offset, b);
            }
        }
        catch (IOException e) {
            // TODO: Not going to rollback here so EEException seems wrong?
            // Seems to indicate invalid Export data which should be hard error?
            // Maybe this should be crashVoltDB?
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void processRecoveryMessage( ByteBuffer buffer, long bufferPointer) {
        nativeProcessRecoveryMessage( pointer, bufferPointer, buffer.position(), buffer.remaining());
    }

    @Override
    public long tableHashCode(int tableId) {
        return nativeTableHashCode( pointer, tableId);
    }

    @Override
    public int hashinate(Object value, int partitionCount) {
        ParameterSet parameterSet = new ParameterSet(true);
        parameterSet.setParameters(value);

        // serialize the param set
        fsForParameterSet.clear();
        try {
            parameterSet.writeExternal(fsForParameterSet);
        } catch (final IOException exception) {
            throw new RuntimeException(exception); // can't happen
        }

        return nativeHashinate(this.pointer, partitionCount);
    }
    
    // ----------------------------------------------------------------------------
    // READ/WRITE SET TRACKING
    // ----------------------------------------------------------------------------
    
    @Override
    public void trackingEnable(Long txnId) throws EEException {
        if (debug.val)
            LOG.debug(String.format("Enabling read/write set tracking for txn #%d at partition %d",
                      txnId, this.executor.getPartitionId()));
        final int errorCode = nativeTrackingEnable(this.pointer, txnId.longValue());
        checkErrorCode(errorCode);
    }
    
    @Override
    public void trackingFinish(Long txnId) throws EEException {
        if (debug.val)
            LOG.debug(String.format("Deleting read/write set tracker for txn #%d at partition %d",
                      txnId, this.executor.getPartitionId()));
        this.trackingRemoveCacheEntry(txnId);
        final int errorCode = nativeTrackingFinish(this.pointer, txnId.longValue());
        checkErrorCode(errorCode);
    }
    
    @Override
    public VoltTable trackingReadSet(Long txnId) throws EEException {
        if (debug.val)
            LOG.debug(String.format("Get READ tracking set for txn #%d at partition %d",
                      txnId, this.executor.getPartitionId()));
        
        // Always check our cache first
        VoltTable cache[] = this.trackingGetCacheEntry(txnId);
        if (cache[0] != null) return (cache[0]);
        
        deserializer.clear();
        final int errorCode = nativeTrackingReadSet(this.pointer, txnId.longValue());
        if (errorCode == ERRORCODE_NO_DATA) {
//            if (debug.val)
                LOG.warn(String.format("No READ tracking set for txn #%d at partition %d",
                         txnId, this.executor.getPartitionId()));
            return (null);
        } else checkErrorCode(errorCode);
        
        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
            cache[0] = (VoltTable)deserializer.readObject(resultTable, this);
            return (cache[0]);
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for getStats" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }
    
    @Override
    public VoltTable trackingWriteSet(Long txnId) throws EEException {
        if (debug.val)
            LOG.debug(String.format("Get WRITE tracking set for txn #%d at partition %d",
                      txnId, this.executor.getPartitionId()));
        
        // Always check our cache first
        VoltTable cache[] = this.trackingGetCacheEntry(txnId);
        if (cache[1] != null) return (cache[1]);
        
        deserializer.clear();
        final int errorCode = nativeTrackingWriteSet(this.pointer, txnId.longValue());
        if (errorCode == ERRORCODE_NO_DATA) {
//            if (debug.val)
                LOG.warn(String.format("No WRITE tracking set for txn #%d at partition %d",
                         txnId, this.executor.getPartitionId()));
            return (null);
        } else checkErrorCode(errorCode);
        
        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
            cache[1] = (VoltTable)deserializer.readObject(resultTable, this);
            return (cache[1]);
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for getStats" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }
    
    private final void trackingRemoveCacheEntry(Long txnId) {
        if (this.trackingCache != null) {
            this.trackingCache.remove(txnId);
        }
    }
    
    private final void trackingResetCacheEntry(Long txnId) {
        if (this.trackingCache != null) {
            VoltTable ret[] = this.trackingGetCacheEntry(txnId);
            ret[0] = null;
            ret[1] = null;
        }
    }
    
    private final VoltTable[] trackingGetCacheEntry(Long txnId) {
        if (this.trackingCache == null) {
            this.trackingCache = new HashMap<Long, VoltTable[]>();
        }
        VoltTable ret[] = this.trackingCache.get(txnId);
        if (ret == null) {
            ret = new VoltTable[2];
            this.trackingCache.put(txnId, ret);
        }
        return (ret);
    }
    
    
    // ----------------------------------------------------------------------------
    // ANTI-CACHING
    // ----------------------------------------------------------------------------

    @Override
    public void antiCacheInitialize(File dbDir, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException {
        assert(m_anticache == false);

        // TODO: Switch to LOG.debug
        if (debug.val) {
            LOG.debug("Initializing anti-cache feature at partition " + this.executor.getPartitionId());
            LOG.debug("****************");
            LOG.debug(String.format("Partition #%d AntiCache Directory: %s",
                      this.executor.getPartitionId(), dbDir.getAbsolutePath()));
            LOG.debug(String.format("AntiCacheDBType: %d", dbType.ordinal()));
        }
      
        final int errorCode = nativeAntiCacheInitialize(this.pointer, dbDir.getAbsolutePath(), blockSize, dbType.ordinal(), blocking,  maxSize, blockMerge);
        checkErrorCode(errorCode);
        m_anticache = true;
    }

    @Override
    public void antiCacheAddDB(File dbDir, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException {
        assert(m_anticache == true);
        final int errorCode = nativeAntiCacheAddDB(this.pointer, dbDir.getAbsolutePath(), blockSize, dbType.ordinal(), blocking, maxSize, blockMerge);
        checkErrorCode(errorCode);
    }

    
    @Override
    public void antiCacheReadBlocks(Table catalog_tbl, int[] block_ids, int[] tuple_offsets) {
        if (m_anticache == false) {
            String msg = "Trying to invoke anti-caching operation but feature is not enabled";
            throw new VoltProcedure.VoltAbortException(msg);
        }
        if (debug.val)
            LOG.debug(String.format("Reading %d evicted tuples across %d blocks for table %s",
                                    tuple_offsets.length, block_ids.length, catalog_tbl.getName()));
        final int errorCode = nativeAntiCacheReadBlocks(this.pointer, catalog_tbl.getRelativeIndex(), block_ids, tuple_offsets);
        checkErrorCode(errorCode);
    }
    
    @Override
    public VoltTable antiCacheEvictBlock(Table catalog_tbl, long block_size, int num_blocks) {
        if (m_anticache == false) {
            String msg = "Trying to invoke anti-caching operation but feature is not enabled";
            throw new VoltProcedure.VoltAbortException(msg);
        }
        deserializer.clear();
        
        final int numResults = nativeAntiCacheEvictBlock(this.pointer, catalog_tbl.getRelativeIndex(), block_size, num_blocks);
        if (numResults == -1) {
            LOG.error("Unexpected error in antiCacheEvictBlock for table " + catalog_tbl.getName());
            throwExceptionForError(ERRORCODE_ERROR);
        }
        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
                results[ii] = (VoltTable)deserializer.readObject(resultTable, this);
            }
            return results[0];
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for antiCacheEvictBlock" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
    }

    @Override
	public VoltTable antiCacheEvictBlockInBatch(Table catalog_tbl,
			Table childTable, long block_size, int num_blocks) {
        if (m_anticache == false) {
            String msg = "Trying to invoke anti-caching operation but feature is not enabled";
            throw new VoltProcedure.VoltAbortException(msg);
        }
        deserializer.clear();
        
        final int numResults = nativeAntiCacheEvictBlockInBatch(this.pointer, catalog_tbl.getRelativeIndex(), childTable.getRelativeIndex(), block_size, num_blocks);
        if (numResults == -1) {
            LOG.error("Unexpected error in antiCacheEvictBlockInBatch for table " + catalog_tbl.getName());
            throwExceptionForError(ERRORCODE_ERROR);
        }
        try {
            deserializer.readInt();//Ignore the length of the result tables
            final VoltTable results[] = new VoltTable[numResults];
            for (int ii = 0; ii < numResults; ii++) {
                final VoltTable resultTable = PrivateVoltTableFactory.createUninitializedVoltTable();
                results[ii] = (VoltTable)deserializer.readObject(resultTable, this);
            }
            return results[0];
        } catch (final IOException ex) {
            LOG.error("Failed to deserialze result table for antiCacheEvictBlock" + ex);
            throw new EEException(ERRORCODE_WRONG_SERIALIZED_BYTES);
        }
	}
    
    @Override
    public void antiCacheMergeBlocks(Table catalog_tbl) {
        assert(m_anticache);
        final int errorCode = nativeAntiCacheMergeBlocks(this.pointer, catalog_tbl.getRelativeIndex());
        checkErrorCode(errorCode);
    }

    
    /*
     * MMAP STORAGE
     */
    
    @Override
    public void MMAPInitialize(File dbDir, long mapSize, long syncFrequency) throws EEException {
        
        LOG.info("Initializing storage mmap feature at partition " + this.executor.getPartitionId());
        LOG.info(String.format("Partition #%d MMAP Directory: %s",
                 this.executor.getPartitionId(), dbDir.getAbsolutePath()));
        final int errorCode = nativeMMAPInitialize(this.pointer, dbDir.getAbsolutePath(), mapSize, syncFrequency);
        checkErrorCode(errorCode);
        m_anticache = true;
    }

    /*
     * ARIES
     */
    
    @Override
    public void ARIESInitialize(File dbDir, File logFile) throws EEException {
        if (debug.val) {
            LOG.debug("Initializing ARIES feature at partition " + this.executor.getPartitionId());
            LOG.debug(String.format("Partition #%d ARIES Directory: %s",
                      this.executor.getPartitionId(), dbDir.getAbsolutePath()));
        }
        final int errorCode = nativeARIESInitialize(this.pointer, dbDir.getAbsolutePath(), logFile.getAbsolutePath());
        checkErrorCode(errorCode);
        m_anticache = true;
    }
    
    @Override
    public void doAriesRecoveryPhase(long replayPointer, long replayLogSize, long replayTxnId) {
        if (debug.val)
            LOG.debug("do ARIES Recovery at partition " + this.executor.getPartitionId());
        nativeDoAriesRecoveryPhase(pointer, replayPointer, replayLogSize, replayTxnId);
    }
    
    @Override
    public long getArieslogBufferLength() {
        return nativeGetArieslogBufferLength(pointer);
    }

    @Override
    public void getArieslogData(int bufferLength, byte[] arieslogDataArray) {
        // rewind this buffer to be able to copy from start
        // XXX: The native methods apparently keep their own offset
        // and unless they are rewinded as well after each call, massive
        // disaster might ensue.
        ariesLogBuffer.rewind();
        ariesLogBuffer.get(arieslogDataArray, 0, bufferLength);

        // now that the data for a transaction
        // has been copied out, time to rewind the C++
        // pointers as well.
        nativeRewindArieslogBuffer(pointer);
    }

    @Override
    public void freePointerToReplayLog(long ariesReplayPointer) {
        nativeFreePointerToReplayLog(pointer, ariesReplayPointer);
    }

    @Override
    public long readAriesLogForReplay(long[] size) {
        return nativeReadAriesLogForReplay(pointer, size);
    }

}
