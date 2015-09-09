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

package org.voltdb.jni;

import java.io.File;

import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.SysProcSelector;
import org.voltdb.TableStreamType;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.EEException;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.utils.NotImplementedException;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.types.AntiCacheDBType;

public class MockExecutionEngine extends ExecutionEngine {

    public MockExecutionEngine() {
        super(null);
    }

    @Override
    public DependencySet executePlanFragment(final long planFragmentId, int outputDepId,
            int inputDepIdfinal, ParameterSet parameterSet, final long txnId,
            final long lastCommittedTxnId, final long undoToken) throws EEException
    {
        // Create a mocked up dependency pair. Required by some tests
        VoltTable vt;
        vt = new VoltTable(new ColumnInfo[] {
                           new ColumnInfo("foo", VoltType.INTEGER)});
        vt.addRow(Integer.valueOf(1));
        return new DependencySet(outputDepId, vt);
    }

    @Override
    public DependencySet executeQueryPlanFragmentsAndGetDependencySet(
            long[] planFragmentIds,
            int numFragmentIds,
            int[] input_depIds,
            int[] output_depIds,
            ParameterSet[] parameterSets,
            int numParameterSets,
            long txnId, long lastCommittedTxnId, long undoToken) throws EEException {
        
        // TODO
        return (null);
    }

    @Override
    public VoltTable executeCustomPlanFragment(final String plan, int outputDepId,
            int inputDepId, final long txnId, final long lastCommittedTxnId, final long undoQuantumToken)
            throws EEException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public VoltTable[] getStats(final SysProcSelector selector, final int[] locators, boolean interval, Long now) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void loadCatalog(final String serializedCatalog) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void updateCatalog(final String catalogDiffs, int catalogVersion) throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public void loadTable(final int tableId, final VoltTable table, final long txnId,
        final long lastCommittedTxnId, final long undoToken, final boolean allowExport)
    throws EEException
    {
        // TODO Auto-generated method stub
    }

    @Override
    public void release() throws EEException {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean releaseUndoToken(final long undoToken) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public VoltTable serializeTable(final int tableId) throws EEException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void tick(final long time, final long lastCommittedTxnId) {
        // TODO Auto-generated method stub
    }

    @Override
    public int toggleProfiler(final int toggle) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean undoUndoToken(final long undoToken) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean setLogLevels(final long logLevels) throws EEException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void quiesce(long lastCommittedTxnId) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean activateTableStream(int tableId, TableStreamType type) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int tableStreamSerializeMore(BBContainer c, int tableId, TableStreamType type) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ExportProtoMessage exportAction(boolean ackAction, boolean pollAction,
            boolean resetAction, boolean syncAction,
            long ackOffset, long seqNo, int partitionId, long mTableId) {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public void processRecoveryMessage( java.nio.ByteBuffer buffer, long pointer) {
        // TODO Auto-generated method stub

    }

    @Override
    public long tableHashCode( int tableId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashinate(Object value, int partitionCount) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void trackingEnable(Long txnId) throws EEException {
        // TODO Auto-generated method stub
    }
    @Override
    public void trackingFinish(Long txnId) throws EEException {
        // TODO Auto-generated method stub
    }
    @Override
    public VoltTable trackingReadSet(Long txnId) throws EEException {
        // TODO Auto-generated method stub
        return null;
    }
    @Override
    public VoltTable trackingWriteSet(Long txnId) throws EEException {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void antiCacheInitialize(File dbFilePath, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException {
        // TODO Auto-generated method stub
    }
    
    @Override
    public void antiCacheAddDB(File dbFilePath, AntiCacheDBType dbType, boolean blocking, long blockSize, long maxSize, boolean blockMerge) throws EEException {
    }

    @Override
    public void antiCacheReadBlocks(Table catalog_tbl, int[] block_ids, int[] tuple_offsets) {
        // TODO Auto-generated method stub
    }
    @Override
    public void antiCacheMergeBlocks(Table catalog_tbl) {
        // TODO Auto-generated method stub
    }

    @Override
    public VoltTable antiCacheEvictBlock(Table catalog_tbl, long block_size, int num_blocks) {
        // TODO Auto-generated method stub
        return (null);
    }


	@Override
	public VoltTable antiCacheEvictBlockInBatch(Table catalog_tbl,
			Table childTable, long block_size, int num_blocks) {
		// TODO Auto-generated method stub
		return null;
	}

    
    @Override
    public void MMAPInitialize(File dbDir, long mapSize, long syncFrequency) throws EEException {
     // TODO Auto-generated method stub        
    }
    
    @Override
    public void ARIESInitialize(File dbDir, File logFile) throws EEException {
     // TODO Auto-generated method stub        
    }

    @Override
    public long getArieslogBufferLength() { 
    // XXX: do nothing, we only implement this for JNI now.
        return 0;
    }

    @Override
    public void getArieslogData(int bufferLength, byte[] arieslogDataArray) { 
    // XXX: do nothing, we only implement this for JNI now.
    }

    @Override
    public void doAriesRecoveryPhase(long replayPointer, long replayLogSize, long replayTxnId) { 
    // TODO Auto-generated method stub
    }

    @Override
    public void freePointerToReplayLog(long ariesReplayPointer) {
        // TODO Auto-generated method stub
    }

    @Override
    public long readAriesLogForReplay(long[] size) {
        // TODO Auto-generated method stub
        return 0;
    }
    

}
