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

package org.voltdb.sysprocs;

public class SysProcFragmentId {
    // @LastCommittedTransaction
    public static final int PF_lastCommittedScan = 1;
    public static final int PF_lastCommittedResults = 2;

    // @UpdateLogging
    public static final int PF_updateLoggers = 3;

    // @Statistics
    public static final int PF_starvationData = 4;
    public static final int PF_starvationDataAggregator = 5;
    public static final int PF_tableData = 6;
    public static final int PF_tableAggregator = 7;
    public static final int PF_indexData = 8;
    public static final int PF_indexAggregator = 9;
    public static final int PF_nodeMemory = 10;
    public static final int PF_nodeMemoryAggregator = 11;
    public static final int PF_procedureData = 13;
    public static final int PF_procedureAggregator = 14;
    public static final int PF_initiatorData = 15;
    public static final int PF_initiatorAggregator = 16;
    public static final int PF_partitionCount = 17;
    public static final int PF_ioData = 18;
    public static final int PF_ioDataAggregator = 19;
    public static final int PF_txnCounterData = 22;
    public static final int PF_txnCounterAggregator = 23;
    public static final int PF_txnProfilerData = 24;
    public static final int PF_txnProfilerAggregator = 25;
    public static final int PF_execProfilerData = 26;
    public static final int PF_execProfilerAggregator = 27;
    public static final int PF_queueProfilerData = 28;
    public static final int PF_queueProfilerAggregator = 29;
    public static final int PF_markovProfilerData = 30;
    public static final int PF_markovProfilerAggregator = 31;
    public static final int PF_specexecProfilerData = 32;
    public static final int PF_specexecProfilerAggregator = 33;
    public static final int PF_siteProfilerData = 34;
    public static final int PF_siteProfilerAggregator = 35;
    public static final int PF_plannerProfilerData = 36;
    public static final int PF_plannerProfilerAggregator = 37;
    public static final int PF_anticacheProfilerData = 38;
    public static final int PF_anticacheProfilerAggregator = 39;
    public static final int PF_anticacheMemoryData = 40;
    public static final int PF_anticacheMemoryAggregator = 41;


    // @Shutdown
    public static final int PF_shutdownCommand = 50;
    public static final int PF_procedureDone = 51;

    // @AdHoc
    public static final int PF_runAdHocFragment = 55;

    // @SnapshotSave
    /*
     * Once per host confirm the file is accessible
     */
    public static final int PF_saveTest = 60;
    /*
     * Agg test results
     */
    public static final int PF_saveTestResults = 61;
    /*
    * Create and distribute tasks and targets to each EE
    */
    public static final int PF_createSnapshotTargets = 62;
    /*
    * Confirm the targets were successfully created
    */
    public static final int PF_createSnapshotTargetsResults = 63;
    /*
    * Quiesce the export data as part of the snapshot
    */
    public static final int PF_snapshotSaveQuiesce = 64;
    /*
    * Aggregate the results of snapshot quiesce
    */
    public static final int PF_snapshotSaveQuiesceResults = 65;

    // @LoadMultipartitionTable
    public static final int PF_loadDistribute = 70;
    public static final int PF_loadAggregate = 71;
    
    // @SnapshotRestoreLocal
    public static final int PF_SRLloadDistribute = 75;
    public static final int PF_SRLloadAggregate = 76;
    public static final int PF_SRLrestoreScan = 77;
    public static final int PF_SRLrestoreScanResults = 78;
    
    // @SnapshotRestore
    public static final int PF_restoreScan = 80;
    public static final int PF_restoreScanResults = 81;
    public static final int PF_restoreLoadReplicatedTable = 82;
    public static final int PF_restoreLoadReplicatedTableResults = 83;
    public static final int PF_restoreDistributeReplicatedTable = 84;
    public static final int PF_restoreDistributePartitionedTable = 85;
    public static final int PF_restoreDistributePartitionedTableResults = 86;
    public static final int PF_restoreSendReplicatedTable = 87;
    public static final int PF_restoreSendReplicatedTableResults = 88;
    public static final int PF_restoreSendPartitionedTable = 89;
    public static final int PF_restoreSendPartitionedTableResults = 90;

    // @StartSampler
    public static final int PF_startSampler = 100;

    // @SystemInformation
    public static final int PF_systemInformation_distribute = 110;
    public static final int PF_systemInformation_aggregate = 111;

    // @Quiesce
    public static final int PF_quiesceDistribute = 120;
    public static final int PF_quiesceAggregate = 121;
    
    // @SnapshotStatus
    public static final int PF_scanSnapshotRegistries = 130;
    public static final int PF_scanSnapshotRegistriesResults = 131;

    // @SnapshotScan
    public static final int PF_snapshotDigestScan = 144;
    public static final int PF_snapshotDigestScanResults = 145;
    public static final int PF_snapshotScan = 150;
    public static final int PF_snapshotScanResults = 151;
    public static final int PF_hostDiskFreeScan = 152;
    public static final int PF_hostDiskFreeScanResults = 153;

    // @SnapshotScan
    public static final int PF_snapshotDelete = 160;
    public static final int PF_snapshotDeleteResults = 161;

    // @InstanceId
    public static final int PF_retrieveInstanceId = 170;
    public static final int PF_retrieveInstanceIdAggregator = 171;
    public static final int PF_setInstanceId = 172;
    public static final int PF_setInstanceIdAggregator = 173;
    
    // @DatabaseDump
    public static final int PF_dumpDistribute = 180;
    public static final int PF_dumpAggregate = 181;
    
    // @RecomputeMarkovs
    public static final int PF_recomputeMarkovsDistribute = 185;
    public static final int PF_recomputeMarkovsAggregate = 186;

    // @GarbageCollection
    public static final int PF_gcDistribute = 190;
    public static final int PF_gcAggregate = 191;
    
    // @ExecutorStatus
    public static final int PF_execStatus = 195;
    
    // @GetCatalog
    public static final int PF_getCatalog = 200;
    
    // @ResetStats
    public static final int PF_resetProfilingDistribute = 205;
    public static final int PF_resetProfilingAggregate = 206;
    
    // @AntiCacheEviction
    public static final int PF_antiCacheEviction = 210;
    public static final int PF_anitCacheHistoryDistribute = 211;
    public static final int PF_anitCacheHistoryAggregate = 212;
    
    // @EvictedAccessHistory
    public static final int PF_anitCacheAccessDistribute = 213;
    public static final int PF_anitCacheAccessAggregate = 214;
    
    // @SetConfiguration
    public static final int PF_setConfDistribute = 300;
    public static final int PF_setConfAggregate = 301;
}
