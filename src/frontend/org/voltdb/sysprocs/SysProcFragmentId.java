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

public class SysProcFragmentId
{
    // @LastCommittedTransaction
    public static final int PF_lastCommittedScan = 1;
    public static final int PF_lastCommittedResults = 2;

    // @UpdateLogging
    public static final int PF_updateLoggers = 3;

    // @Statistics
    public static final int PF_tableData = 11;
    public static final int PF_tableAggregator = 12;
    public static final int PF_procedureData = 13;
    public static final int PF_procedureAggregator = 14;
    public static final int PF_initiatorData = 15;
    public static final int PF_initiatorAggregator = 16;
    public static final int PF_partitionCount = 17;
    public static final int PF_ioData = 18;
    public static final int PF_ioDataAggregator = 19;

    // @Shutdown
    public static final int PF_shutdownCommand = 21;
    public static final int PF_procedureDone = 22;

    // @AdHoc
    public static final int PF_runAdHocFragment = 31;

    // @SnapshotSave
    /*
     * Once per host confirm the file is accessible
     */
    public static final int PF_saveTest = 40;
    /*
     * Agg test results
     */
    public static final int PF_saveTestResults = 41;
    /*
    * Create and distribute tasks and targets to each EE
    */
    public static final int PF_createSnapshotTargets = 42;
    /*
    * Confirm the targets were successfully created
    */
    public static final int PF_createSnapshotTargetsResults = 43;
    /*
    * Quiesce the export data as part of the snapshot
    */
    public static final int PF_snapshotSaveQuiesce = 44;
    /*
    * Aggregate the results of snapshot quiesce
    */
    public static final int PF_snapshotSaveQuiesceResults = 45;

    // @LoadMultipartitionTable
    public static final int PF_loadDistribute = 50;
    public static final int PF_loadAggregate = 51;
    
    // @SnapshotRestore
    public static final int PF_restoreScan = 60;
    public static final int PF_restoreScanResults = 61;
    public static final int PF_restoreLoadReplicatedTable = 62;
    public static final int PF_restoreLoadReplicatedTableResults = 63;
    public static final int PF_restoreDistributeReplicatedTable = 64;
    public static final int PF_restoreDistributePartitionedTable = 65;
    public static final int PF_restoreDistributePartitionedTableResults = 66;
    public static final int PF_restoreSendReplicatedTable = 67;
    public static final int PF_restoreSendReplicatedTableResults = 68;
    public static final int PF_restoreSendPartitionedTable = 69;
    public static final int PF_restoreSendPartitionedTableResults = 70;

    // @StartSampler
    public static final int PF_startSampler = 80;

    // @SystemInformation
    public static final int PF_systemInformation_distribute = 90;
    public static final int PF_systemInformation_aggregate = 91;

    // @Quiesce
    public static final int PF_quiesce_sites = 100;
    public static final int PF_quiesce_processed_sites = 101;
    
    // @SnapshotStatus
    public static final long PF_scanSnapshotRegistries = 110;
    public static final long PF_scanSnapshotRegistriesResults = 111;

    // @SnapshotScan
    public static final int PF_snapshotDigestScan = 124;
    public static final int PF_snapshotDigestScanResults = 125;
    public static final int PF_snapshotScan = 120;
    public static final int PF_snapshotScanResults = 121;
    public static final int PF_hostDiskFreeScan = 122;
    public static final int PF_hostDiskFreeScanResults = 123;

    // @SnapshotScan
    public static final int PF_snapshotDelete = 130;
    public static final int PF_snapshotDeleteResults = 131;

    // @InstanceId
    public static final int PF_retrieveInstanceId = 160;
    public static final int PF_retrieveInstanceIdAggregator = 161;
    public static final int PF_setInstanceId = 162;
    public static final int PF_setInstanceIdAggregator = 163;
    
    // @DatabaseDump
    public static final int PF_dumpDistribute = 170;
    public static final int PF_dumpAggregate = 171;
    
    // @RecomputeMarkovs
    public static final int PF_recomputeMarkovsDistribute = 180;
    public static final int PF_recomputeMarkovsAggregate = 181;

    // @GarbageCollection
    public static final int PF_gcDistribute = 185;
    public static final int PF_gcAggregate = 186;
    
    // @ExecutorStatus
    public static final int PF_execStatus = 190;
    
}
