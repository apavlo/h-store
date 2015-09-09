/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.SysProcSelector;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Index;
import org.voltdb.exceptions.ServerFaultException;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTableUtil;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.internal.UtilityWorkMessage.UpdateMemoryMessage;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

/**
 * Access the TABLE, PRCOEDURE, INITIATOR, IOSTATS, or PARTITIONCOUNT statistics.
 */
@ProcInfo(
    // partitionInfo = "TABLE.ATTR: 0",
    singlePartition = false
)

public class Statistics extends VoltSystemProcedure {
    private static final Logger HOST_LOG = Logger.getLogger(Statistics.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(HOST_LOG, debug, trace);
    }

    static final int DEP_tableData = (int)
        SysProcFragmentId.PF_tableData | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_tableAggregator = (int) SysProcFragmentId.PF_tableAggregator;

    static final int DEP_procedureData = (int)
        SysProcFragmentId.PF_procedureData | HStoreConstants.MULTIPARTITION_DEPENDENCY;

    static final int DEP_procedureAggregator = (int)
        SysProcFragmentId.PF_procedureAggregator;

    static final int DEP_initiatorData = (int)
        SysProcFragmentId.PF_initiatorData | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_initiatorAggregator = (int)
        SysProcFragmentId.PF_initiatorAggregator;

    static final int DEP_ioData = (int)
        SysProcFragmentId.PF_ioData | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_ioDataAggregator = (int)
        SysProcFragmentId.PF_ioDataAggregator;

    static final int DEP_partitionCount = (int)
        SysProcFragmentId.PF_partitionCount;

    static final int DEP_indexData = (int)
        SysProcFragmentId.PF_indexData | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    static final int DEP_indexAggregator = (int) SysProcFragmentId.PF_indexAggregator;

    /**
     * DataSysProcFragmentId -> <SysProcSelector, AggSysProcFragmentId>
     */
    private static final Map<Integer, Pair<SysProcSelector, Integer>> STATS_DATA = new HashMap<Integer, Pair<SysProcSelector,Integer>>();
    private static final void addStatsFragments(SysProcSelector selector, int dataFragmentId, int aggFragmentId) {
        STATS_DATA.put(dataFragmentId, Pair.of(selector, aggFragmentId));
    }
    
    static {
        addStatsFragments(SysProcSelector.MEMORY, SysProcFragmentId.PF_nodeMemory, SysProcFragmentId.PF_nodeMemoryAggregator);
        addStatsFragments(SysProcSelector.TXNCOUNTER, SysProcFragmentId.PF_txnCounterData, SysProcFragmentId.PF_txnCounterAggregator);
        addStatsFragments(SysProcSelector.TXNPROFILER, SysProcFragmentId.PF_txnProfilerData, SysProcFragmentId.PF_txnProfilerAggregator);
        addStatsFragments(SysProcSelector.EXECPROFILER, SysProcFragmentId.PF_execProfilerData, SysProcFragmentId.PF_execProfilerAggregator);
        addStatsFragments(SysProcSelector.QUEUEPROFILER, SysProcFragmentId.PF_queueProfilerData, SysProcFragmentId.PF_queueProfilerAggregator);
        addStatsFragments(SysProcSelector.MARKOVPROFILER, SysProcFragmentId.PF_markovProfilerData, SysProcFragmentId.PF_markovProfilerAggregator);
        addStatsFragments(SysProcSelector.SPECEXECPROFILER, SysProcFragmentId.PF_specexecProfilerData, SysProcFragmentId.PF_specexecProfilerAggregator);
        addStatsFragments(SysProcSelector.SITEPROFILER, SysProcFragmentId.PF_siteProfilerData, SysProcFragmentId.PF_siteProfilerAggregator);
        addStatsFragments(SysProcSelector.PLANNERPROFILER, SysProcFragmentId.PF_plannerProfilerData, SysProcFragmentId.PF_plannerProfilerAggregator);
        addStatsFragments(SysProcSelector.ANTICACHE, SysProcFragmentId.PF_anticacheProfilerData, SysProcFragmentId.PF_anticacheProfilerAggregator);
        addStatsFragments(SysProcSelector.MULTITIER_ANTICACHE, SysProcFragmentId.PF_anticacheMemoryData, SysProcFragmentId.PF_anticacheMemoryAggregator);
    } // STATIC
    
    @Override
    public void initImpl() {
        registerPlanFragment(SysProcFragmentId.PF_tableData);
        registerPlanFragment(SysProcFragmentId.PF_tableAggregator);
        registerPlanFragment(SysProcFragmentId.PF_procedureData);
        registerPlanFragment(SysProcFragmentId.PF_procedureAggregator);
        registerPlanFragment(SysProcFragmentId.PF_initiatorData);
        registerPlanFragment(SysProcFragmentId.PF_initiatorAggregator);
        registerPlanFragment(SysProcFragmentId.PF_partitionCount);
        registerPlanFragment(SysProcFragmentId.PF_ioData);
        registerPlanFragment(SysProcFragmentId.PF_ioDataAggregator);
        
        registerPlanFragment(SysProcFragmentId.PF_indexData);
        registerPlanFragment(SysProcFragmentId.PF_indexAggregator);

        // Automatically register our STATS_DATA entries
        for (Integer id : STATS_DATA.keySet()) {
            registerPlanFragment(id.intValue());
            registerPlanFragment(STATS_DATA.get(id).getSecond().intValue());
        } // FOR
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             SystemProcedureExecutionContext context) {
        switch (fragmentId) {
            // ----------------------------------------------------------------------------
            // PROFILER DATA COLLECTION
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_nodeMemory: {
                // Tell the PartitionExecutors to update their memory stats
                this.executor.queueUtilityWork(new UpdateMemoryMessage());
            }
            case SysProcFragmentId.PF_txnCounterData:
            case SysProcFragmentId.PF_txnProfilerData:
            case SysProcFragmentId.PF_execProfilerData:
            case SysProcFragmentId.PF_queueProfilerData:
            case SysProcFragmentId.PF_markovProfilerData:
            case SysProcFragmentId.PF_specexecProfilerData:
            case SysProcFragmentId.PF_siteProfilerData:
            case SysProcFragmentId.PF_plannerProfilerData:
            case SysProcFragmentId.PF_anticacheProfilerData:
            case SysProcFragmentId.PF_anticacheMemoryData: {
                assert(params.toArray().length == 2);
                final boolean interval =
                    ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                final Long now = (Long)params.toArray()[1];
                ArrayList<Integer> catalogIds = new ArrayList<Integer>();
                catalogIds.add(0);
                
                VoltTable result = null;

                // Choose the lowest site ID on this host to do the scan
                // All other sites should just return empty results tables.
                if (this.isFirstLocalPartition()) {
                    Pair<SysProcSelector, Integer> pair = STATS_DATA.get(fragmentId);
                    result = executor.getHStoreSite().getStatsAgent().getStats(
                                    pair.getFirst(),
                                    catalogIds,
                                    interval,
                                    now);
                    if (debug.val) HOST_LOG.debug(pair.getFirst() + ":\n" + result);
                    assert(result.getRowCount() >= 0);
                }
                else {
                    String msg = String.format("Unexpected execution of SysProc #%d on partition %d",
                                               fragmentId, this.partitionId);
                    throw new ServerFaultException(msg, txn_id);
                }
                return new DependencySet(fragmentId, result);
            }
            // ----------------------------------------------------------------------------
            // PROFILER DATA AGGREGATION
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_nodeMemoryAggregator:
            case SysProcFragmentId.PF_txnCounterAggregator:
            case SysProcFragmentId.PF_txnProfilerAggregator:
            case SysProcFragmentId.PF_execProfilerAggregator:
            case SysProcFragmentId.PF_queueProfilerAggregator:
            case SysProcFragmentId.PF_markovProfilerAggregator:
            case SysProcFragmentId.PF_specexecProfilerAggregator:
            case SysProcFragmentId.PF_siteProfilerAggregator:
            case SysProcFragmentId.PF_plannerProfilerAggregator:
            case SysProcFragmentId.PF_anticacheProfilerAggregator:
            case SysProcFragmentId.PF_anticacheMemoryAggregator: {
                // Do a reverse look up to find the input dependency id
                int dataFragmentId = -1;
                for (Integer id : STATS_DATA.keySet()) {
                    Pair<SysProcSelector, Integer> pair = STATS_DATA.get(id);
                    if (pair.getSecond().equals(fragmentId)) {
                        dataFragmentId = id.intValue();
                        break;
                    }
                } // FOR
                if (dataFragmentId == -1) {
                    String msg = "Failed to find input data dependency for SysProc #" + fragmentId;
                    throw new ServerFaultException(msg, txn_id);
                }
                VoltTable result = VoltTableUtil.union(dependencies.get(dataFragmentId));
                return new DependencySet(fragmentId, result);
            }
            
            // ----------------------------------------------------------------------------
            //  TABLE statistics
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_tableData: {
                assert(params.toArray().length == 2);
                final boolean interval =
                    ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                final Long now = (Long)params.toArray()[1];
                // create an array of the table ids for which statistics are required.
                // pass this to EE owned by the execution site running this plan fragment.
                CatalogMap<Table> tables = context.getDatabase().getTables();
                int[] tableGuids = new int[tables.size()];
                int ii = 0;
                for (Table table : tables) {
                    tableGuids[ii++] = table.getRelativeIndex();
                    //System.err.println("TABLE ID: " + table.getRelativeIndex());
                }
                VoltTable result = executor.getExecutionEngine().getStats(
                            SysProcSelector.TABLE,
                            tableGuids,
                            interval,
                            now)[0];
                return new DependencySet(DEP_tableData, result);
            }
            case SysProcFragmentId.PF_tableAggregator: {
                VoltTable result = VoltTableUtil.union(dependencies.get(DEP_tableData));
                return new DependencySet(DEP_tableAggregator, result);
            }
            
            // ----------------------------------------------------------------------------
            //  INDEX statistics
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_indexData: {
                assert(params.toArray().length == 2);
                final boolean interval =
                    ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                final Long now = (Long)params.toArray()[1];

                // create an array of the table ids for which statistics are required.
                // pass this to EE owned by the execution site running this plan fragment.
                CatalogMap<Table> tables = context.getDatabase().getTables();
                int[] tableGuids = new int[tables.size()];
                int ii = 0;
                for (Table table : tables) {
                    tableGuids[ii++] = table.getRelativeIndex();
                    // System.err.println("TABLE ID: " + table.getRelativeIndex());
                }

                VoltTable result = executor.getExecutionEngine().getStats(
                            SysProcSelector.INDEX,
                            tableGuids,
                            interval,
                            now)[0];
                // System.err.println(VoltTableUtil.format(result));
                return new DependencySet(DEP_indexData, result);
            }
            case SysProcFragmentId.PF_indexAggregator: {
                VoltTable result = VoltTableUtil.union(dependencies.get(DEP_indexData));
                return new DependencySet(DEP_indexAggregator, result);
            }
    
            // ----------------------------------------------------------------------------
            //  PROCEDURE statistics
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_procedureData: {
                // procedure stats are registered to VoltDB's statsagent with the site's catalog id.
                // piece this information together and the stats agent returns a table. pretty sweet.
                assert(params.toArray().length == 2);
                final boolean interval =
                    ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                final Long now = (Long)params.toArray()[1];
                ArrayList<Integer> catalogIds = new ArrayList<Integer>();
                catalogIds.add(context.getSite().getId());
                VoltTable result = executor.getHStoreSite().getStatsAgent().getStats(
                                SysProcSelector.PROCEDURE,
                                catalogIds,
                                interval,
                                now);
                return new DependencySet(DEP_procedureData, result);
            }
            case SysProcFragmentId.PF_procedureAggregator: {
                VoltTable result = VoltTableUtil.union(dependencies.get(DEP_procedureData));
                return new DependencySet(DEP_procedureAggregator, result);
            }
            
            // ----------------------------------------------------------------------------
            // IO statistics
            // ----------------------------------------------------------------------------
            case SysProcFragmentId.PF_ioData: {
                ColumnInfo ioColumnInfo[] = new ColumnInfo[] {
                        new ColumnInfo( "TIMESTAMP", VoltType.BIGINT),
                        new ColumnInfo( VoltSystemProcedure.CNAME_HOST_ID, VoltSystemProcedure.CTYPE_ID),
                        new ColumnInfo( "HOSTNAME", VoltType.STRING),
                        new ColumnInfo( "CONNECTION_ID", VoltType.BIGINT),
                        new ColumnInfo( "CONNECTION_HOSTNAME", VoltType.STRING),
                        new ColumnInfo( "BYTES_READ", VoltType.BIGINT),
                        new ColumnInfo( "MESSAGES_READ", VoltType.BIGINT),
                        new ColumnInfo( "BYTES_WRITTEN", VoltType.BIGINT),
                        new ColumnInfo( "MESSAGES_WRITTEN", VoltType.BIGINT)
                };
                final VoltTable result = new VoltTable(ioColumnInfo);
                // Choose the lowest site ID on this host to do the scan
                // All other sites should just return empty results tables.
                if (isFirstLocalPartition()) {
                    assert(params.toArray() != null);
                    assert(params.toArray().length == 2);
                    final boolean interval =
                        ((Byte)params.toArray()[0]).byteValue() == 0 ? false : true;
                    final Long now = (Long)params.toArray()[1];
                    try {
                        final Map<Long, Pair<String,long[]>> stats = executor.getHStoreSite().getVoltNetwork().getIOStats(interval);
    
                        final Integer hostId = executor.getHStoreSite().getSiteId();
                        final String hostname = executor.getHStoreSite().getSiteName();
                        for (Map.Entry<Long, Pair<String, long[]>> e : stats.entrySet()) {
                            final Long connectionId = e.getKey();
                            final String remoteHostname = e.getValue().getFirst();
                            final long counters[] = e.getValue().getSecond();
                            result.addRow(
                                          now,
                                          hostId,
                                          hostname,
                                          connectionId,
                                          remoteHostname,
                                          counters[0],
                                          counters[1],
                                          counters[2],
                                          counters[3]);
                        }
                    } catch (Exception e) {
                        HOST_LOG.warn("Error retrieving stats", e);
                    }
                }
                return new DependencySet(DEP_ioData, result);
            }
            case SysProcFragmentId.PF_ioDataAggregator: {
                VoltTable result = null;
                List<VoltTable> dep = dependencies.get(DEP_ioData);
                for (VoltTable t : dep) {
                    if (result == null) {
                        result = new VoltTable(t);
                    }
                    while (t.advanceRow()) {
                        result.add(t);
                    }
                }
                return new DependencySet(DEP_ioDataAggregator, result);
            }
            case SysProcFragmentId.PF_partitionCount: {
                VoltTable result = new VoltTable(new VoltTable.ColumnInfo("PARTITION_COUNT", VoltType.INTEGER));
                result.addRow(executor.getHStoreSite().getLocalPartitionIds().size());
                return new DependencySet(DEP_partitionCount, result);
            }
        } // SWITCH

        assert (false);
        return null;
    }

    /**
     * Returns a table stats.
     * requested.
     * @param selector     Selector requested TABLE, PROCEDURE, INITIATOR,
     *                     PARTITIONCOUNT, IOSTATS, MANAGEMENT, INDEX
     * @param interval     1 for interval statistics. 0 for full statistics.
     * @return             The returned schema is specific to the selector.
     * @throws VoltAbortException
     */
    public VoltTable[] run(String selector, long interval) throws VoltAbortException {
        VoltTable[] results;
        final long now = System.currentTimeMillis();
        selector = selector.toUpperCase();
        
        // Check STATS_DATA first so that we can automatically invoke this
        Integer dataFragmentId = null;
        for (Entry<Integer, Pair<SysProcSelector, Integer>> e : STATS_DATA.entrySet()) {
            if (e.getValue().getFirst().name().toUpperCase().startsWith(selector)) {
                dataFragmentId = e.getKey();
                break;
            }
        } // FOR
        
        if (dataFragmentId != null) {
            results = getData(dataFragmentId.intValue(), interval, now);
        }
        else if (selector.toUpperCase().startsWith(SysProcSelector.TABLE.name())) {
            results = getTableData(interval, now);
        }
        else if (selector.toUpperCase().startsWith(SysProcSelector.INDEX.name())) {
            results = getIndexData(interval, now);
        }
        else if (selector.toUpperCase().startsWith(SysProcSelector.PROCEDURE.name())) {
            results = getProcedureData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.PARTITIONCOUNT.name())) {
            results = getPartitionCountData();
        }
        else if (selector.toUpperCase().equals(SysProcSelector.IOSTATS.name())) {
            results = getIOStatsData(interval, now);
        }
        else if (selector.toUpperCase().equals(SysProcSelector.MANAGEMENT.name())) {
            VoltTable[] memoryResults = getData(SysProcFragmentId.PF_nodeMemory, interval, now);
            VoltTable[] txnResults = getData(SysProcFragmentId.PF_txnCounterData, interval, now);
            VoltTable[] tableResults = getTableData(interval, now);
            VoltTable[] procedureResults = getProcedureData(interval, now);
            VoltTable[] initiatorResults = getInitiatorData(interval, now);
            VoltTable[] ioResults = getIOStatsData(interval, now);
            VoltTable[] starvationResults = getIOStatsData(interval, now);
            results = new VoltTable[] {
                    memoryResults[0],
                    txnResults[0],
                    initiatorResults[0],
                    procedureResults[0],
                    ioResults[0],
                    tableResults[0],
                    starvationResults[0]
            };
            final long endTime = System.currentTimeMillis();
            final long delta = endTime - now;
            if (debug.val) HOST_LOG.debug("Statistics invocation of MANAGEMENT selector took " + delta + " milliseconds");
        } else {
            String msg = String.format("Invalid Statistics selector %s.\nValid Options: %s",
                                       selector, Arrays.toString(SysProcSelector.values()));
            throw new VoltAbortException(msg);
        }

        return results;
    }

    /**
     * All-in-one method for getting the profile data we need
     * The input dataFragmentId must be included in the STATS_DATA mapping
     * @param dataFragmentId
     * @param interval
     * @param now
     * @return
     */
    private VoltTable[] getData(int dataFragmentId, long interval, final long now) {
        // create a work fragment to gather node memory data
        ParameterSet parameters = new ParameterSet();
        parameters.setParameters((byte)interval, now);
        
        Pair<SysProcSelector, Integer> pair = STATS_DATA.get(dataFragmentId);
        assert(pair != null);
        
        return this.executeOncePerSite(dataFragmentId,
                                       pair.getSecond().intValue(),
                                       parameters);
    }
    
    private VoltTable[] getIOStatsData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_ioData;
        pfs[1].outputDependencyIds = new int[]{ DEP_ioData };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_ioDataAggregator;
        pfs[0].outputDependencyIds = new int[]{ DEP_ioDataAggregator };
        pfs[0].inputDependencyIds = new int[]{DEP_ioData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_ioDataAggregator);
        return results;
    }

    private VoltTable[] getPartitionCountData() {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[1];
        // create a work fragment to gather the partition count the catalog.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_partitionCount;
        pfs[0].outputDependencyIds = new int[]{ DEP_partitionCount };
        pfs[0].inputDependencyIds = new int[]{};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        results = executeSysProcPlanFragments(pfs, DEP_partitionCount);
        return results;
    }

    private VoltTable[] getInitiatorData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather initiator data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_initiatorData;
        pfs[1].outputDependencyIds = new int[]{ DEP_initiatorData };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_initiatorAggregator;
        pfs[0].outputDependencyIds = new int[]{ DEP_initiatorAggregator };
        pfs[0].inputDependencyIds = new int[]{DEP_initiatorData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_initiatorAggregator);
        return results;
    }

    private VoltTable[] getProcedureData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather procedure data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_procedureData;
        pfs[1].outputDependencyIds = new int[]{ DEP_procedureData };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_procedureAggregator;
        pfs[0].outputDependencyIds = new int[]{ DEP_procedureAggregator };
        pfs[0].inputDependencyIds = new int[]{DEP_procedureData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_procedureAggregator);
        return results;
    }

    private VoltTable[] getTableData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather table data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_tableData;
        pfs[1].outputDependencyIds = new int[]{ DEP_tableData };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_tableAggregator;
        pfs[0].outputDependencyIds = new int[]{ DEP_tableAggregator };
        pfs[0].inputDependencyIds = new int[]{DEP_tableData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_tableAggregator);
        return results;
    }

    private VoltTable[] getIndexData(long interval, final long now) {
        VoltTable[] results;
        SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        // create a work fragment to gather table data from each of the sites.
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_indexData;
        pfs[1].outputDependencyIds = new int[]{ DEP_indexData };
        pfs[1].inputDependencyIds = new int[]{};
        pfs[1].multipartition = true;
        pfs[1].parameters = new ParameterSet();
        pfs[1].parameters.setParameters((byte)interval, now);

        // create a work fragment to aggregate the results.
        // Set the MULTIPARTITION_DEPENDENCY bit to require a dependency from every site.
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_indexAggregator;
        pfs[0].outputDependencyIds = new int[]{ DEP_indexAggregator };
        pfs[0].inputDependencyIds = new int[]{DEP_indexData};
        pfs[0].multipartition = false;
        pfs[0].parameters = new ParameterSet();

        // distribute and execute these fragments providing pfs and id of the
        // aggregator's output dependency table.
        results = executeSysProcPlanFragments(pfs, DEP_indexAggregator);
        return results;
    }
}
