/* This file is part of VoltDB.
 * Copyright (C) 2008-2009 VoltDB L.L.C.
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
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencySet;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.DtxnConstants;
import org.voltdb.exceptions.MispredictionException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.Histogram;
import edu.brown.utils.PartitionEstimator;

/**
 * Given a VoltTable with a schema corresponding to a persistent table, load all
 * of the rows applicable to the current partitioning at each node in the
 * cluster.
 */
@ProcInfo(singlePartition = false)
public class LoadMultipartitionTable extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(LoadMultipartitionTable.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    

    static final long DEP_distribute = SysProcFragmentId.PF_loadDistribute | DtxnConstants.MULTIPARTITION_DEPENDENCY;
    static final long DEP_aggregate = SysProcFragmentId.PF_loadAggregate;

    private Histogram<Integer> allPartitionsHistogram = new Histogram<Integer>();
    
    @Override
    public void globalInit(PartitionExecutor site, Procedure catalog_proc,
            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator);
        
        site.registerPlanFragment(SysProcFragmentId.PF_loadDistribute, this);
        site.registerPlanFragment(SysProcFragmentId.PF_loadAggregate, this);
        
        this.allPartitionsHistogram.putAll(CatalogUtil.getAllPartitionIds(catalog_proc));
    }
    
    @Override
    public DependencySet executePlanFragment(long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             SystemProcedureExecutionContext context) {
        
        // need to return something ..
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        result[0].addRow(txn_id);

        if (fragmentId == SysProcFragmentId.PF_loadDistribute) {
            assert context.getCluster().getName() != null;
            assert context.getDatabase().getName() != null;
            assert params != null;
            assert params.toArray() != null;
            assert params.toArray()[0] != null;
            assert params.toArray()[1] != null;
            String table_name = (String) (params.toArray()[0]);
            
            if (debug.get()) LOG.debug("Executing voltLoadTable() sysproc fragment for table '" + table_name + "' in txn #" + txn_id);
            assert(this.isInitialized()) : " The sysproc " + this.getClass().getSimpleName() + " was not initialized properly";
            try {
                // voltLoadTable is void. Assume success or exception.
                super.voltLoadTable(context.getCluster().getName(), context.getDatabase().getName(),
                                    table_name, (VoltTable)(params.toArray()[1]), 0);
            } catch (VoltAbortException e) {
                // must continue and reply with dependency.
                e.printStackTrace();
            }
            if (debug.get()) LOG.debug("Finished loading table. Things look good...");
            return new DependencySet(new int[] { (int)DEP_distribute }, result);

        } else if (fragmentId == SysProcFragmentId.PF_loadAggregate) {
            if (debug.get()) LOG.debug("Aggregating results from loading fragments in txn #" + txn_id);
            return new DependencySet(new int[] { (int)DEP_aggregate }, result);
        }
        // must handle every dependency id.
        assert (false);
        return null;
    }
    
    private SynthesizedPlanFragment[] createReplicatedPlan(Table catalog_tbl, VoltTable table) {
        if (debug.get()) LOG.debug(catalog_tbl + " is replicated. Creating " + num_partitions + " fragments to send to all partitions");
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[num_partitions];

        ParameterSet params = new ParameterSet(catalog_tbl.getName(), table);
        
        // create a work unit to invoke super.loadTable() on each site.
        for (int i = 0; i < num_partitions; ++i) {
            int partition = i;
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = SysProcFragmentId.PF_loadDistribute;
            pfs[i].outputDependencyIds = new int[] { (int)DEP_distribute };
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].multipartition = false; // true
            pfs[i].nonExecSites = false;
            pfs[i].parameters = params;
            pfs[i].destPartitionId = partition;
        } // FOR

        // create a work unit to aggregate the results.
        // MULTIPARTION_DEPENDENCY bit set, requiring result from ea. site
//        pfs[0] = new SynthesizedPlanFragment();
//        pfs[0].fragmentId = SysProcFragmentId.PF_loadAggregate;
//        pfs[0].outputDependencyIds = new int[] { (int)DEP_aggregate };
//        pfs[0].inputDependencyIds = new int[] { (int)DEP_distribute };
//        pfs[0].multipartition = false;
//        pfs[0].nonExecSites = false;
//        pfs[0].parameters = new ParameterSet();
//        pfs[0].destPartitionId = partitionId;

        return (pfs);
    }

    private SynthesizedPlanFragment[] createNonReplicatedPlan(Table catalog_tbl, VoltTable table) {
        if (debug.get()) LOG.debug(catalog_tbl + " is not replicated. Splitting table data into separate pieces for partitions");
        
        // Create a table for each partition
        VoltTable partitionedTables[] = new VoltTable[num_partitions];

        // Split the input table into per-partition units
        if (debug.get()) LOG.debug("Splitting original table of " + table.getRowCount() + " rows into partitioned tables");
        boolean mispredict = false;
        table.resetRowPosition();
        while (table.advanceRow()) {
            int p = -1;
            try {
                p = this.p_estimator.getTableRowPartition(catalog_tbl, table.fetchRow(table.getActiveRowIndex()));
            } catch (Exception e) {
                LOG.fatal("Failed to split input table into partitions", e);
                throw new RuntimeException(e.getMessage());
            }
            assert(p >= 0);
            assert(p < partitionedTables.length) :
                String.format("Invalid partition %d [numPartitions=%d]", p, partitionedTables.length);
            
            if (partitionedTables[p] == null) {
                partitionedTables[p] = table.clone(1024 * 1024);
                this.m_localTxnState.getTouchedPartitions().put(p);
                if (this.m_localTxnState.getPredictTouchedPartitions().contains(p) == false) {
                    mispredict = true;
                }
                if (trace.get()) LOG.trace("Cloned VoltTable for Partition #" + p);
            }
            
            // Add the active row from table
            // Don't bother doing it if we know that we're going to mispredict afterwards 
            if (mispredict == false) {
                partitionedTables[p].add(table);
                if (trace.get() && table.getActiveRowIndex() > 0 && table.getActiveRowIndex() % 1000 == 0)
                    LOG.trace(String.format("Processed %s tuples for " + catalog_tbl, table.getActiveRowIndex()));
            }
        } // WHILE
        
        // Allow them to restart and lock on the partitions that they need to load
        // data on. This will help speed up concurrent bulk loading
        if (mispredict) {
            if (debug.get()) LOG.warn(String.format("%s - Restarting as a distributed transaction on partitions %s",
                                                    this.m_localTxnState, this.m_localTxnState.getTouchedPartitions().values()));
            throw new MispredictionException(this.getTransactionId(), this.m_localTxnState.getTouchedPartitions());
        }
        StringBuilder sb = null;
        if (trace.get()) {
            sb = new StringBuilder();
            sb.append("LoadMultipartition Info for ").append(catalog_tbl.getName()).append(":");
        }

        // generate a plan fragment for each site using the sub-tables
        List<SynthesizedPlanFragment> pfs = new ArrayList<SynthesizedPlanFragment>();
        for (int i = 0; i < partitionedTables.length; ++i) {
            int partition = i;
            if (partitionedTables[partition] == null || partitionedTables[partition].getRowCount() == 0) continue;
            ParameterSet params = new ParameterSet(catalog_tbl.getName(), partitionedTables[partition]);
            SynthesizedPlanFragment pf = new SynthesizedPlanFragment();
            pf.fragmentId = SysProcFragmentId.PF_loadDistribute;
            pf.inputDependencyIds = new int[] { };
            pf.outputDependencyIds = new int[] { (int)DEP_distribute };
            pf.multipartition = false;
            pf.nonExecSites = false;
            pf.destPartitionId = partition; // partitionsToSites[i - 1];
            pf.parameters = params;
            pf.last_task = true;
            pfs.add(pf);
            if (trace.get()) sb.append("\n  Partition #").append(partition).append(": ")
                         .append(partitionedTables[partition].getRowCount()).append(" tuples");
        } // FOR
        if (trace.get()) LOG.trace(sb.toString());

        // a final plan fragment to aggregate the results
//        pfs[0] = new SynthesizedPlanFragment();
//        pfs[0].destPartitionId = partitionId;
//        pfs[0].fragmentId = SysProcFragmentId.PF_loadAggregate;
//        pfs[0].inputDependencyIds = new int[] { (int)DEP_distribute };
//        pfs[0].outputDependencyIds = new int[] { (int)DEP_aggregate };
//        pfs[0].multipartition = false;
//        pfs[0].nonExecSites = false;
//        pfs[0].last_task = true;
//        pfs[0].parameters = new ParameterSet();

        return (pfs.toArray(new SynthesizedPlanFragment[0]));
    }
    
    private SynthesizedPlanFragment[] createVerticalPartitionPlan(MaterializedViewInfo catalog_view, VoltTable table) {
        Table virtual_tbl = catalog_view.getDest();
        VoltTable vt = CatalogUtil.getVoltTable(virtual_tbl);
        Collection<Column> virtual_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
        
        table.resetRowPosition();
        while (table.advanceRow()) {
            int i = 0;
            Object row[] = new Object[virtual_cols.size()];
            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(virtual_cols, "index")) {
                if (trace.get())
                    LOG.trace(String.format("Adding %s [%d] to virtual column %d",
                                            table.getColumnName(catalog_col.getIndex()), catalog_col.getIndex(), i));
                row[catalog_col.getIndex()] = table.get(catalog_col.getIndex());
            } // FOR
            vt.addRow(row);
        } // WHILE
        if (debug.get()) LOG.info(String.format("Vertical Partition %s -> %s\n", catalog_view.getParent().getName(), virtual_tbl.getName()) + vt);
        
        return (createReplicatedPlan(virtual_tbl, vt));
    }
    
    
    public VoltTable[] run(String tableName, VoltTable table) throws VoltAbortException {
        assert(table != null) : "VoltTable to be loaded into " + tableName + " is null in txn #" + this.getTransactionId();
        
        if (debug.get()) LOG.debug("Executing multi-partition loader for " + tableName + " with " + table.getRowCount() + 
                             " tuples in txn #" + this.getTransactionId() +
                             " [bytes="  + table.getUnderlyingBufferSize() + "]");
        
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        Table catalog_tbl = database.getTables().getIgnoreCase(tableName);
        if (catalog_tbl == null) {
            throw new VoltAbortException("Table '" + tableName + "' does not exist");
        }

        // if tableName is replicated, just send table everywhere.
        if (catalog_tbl.getIsreplicated()) {
            // If they haven't locked all of the partitions in teh cluster, then we'll 
            // stop them right here and force them to get those
            if (this.m_localTxnState.getPredictTouchedPartitions().size() != this.allPartitionsHistogram.getValueCount()) { 
                throw new MispredictionException(this.getTransactionId(), this.allPartitionsHistogram);
            }
            pfs = this.createReplicatedPlan(catalog_tbl, table);
        }
        // Otherwise, create a VoltTable for each partition and split up the incoming table
        // then send those partial tables to the appropriate sites.
        else {
            pfs = this.createNonReplicatedPlan(catalog_tbl, table);
        }
        
        // distribute and execute the fragments providing pfs and id
        // of the aggregator's output dependency table.
        if (debug.get()) LOG.debug("Passing " + pfs.length + " sysproc fragments to executeSysProcPlanFragments()");
        results = executeSysProcPlanFragments(pfs, (int)DEP_aggregate);
        
        // Check whether this table has a vertical partition
        // If so, then we'll automatically blast out the data that it needs
        MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition(catalog_tbl);
        if (debug.get()) LOG.debug(String.format("%s Vertical Partition: %s", catalog_tbl.getName(), catalog_view));
        if (catalog_view != null) {
            if (debug.get()) 
                LOG.debug(String.format("Updating %s's vertical partition %s", catalog_tbl.getName(), catalog_view.getDest().getName()));
            executeSysProcPlanFragments(createVerticalPartitionPlan(catalog_view, table), (int)DEP_aggregate);
        }
        
        return (results);
    }
}
