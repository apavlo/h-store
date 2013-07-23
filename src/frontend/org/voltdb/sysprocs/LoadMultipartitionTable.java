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
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.exceptions.MispredictionException;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.ObjectHistogram;

/**
 * Given a VoltTable with a schema corresponding to a persistent table, load all
 * of the rows applicable to the current partitioning at each node in the
 * cluster.
 */
@ProcInfo(singlePartition = false)
public class LoadMultipartitionTable extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(LoadMultipartitionTable.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static final long DEP_distribute = SysProcFragmentId.PF_loadDistribute | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    private static final long DEP_aggregate = SysProcFragmentId.PF_loadAggregate;

    private ObjectHistogram<Integer> allPartitionsHistogram = new ObjectHistogram<Integer>();
    
    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_loadDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_loadAggregate, this);
        this.allPartitionsHistogram.put(catalogContext.getAllPartitionIds());
    }
    
    @Override
    public DependencySet executePlanFragment(Long txn_id,
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
            VoltTable table = (VoltTable)(params.toArray()[1]);
            
            if (debug.val) LOG.debug(String.format("Loading %d tuples for table '%s' in txn #%d",
                                       table.getRowCount(), table_name, txn_id));
            assert(this.isInitialized()) : " The sysproc " + this.getClass().getSimpleName() + " was not initialized properly";
            try {
                AbstractTransaction ts = this.hstore_site.getTransaction(txn_id); 
                this.executor.loadTable(ts,
                                        context.getCluster().getName(),
                                        context.getDatabase().getName(),
                                        table_name, table, 0);    
            } catch (VoltAbortException e) {
                // must continue and reply with dependency.
                e.printStackTrace();
            }
            if (debug.val) LOG.debug("Finished loading table. Things look good...");
            return new DependencySet(new int[] { (int)DEP_distribute }, result);

        } else if (fragmentId == SysProcFragmentId.PF_loadAggregate) {
            if (debug.val) LOG.debug("Aggregating results from loading fragments in txn #" + txn_id);
            return new DependencySet(new int[] { (int)DEP_aggregate }, result);
        }
        // must handle every dependency id.
        assert (false);
        return null;
    }
    
    private SynthesizedPlanFragment[] createReplicatedPlan(LocalTransaction ts, Table catalog_tbl, VoltTable table) {
        if (debug.val)
            LOG.debug(String.format("%s - %s is replicated. Creating %d fragments to send to all partitions",
                      ts, catalog_tbl.getName(), catalogContext.numberOfPartitions));
        ParameterSet params = new ParameterSet(catalog_tbl.getName(), table);
        
        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        int idx = 0;
        
        // Create a work unit to invoke super.loadTable() on each partition
        pfs[idx] = new SynthesizedPlanFragment();
        pfs[idx].fragmentId = SysProcFragmentId.PF_loadDistribute;
        pfs[idx].outputDependencyIds = new int[] { (int)DEP_distribute };
        pfs[idx].inputDependencyIds = new int[] { };
        pfs[idx].multipartition = true;
        pfs[idx].nonExecSites = false;
        pfs[idx].parameters = params;

        // Create a work unit to aggregate the results.
        idx += 1;
        pfs[idx] = new SynthesizedPlanFragment();
        pfs[idx].fragmentId = SysProcFragmentId.PF_loadAggregate;
        pfs[idx].outputDependencyIds = new int[] { (int)DEP_aggregate };
        pfs[idx].inputDependencyIds = new int[] { (int)DEP_distribute };
        pfs[idx].multipartition = false;
        pfs[idx].nonExecSites = false;
        pfs[idx].parameters = new ParameterSet();
        pfs[idx].destPartitionId = this.partitionId;

        return (pfs);
    }

    private SynthesizedPlanFragment[] createNonReplicatedPlan(LocalTransaction ts, Table catalog_tbl, VoltTable table) {
        if (debug.val)
            LOG.debug(catalog_tbl + " is not replicated. Splitting table data into separate pieces for partitions");
        
        // Create a table for each partition
        VoltTable partitionedTables[] = new VoltTable[catalogContext.numberOfPartitions];

        // Split the input table into per-partition units
        if (debug.val)
            LOG.debug(String.format("Splitting original %d %s rows into partitioned tables",
                      table.getRowCount(), catalog_tbl));
        boolean mispredict = false;
        table.resetRowPosition();
        while (table.advanceRow()) {
            int p = -1;
            try {
                p = this.p_estimator.getTableRowPartition(catalog_tbl, table);
            } catch (Exception e) {
                LOG.fatal("Failed to split input table into partitions", e);
                throw new RuntimeException(e.getMessage());
            }
            assert(p >= 0);
            
            if (partitionedTables[p] == null) {
                partitionedTables[p] = table.clone(1024 * 1024);
                ts.getTouchedPartitions().put(p);
                if (ts.getPredictTouchedPartitions().contains(p) == false) {
                    mispredict = true;
                }
                if (trace.val) LOG.trace("Cloned VoltTable for Partition #" + p);
            }
            
            // Add the active row from table
            // Don't bother doing it if we know that we're going to mispredict afterwards 
            if (mispredict == false) {
                partitionedTables[p].add(table);
                if (trace.val && table.getActiveRowIndex() > 0 && table.getActiveRowIndex() % 1000 == 0)
                    LOG.trace(String.format("Processed %s tuples for " + catalog_tbl, table.getActiveRowIndex()));
            }
        } // WHILE
        
        // Allow them to restart and lock on the partitions that they need to load
        // data on. This will help speed up concurrent bulk loading
        if (mispredict) {
            if (debug.val)
                LOG.warn(String.format("%s - Restarting as a distributed transaction on partitions %s",
                         ts, ts.getTouchedPartitions().values()));
            throw new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions());
        }
        StringBuilder sb = null;
        if (trace.val) {
            sb = new StringBuilder();
            sb.append("LoadMultipartition Info for ").append(catalog_tbl.getName()).append(":");
        }

        // Generate a plan fragment for each site using the sub-tables
        // Note that we only need to create a PlanFragment for a partition if its portion
        // of the table that we just split up doesn't have any rows 
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
            pf.last_task = false;
            pfs.add(pf);
            if (trace.val)
                sb.append(String.format("\n  Partition #%d: %d tuples",
                          partition, partitionedTables[partition].getRowCount()));
        } // FOR
        if (trace.val) LOG.trace(sb.toString());

        // a final plan fragment to aggregate the results
        SynthesizedPlanFragment pf = new SynthesizedPlanFragment();
        pf.destPartitionId = this.partitionId;
        pf.fragmentId = SysProcFragmentId.PF_loadAggregate;
        pf.inputDependencyIds = new int[] { (int)DEP_distribute };
        pf.outputDependencyIds = new int[] { (int)DEP_aggregate };
        pf.multipartition = false;
        pf.nonExecSites = false;
        pf.last_task = true;
        pf.parameters = new ParameterSet();
        pfs.add(pf);

        return (pfs.toArray(new SynthesizedPlanFragment[0]));
    }
    
    private SynthesizedPlanFragment[] createVerticalPartitionPlan(LocalTransaction ts, MaterializedViewInfo catalog_view, VoltTable table) {
        Table virtual_tbl = catalog_view.getDest();
        VoltTable vt = CatalogUtil.getVoltTable(virtual_tbl);
        Collection<Column> virtual_cols = CatalogUtil.getColumns(catalog_view.getGroupbycols());
        
        table.resetRowPosition();
        while (table.advanceRow()) {
            int i = 0;
            Object row[] = new Object[virtual_cols.size()];
            for (Column catalog_col : CatalogUtil.getSortedCatalogItems(virtual_cols, "index")) {
                if (trace.val)
                    LOG.trace(String.format("Adding %s [%d] to virtual column %d",
                              table.getColumnName(catalog_col.getIndex()), catalog_col.getIndex(), i));
                row[catalog_col.getIndex()] = table.get(catalog_col.getIndex());
            } // FOR
            vt.addRow(row);
        } // WHILE
        if (debug.val)
            LOG.info(String.format("Vertical Partition %s -> %s\n",
                     catalog_view.getParent().getName(), virtual_tbl.getName()) + vt);
        
        return (createReplicatedPlan(ts, virtual_tbl, vt));
    }
    
    
    public VoltTable[] run(String tableName, VoltTable table) throws VoltAbortException {
        assert(table != null) : 
            "VoltTable to be loaded into " + tableName + " is null in txn #" + this.getTransactionId();
        
        if (debug.val)
            LOG.debug(String.format("Executing multi-partition loader for %s with %d tuples in txn #%d [bytes=%d]",
                      tableName, table.getRowCount(), this.getTransactionId(), table.getUnderlyingBufferSize()));
        
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        Table catalog_tbl = catalogContext.database.getTables().getIgnoreCase(tableName);
        if (catalog_tbl == null) {
            throw new VoltAbortException("Table '" + tableName + "' does not exist");
        }
        else if (table.getRowCount() == 0) {
            throw new VoltAbortException("The VoltTable for table '" + tableName + "' is empty");
        }

        LocalTransaction ts = this.getTransactionState();
        
        // if tableName is replicated, just send table everywhere.
        if (catalog_tbl.getIsreplicated()) {
            // If they haven't locked all of the partitions in the cluster, then we'll 
            // stop them right here and force them to get those
            if (ts.getPredictTouchedPartitions().size() != this.allPartitionsHistogram.getValueCount()) { 
                throw new MispredictionException(this.getTransactionId(), this.allPartitionsHistogram);
            }
            pfs = this.createReplicatedPlan(ts, catalog_tbl, table);
        }
        // Otherwise, create a VoltTable for each partition and split up the incoming table
        // then send those partial tables to the appropriate sites.
        else {
            pfs = this.createNonReplicatedPlan(ts, catalog_tbl, table);
        }
        
        // distribute and execute the fragments providing pfs and id
        // of the aggregator's output dependency table.
        if (debug.val) LOG.debug("Passing " + pfs.length + " sysproc fragments to executeSysProcPlanFragments()");
        results = executeSysProcPlanFragments(pfs, (int)DEP_aggregate);
        
        // Check whether this table has a vertical partition
        // If so, then we'll automatically blast out the data that it needs
        MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition(catalog_tbl);
        if (debug.val)
            LOG.debug(String.format("%s - %s Vertical Partition: %s",
                      ts, catalog_tbl.getName(), catalog_view));
        if (catalog_view != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Updating %s's vertical partition %s",
                          ts, catalog_tbl.getName(), catalog_view.getDest().getName()));
            executeSysProcPlanFragments(createVerticalPartitionPlan(ts, catalog_view, table), (int)DEP_aggregate);
        }
        
        return (results);
    }
}
