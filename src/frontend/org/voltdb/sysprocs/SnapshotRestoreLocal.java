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

package org.voltdb.sysprocs;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Collection;
import java.nio.BufferOverflowException;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.ProcInfo;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltSystemProcedure.SynthesizedPlanFragment;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Partition;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.exceptions.MispredictionException;
import org.voltdb.sysprocs.LoadMultipartitionTable;
import org.voltdb.sysprocs.saverestore.ClusterSaveFileState;
import org.voltdb.sysprocs.saverestore.SavedTableConverter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.sysprocs.saverestore.TableSaveFileState;
import org.voltdb.utils.DBBPool.BBContainer;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;

@ProcInfo(singlePartition = false)
public class SnapshotRestoreLocal extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(SnapshotRestoreLocal.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    private static final int DEP_SRLrestoreScan = (int) SysProcFragmentId.PF_SRLrestoreScan | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_SRLrestoreScanResults = (int) SysProcFragmentId.PF_SRLrestoreScanResults;

    private static HashSet<String> m_initializedTableSaveFiles = new HashSet<String>();
    private static ArrayDeque<TableSaveFile> m_saveFiles = new ArrayDeque<TableSaveFile>();

    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_SRLloadDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_SRLloadAggregate, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_SRLrestoreScan, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_SRLrestoreScanResults, this);
        this.allPartitionsHistogram.put(catalogContext.getAllPartitionIds());

        m_siteId = executor.getSiteId();
        m_hostId = ((Site) executor.getPartition().getParent()).getHost().getId();
    }

    private static final long DEP_SRLdistribute = SysProcFragmentId.PF_SRLloadDistribute | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    private static final long DEP_SRLaggregate = SysProcFragmentId.PF_SRLloadAggregate;

    private ObjectHistogram<Integer> allPartitionsHistogram = new ObjectHistogram<Integer>();

    private SynthesizedPlanFragment[] createReplicatedPlan(LocalTransaction ts, Table catalog_tbl, VoltTable table) {
        if (debug.val)
            LOG.debug(String.format("%s - %s is replicated. Creating %d fragments to send to all partitions", ts, catalog_tbl.getName(), catalogContext.numberOfPartitions));
        ParameterSet params = new ParameterSet(catalog_tbl.getName(), table, catalog_tbl.getIsreplicated());

        final SynthesizedPlanFragment pfs[] = new SynthesizedPlanFragment[2];
        int idx = 0;

        // Create a work unit to invoke super.loadTable() on each partition
        pfs[idx] = new SynthesizedPlanFragment();
        pfs[idx].fragmentId = SysProcFragmentId.PF_SRLloadDistribute;
        pfs[idx].outputDependencyIds = new int[] { (int) DEP_SRLdistribute };
        pfs[idx].inputDependencyIds = new int[] {};
        pfs[idx].multipartition = true;
        pfs[idx].nonExecSites = false;
        pfs[idx].parameters = params;

        // Create a work unit to aggregate the results.
        idx += 1;
        pfs[idx] = new SynthesizedPlanFragment();
        pfs[idx].fragmentId = SysProcFragmentId.PF_SRLloadAggregate;
        pfs[idx].outputDependencyIds = new int[] { (int) DEP_SRLaggregate };
        pfs[idx].inputDependencyIds = new int[] { (int) DEP_SRLdistribute };
        pfs[idx].multipartition = false;
        pfs[idx].nonExecSites = false;
        pfs[idx].parameters = new ParameterSet();
        pfs[idx].destPartitionId = this.partitionId;

        return (pfs);
    }

    private SynthesizedPlanFragment[] createNonReplicatedPlan(LocalTransaction ts, Table catalog_tbl, VoltTable table) {
        if (debug.val)
            LOG.debug(catalog_tbl + " is not replicated. Splitting table data into separate pieces for " + catalogContext.numberOfPartitions + " partitions");

        // Create a table for each partition
        VoltTable partitionedTables[] = new VoltTable[catalogContext.numberOfPartitions];

        // Split the input table into per-partition units
        if (debug.val)
            LOG.debug(String.format("Splitting original %d %s rows into partitioned tables", table.getRowCount(), catalog_tbl));
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
            assert (p >= 0);

            if (partitionedTables[p] == null) {
                partitionedTables[p] = table.clone(1024 * 1024);
                ts.getTouchedPartitions().put(p);
                if (ts.getPredictTouchedPartitions().contains(p) == false) {
                    mispredict = true;
                }
                if (trace.val)
                    LOG.trace("Cloned VoltTable for Partition #" + p);
            }

            // Add the active row from table
            // Don't bother doing it if we know that we're going to mispredict
            // afterwards
            if (mispredict == false) {
                partitionedTables[p].add(table);
                if (trace.val && table.getActiveRowIndex() > 0 && table.getActiveRowIndex() % 1000 == 0)
                    LOG.trace(String.format("Processed %s tuples for " + catalog_tbl, table.getActiveRowIndex()));
            }
        } // WHILE

        // Allow them to restart and lock on the partitions that they need to
        // load
        // data on. This will help speed up concurrent bulk loading
        if (mispredict) {
            if (debug.val)
                LOG.warn(String.format("%s - Restarting as a distributed transaction on partitions %s", ts, ts.getTouchedPartitions().values()));
            throw new MispredictionException(ts.getTransactionId(), ts.getTouchedPartitions());
        }
        StringBuilder sb = null;
        if (trace.val) {
            sb = new StringBuilder();
            sb.append("LoadMultipartition Info for ").append(catalog_tbl.getName()).append(":");
        }

        // Generate a plan fragment for each site using the sub-tables
        // Note that we only need to create a PlanFragment for a partition if
        // its portion
        // of the table that we just split up doesn't have any rows
        List<SynthesizedPlanFragment> pfs = new ArrayList<SynthesizedPlanFragment>();
        for (int i = 0; i < partitionedTables.length; ++i) {
            int partition = i;

            if (partitionedTables[partition] == null || partitionedTables[partition].getRowCount() == 0) {
                if (partitionedTables[partition] == null)
                    LOG.trace("partition :" + partition + " partitionedTables null");
                //continue;
            }

            ParameterSet params = new ParameterSet(catalog_tbl.getName(), partitionedTables[partition], catalog_tbl.getIsreplicated());
            SynthesizedPlanFragment pf = new SynthesizedPlanFragment();
            pf.fragmentId = SysProcFragmentId.PF_SRLloadDistribute;
            pf.inputDependencyIds = new int[] {};
            pf.outputDependencyIds = new int[] { (int) DEP_SRLdistribute };
            // Spread the task across all partitions
            pf.multipartition = true;
            pf.nonExecSites = false;
            pf.destPartitionId = partition; // partitionsToSites[i - 1];
            pf.parameters = params;
            pf.last_task = false;
            pfs.add(pf);
            if (trace.val && partitionedTables[partition]!=null)
                sb.append(String.format("\n  Partition #%d: %d tuples", partition, partitionedTables[partition].getRowCount()));
        } // FOR
        if (trace.val)
            LOG.trace(sb.toString());

        // a final plan fragment to aggregate the results
        SynthesizedPlanFragment pf = new SynthesizedPlanFragment();
        pf.destPartitionId = this.partitionId;
        pf.fragmentId = SysProcFragmentId.PF_SRLloadAggregate;
        pf.inputDependencyIds = new int[] { (int) DEP_SRLdistribute };
        pf.outputDependencyIds = new int[] { (int) DEP_SRLaggregate };
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
                    LOG.trace(String.format("Adding %s [%d] to virtual column %d", table.getColumnName(catalog_col.getIndex()), catalog_col.getIndex(), i));
                row[catalog_col.getIndex()] = table.get(catalog_col.getIndex());
            } // FOR
            vt.addRow(row);
        } // WHILE
        if (debug.val)
            LOG.info(String.format("Vertical Partition %s -> %s\n", catalog_view.getParent().getName(), virtual_tbl.getName()) + vt);

        return (createReplicatedPlan(ts, virtual_tbl, vt));
    }

    public VoltTable[] loadMultipartitionTable(String tableName, VoltTable table) throws VoltAbortException {
        assert (table != null) : "VoltTable to be loaded into " + tableName + " is null in txn #" + this.getTransactionId();

        if (debug.val)
            LOG.debug(String.format("Executing multi-partition loader for %s with %d tuples in txn #%d [bytes=%d]", tableName, table.getRowCount(), this.getTransactionId(),
                    table.getUnderlyingBufferSize()));

        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        Table catalog_tbl = catalogContext.database.getTables().getIgnoreCase(tableName);
        if (catalog_tbl == null) {
            throw new VoltAbortException("Table '" + tableName + "' does not exist");
        } else if (table.getRowCount() == 0) {
            throw new VoltAbortException("The VoltTable for table '" + tableName + "' is empty");
        }

        LocalTransaction ts = this.getTransactionState();

        // if tableName is replicated, just send table everywhere.
        if (catalog_tbl.getIsreplicated()) {
            // If they haven't locked all of the partitions in the cluster, then
            // we'll
            // stop them right here and force them to get those
            if (ts.getPredictTouchedPartitions().size() != this.allPartitionsHistogram.getValueCount()) {
                throw new MispredictionException(this.getTransactionId(), this.allPartitionsHistogram);
            }
            pfs = this.createReplicatedPlan(ts, catalog_tbl, table);
        }
        // Otherwise, create a VoltTable for each partition and split up the
        // incoming table
        // then send those partial tables to the appropriate sites.
        else {
            pfs = this.createNonReplicatedPlan(ts, catalog_tbl, table);
        }

        // distribute and execute the fragments providing pfs and id
        // of the aggregator's output dependency table.
        if (debug.val)
            LOG.debug("Passing " + pfs.length + " sysproc fragments to executeSysProcPlanFragments()");
        results = executeSysProcPlanFragments(pfs, (int) DEP_SRLaggregate);

        // Check whether this table has a vertical partition
        // If so, then we'll automatically blast out the data that it needs
        MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition(catalog_tbl);
        if (debug.val)
            LOG.debug(String.format("%s - %s Vertical Partition: %s", ts, catalog_tbl.getName(), catalog_view));
        if (catalog_view != null) {
            if (debug.val)
                LOG.debug(String.format("%s - Updating %s's vertical partition %s", ts, catalog_tbl.getName(), catalog_view.getDest().getName()));
            executeSysProcPlanFragments(createVerticalPartitionPlan(ts, catalog_view, table), (int) DEP_SRLaggregate);
        }

        return (results);
    }

    @SuppressWarnings("deprecation")
    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, final SystemProcedureExecutionContext context) {
        AbstractTransaction ts = hstore_site.getTransaction(txn_id);
        String hostname = ConnectionUtil.getHostnameOrAddress();

        // need to return something ..
        VoltTable[] results = new VoltTable[1];
        results[0] = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        results[0].addRow(txn_id);

        if (fragmentId == SysProcFragmentId.PF_SRLrestoreScan) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.
            Host catalog_host = context.getHost();
            Site catalog_site = CollectionUtil.first(CatalogUtil.getSitesForHost(catalog_host));
            Integer lowest_site_id = catalog_site.getId();

            if (context.getPartitionExecutor().getSiteId() == lowest_site_id) {
                // implicitly synchronized by the way restore operates.
                // this scan must complete on every site and return results
                // to the coordinator for aggregation before it will send out
                // distribution fragments, so two sites on the same node
                // can't be attempting to set and clear this HashSet
                // simultaneously
                m_initializedTableSaveFiles.clear();
                m_saveFiles.clear();// Tests will reused a VoltDB process that
                // fails a restore

                m_filePath = (String) params.toArray()[0];
                m_fileNonce = (String) params.toArray()[1];
                LOG.trace("Checking saved table state for restore of: " + m_filePath + ", " + m_fileNonce);
                File[] savefiles = retrieveRelevantFiles(m_filePath, m_fileNonce);
                for (File file : savefiles) {
                    LOG.trace("Retrieving File :" + file);
                    TableSaveFile savefile = null;
                    try {
                        savefile = getTableSaveFile(file, 1, null);
                        try {

                            if (!savefile.getCompleted()) {
                                continue;
                            }

                            String is_replicated = "FALSE";
                            if (savefile.isReplicated()) {
                                is_replicated = "TRUE";
                            }
                            int partitionIds[] = savefile.getPartitionIds();
                            for (int pid : partitionIds) {
                                result.addRow(m_hostId, hostname, savefile.getHostId(), savefile.getHostname(), savefile.getClusterName(), savefile.getDatabaseName(), savefile.getTableName(),
                                        is_replicated, pid, savefile.getTotalPartitions());
                            }
                        } finally {
                            savefile.close();
                        }
                    } catch (FileNotFoundException e) {
                        // retrieveRelevantFiles should always generate a list
                        // of valid present files in m_filePath, so if we end up
                        // getting here, something has gone very weird.
                        e.printStackTrace();
                    } catch (IOException e) {
                        // For the time being I'm content to treat this as a
                        // missing file and let the coordinator complain if
                        // it discovers that it can't build a consistent
                        // database out of the files it sees available.
                        //
                        // Maybe just a log message? Later.
                        e.printStackTrace();
                    }
                }
            } else {
                // Initialize on other sites
                m_filePath = (String) params.toArray()[0];
                m_fileNonce = (String) params.toArray()[1];
            }

            return new DependencySet(DEP_SRLrestoreScan, result);
        } else if (fragmentId == SysProcFragmentId.PF_SRLrestoreScanResults) {
            LOG.trace("Aggregating saved table state");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_SRLrestoreScan);
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
            for (VoltTable table : dep) {
                while (table.advanceRow()) {
                    // the actually adds the active row... weird...
                    result.add(table);
                }
            }
            return new DependencySet(DEP_SRLrestoreScanResults, result);
        } else if (fragmentId == SysProcFragmentId.PF_SRLloadDistribute) {
            assert context.getCluster().getName() != null;
            assert context.getDatabase().getName() != null;
            assert params != null;
            assert params.toArray() != null;
            assert params.toArray()[0] != null;
            
            String table_name = (String) (params.toArray()[0]);
            VoltTable table = (VoltTable) (params.toArray()[1]);
            boolean is_replicated = (boolean) (params.toArray()[2]);
            if(is_replicated)
                assert params.toArray()[1] != null;
            
            Host catalog_host = context.getHost();
            Site catalog_site = context.getSite();
            Partition catalog_partition = context.getPartitionExecutor().getPartition();            
            Collection<Partition> pset = CatalogUtil.getAllPartitions(catalog_host);
            
            int p_itr = 0;
            int[] relevantPartitionIds = new int[pset.size()];
            for(Partition p : pset){
                relevantPartitionIds[p_itr++] = p.getId();
            }
                        
            LOG.trace("RelevantPartitions :"+pset.size());
            LOG.trace("loadDistribute at host :" + catalog_host.getId());

            // Construct table again if it is not replicated
            // As by default, it gets constructed only at one partition
            if (is_replicated == false) {
                // BBContainer is not backed by an array (uses allocateDirect),
                // so clean aggregation using ByteArrayOutputStream is not
                // possible
                // FIXME : Constant hacky
                int c_size = 1024 * 1024;
                ByteBuffer c_aggregate = ByteBuffer.allocate(c_size);

                try {

                    TableSaveFile savefile = getTableSaveFile(getSaveFileForPartitionedTable(m_filePath, m_fileNonce, table_name, 
                            catalog_host.getId(), 
                            catalog_site.getId(), 
                            catalog_partition.getId()),                             
                            3, relevantPartitionIds);

                    assert (savefile.getCompleted());                    

                    table = null;
                    c_aggregate.clear();

                    while (savefile.hasMoreChunks()) {

                        final org.voltdb.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                        if (c == null) {
                            continue;// Should be equivalent to break
                        }

                        c_aggregate.put(c.b);
                        c.discard();
                    }

                    // LOG.trace("c_aggregate position :" +
                    // c_aggregate.position());
                    // LOG.trace("c_aggregate capacity :" +
                    // c_aggregate.capacity());

                    if (c_aggregate.position() > 0) {
                        table = PrivateVoltTableFactory.createVoltTableFromBuffer(c_aggregate, true);
                        LOG.trace("VoltTable reconstructed :" + table);
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (debug.val)
                LOG.debug(String.format("Loading %d tuples for table '%s' in txn #%d", table.getRowCount(), table_name, txn_id));
            assert (this.isInitialized()) : " The sysproc " + this.getClass().getSimpleName() + " was not initialized properly";
            try {

                this.executor.loadTable(ts, context.getCluster().getName(), context.getDatabase().getName(), table_name, table, 0);

            } catch (VoltAbortException e) {
                // must continue and reply with dependency.
                e.printStackTrace();
            }
            if (debug.val)
                LOG.debug("Finished loading table. Things look good...");
            return new DependencySet(new int[] { (int) DEP_SRLdistribute }, results);

        } else if (fragmentId == SysProcFragmentId.PF_SRLloadAggregate) {
            if (debug.val)
                LOG.debug("Aggregating results from loading fragments in txn #" + txn_id);
            return new DependencySet(new int[] { (int) DEP_SRLaggregate }, results);
        }

        // must handle every dependency id.

        assert (false);
        return null;
    }

    public VoltTable[] run(String path, String nonce, long allowExport) throws VoltAbortException {
        final long startTime = System.currentTimeMillis();
        LOG.info("Restoring from path: " + path + ", with ID: " + nonce + " at " + startTime);

        // Fetch all the savefile metadata from the cluster
        VoltTable[] savefile_data;
        savefile_data = performRestoreScanWork(path, nonce);

        ClusterSaveFileState savefile_state = null;
        try {
            savefile_state = new ClusterSaveFileState(savefile_data[0], execution_context, (int) allowExport);
        } catch (IOException e) {
            throw new VoltAbortException(e.getMessage());
        }

        List<String> relevantTableNames = null;
        try {
            relevantTableNames = SnapshotUtil.retrieveRelevantTableNames(path, nonce);
        } catch (Exception e) {
            ColumnInfo[] result_columns = new ColumnInfo[2];
            int ii = 0;
            result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
            result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
            VoltTable results[] = new VoltTable[] { new VoltTable(result_columns) };
            results[0].addRow("FAILURE", e.toString());
            return results;
        }
        assert (relevantTableNames != null);
        assert (relevantTableNames.size() > 0);

        VoltTable[] results = null;
        for (String tableName : relevantTableNames) {
            if (!savefile_state.getSavedTableNames().contains(tableName)) {
                if (results == null) {
                    ColumnInfo[] result_columns = new ColumnInfo[2];
                    int ii = 0;
                    result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
                    result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
                    results = new VoltTable[] { new VoltTable(result_columns) };
                }
                results[0].addRow("FAILURE", "Save data contains no information for table " + tableName);
            }

            final TableSaveFileState saveFileState = savefile_state.getTableState(tableName);
            if (saveFileState == null || !saveFileState.isConsistent()) {
                if (results == null) {
                    ColumnInfo[] result_columns = new ColumnInfo[2];
                    int ii = 0;
                    result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
                    result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
                    results = new VoltTable[] { new VoltTable(result_columns) };
                }
                results[0].addRow("FAILURE", "Save data for " + tableName + " is inconsistent " + "(potentially missing partitions) or corrupted");
            }
        }
        if (results != null) {
            return results;
        }

        LOG.trace("performTableRestoreWork starts at partition :" + this.partitionId);

        results = performTableRestoreWork(savefile_state);

        final long endTime = System.currentTimeMillis();
        final double duration = (endTime - startTime) / 1000.0;
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        pw.toString();
        pw.printf("%.2f", duration);
        LOG.info("Finished restored of " + path + ", with ID: " + nonce + " at " + endTime + " took " + sw.toString() + " seconds");

        return results;
    }

    private final File[] retrieveRelevantFiles(String filePath, final String fileNonce) {
        FilenameFilter has_nonce = new FilenameFilter() {
            public boolean accept(File dir, String file) {
                return file.startsWith(fileNonce) && file.endsWith(".vpt");
            }
        };

        File save_dir = new File(filePath);
        File[] save_files = save_dir.listFiles(has_nonce);
        return save_files;
    }

    private VoltTable constructResultsTable() {
        ColumnInfo[] result_columns = new ColumnInfo[7];
        int ii = 0;
        result_columns[ii++] = new ColumnInfo(CNAME_HOST_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("HOSTNAME", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo(CNAME_SITE_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("TABLE", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo(CNAME_PARTITION_ID, CTYPE_ID);
        result_columns[ii++] = new ColumnInfo("RESULT", VoltType.STRING);
        result_columns[ii++] = new ColumnInfo("ERR_MSG", VoltType.STRING);
        return new VoltTable(result_columns);
    }

    private File getSaveFileForReplicatedTable(String tableName) {
        assert (m_fileNonce != null);
        StringBuilder filename_builder = new StringBuilder(m_fileNonce);
        filename_builder.append("-");
        filename_builder.append(tableName);
        filename_builder.append(".vpt");
        return new File(m_filePath, new String(filename_builder));
    }

    private static File getSaveFileForPartitionedTable(String filePath, String fileNonce, String tableName, int originalHostId, int siteId, int partitionId) {
        StringBuilder filename_builder = new StringBuilder(fileNonce);
        filename_builder.append("-");
        filename_builder.append(tableName);
        
        filename_builder.append("-host_");
        filename_builder.append(originalHostId);
        filename_builder.append("-site_");
        filename_builder.append(siteId);
        filename_builder.append("-partition_");
        filename_builder.append(partitionId);

        
        filename_builder.append(".vpt");
        return new File(filePath, new String(filename_builder));
    }

    private static TableSaveFile getTableSaveFile(File saveFile, int readAheadChunks, int relevantPartitionIds[]) throws IOException {
        FileInputStream savefile_input = new FileInputStream(saveFile);
        TableSaveFile savefile = new TableSaveFile(savefile_input.getChannel(), readAheadChunks, relevantPartitionIds);
        savefile.setFilePath(saveFile.getAbsolutePath());
        return savefile;
    }

    private final VoltTable[] performRestoreScanWork(String filePath, String fileNonce) {
        SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

        // This fragment causes each execution site to confirm the likely
        // success of writing tables to disk
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].fragmentId = SysProcFragmentId.PF_SRLrestoreScan;
        pfs[0].outputDependencyIds = new int[] { DEP_SRLrestoreScan };
        pfs[0].inputDependencyIds = new int[] {};
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce);
        pfs[0].parameters = params;

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_SRLrestoreScanResults;
        pfs[1].outputDependencyIds = new int[] { DEP_SRLrestoreScanResults };
        pfs[1].inputDependencyIds = new int[] { DEP_SRLrestoreScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_SRLrestoreScanResults);
        return results;
    }

    private Set<Table> getTablesToRestore(Set<String> savedTableNames) {
        Set<Table> tables_to_restore = new HashSet<Table>();
        for (Table table : this.catalogContext.database.getTables()) {
            if (savedTableNames.contains(table.getTypeName())) {
                if (table.getMaterializer() == null) {
                    tables_to_restore.add(table);
                } else {
                    LOG.info("Table: " + table.getTypeName() + " was saved " + "but is now a materialized table and will " + "not be loaded from disk");
                }
            } else {
                if (table.getMaterializer() == null) {
                    LOG.info("Table: " + table.getTypeName() + " does not have " + "any savefile data and so will not be loaded " + "from disk");
                }
            }
        }
        // XXX consider logging the list of tables that were saved but not
        // in the current catalog
        return tables_to_restore;
    }

    private VoltTable[] performTableRestoreWork(ClusterSaveFileState savefileState) throws VoltAbortException {
        Set<Table> tables_to_restore = getTablesToRestore(savefileState.getSavedTableNames());
        SystemProcedureExecutionContext context = savefileState.getSystemProcedureExecutionContext();
        assert (context != null);

        Host catalog_host = context.getHost();
        Site catalog_site = context.getSite();
        Partition catalog_partition = context.getPartitionExecutor().getPartition();            
        Collection<Partition> catalog_partitions = CatalogUtil.getPartitionsForHost(catalog_host);

        int[] relevantPartitionIds = new int[catalog_partitions.size()];

        int pt_itr = 0;
        for (Partition p : catalog_partitions) {
            relevantPartitionIds[pt_itr++] = p.getId();
        }

        String hostname = catalog_host.getName();

        VoltTable[] results = new VoltTable[1];
        VoltTable[] load_response = null;
        results[0] = PrivateVoltTableFactory.createUninitializedVoltTable();
        results[0] = constructResultsTable();

        // BBContainer is not backed by an array (uses allocateDirect),
        // so clean aggregation using ByteArrayOutputStream is not
        // possible
        // FIXME : Constant hacky
        int c_size = 1024 * 1024;
        ByteBuffer c_aggregate = ByteBuffer.allocate(c_size);

        for (Table t : tables_to_restore) {
            String tableName = t.getName();
            TableSaveFile savefile = null;

            LOG.trace("Restoring table :" + tableName);

            try {
                if (t.getIsreplicated()) {
                    savefile = getTableSaveFile(getSaveFileForReplicatedTable(tableName), 3, null);
                } else {
                    savefile = getTableSaveFile(getSaveFileForPartitionedTable(m_filePath, m_fileNonce, tableName, 
                            catalog_host.getId(),
                            catalog_site.getId(), 
                            catalog_partition.getId()),                             
                            3, relevantPartitionIds);
                }

                assert (savefile.getCompleted());

                VoltTable table = null;
                c_aggregate.clear();

                while (savefile.hasMoreChunks()) {

                    final org.voltdb.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                    if (c == null) {
                        continue;// Should be equivalent to break
                    }

                    c_aggregate.put(c.b);
                    c.discard();
                }

                // LOG.trace("c_aggregate position :" + c_aggregate.position());
                // LOG.trace("c_aggregate capacity :" + c_aggregate.capacity());

                if (c_aggregate.position() > 0) {
                    table = PrivateVoltTableFactory.createVoltTableFromBuffer(c_aggregate, true);

                    //LOG.trace("VoltTable to be loaded :" + table);

                    // Load the table on all partitions
                    load_response = loadMultipartitionTable(tableName, table);
                    results[0].addRow(m_hostId, hostname, m_siteId, tableName, 0, "SUCCESS", "Loaded table: " + tableName);
                } else {
                    results[0].addRow(m_hostId, hostname, m_siteId, tableName, 0, "SUCCESS", "No data to load : " + tableName);
                }

            } catch (Exception e) {
                e.printStackTrace();
                results[0].addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
                return results;
            }

        }

        return results;
    }

    private Table getCatalogTable(String tableName) {
        return this.catalogContext.database.getTables().get(tableName);
    }

    private int m_siteId;
    private int m_hostId;
    private static volatile String m_filePath;
    private static volatile String m_fileNonce;
}
