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
import java.io.ByteArrayOutputStream;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.ProcInfo;
import org.voltdb.TheHashinator;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Partition;
import org.voltdb.client.ConnectionUtil;
import org.voltdb.sysprocs.saverestore.ClusterSaveFileState;
import org.voltdb.sysprocs.saverestore.SavedTableConverter;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.TableSaveFile;
import org.voltdb.sysprocs.saverestore.TableSaveFileState;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DBBPool;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.CollectionUtil;

@ProcInfo(singlePartition = false)
public class SnapshotRestore extends VoltSystemProcedure {
    private static final Logger LOG = Logger.getLogger(SnapshotRestore.class);

    private static final int DEP_restoreScan = (int) SysProcFragmentId.PF_restoreScan | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    private static final int DEP_restoreScanResults = (int) SysProcFragmentId.PF_restoreScanResults;
    public static final int DEP_restoreDistributePartitionedTable = (int) SysProcFragmentId.PF_restoreDistributePartitionedTable | HStoreConstants.MULTIPARTITION_DEPENDENCY;
    public static final int DEP_restoreDistributePartitionedTableResults = (int) SysProcFragmentId.PF_restoreDistributePartitionedTable;

    private static HashSet<String> m_initializedTableSaveFiles = new HashSet<String>();
    private static ArrayDeque<TableSaveFile> m_saveFiles = new ArrayDeque<TableSaveFile>();

    private static synchronized void initializeTableSaveFiles(String filePath, String fileNonce, String tableName, int originalHostIds[], int relevantPartitionIds[],
            SystemProcedureExecutionContext context) throws IOException {
        // This check ensures that only one site per host attempts to
        // distribute this table. @SnapshotRestore sends plan fragments
        // to every site on this host with the tables and partition ID that
        // this host is going to distribute to the cluster. The first
        // execution site to get into this synchronized method is going to
        // 'win', add the table it's doing to this set, and then do the rest
        // of the work. Subsequent sites will just return here.
        if (!m_initializedTableSaveFiles.add(tableName)) {
            return;
        }
        
        int siteId = context.getHStoreSite().getSiteId();
        int partitionId = context.getPartitionExecutor().getPartitionId();
        
        for (int originalHostId : originalHostIds) {
            final File f = getSaveFileForPartitionedTable(filePath, fileNonce, tableName, originalHostId, siteId, partitionId);

            Host catalog_host = context.getHost();
            Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);
            
            m_saveFiles.offer(getTableSaveFile(f, catalog_sites.size() * 4, relevantPartitionIds));
            assert (m_saveFiles.peekLast().getCompleted());
        }
    }

    private static synchronized boolean hasMoreChunks() throws IOException {
        boolean hasMoreChunks = false;
        while (!hasMoreChunks && m_saveFiles.peek() != null) {
            TableSaveFile f = m_saveFiles.peek();
            hasMoreChunks = f.hasMoreChunks();
            if (!hasMoreChunks) {
                try {
                    f.close();
                } catch (IOException e) {
                }
                m_saveFiles.poll();
            }
        }
        return hasMoreChunks;
    }

    private static synchronized BBContainer getNextChunk() throws IOException {
        BBContainer c = null;
        while (c == null && m_saveFiles.peek() != null) {
            TableSaveFile f = m_saveFiles.peek();
            LOG.trace("File Channel Size :" + f.getFileChannel().size());
            LOG.trace("File Channel Position :" + f.getFileChannel().position());
            c = f.getNextChunk();
            if (c == null) {
                LOG.trace("getNextChunk null");
                f.close();
                m_saveFiles.poll();
            }
        }
        return c;
    }

    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreScan, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreScanResults, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreLoadReplicatedTable, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreLoadReplicatedTableResults, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreDistributeReplicatedTable, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreDistributePartitionedTable, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreDistributePartitionedTableResults, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreSendReplicatedTable, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreSendReplicatedTableResults, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreSendPartitionedTable, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_restoreSendPartitionedTableResults, this);
        m_siteId = executor.getSiteId();
        m_hostId = ((Site) executor.getPartition().getParent()).getHost().getId();
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, final SystemProcedureExecutionContext context) {
        AbstractTransaction ts = hstore_site.getTransaction(txn_id);
        String hostname = ConnectionUtil.getHostnameOrAddress();
        if (fragmentId == SysProcFragmentId.PF_restoreScan) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
         
            // Choose the lowest site ID on this host to do the file scan
            // All other sites should just return empty results tables.            
            Host catalog_host = context.getHost();            
            Site catalog_site = CollectionUtil.first(CatalogUtil.getSitesForHost(catalog_host));
            
            CatalogMap<Partition> partition_map = catalog_site.getPartitions();
            Integer lowest_partition_id = Integer.MAX_VALUE, p_id;        
            for (Partition pt : partition_map) {
                p_id = pt.getId();
                lowest_partition_id = Math.min(p_id, lowest_partition_id);
            }        
            assert (lowest_partition_id != Integer.MAX_VALUE);
            
            int partition_id = context.getPartitionExecutor().getPartitionId();                                            

            if (partition_id == lowest_partition_id) {
                LOG.trace("restoreScan :: Partition id :" + partition_id);
                
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

            return new DependencySet(DEP_restoreScan, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreScanResults) {
            LOG.trace("Aggregating saved table state");
            assert (dependencies.size() > 0);
            List<VoltTable> dep = dependencies.get(DEP_restoreScan);
            VoltTable result = ClusterSaveFileState.constructEmptySaveFileStateVoltTable();
            for (VoltTable table : dep) {
                while (table.advanceRow()) {
                    // the actually adds the active row... weird...
                    result.add(table);
                }
            }
            return new DependencySet(DEP_restoreScanResults, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreLoadReplicatedTable) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            assert (params.toArray()[2] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            int allowExport = (Integer) params.toArray()[2];
            LOG.trace("restoreLoadReplicatedTable :: Partition id :" + context.getPartitionExecutor().getPartitionId());
            //LOG.trace("Dependency_id :" + dependency_id + " - Loading replicated table: " + table_name);
            String result_str = "SUCCESS";
            String error_msg = "";
            TableSaveFile savefile = null;

            /**
             * For replicated tables this will do the slow thing and read the
             * file once for each ExecutionSite. This could use optimization
             * like is done with the partitioned tables.
             */
            try {
                savefile = getTableSaveFile(getSaveFileForReplicatedTable(table_name), 3, null);
                assert (savefile.getCompleted());
            } catch (IOException e) {
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, m_siteId, table_name, -1, "FAILURE", "Unable to load table: " + table_name + " error: " + e.getMessage());
                return new DependencySet(dependency_id, result);
            }

            try {

                while (savefile.hasMoreChunks()) {
                    VoltTable table = null;
                    final org.voltdb.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                    if (c == null) {
                        continue;// Should be equivalent to break
                    }
                    VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                    Table new_catalog_table = getCatalogTable(table_name);
                    table = SavedTableConverter.convertTable(old_table, new_catalog_table);
                    c.discard();
                    try {
                        LOG.trace("LoadTable " + table_name);
                        this.executor.loadTable(ts, context.getCluster().getTypeName(), context.getDatabase().getTypeName(), table_name, table, allowExport);
                    } catch (VoltAbortException e) {
                        result_str = "FAILURE";
                        error_msg = e.getMessage();
                        break;
                    }
                }

            } catch (IOException e) {
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, m_siteId, table_name, -1, "FAILURE", "Unable to load table: " + table_name + " error: " + e.getMessage());
                return new DependencySet(dependency_id, result);
            } catch (VoltTypeException e) {
                VoltTable result = constructResultsTable();
                result.addRow(m_hostId, hostname, m_siteId, table_name, -1, "FAILURE", "Unable to load table: " + table_name + " error: " + e.getMessage());
                return new DependencySet(dependency_id, result);
            }

            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, table_name, -1, result_str, error_msg);
            try {
                savefile.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreDistributeReplicatedTable) {
            // XXX I tested this with a hack that cannot be replicated
            // in a unit test since it requires hacks to this sysproc that
            // effectively break it
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            assert (params.toArray()[2] != null);
            assert (params.toArray()[3] != null);
            String table_name = (String) params.toArray()[0];
            int site_id = (Integer) params.toArray()[1];
            int dependency_id = (Integer) params.toArray()[2];
            int allowExport = (Integer) params.toArray()[3];
            LOG.trace("Distributing replicated table: " + table_name + " to: " + site_id);
            VoltTable result = performDistributeReplicatedTable(table_name, site_id, context, allowExport);
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreSendReplicatedTable) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            assert (params.toArray()[2] != null);
            assert (params.toArray()[3] != null);
            String table_name = (String) params.toArray()[0];
            int dependency_id = (Integer) params.toArray()[1];
            VoltTable table = (VoltTable) params.toArray()[2];
            int allowExport = (Integer) params.toArray()[3];
            LOG.trace("Received replicated table: " + table_name);
            String result_str = "SUCCESS";
            String error_msg = "";
            try {
                this.executor.loadTable(ts, context.getCluster().getTypeName(), context.getDatabase().getTypeName(), table_name, table, allowExport);
            } catch (VoltAbortException e) {
                result_str = "FAILURE";
                error_msg = e.getMessage();
            }
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, table_name, -1, result_str, error_msg);
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreSendReplicatedTableResults) {
            assert (params.toArray()[0] != null);
            int dependency_id = (Integer) params.toArray()[0];
            LOG.trace("Received confirmmation of successful replicated table load");
            VoltTable result = constructResultsTable();
            for (int dep_id : dependencies.keySet()) {
                List<VoltTable> table_list = dependencies.get(dep_id);
                assert (table_list.size() == 1);
                VoltTable t = table_list.get(0);
                while (t.advanceRow()) {
                    // this will actually add the active row of t
                    result.add(t);
                }
            }
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreLoadReplicatedTableResults) {
            LOG.trace("Aggregating replicated table restore results");
            assert (params.toArray()[0] != null);
            int dependency_id = (Integer) params.toArray()[0];
            assert (dependencies.size() > 0);
            VoltTable result = constructResultsTable();
            for (int dep_id : dependencies.keySet()) {
                List<VoltTable> table_list = dependencies.get(dep_id);
                assert (table_list.size() == 1);
                VoltTable t = table_list.get(0);
                while (t.advanceRow()) {
                    // this will actually add the active row of t
                    result.add(t);
                }
            }
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreDistributePartitionedTable) {
            Object paramsA[] = params.toArray();
            assert (paramsA[0] != null);
            assert (paramsA[1] != null);
            assert (paramsA[2] != null);
            assert (paramsA[3] != null);

            String table_name = (String) paramsA[0];
            int originalHosts[] = (int[]) paramsA[1];
            int relevantPartitions[] = (int[]) paramsA[2];
            int dependency_id = (Integer) paramsA[3];
            int allowExport = (Integer) paramsA[4];

            // Using Localized Version
            VoltTable result = performLoadPartitionedTable(table_name, originalHosts, relevantPartitions, context, allowExport, ts);

            // Distributed Version - Invokes another round of plan fragments
            // which does not work
            // VoltTable result =
            // performDistributePartitionedTable(table_name, originalHosts,
            // relevantPartitions, context, allowExport);
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreDistributePartitionedTableResults) {
            LOG.trace("Aggregating partitioned table restore results");
            assert (params.toArray()[0] != null);
            int dependency_id = (Integer) params.toArray()[0];
            VoltTable result = constructResultsTable();
            for (int dep_id : dependencies.keySet()) {
                List<VoltTable> table_list = dependencies.get(dep_id);
                assert (table_list.size() == 1);
                VoltTable t = table_list.get(0);
                while (t.advanceRow()) {
                    // this will actually add the active row of t
                    result.add(t);
                }
            }
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreSendPartitionedTable) {
            assert (params.toArray()[0] != null);
            assert (params.toArray()[1] != null);
            assert (params.toArray()[2] != null);
            assert (params.toArray()[3] != null);
            assert (params.toArray()[4] != null);
            String table_name = (String) params.toArray()[0];
            int partition_id = (Integer) params.toArray()[1];
            int dependency_id = (Integer) params.toArray()[2];
            VoltTable table = (VoltTable) params.toArray()[3];
            int allowExport = (Integer) params.toArray()[4];
            LOG.trace("Received partitioned table: " + table_name);
            String result_str = "SUCCESS";
            String error_msg = "";
            try {
                this.executor.loadTable(ts, context.getCluster().getTypeName(), context.getDatabase().getTypeName(), table_name, table, allowExport);
            } catch (VoltAbortException e) {
                result_str = "FAILURE";
                error_msg = e.getMessage();
            }
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, table_name, partition_id, result_str, error_msg);
            return new DependencySet(dependency_id, result);
        } else if (fragmentId == SysProcFragmentId.PF_restoreSendPartitionedTableResults) {
            assert (params.toArray()[0] != null);
            int dependency_id = (Integer) params.toArray()[0];
            LOG.trace("Received confirmation of successful partitioned table load");
            VoltTable result = constructResultsTable();
            for (int dep_id : dependencies.keySet()) {
                List<VoltTable> table_list = dependencies.get(dep_id);
                assert (table_list.size() == 1);
                VoltTable t = table_list.get(0);
                while (t.advanceRow()) {
                    // this will actually add the active row of t
                    result.add(t);
                }
            }
            return new DependencySet(dependency_id, result);
        }

        assert (false);
        return null;
    }

    // private final VoltSampler m_sampler = new VoltSampler(10, "sample" +
    // String.valueOf(new Random().nextInt() % 10000) + ".txt");

    public VoltTable[] run(String path, String nonce, long allowExport) throws VoltAbortException {
        // m_sampler.start();
        final long startTime = System.currentTimeMillis();
        LOG.info("Restoring from path: " + path + ", with ID: " + nonce + " at " + startTime);

        // Fetch all the savefile metadata from the cluster
        VoltTable[] savefile_data;
        savefile_data = performRestoreScanWork(path, nonce);
        
        //LOG.trace("Restore Scan Results "+savefile_data[0]);

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

        LOG.trace("performTableRestoreWork starts at Site :" + execution_context.getSite().getId());

        results = performTableRestoreWork(savefile_state);

        final long endTime = System.currentTimeMillis();
        final double duration = (endTime - startTime) / 1000.0;
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        pw.toString();
        pw.printf("%.2f", duration);
        LOG.info("Finished restored of " + path + ", with ID: " + nonce + " at " + endTime + " took " + sw.toString() + " seconds");
        // m_sampler.setShouldStop();
        // try {
        // m_sampler.join();
        // } catch (InterruptedException e) {
        // // TODO Auto-generated catch block
        // e.printStackTrace();
        // }
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
        pfs[0].fragmentId = SysProcFragmentId.PF_restoreScan;
        pfs[0].outputDependencyIds = new int[] { DEP_restoreScan };
        pfs[0].inputDependencyIds = new int[] {};
        pfs[0].multipartition = true;
        ParameterSet params = new ParameterSet();
        params.setParameters(filePath, fileNonce);
        pfs[0].parameters = params;

        // This fragment aggregates the save-to-disk sanity check results
        pfs[1] = new SynthesizedPlanFragment();
        pfs[1].fragmentId = SysProcFragmentId.PF_restoreScanResults;
        pfs[1].outputDependencyIds = new int[] { DEP_restoreScanResults };
        pfs[1].inputDependencyIds = new int[] { DEP_restoreScan };
        pfs[1].multipartition = false;
        pfs[1].parameters = new ParameterSet();

        VoltTable[] results;
        results = executeSysProcPlanFragments(pfs, DEP_restoreScanResults);
        return results;
    }

    private Set<Table> getTablesToRestore(Set<String> savedTableNames) {
        Set<Table> tables_to_restore = new HashSet<Table>();
        for (Table table : this.catalogContext.database.getTables()) {
            if (savedTableNames.contains(table.getTypeName())) {
                if (table.getMaterializer() == null) {
                    tables_to_restore.add(table);
                } else {
                    // LOG_TRIAGE reconsider info level here?
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
        VoltTable[] restore_results = new VoltTable[1];
        restore_results[0] = constructResultsTable();
        ArrayList<SynthesizedPlanFragment[]> restorePlans = new ArrayList<SynthesizedPlanFragment[]>();

        for (Table t : tables_to_restore) {
            TableSaveFileState table_state = savefileState.getTableState(t.getTypeName());
            SynthesizedPlanFragment[] restore_plan = table_state.generateRestorePlan(t);
            if (restore_plan == null) {
                LOG.error("Unable to generate restore plan for " + t.getTypeName() + " table not restored");
                throw new VoltAbortException("Unable to generate restore plan for " + t.getTypeName() + " table not restored");
            }
            restorePlans.add(restore_plan);
        }

        Iterator<Table> tableIterator = tables_to_restore.iterator();
        for (SynthesizedPlanFragment[] restore_plan : restorePlans) {
            Table table = tableIterator.next();
            TableSaveFileState table_state = savefileState.getTableState(table.getTypeName());
            LOG.trace("Performing restore for table: " + table.getTypeName());
            // LOG.trace("Plan has fragments: " + restore_plan.length);
            VoltTable[] results = executeSysProcPlanFragments(restore_plan, table_state.getRootDependencyId());
            while (results[0].advanceRow()) {
                // this will actually add the active row of results[0]
                restore_results[0].add(results[0]);
            }
        }
        return restore_results;
    }

    // XXX I hacked up a horrible one-off in my world to test this code.
    // I believe that it will work for at least one new node, but
    // there's not a good way to add a unit test for this at the moment,
    // so the emma coverage is weak.
    private VoltTable performDistributeReplicatedTable(String tableName, int siteId, SystemProcedureExecutionContext context, int allowExport) {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        TableSaveFile savefile = null;
        try {
            savefile = getTableSaveFile(getSaveFileForReplicatedTable(tableName), 3, null);
            assert (savefile.getCompleted());
        } catch (IOException e) {
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        LOG.trace("Starting performDistributeReplicatedTable" + tableName);

        VoltTable[] results = null;
        results[0].addRow(m_hostId, hostname, m_siteId, tableName, -1, "SUCCESS", "NO DATA TO DISTRIBUTE");
        final Table new_catalog_table = getCatalogTable(tableName);
        Boolean needsConversion = null;
        try {

            while (savefile.hasMoreChunks()) {
                VoltTable table = null;
                final org.voltdb.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                if (c == null) {
                    continue;// Should be equivalent to break
                }

                if (needsConversion == null) {
                    VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b.duplicate(), true);
                    needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table);
                }
                if (needsConversion.booleanValue()) {
                    VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                    table = SavedTableConverter.convertTable(old_table, new_catalog_table);
                } else {
                    ByteBuffer copy = ByteBuffer.allocate(c.b.remaining());
                    copy.put(c.b);
                    copy.flip();
                    table = PrivateVoltTableFactory.createVoltTableFromBuffer(copy, true);
                }
                c.discard();

                SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[2];

                int result_dependency_id = TableSaveFileState.getNextDependencyId();
                pfs[0] = new SynthesizedPlanFragment();
                pfs[0].fragmentId = SysProcFragmentId.PF_restoreSendReplicatedTable;
                // XXX pfs[0].siteId = siteId;
                pfs[0].destPartitionId = siteId;
                pfs[0].outputDependencyIds = new int[] { result_dependency_id };
                pfs[0].inputDependencyIds = new int[] {};
                pfs[0].multipartition = false;
                ParameterSet params = new ParameterSet();
                params.setParameters(tableName, result_dependency_id, table, allowExport);
                pfs[0].parameters = params;

                int final_dependency_id = TableSaveFileState.getNextDependencyId();
                pfs[1] = new SynthesizedPlanFragment();
                pfs[1].fragmentId = SysProcFragmentId.PF_restoreSendReplicatedTableResults;
                pfs[1].outputDependencyIds = new int[] { final_dependency_id };
                pfs[1].inputDependencyIds = new int[] { result_dependency_id };
                pfs[1].multipartition = false;
                ParameterSet result_params = new ParameterSet();
                result_params.setParameters(final_dependency_id);
                pfs[1].parameters = result_params;
                LOG.trace("Sending replicated table: " + tableName + " to site id:" + siteId);
                results = executeSysProcPlanFragments(pfs, final_dependency_id);
            }

            for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
                System.out.println(ste);
            }

        } catch (IOException e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        } catch (VoltTypeException e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        return results[0];
    }

    private VoltTable performLoadPartitionedTable(String tableName, int originalHostIds[], int relevantPartitionIds[], SystemProcedureExecutionContext context, int allowExport, AbstractTransaction ts) {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        // XXX This is all very similar to the splitting code in
        // LoadMultipartitionTable. Consider ways to consolidate later
        Map<Integer, Integer> sites_to_partitions = new HashMap<Integer, Integer>();

        // CHANGE : Up Sites
        Host catalog_host = context.getHost();
        Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);
        Site catalog_site = context.getSite();
        Partition catalog_partition = context.getPartitionExecutor().getPartition();            

        LOG.trace("Table :" + tableName);

        for (Site site : catalog_sites) {
            for (Partition partition : site.getPartitions()) {
                sites_to_partitions.put(site.getId(), partition.getId());
            }
        }

        try {
            initializeTableSaveFiles(m_filePath, m_fileNonce, tableName, originalHostIds, relevantPartitionIds, context);
        } catch (IOException e) {
            VoltTable result = constructResultsTable();
            // e.printStackTrace();
            result.addRow(m_hostId, hostname, m_siteId, tableName, relevantPartitionIds[0], "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        int partition_id = context.getPartitionExecutor().getPartitionId();
        LOG.trace("Starting performLoadPartitionedTable " + tableName + " at partition - " + partition_id);

        String result_str = "SUCCESS";
        String error_msg = "";
        TableSaveFile savefile = null;

        /**
         * For partitioned tables
         */
        try {
            savefile = getTableSaveFile(getSaveFileForPartitionedTable(m_filePath, m_fileNonce, tableName, 
                    catalog_host.getId(),
                    catalog_site.getId(), 
                    catalog_partition.getId()),                             
                    3, null);
            assert (savefile.getCompleted());
        } catch (IOException e) {
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        try {

            while (savefile.hasMoreChunks()) {
                VoltTable table = null;
                final org.voltdb.utils.DBBPool.BBContainer c = savefile.getNextChunk();
                if (c == null) {
                    continue; // Should be equivalent to break
                }
                VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c.b, true);
                Table new_catalog_table = getCatalogTable(tableName);
                table = SavedTableConverter.convertTable(old_table, new_catalog_table);
                c.discard();
                try {
                    LOG.trace("LoadTable " + tableName);

                    this.executor.loadTable(ts, context.getCluster().getTypeName(), context.getDatabase().getTypeName(), tableName, table, allowExport);
                } catch (VoltAbortException e) {
                    result_str = "FAILURE";
                    error_msg = e.getMessage();
                    break;
                }
            }

        } catch (Exception e) {
            VoltTable result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, -1, "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        VoltTable result = constructResultsTable();
        result.addRow(m_hostId, hostname, m_siteId, tableName, -1, result_str, error_msg);
        try {
            savefile.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return result;
    }

    private VoltTable performDistributePartitionedTable(String tableName, int originalHostIds[], int relevantPartitionIds[], SystemProcedureExecutionContext context, int allowExport) {
        String hostname = ConnectionUtil.getHostnameOrAddress();
        // XXX This is all very similar to the splitting code in
        // LoadMultipartitionTable. Consider ways to consolidate later
        Map<Integer, Integer> sites_to_partitions = new HashMap<Integer, Integer>();

        // CHANGE : Up Sites
        Host catalog_host = context.getHost();
        Collection<Site> catalog_sites = CatalogUtil.getSitesForHost(catalog_host);
        // Collection<Site> catalog_sites =
        // CatalogUtil.getAllSites(HStore.instance().getCatalog());

        LOG.trace("Table :" + tableName);

        for (Site site : catalog_sites) {
            for (Partition partition : site.getPartitions()) {
                sites_to_partitions.put(site.getId(), partition.getId());
            }
        }

        try {
            initializeTableSaveFiles(m_filePath, m_fileNonce, tableName, originalHostIds, relevantPartitionIds, context);
        } catch (IOException e) {
            VoltTable result = constructResultsTable();
            // e.printStackTrace();
            result.addRow(m_hostId, hostname, m_siteId, tableName, relevantPartitionIds[0], "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        LOG.trace("Starting performDistributePartitionedTable " + tableName);

        VoltTable[] results = new VoltTable[] { constructResultsTable() };
        results[0].addRow(m_hostId, hostname, m_siteId, tableName, 0, "NO DATA TO DISTRIBUTE", "");
        final Table new_catalog_table = getCatalogTable(tableName);
        Boolean needsConversion = null;
        BBContainer c = null;

        int c_size = 1024 * 1024;
        ByteBuffer c_aggregate = ByteBuffer.allocateDirect(c_size);

        try {
            VoltTable table = null;
            c = null;

            while (hasMoreChunks()) {
                c = getNextChunk();
                if (c == null) {
                    continue; // Should be equivalent to break
                }

                c_aggregate.put(c.b);
            }

            LOG.trace("c_aggregate position :" + c_aggregate.position());
            LOG.trace("c_aggregate capacity :" + c_aggregate.capacity());

            if (needsConversion == null) {
                VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c_aggregate.duplicate(), true);
                needsConversion = SavedTableConverter.needsConversion(old_table, new_catalog_table);
            }

            final VoltTable old_table = PrivateVoltTableFactory.createVoltTableFromBuffer(c_aggregate, true);
            if (needsConversion) {
                table = SavedTableConverter.convertTable(old_table, new_catalog_table);
            } else {
                table = old_table;
            }

            LOG.trace("createPartitionedTables :" + tableName);

            VoltTable[] partitioned_tables = createPartitionedTables(tableName, table);
            if (c != null) {
                c.discard();
            }

            // LoadMultipartitionTable -- send data to all ..

            int[] dependencyIds = new int[sites_to_partitions.size()];

            SynthesizedPlanFragment[] pfs = new SynthesizedPlanFragment[sites_to_partitions.size() + 1];
            int pfs_index = 0;
            for (int site_id : sites_to_partitions.keySet()) {
                int partition_id = sites_to_partitions.get(site_id);
                LOG.trace("Site id :" + site_id + " Partition id :" + partition_id);

                dependencyIds[pfs_index] = TableSaveFileState.getNextDependencyId();
                pfs[pfs_index] = new SynthesizedPlanFragment();
                pfs[pfs_index].fragmentId = SysProcFragmentId.PF_restoreSendPartitionedTable;
                // XXX pfs[pfs_index].siteId = site_id;
                pfs[pfs_index].destPartitionId = site_id;
                pfs[pfs_index].multipartition = false;
                pfs[pfs_index].outputDependencyIds = new int[] { dependencyIds[pfs_index] };
                pfs[pfs_index].inputDependencyIds = new int[] {};
                ParameterSet params = new ParameterSet();
                params.setParameters(tableName, partition_id, dependencyIds[pfs_index], partitioned_tables[partition_id], allowExport);
                pfs[pfs_index].parameters = params;
                ++pfs_index;
            }

            int result_dependency_id = TableSaveFileState.getNextDependencyId();
            pfs[sites_to_partitions.size()] = new SynthesizedPlanFragment();
            pfs[sites_to_partitions.size()].fragmentId = SysProcFragmentId.PF_restoreSendPartitionedTableResults;
            pfs[sites_to_partitions.size()].multipartition = false;
            pfs[sites_to_partitions.size()].outputDependencyIds = new int[] { result_dependency_id };
            pfs[sites_to_partitions.size()].inputDependencyIds = dependencyIds;
            ParameterSet params = new ParameterSet();
            params.setParameters(result_dependency_id);
            pfs[sites_to_partitions.size()].parameters = params;
            results = executeSysProcPlanFragments(pfs, result_dependency_id);

        } catch (IOException e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, relevantPartitionIds[0], "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        } catch (BufferOverflowException e) {
            LOG.trace("BufferOverflowException " + e.getMessage());
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, relevantPartitionIds[0], "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        } catch (VoltTypeException e) {
            VoltTable result = PrivateVoltTableFactory.createUninitializedVoltTable();
            result = constructResultsTable();
            result.addRow(m_hostId, hostname, m_siteId, tableName, relevantPartitionIds[0], "FAILURE", "Unable to load table: " + tableName + " error: " + e.getMessage());
            return result;
        }

        return results[0];
    }

    private VoltTable[] createPartitionedTables(String tableName, VoltTable loadedTable) {
        int number_of_partitions = this.catalogContext.numberOfPartitions;
        Table catalog_table = catalogContext.database.getTables().getIgnoreCase(tableName);
        assert (!catalog_table.getIsreplicated());
        // XXX blatantly stolen from LoadMultipartitionTable
        // find the index and type of the partitioning attribute
        int partition_col = catalog_table.getPartitioncolumn().getIndex();
        VoltType partition_type = VoltType.get((byte) catalog_table.getPartitioncolumn().getType());

        // create a table for each partition
        VoltTable[] partitioned_tables = new VoltTable[number_of_partitions];
        for (int i = 0; i < partitioned_tables.length; i++) {
            partitioned_tables[i] = loadedTable.clone(loadedTable.getUnderlyingBufferSize() / number_of_partitions);
        }

        // split the input table into per-partition units
        while (loadedTable.advanceRow()) {
            int partition = 0;
            try {
                partition = TheHashinator.hashToPartition(loadedTable.get(partition_col, partition_type));
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e.getMessage());
            }
            // this adds the active row of loadedTable
            partitioned_tables[partition].add(loadedTable);
        }

        return partitioned_tables;
    }

    private Table getCatalogTable(String tableName) {
        return this.catalogContext.database.getTables().get(tableName);
    }

    private int m_siteId;
    private int m_hostId;
    private static volatile String m_filePath;
    private static volatile String m_fileNonce;
}
