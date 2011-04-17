package org.voltdb.sysprocs;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.BackendTarget;
import org.voltdb.DependencySet;
import org.voltdb.ExecutionSite;
import org.voltdb.HsqlBackend;
import org.voltdb.ParameterSet;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import au.com.bytecode.opencsv.CSVWriter;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.PartitionEstimator;

public class DatabaseDump extends VoltSystemProcedure {
    
    private static final Logger LOG = Logger.getLogger(DatabaseDump.class);

    private Cluster m_cluster = null;
    private Database m_database = null;
    
    @Override
    public void globalInit(ExecutionSite site, Procedure catProc,
            BackendTarget eeType, HsqlBackend hsql, Cluster cluster,
            PartitionEstimator p_estimator, Integer local_partition) {
        super.globalInit(site, catProc, eeType, hsql, cluster, p_estimator, local_partition);
        m_cluster = cluster;
        m_database = CatalogUtil.getDatabase(m_cluster);
        site.registerPlanFragment(SysProcFragmentId.PF_distribute, this);
        site.registerPlanFragment(SysProcFragmentId.PF_aggregate, this);
    }
    
    @Override
    public DependencySet executePlanFragment(long txnId, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        // The first parameter should be the directory
        String directory = params.toArray()[0].toString();
        LOG.debug(String.format("Dumping out database contents for partition %d to '%s'", this.executor.getPartitionId(), directory));
        
        for (Table catalog_tbl : m_database.getTables()) {
            VoltTable vt = this.executor.getExecutionEngine().serializeTable(catalog_tbl.getRelativeIndex());
            assert(vt != null) : "Failed to get serialized table for " + catalog_tbl;
            
            File csv_file = new File(String.format("%s/%s.%02d.csv", directory, catalog_tbl.getName(), this.executor.getPartitionId()));
            LOG.debug(String.format("Writing %d tuples to '%s'", vt.getRowCount(), csv_file.getName()));
            try {
                CSVWriter writer = new CSVWriter(new FileWriter(csv_file));
                while (vt.advanceRow()) {
                    String row[] = vt.getRowStringArray();
                    assert(row != null);
                    assert(row.length == vt.getColumnCount());
                    writer.writeNext(row);
                } // WHILE
                writer.close();
            } catch (Exception ex) {
                LOG.fatal(String.format("Failed to write data to '%s'", csv_file), ex);
                throw new RuntimeException(ex);
            }
        }
        return null;
    }
    
    /**
     * 
     * @param tableName
     * @param table
     * @return
     * @throws VoltAbortException
     */
    public VoltTable[] run(String directory) throws VoltAbortException {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];
        int numPartitions = m_cluster.getNum_partitions();

        // Generate a plan fragment for each site using the sub-tables
        pfs = new SynthesizedPlanFragment[numPartitions  + 1];
        for (int i = 1; i <= numPartitions; ++i) {
            int partition = i - 1;
            ParameterSet params = new ParameterSet();
            params.setParameters(directory);
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = SysProcFragmentId.PF_dumpScan;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpScan };
            pfs[i].multipartition = false;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = partition;
            pfs[i].parameters = params;
            pfs[i].last_task = true;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].destPartitionId = base_partition;
        pfs[0].fragmentId = SysProcFragmentId.PF_dumpAggregate;
        pfs[0].inputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpScan };
        pfs[0].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpAggregate };
        pfs[0].multipartition = false;
        pfs[0].nonExecSites = false;
        pfs[0].parameters = new ParameterSet();

        // send these forth in to the world .. and wait
        if (debug) LOG.debug("Passing " + pfs.length + " sysproc fragments to executeSysProcPlanFragments()");
        results = executeSysProcPlanFragments(pfs, (int)SysProcFragmentId.PF_dumpAggregate);
        return results;
    }

}
