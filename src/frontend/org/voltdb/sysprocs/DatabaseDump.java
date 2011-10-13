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
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.ExecutionSite.SystemProcedureExecutionContext;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import au.com.bytecode.opencsv.CSVWriter;

import edu.brown.utils.PartitionEstimator;

@ProcInfo(singlePartition = false)
public class DatabaseDump extends VoltSystemProcedure {
    
    private static final Logger LOG = Logger.getLogger(DatabaseDump.class);

    @Override
    public void globalInit(ExecutionSite site, Procedure catalog_proc,
            BackendTarget eeType, HsqlBackend hsql, PartitionEstimator p_estimator) {
        super.globalInit(site, catalog_proc, eeType, hsql, p_estimator);
        site.registerPlanFragment(SysProcFragmentId.PF_dumpDistribute, this);
        site.registerPlanFragment(SysProcFragmentId.PF_dumpAggregate, this);
    }
    
    @Override
    public DependencySet executePlanFragment(long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        final boolean debug = LOG.isDebugEnabled();
        
        // need to return something ..
        VoltTable[] result = new VoltTable[1];
        result[0] = new VoltTable(new VoltTable.ColumnInfo("TxnId", VoltType.BIGINT));
        result[0].addRow(txn_id);
        
        if (fragmentId == SysProcFragmentId.PF_dumpDistribute) {
            // The first parameter should be the directory
            File directory = new File(params.toArray()[0].toString());
            LOG.debug(String.format("Dumping out database contents for partition %d to '%s'", this.executor.getPartitionId(), directory));
            
            if (directory.exists() == false) {
                LOG.debug("Creating dump directory '" + directory + "'");
                directory.mkdirs();
            }
            assert(directory.isDirectory());
            
            int batch_size = 1000;
            
            // Only write column names if it's the first partition (hack)
            boolean write_header = (this.executor.getPartitionId() == 0);
            
            for (Table catalog_tbl : this.database.getTables()) {
                File csv_file = new File(String.format("%s/%s.%02d.csv", directory, catalog_tbl.getName(), this.executor.getPartitionId()));
                CSVWriter writer = null;
                try {
                    writer = new CSVWriter(new FileWriter(csv_file));
                } catch (Exception ex) {
                    LOG.fatal(String.format("Failed to create CSVWriter for '%s'", csv_file), ex);
                    throw new RuntimeException(ex);
                }
                
                if (write_header) {
                    String cols[] = new String[catalog_tbl.getColumns().size()];
                    for (int i = 0; i < cols.length; i++) {
                        cols[i] = catalog_tbl.getColumns().get(i).getName();
                    } // FOR
                    writer.writeNext(cols);
                }

                int total = 0;
                while (true) {
                    LOG.debug(String.format("%s: offset=%d, limit=%d", catalog_tbl.getName(), total, batch_size));
                    VoltTable vt = this.executor.getExecutionEngine().serializeTable(catalog_tbl, total, batch_size);
                    assert(vt != null) : "Failed to get serialized table for " + catalog_tbl;
                    if (vt.getRowCount() == 0) break;
                    total += vt.getRowCount();
                    LOG.debug(String.format("Writing %d / %d tuples to '%s'", vt.getRowCount(), total, csv_file.getName()));
                    
                    // Dump table contents
                    while (vt.advanceRow()) {
                        String row[] = vt.getRowStringArray();
                        assert(row != null);
                        assert(row.length == vt.getColumnCount());
                        writer.writeNext(row);
                    } // WHILE
                } // WHILE
                
                try {
                    writer.close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            } // FOR
            return new DependencySet(new int[] { (int)SysProcFragmentId.PF_dumpDistribute }, result);
            
        } else if (fragmentId == SysProcFragmentId.PF_dumpAggregate) {
            if (debug) LOG.debug("Aggregating results from loading fragments in txn #" + txn_id);
            return new DependencySet(new int[] { (int)SysProcFragmentId.PF_dumpAggregate }, result);
        }
        assert(false) : "Unexpected FragmentId " + fragmentId;
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
//        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        VoltTable[] results;
        SynthesizedPlanFragment pfs[];

        // Generate a plan fragment for each site using the sub-tables
        pfs = new SynthesizedPlanFragment[num_partitions  + 1];
        for (int i = 1; i <= num_partitions; ++i) {
            int partition = i - 1;
            ParameterSet params = new ParameterSet();
            params.setParameters(directory);
            pfs[i] = new SynthesizedPlanFragment();
            pfs[i].fragmentId = SysProcFragmentId.PF_dumpDistribute;
            pfs[i].inputDependencyIds = new int[] { };
            pfs[i].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpDistribute };
            pfs[i].multipartition = false;
            pfs[i].nonExecSites = false;
            pfs[i].destPartitionId = partition;
            pfs[i].parameters = params;
            pfs[i].last_task = true;
        } // FOR

        // a final plan fragment to aggregate the results
        pfs[0] = new SynthesizedPlanFragment();
        pfs[0].destPartitionId = partitionId;
        pfs[0].fragmentId = SysProcFragmentId.PF_dumpAggregate;
        pfs[0].inputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpDistribute };
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
