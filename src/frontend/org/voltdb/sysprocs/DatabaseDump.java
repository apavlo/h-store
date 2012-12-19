package org.voltdb.sysprocs;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;

import au.com.bytecode.opencsv.CSVWriter;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;

@ProcInfo(singlePartition = false)
public class DatabaseDump extends VoltSystemProcedure {
    
    private static final Logger LOG = Logger.getLogger(DatabaseDump.class);

    @Override
    public void initImpl() {
        executor.registerPlanFragment(SysProcFragmentId.PF_dumpDistribute, this);
        executor.registerPlanFragment(SysProcFragmentId.PF_dumpAggregate, this);
    }
    
    @Override
    public DependencySet executePlanFragment(Long txn_id, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
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
            
            for (Table catalog_tbl : catalogContext.database.getTables().values()) {
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
                    VoltTable vt = this.executor.getExecutionEngine().serializeTable(catalog_tbl.getRelativeIndex());
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
        final boolean debug = LOG.isDebugEnabled();
        
        final ParameterSet params = new ParameterSet(directory);
        
        // Generate a plan fragment for each site using the sub-tables
        final List<SynthesizedPlanFragment> pfs = new ArrayList<SynthesizedPlanFragment>();
        for (int i = 0; i < catalogContext.numberOfPartitions; i++) {
            int partition = i;
            SynthesizedPlanFragment pf = new SynthesizedPlanFragment();
            pf.fragmentId = SysProcFragmentId.PF_dumpDistribute;
            pf.inputDependencyIds = new int[] { };
            pf.outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpDistribute };
            pf.multipartition = false;
            pf.nonExecSites = false;
            pf.destPartitionId = partition;
            pf.parameters = params;
            pf.last_task = true;
            pfs.add(pf);
        } // FOR

        // a final plan fragment to aggregate the results
//        pfs[0] = new SynthesizedPlanFragment();
//        pfs[0].destPartitionId = partitionId;
//        pfs[0].fragmentId = SysProcFragmentId.PF_dumpAggregate;
//        pfs[0].inputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpDistribute };
//        pfs[0].outputDependencyIds = new int[] { (int)SysProcFragmentId.PF_dumpAggregate };
//        pfs[0].multipartition = false;
//        pfs[0].nonExecSites = false;
//        pfs[0].parameters = new ParameterSet();

        // send these forth in to the world .. and wait
        if (debug) LOG.debug("Passing " + pfs.size() + " sysproc fragments to executeSysProcPlanFragments()");
        VoltTable[] results = executeSysProcPlanFragments(pfs.toArray(new SynthesizedPlanFragment[0]), (int)SysProcFragmentId.PF_dumpDistribute);
        return results;
    }

}
