package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.conf.HStoreConf;

/** 
 * Get the value of a HStoreConf parameter from an HStoreSite
 */
@ProcInfo(
    singlePartition = true,
    partitionParam = 0
)
public class GetConfiguration extends VoltSystemProcedure {

    public static final ColumnInfo nodeResultsColumns[] = {
        new ColumnInfo("SITE", VoltType.INTEGER),
        new ColumnInfo("NAME", VoltType.STRING),
        new ColumnInfo("VALUE", VoltType.STRING),
        new ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    @Override
    public void initImpl() {
        // Nothing
    }

    @Override
    public DependencySet executePlanFragment(Long txn_id,
                                             Map<Integer, List<VoltTable>> dependencies,
                                             int fragmentId,
                                             ParameterSet params,
                                             PartitionExecutor.SystemProcedureExecutionContext context) {
        // Nothing to do
        return (null);
    }
    
    public VoltTable run(String confNames[]) {
        HStoreConf hstore_conf = executor.getHStoreConf();
        for (int i = 0; i < confNames.length; i++) {
            if (hstore_conf.hasParameter(confNames[i]) == false) {
                String msg = String.format("Invalid configuration parameter '%s'", confNames[i]);
                throw new VoltAbortException(msg);
            }
        } // FOR
        
        VoltTable result = new VoltTable(nodeResultsColumns);
        TimestampType timestamp = new TimestampType();
        for (int i = 0; i < confNames.length; i++) {
            Object val = hstore_conf.get(confNames[i]);
            result.addRow(executor.getSiteId(),
                          confNames[i], 
                          val.toString(),
                          timestamp);
        } // FOR
        return (result);
    }
}
