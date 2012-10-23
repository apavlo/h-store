package org.voltdb.sysprocs;

import java.util.List;
import java.util.Map;

import org.voltdb.DependencySet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcInfo;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltTable;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.PartitionExecutor.SystemProcedureExecutionContext;

@ProcInfo(singlePartition = false)
public class NoOp extends VoltSystemProcedure {

    @Override
    public void initImpl() {
        // Nothing
    }
    
    @Override
    public DependencySet executePlanFragment(Long txnId, Map<Integer, List<VoltTable>> dependencies, int fragmentId, ParameterSet params, SystemProcedureExecutionContext context) {
        // TODO Auto-generated method stub
        return null;
    }
    
    public VoltTable[] run() {
        return HStoreConstants.EMPTY_RESULT;
    }

}
