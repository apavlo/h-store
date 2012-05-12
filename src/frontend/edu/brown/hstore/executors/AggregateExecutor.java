package edu.brown.hstore.executors;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.DependencySet;
import org.voltdb.PrivateVoltTableFactory;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.PlanFragment;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.dtxn.LocalTransaction;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.optimizer.optimizations.AggregatePushdownOptimization;

public class AggregateExecutor extends FastExecutor {
    private static final Logger LOG = Logger.getLogger(AggregatePushdownOptimization.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

    public AggregateExecutor(PartitionExecutor executor) {
        super(executor);
        // TODO Auto-generated constructor stub
    }

    @Override
    public VoltTable execute(LocalTransaction ts, PlanFragment catalog_frag, VoltTable[] input) {
        // TODO Auto-generated method stub
        return null;
    }

    public DependencySet executeFastAggregate(int id, Map<Integer, List<VoltTable>> tmp_dependencies) {

        LOG.debug("do fastcombine in Java!");
        Set<Integer> keys = tmp_dependencies.keySet();
        Object[] Okey = keys.toArray();
        Long fsum = null;
        int finalsum = 0;
        VoltTable record = null;

        Object key = Okey[0];
        List<VoltTable> tmp = tmp_dependencies.get(key);
        record = tmp.get(0).clone(0);

        for (int i = 0; i < tmp.size(); i++) {
            VoltTable t = tmp.get(i);

            while (t.advanceRow()) {
                VoltTableRow r = t.getRow();
                finalsum += ((Long) r.get(0)).intValue(); // do the sum in Java

            }// while

        }// for

        fsum = new Long((long) finalsum);
        record.addRow(fsum); // add the final result
        int[] depid;
        depid = new int[1];
        depid[0] = id;
        VoltTable[] vt;
        vt = new VoltTable[1];
        vt[0] = record;
        DependencySet result = new DependencySet(depid, vt);
        assert (result != null);
        return result;
    }

}
