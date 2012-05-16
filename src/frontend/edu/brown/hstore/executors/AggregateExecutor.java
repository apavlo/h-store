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

/**
 * @author mimosally
 * 
 */
public class AggregateExecutor extends FastExecutor {

	public AggregateExecutor(PartitionExecutor executor) {
		super(executor);
		// TODO Auto-generated constructor stub
	}

	@Override
	public DependencySet execute(int[] outputid, int[] inputid,
			Map<Integer, List<VoltTable>> tmp_dependencies) {
		Long finalsum = new Long(0);
		VoltTable record = null;
		List<VoltTable> tmp = tmp_dependencies.get(inputid[0]);
		record = tmp.get(0).clone(0);

		for (VoltTable t : tmp) {

			if (t.advanceRow()) {
				finalsum += (Long) t.get(0); // do the sum in Java
			}

		}// for
		record.addRow(finalsum); // add the final result
		VoltTable[] vt = new VoltTable[1];
		vt[0] = record;

		return (new DependencySet(outputid, vt));
	}

}
