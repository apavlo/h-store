package edu.brown.hstore.executors;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.DependencySet;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.catalog.PlanFragment;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.dtxn.LocalTransaction;

/**
 * @author mimosally
 * 
 */
public final class CombineExecutor extends FastExecutor {

	public CombineExecutor(PartitionExecutor executor) {
		super(executor);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Execute a Java-only operation to generate the output of a PlanFragment
	 * for the given transaction without needing to go down in to
	 * ExecutionEngine
	 * 
	 * @param ts
	 * @param catalog_frag
	 * @param input
	 * @return
	 */
	@Override
	public final DependencySet execute(final int[] outputid, final int[] inputid,
			final Map<Integer, List<VoltTable>> tmp_dependencies) {

		List<VoltTable> tmp = tmp_dependencies.get(inputid[0]);
		// record = tmp.get(0).clone(0);
		VoltTable record = tmp.get(0).clone(0);

		for (VoltTable t : tmp) {

			while (t.advanceRow()) {

				record.add(t.getRow());

			}// while

		}// for

		VoltTable[] vt = new VoltTable[1];
		vt[0] = record;

		return (new DependencySet(outputid, vt));

	}

}
