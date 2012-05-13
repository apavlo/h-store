package edu.brown.optimizer;

import edu.brown.BaseTestCase;
import org.voltdb.benchmark.tpcc.procedures.GetTableCounts;
import org.voltdb.catalog.Procedure;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.utils.ProjectType;

public class TestFastAggregateOptimization extends BaseTestCase {

	@Override
	protected void setUp() throws Exception {
		// TODO Auto-generated method stub
		super.setUp(ProjectType.TPCC);
	}

	@Override
	protected void tearDown() throws Exception {
		// TODO Auto-generated method stub
		// super.tearDown();
	}

	public void testSimpleTest() {
		Procedure catalog_proc = this.getProcedure(GetTableCounts.class);
		assertNotNull(catalog_proc.getName());
	}

}
