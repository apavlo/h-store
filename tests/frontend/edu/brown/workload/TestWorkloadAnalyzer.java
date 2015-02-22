package edu.brown.workload;

import java.io.File;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestWorkloadAnalyzer extends BaseTestCase {
	protected static final int WORKLOAD_XACT_LIMIT = 1000;

	public void testGroupingsPossible() throws Exception {
		super.setUp(ProjectType.AUCTIONMARK);

		Workload workload;
		File workload_file = this.getWorkloadFile(ProjectType.AUCTIONMARK);
		workload = new Workload(catalogContext.catalog);
		((Workload) workload).load(workload_file, catalogContext.database, null);
		assert (workload.getTransactionCount() > 0) : "No transaction loaded from workload";

		WorkloadAnalyzer analyzer = new WorkloadAnalyzer(this.getDatabase(), workload);
		int result = analyzer.getCountOfGroupingsPossible();

		assertNotNull(result);
		assertTrue("Result: " + result, result > 2000);
	}

}
