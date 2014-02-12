package edu.brown.workload;

import edu.brown.BaseTestCase;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProjectType;

public class TestWorkloadAnalyzer extends BaseTestCase{
    public void testCountOfReferences() throws Exception {
        super.setUp(ProjectType.TM1);

        ArgumentsParser args = null;

		Workload workload = null;
		WorkloadAnalyzer analyzer = new WorkloadAnalyzer(this.getDatabase(), workload);

        int timeInterval = 20000;
		int result = analyzer.getCountOfReferencesInInterval(timeInterval );
        assertNotNull(result);
        assertEquals(2210, result);
    }

}
