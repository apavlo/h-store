package edu.brown.benchmark.simplestreamtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.simplestreamtrigger.procedures.SimpleCall; 
import edu.brown.benchmark.simplestreamtrigger.procedures.SimpleTrigger; 
public class SimpleStreamTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleStreamTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleStreamTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleTrigger.class,
        SimpleCall.class
    };
	
	{
		//addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public SimpleStreamTriggerProjectBuilder() {
        super("simplestreamtrigger", SimpleStreamTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

