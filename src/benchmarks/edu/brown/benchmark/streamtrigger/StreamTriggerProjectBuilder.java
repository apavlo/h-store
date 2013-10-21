package edu.brown.benchmark.streamtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.streamtrigger.procedures.SimpleCall; 
import edu.brown.benchmark.streamtrigger.procedures.SimpleTrigger; 

public class StreamTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = StreamTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = StreamTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
         SimpleCall.class,
         SimpleTrigger.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public StreamTriggerProjectBuilder() {
        super("streamtrigger", StreamTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

