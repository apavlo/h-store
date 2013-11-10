package edu.brown.benchmark.nostreamtrigger4;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger4.procedures.SimpleCall; 
import edu.brown.benchmark.nostreamtrigger4.procedures.AnotherCall1; 
import edu.brown.benchmark.nostreamtrigger4.procedures.AnotherCall2; 
import edu.brown.benchmark.nostreamtrigger4.procedures.AnotherCall3; 
import edu.brown.benchmark.nostreamtrigger4.procedures.AnotherCall4; 

public class NoStreamTrigger4ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger4Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger4Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AnotherCall4.class,
		AnotherCall3.class,
		AnotherCall2.class,
		AnotherCall1.class,
        SimpleCall.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public NoStreamTrigger4ProjectBuilder() {
        super("nostreamtrigger4", NoStreamTrigger4ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

