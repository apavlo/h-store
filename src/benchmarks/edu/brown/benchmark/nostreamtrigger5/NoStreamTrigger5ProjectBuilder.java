package edu.brown.benchmark.nostreamtrigger5;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger5.procedures.SimpleCall; 
import edu.brown.benchmark.nostreamtrigger5.procedures.AnotherCall1; 
import edu.brown.benchmark.nostreamtrigger5.procedures.AnotherCall2; 
import edu.brown.benchmark.nostreamtrigger5.procedures.AnotherCall3; 
import edu.brown.benchmark.nostreamtrigger5.procedures.AnotherCall4; 
import edu.brown.benchmark.nostreamtrigger5.procedures.AnotherCall5; 

public class NoStreamTrigger5ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger5Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger5Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AnotherCall5.class,
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

    public NoStreamTrigger5ProjectBuilder() {
        super("nostreamtrigger5", NoStreamTrigger5ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

