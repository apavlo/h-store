package edu.brown.benchmark.nostreamtrigger2;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger2.procedures.SimpleCall; 
import edu.brown.benchmark.nostreamtrigger2.procedures.AnotherCall1; 
import edu.brown.benchmark.nostreamtrigger2.procedures.AnotherCall2; 

public class NoStreamTrigger2ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger2Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger2Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
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

    public NoStreamTrigger2ProjectBuilder() {
        super("nostreamtrigger2", NoStreamTrigger2ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

