package edu.brown.benchmark.nostreamtrigger10;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger10.procedures.SimpleCall; 

public class NoStreamTrigger10ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger10Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger10Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleCall.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public NoStreamTrigger10ProjectBuilder() {
        super("nostreamtrigger10", NoStreamTrigger10ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

