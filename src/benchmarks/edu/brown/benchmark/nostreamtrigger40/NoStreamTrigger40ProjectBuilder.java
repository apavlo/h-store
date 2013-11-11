package edu.brown.benchmark.nostreamtrigger40;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger40.procedures.SimpleCall; 

public class NoStreamTrigger40ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger40Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger40Loader.class;

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

    public NoStreamTrigger40ProjectBuilder() {
        super("nostreamtrigger40", NoStreamTrigger40ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

