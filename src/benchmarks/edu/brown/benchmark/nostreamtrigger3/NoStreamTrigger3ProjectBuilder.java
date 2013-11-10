package edu.brown.benchmark.nostreamtrigger3;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger3.procedures.SimpleCall; 
import edu.brown.benchmark.nostreamtrigger3.procedures.AnotherCall1; 
import edu.brown.benchmark.nostreamtrigger3.procedures.AnotherCall2; 
import edu.brown.benchmark.nostreamtrigger3.procedures.AnotherCall3; 

public class NoStreamTrigger3ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger3Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger3Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AnotherCall1.class,
		AnotherCall2.class,
		AnotherCall3.class,
        SimpleCall.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public NoStreamTrigger3ProjectBuilder() {
        super("nostreamtrigger3", NoStreamTrigger3ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

