package edu.brown.benchmark.nostreamtrigger1;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.nostreamtrigger1.procedures.SimpleCall; 
import edu.brown.benchmark.nostreamtrigger1.procedures.SimpleCallT; 
import edu.brown.benchmark.nostreamtrigger1.procedures.AnotherCall1; 

public class NoStreamTrigger1ProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = NoStreamTrigger1Client.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = NoStreamTrigger1Loader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AnotherCall1.class,
        SimpleCallT.class,
        SimpleCall.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes_by_phone_number", "phone_number" }
    };

    public NoStreamTrigger1ProjectBuilder() {
        super("nostreamtrigger1", NoStreamTrigger1ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

