package edu.brown.benchmark.complexnostreamtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.complexnostreamtrigger.procedures.Initialize; 
import edu.brown.benchmark.complexnostreamtrigger.procedures.SimpleCall; 

public class ComplexNoStreamTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = ComplexNoStreamTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = ComplexNoStreamTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		Initialize.class,
        SimpleCall.class
    };
	
	{
		//addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
		{ "votes_by_phone_number", "phone_number" }
    };

    public ComplexNoStreamTriggerProjectBuilder() {
        super("complexnostreamtrigger", ComplexNoStreamTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

