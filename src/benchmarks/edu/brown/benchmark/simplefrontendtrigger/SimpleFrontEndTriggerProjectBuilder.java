package edu.brown.benchmark.simplefrontendtrigger;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.simplefrontendtrigger.procedures.SimpleCall; 
import edu.brown.benchmark.simplefrontendtrigger.procedures.AnotherCall; 

public class SimpleFrontEndTriggerProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleFrontEndTriggerClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleFrontEndTriggerLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleCall.class,
        AnotherCall.class
    };
	
	{
		//addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    };

    public SimpleFrontEndTriggerProjectBuilder() {
        super("simplefrontendtrigger", SimpleFrontEndTriggerProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

