package edu.brown.benchmark.recovery;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.recovery.procedures.SimpleCall; 
import edu.brown.benchmark.recovery.procedures.SimpleCallT; 
import edu.brown.benchmark.recovery.procedures.AnotherCall1; 
import edu.brown.benchmark.recovery.procedures.CountingTrigger; 

public class RecoveryProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = RecoveryClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = RecoveryLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AnotherCall1.class,
		//CountingTrigger.class,
        SimpleCallT.class,
        SimpleCall.class
    };
	
	{
		//addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "W1", "value" },
        { "T1", "value" },
        { "T2", "value" }
    };

    public RecoveryProjectBuilder() {
        super("recovery", RecoveryProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

