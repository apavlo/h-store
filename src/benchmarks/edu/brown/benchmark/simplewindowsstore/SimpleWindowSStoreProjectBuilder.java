package edu.brown.benchmark.simplewindowsstore;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.simplewindowsstore.procedures.SimpleCall; 
import edu.brown.benchmark.simplewindowsstore.procedures.WindowTrigger;
import edu.brown.benchmark.simplewindowsstore.procedures.AvgWindow;

public class SimpleWindowSStoreProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleWindowSStoreClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleWindowSStoreLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		WindowTrigger.class,
        SimpleCall.class,
        AvgWindow.class
    };
	
	{
		addTransactionFrequency(SimpleCall.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    	{ "AVG_FROM_WIN","time"}
    };

    public SimpleWindowSStoreProjectBuilder() {
        super("simplewindowsstore", SimpleWindowSStoreProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

