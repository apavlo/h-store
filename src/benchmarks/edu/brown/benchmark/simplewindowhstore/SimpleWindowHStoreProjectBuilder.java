package edu.brown.benchmark.simplewindowhstore;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.simplewindowhstore.procedures.AddNewValue; 

public class SimpleWindowHStoreProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleWindowHStoreClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleWindowHStoreLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
		AddNewValue.class
    };
	
	{
		addTransactionFrequency(AddNewValue.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
    	{ "AVG_FROM_WIN","time"},
    	{ "W_STAGING","time"},
    	{ "W_ROWS","time"}
    };

    public SimpleWindowHStoreProjectBuilder() {
        super("simplewindowhstore", SimpleWindowHStoreProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

