package edu.brown.benchmark.simpledistribution;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.simpledistribution.procedures.Initialize;
import edu.brown.benchmark.simpledistribution.procedures.SimpleCall;

public class SimpleDistributionProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleDistributionClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleDistributionLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        Initialize.class,
        SimpleCall.class
    };
	
    {
    }
	
     // a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "TABLEA", "A_ID" },
    };

    public SimpleDistributionProjectBuilder() {
        super("simpledistribution", SimpleDistributionProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

