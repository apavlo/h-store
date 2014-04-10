package edu.brown.benchmark.simpledistribution;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.simpledistribution.procedures.Initialize;
import edu.brown.benchmark.simpledistribution.procedures.SP2;
import edu.brown.benchmark.simpledistribution.procedures.SP3;
import edu.brown.benchmark.simpledistribution.procedures.SP4;
import edu.brown.benchmark.simpledistribution.procedures.SP5;
import edu.brown.benchmark.simpledistribution.procedures.SP6;
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
        SP2.class,
        SP3.class,
        SP4.class,
        SP5.class,
        SP6.class,
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

