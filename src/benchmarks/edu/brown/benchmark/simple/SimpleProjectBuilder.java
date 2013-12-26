package edu.brown.benchmark.simple;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.simple.procedures.GetData;

public class SimpleProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SimpleClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SimpleLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        GetData.class
    };
    
    public static final String PARTITIONING[][] = new String[][] {
            // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
            { "STABLE", "S_KEY" } 
    };

    public SimpleProjectBuilder() {
        super("simple", SimpleProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}
