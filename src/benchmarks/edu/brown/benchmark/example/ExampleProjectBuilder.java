package edu.brown.benchmark.example;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.example.procedures.GetData;

public class ExampleProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = ExampleClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = ExampleLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        GetData.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
            // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
            { "TABLEA", "A_ID" }, { "TABLEB", "B_A_ID" }, };

    public ExampleProjectBuilder() {
        super("example", ExampleProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}