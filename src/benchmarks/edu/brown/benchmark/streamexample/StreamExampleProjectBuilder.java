package edu.brown.benchmark.streamexample;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.streamexample.procedures.*;

public class StreamExampleProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = StreamExampleClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = StreamExampleLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        GetData.class,
        Initialize.class,
        //InsertIntoSelect.class,
        GetAllData.class,
        Initialize2.class,
        GetDataTrigger.class,
    };
    public static final String PARTITIONING[][] = new String[][] {
            // { "TABLE NAME", "PARTITIONING COLUMN NAME" }
            { "TABLEA", "A_ID" }, {"TABLEC", "A_ID"}, {"TABLEB", "B_ID"},
            { "TABLEJ", "J_ID" }
    };

    public StreamExampleProjectBuilder() {
        super("streamexample", StreamExampleProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}
