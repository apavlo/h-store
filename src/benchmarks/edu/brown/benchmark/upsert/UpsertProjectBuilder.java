package edu.brown.benchmark.upsert;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.upsert.procedures.Update;
import edu.brown.benchmark.upsert.procedures.Upsert;
import edu.brown.benchmark.upsert.procedures.Initialize;

public class UpsertProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = UpsertClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = UpsertLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        Initialize.class,
        Update.class,
        Upsert.class,
    };

	public static final String PARTITIONING[][] = new String[][] {
        { "votes_by_phone_number", "phone_number" }
    };
    
    public UpsertProjectBuilder() {
        super("upsert", UpsertProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}