package edu.brown.benchmark.wordcounthstore;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.wordcounthstore.procedures.SimpleCall; 
import edu.brown.benchmark.wordcounthstore.procedures.NextBatch; 
 
public class WordCountHStoreProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = WordCountHStoreClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = WordCountHStoreLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleCall.class,
        NextBatch.class
    };
	
	{
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "counts", "word"},
        { "persecond", "word"}
    };

    public WordCountHStoreProjectBuilder() {
        super("wordcounthstore", WordCountHStoreProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

