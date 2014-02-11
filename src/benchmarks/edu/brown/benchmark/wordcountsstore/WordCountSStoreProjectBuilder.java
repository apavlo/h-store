package edu.brown.benchmark.wordcountsstore;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.wordcountsstore.procedures.SimpleCall; 
import edu.brown.benchmark.wordcountsstore.procedures.WindowTrigger; 
import edu.brown.benchmark.wordcountsstore.procedures.CountTrigger; 
 
public class WordCountSStoreProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = WordCountSStoreClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = WordCountSStoreLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleCall.class,
        WindowTrigger.class,
        CountTrigger.class
    };
	
	{
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "words", "word"},
        { "results", "word"}
    };

    public WordCountSStoreProjectBuilder() {
        super("wordcountsstore", WordCountSStoreProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

