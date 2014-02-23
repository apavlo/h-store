package edu.brown.benchmark.wordcountsstorewithbatch;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.wordcountsstorewithbatch.procedures.SimpleCall;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.WindowTrigger;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.MidStreamTrigger;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.ResultsWinTrigger;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.CountTrigger;
import edu.brown.benchmark.wordcountsstorewithbatch.procedures.GetResults;
 
public class WordCountSStoreWithBatchProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = WordCountSStoreWithBatchClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = WordCountSStoreWithBatchLoader.class;

// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        SimpleCall.class,
        WindowTrigger.class,
        //MidStreamTrigger.class,
        //ResultsWinTrigger.class,
        //CountTrigger.class,
        GetResults.class
    };

{
}

// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "words", "word"},
        { "words_full", "word"},
        { "midstream", "word"},
        { "results", "word"}
    };

    public WordCountSStoreWithBatchProjectBuilder() {
        super("wordcountsstorewithbatch", WordCountSStoreWithBatchProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}