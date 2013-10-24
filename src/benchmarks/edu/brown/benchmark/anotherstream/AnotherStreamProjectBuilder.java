package edu.brown.benchmark.anotherstream;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.anotherstream.procedures.Initialize; 
import edu.brown.benchmark.anotherstream.procedures.Vote; 


// triggers
import edu.brown.benchmark.anotherstream.procedures.ValidateContestantsTrigger; 
import edu.brown.benchmark.anotherstream.procedures.ValidateVotesNumberLimitTrigger; 
import edu.brown.benchmark.anotherstream.procedures.UpdateVotesAndTotalVotesTrigger; 

public class AnotherStreamProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = AnotherStreamClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = AnotherStreamLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
         Initialize.class,
         Vote.class,
         ValidateContestantsTrigger.class,
         ValidateVotesNumberLimitTrigger.class,
         UpdateVotesAndTotalVotesTrigger.class
    };
	
	{
		//addTransactionFrequency(Vote.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes", "phone_number" }//,
//        { "votes_by_phone_number", "phone_number" },
//        { "votes_by_contestant_number_state", "contestant_number"}
    };

    public AnotherStreamProjectBuilder() {
        super("anotherstream", AnotherStreamProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

