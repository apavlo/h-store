package edu.brown.benchmark.anothervoterstream;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.anothervoterstream.procedures.Initialize; 
import edu.brown.benchmark.anothervoterstream.procedures.Vote; 


// triggers
import edu.brown.benchmark.anothervoterstream.procedures.ValidateContestantsTrigger; 
import edu.brown.benchmark.anothervoterstream.procedures.ValidateVotesNumberLimitTrigger; 
import edu.brown.benchmark.anothervoterstream.procedures.UpdateVotesAndTotalVotesTrigger; 
import edu.brown.benchmark.anothervoterstream.procedures.UpdateVotesByPhoneNumberTrigger; 
import edu.brown.benchmark.anothervoterstream.procedures.UpdateVotesByContestantNumberStateTrigger; 

public class AnotherVoterStreamProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = AnotherVoterStreamClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = AnotherVoterStreamLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
         Initialize.class,
         Vote.class,
         ValidateContestantsTrigger.class,
         ValidateVotesNumberLimitTrigger.class,
         UpdateVotesAndTotalVotesTrigger.class,
         UpdateVotesByPhoneNumberTrigger.class,
         UpdateVotesByContestantNumberStateTrigger.class
    };
	
	{
		addTransactionFrequency(Initialize.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes", "phone_number" },
        { "votes_by_phone_number", "phone_number" },
        { "votes_by_contestant_number_state", "contestant_number"}
    };

    public AnotherVoterStreamProjectBuilder() {
        super("anothervoterstream", AnotherVoterStreamProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

