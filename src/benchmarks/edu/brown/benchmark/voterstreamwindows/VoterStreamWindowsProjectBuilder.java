package edu.brown.benchmark.voterstreamwindows;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.voterstreamwindows.procedures.Initialize; 
import edu.brown.benchmark.voterstreamwindows.procedures.Vote; 


import edu.brown.benchmark.voterstreamwindows.procedures.UpdateContestantWindow;
// triggers
import edu.brown.benchmark.voterstreamwindows.procedures.ValidateContestantsTrigger; 
import edu.brown.benchmark.voterstreamwindows.procedures.ValidateVotesNumberLimitTrigger; 
import edu.brown.benchmark.voterstreamwindows.procedures.UpdateVotesAndTotalVotesTrigger; 
import edu.brown.benchmark.voterstreamwindows.procedures.UpdateVotesByPhoneNumberTrigger; 
import edu.brown.benchmark.voterstreamwindows.procedures.UpdateVotesByContestantNumberStateTrigger;
import edu.brown.benchmark.voterstreamwindows.procedures.UpdateLeaderBoard;


public class VoterStreamWindowsProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = VoterStreamWindowsClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = VoterStreamWindowsLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
         Initialize.class,
         Vote.class,
         ValidateContestantsTrigger.class,
         ValidateVotesNumberLimitTrigger.class,
         UpdateVotesAndTotalVotesTrigger.class,
         UpdateVotesByPhoneNumberTrigger.class,
         UpdateVotesByContestantNumberStateTrigger.class,
         UpdateContestantWindow.class,
         UpdateLeaderBoard.class
    };
	
	{
		addTransactionFrequency(Initialize.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes", "phone_number" },
        { "votes_by_phone_number", "phone_number" },
        { "votes_by_contestant_number_state", "contestant_number"},
        { "current_leader", "contestant_number"}
    };

    public VoterStreamWindowsProjectBuilder() {
        super("voterstreamwindows", VoterStreamWindowsProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}

