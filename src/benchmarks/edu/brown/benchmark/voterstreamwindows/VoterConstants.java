package edu.brown.benchmark.voterstreamwindows;

public abstract class VoterConstants {

    public static final String TABLENAME_CONTESTANTS     = "contestants";
    public static final String TABLENAME_AREA_CODE_STATE = "area_code_state";
    public static final String TABLENAME_VOTES           = "votes";
    
	public static final int MAX_VOTES = 1000; 
	public static final int NUM_CONTESTANTS = 6; 

	// Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
											   "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
											   "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    // potential return codes
    public static final long VOTE_SUCCESSFUL = 0;
    public static final long ERR_INVALID_CONTESTANT = 1;
    public static final long ERR_VOTER_OVER_VOTE_LIMIT = 2;
}

