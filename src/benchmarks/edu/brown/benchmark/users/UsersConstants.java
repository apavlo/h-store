package edu.brown.benchmark.users;

public abstract class UsersConstants {

    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------

	public static final int FREQUENCY_GET_USERS = 100; 

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_USERS = "USERS";

    public static final int USERS_SIZE = 100000;
    
    public static final int BATCH_SIZE = 500;

    public static final String TABLENAMES[] = { 
                                                TABLENAME_USERS
                                                };
}
