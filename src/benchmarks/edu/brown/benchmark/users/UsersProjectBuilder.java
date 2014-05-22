package edu.brown.benchmark.users;
import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.users.procedures.GetUsers;

public class UsersProjectBuilder extends AbstractProjectBuilder{
	 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = UsersClient.class;
 
    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = UsersLoader.class;
    
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        GetUsers.class
    };
    {
        // Transaction Frequencies
        addTransactionFrequency(GetUsers.class, UsersConstants.FREQUENCY_GET_USERS);
        
    }

    public static final String PARTITIONING[][] = new String[][] {
        { UsersConstants.TABLENAME_USERS, "U_ID" }
    };
    
	public UsersProjectBuilder() {
    	super("users", UsersProjectBuilder.class, PROCEDURES, PARTITIONING);
 
    }

}
