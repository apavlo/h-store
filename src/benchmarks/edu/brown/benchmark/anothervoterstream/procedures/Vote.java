package edu.brown.benchmark.anothervoterstream.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;


import edu.brown.benchmark.anothervoterstream.VoterConstants;

@ProcInfo (
        singlePartition = false
    )
public class Vote extends VoltProcedure {
    
    // Checks to determine if we need to initialize the voter information in table - votes_by_phone_number
    public final SQLStmt checkVoterStmt = new SQLStmt(
        "SELECT num_votes FROM votes_by_phone_number WHERE phone_number = ?;"
    );
    
    public final SQLStmt insertVoterStmt = new SQLStmt(
            "INSERT INTO votes_by_phone_number (phone_number, num_votes) VALUES (?, 0);"
        );
    
    // Checks an area code to retrieve the corresponding state
    public final SQLStmt checkStateStmt = new SQLStmt(
        "SELECT state FROM area_code_state WHERE area_code = ?;"
    );
    
    // Records a vote
    public final SQLStmt insertVoteStmt = new SQLStmt(
        "INSERT INTO votes_stream (vote_id, phone_number, state, contestant_number, created) VALUES (?, ?, ?, ?, ?);"
    );



    public long run(long voteId, long phoneNumber, int contestantNumber) {

        // PHASE ONE : initialization
        // check if there is voter's record already
        voltQueueSQL(checkVoterStmt, phoneNumber);
        voltQueueSQL(checkStateStmt, (short)(phoneNumber / 10000000l));
        VoltTable validation[] = voltExecuteSQL();
        
        // initialize the voter's record
        if (validation[0].getRowCount() == 0) {
            voltQueueSQL(insertVoterStmt, phoneNumber);
        }
        
        // Some sample client libraries use the legacy random phone generation that mostly
        // created invalid phone numbers. Until refactoring, re-assign all such votes to
        // the "XX" fake state (those votes will not appear on the Live Statistics dashboard,
        // but are tracked as legitimate instead of invalid, as old clients would mostly get
        // it wrong and see all their transactions rejected).
        final String state = (validation[1].getRowCount() > 0) ? validation[1].fetchRow(0).getString(0) : "XX";
        
        //
        // PHASE TWO: post the vote, insert record into vote stream
        TimestampType timestamp = new TimestampType();
        voltQueueSQL(insertVoteStmt, voteId, phoneNumber, state, contestantNumber, timestamp);
        voltExecuteSQL(true);

        return VoterConstants.VOTE_SUCCESSFUL;
    }
}
