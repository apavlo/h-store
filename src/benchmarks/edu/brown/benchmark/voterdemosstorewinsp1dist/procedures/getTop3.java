/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

//
// Accepts a vote, enforcing business logic: make sure the vote is for a valid
// contestant and that the voterdemosstorewinsp1 (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterdemosstorewinsp1dist.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voter.VoterConstants;
import edu.brown.benchmark.voterdemosstorewinsp1dist.VoterDemoSStoreWinSP1DistConstants;
import edu.brown.benchmark.voterwintimesstore.VoterWinTimeSStoreConstants;

@ProcInfo (
//    partitionInfo = "votes.phone_number:1",
    singlePartition = true
)
public class getTop3 extends VoltProcedure {
	
	@StmtInfo(
            upsertable=true
        )

    public final SQLStmt getTopLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes DESC LIMIT 3;"	
    );
    
    public final SQLStmt getBottomLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM v_votes_by_contestant ORDER BY num_votes ASC LIMIT 3;"	
    );
    
    public final SQLStmt getTrendingLeaderboard = new SQLStmt(
    	"SELECT contestant_number, num_votes FROM top_three_last_30_sec ORDER BY num_votes DESC LIMIT 3;"	
    );
	
public long run() {
		
//        voltQueueSQL(getTrendingLeaderboard);
        voltQueueSQL(getTopLeaderboard);
//        voltQueueSQL(getBottomLeaderboard);
        VoltTable validation[] = voltExecuteSQL();
        
        // Set the return value to 0: successful vote
        return VoterDemoSStoreWinSP1DistConstants.VOTE_SUCCESSFUL;
    }
}