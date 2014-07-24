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
// Returns the results of the votes.
//

package edu.brown.benchmark.voterexperiments.demosstorecorrect.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

@ProcInfo (
singlePartition = false
)
public class Results extends VoltProcedure
{
    // Gets the results
    public final SQLStmt getTopThreeVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
												  + "        , b.num_votes          AS num_votes"
												  + "     FROM v_votes_by_contestant b"
												  + "        , contestants AS a"
												  + "    WHERE a.contestant_number = b.contestant_number"
												  + " ORDER BY num_votes DESC"
												  + "        , contestant_number ASC"
												  + " LIMIT 3");
    
    public final SQLStmt getBottomThreeVotesStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
												  + "        , b.num_votes          AS num_votes"
												  + "     FROM v_votes_by_contestant b"
												  + "        , contestants AS a"
												  + "    WHERE a.contestant_number = b.contestant_number"
												  + " ORDER BY num_votes ASC"
												  + "        , contestant_number ASC"
												  + " LIMIT 3");
    
    public final SQLStmt getTrendingStmt = new SQLStmt( "   SELECT a.contestant_name   AS contestant_name"
												  + "        , b.num_votes          AS num_votes"
												  + "     FROM leaderboard b"
												  + "        , contestants AS a"
												  + "    WHERE a.contestant_number = b.contestant_number"
												  + " ORDER BY num_votes DESC"
												  + "        , contestant_number ASC"
												  + " LIMIT 3");
    
    public final SQLStmt getVoteCountStmt = new SQLStmt( "SELECT count(*) FROM votes;");
    public final SQLStmt getTrendingCountStmt = new SQLStmt("SELECT count(*) FROM trending_leaderboard;");
    
    public VoltTable[] run() {
        voltQueueSQL(getTopThreeVotesStmt);
        voltQueueSQL(getBottomThreeVotesStmt);
        voltQueueSQL(getTrendingStmt);
        voltQueueSQL(getVoteCountStmt);
        voltQueueSQL(getTrendingCountStmt);
        return voltExecuteSQL(true);
    }
}