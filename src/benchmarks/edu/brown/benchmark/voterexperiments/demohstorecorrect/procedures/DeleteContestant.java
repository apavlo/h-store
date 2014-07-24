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
// contestant and that the voterdemosstore (phone number of the caller) is not above the
// number of allowed votes.
//

package edu.brown.benchmark.voterexperiments.demohstorecorrect.procedures;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.StmtInfo;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.voterexperiments.demohstorecorrect.VoterDemoHStoreConstants;

@ProcInfo (
	//partitionInfo = "contestants.contestant_number:1",
    singlePartition = true
)
public class DeleteContestant extends VoltProcedure {
    
    public final SQLStmt findLowestContestant = new SQLStmt(
		"SELECT * FROM v_votes_by_contestant ORDER BY num_votes DESC LIMIT 1;"
    );
    
    public final SQLStmt deleteLowestContestant = new SQLStmt(
		"DELETE FROM contestants WHERE contestant_number = ?;"
    );
    
    public final SQLStmt deleteLowestVotes = new SQLStmt(
		"DELETE FROM votes WHERE contestant_number = ?;"
    );
    
 // Pull aggregate from window
    public final SQLStmt deleteLeaderBoardStmt = new SQLStmt(
		"DELETE FROM leaderboard WHERE contestant_number = ?;"
    );
    
	
    public long run() {
		
        voltQueueSQL(findLowestContestant);
        VoltTable validation[] = voltExecuteSQL();
        
        if(validation[0].getRowCount() < 1)
        {
        	return VoterDemoHStoreConstants.ERR_NOT_ENOUGH_CONTESTANTS;
        }
        
        int lowestContestant = (int)(validation[0].fetchRow(0).getLong(0));
        
        try {
			InetAddress host = InetAddress.getLocalHost();
			//System.out.println("Host: " + host);
			//System.out.println("Host Name: " + host.getHostName());
			String hostname;
			if(host.getHostName().startsWith(VoterDemoHStoreConstants.HOST_PREFIX))
			{
				hostname = VoterDemoHStoreConstants.SERVER_HOST_NAME;
			}
			else
			{
				hostname = host.getHostName();
			}
			Socket socket = new Socket(hostname, VoterDemoHStoreConstants.SERVER_PORT_NUM);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			//ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			String response;
			
			
			
			String mes = "h-store ready";
			out.print(mes);
			out.flush();
			
			response = in.readLine();
			if(response.equals("READY"))
			{
				System.out.println("CONFIRMED READY");
			}
			else
			{
				System.out.println("ERROR: NOT READY - " + response);
			}
			Thread.sleep(500);
			out.close();
			in.close();
			socket.close();
			//oos.writeObject("ant hstore-invoke -Dproject=tickertest -Dproc=InsertTickerRow -Dparam0='AAA' -Dparam1=12345 -Dparam2=5");
			
			//oos.close();
		}
		catch (UnknownHostException e){
			System.err.println("ERROR 1");
			e.printStackTrace();
		}
		catch (IOException e) {
			System.err.println("ERROR 2");
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			System.err.println("ERROR 3");
			e.printStackTrace(); 
		}
        
        voltQueueSQL(deleteLowestContestant, lowestContestant);
        voltQueueSQL(deleteLowestVotes, lowestContestant);
        voltQueueSQL(deleteLeaderBoardStmt, lowestContestant);
        voltExecuteSQL(true);
		
        // Set the return value to 0: successful vote
        return VoterDemoHStoreConstants.DELETE_SUCCESSFUL;
    }
}