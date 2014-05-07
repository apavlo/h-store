package edu.brown.stream;

import java.net.*;
import java.io.*;
import java.lang.System;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteGenServer {
    
    public static void main(String[] vargs) throws Exception {

         if( vargs.length == 0 )
	    return;

         String filename = vargs[0];
         System.out.println(filename);

         edu.brown.stream.VoteGenerator switchboard;
         try{
         	switchboard = new edu.brown.stream.VoteGenerator(filename);
         }
         catch(Exception ex)
         {
             System.out.println(filename + " is not a valid vote file!");
             return;
         }

         String clientRequest;
         String voteContent;
         ServerSocket welcomeSocket = new ServerSocket(6789);

         while(true)
         {
            System.out.println("Waiting for connection ... ");
            Socket connectionSocket = welcomeSocket.accept();
            BufferedReader inFromClient =
               new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
            DataOutputStream outToClient = new DataOutputStream(connectionSocket.getOutputStream());
            clientRequest = inFromClient.readLine();
            System.out.println("Received: " + clientRequest);
            if(clientRequest.equals("next")==true)
            {
                edu.brown.stream.PhoneCallGenerator.PhoneCall call = switchboard.nextVote();
                               
                if(call != null)    
                    voteContent = call.getContent();
                else
                    voteContent = "0";
            }
            else
            {
                voteContent = "-1"; // this is indicate invalid result
            }
            outToClient.writeBytes(voteContent + "\n");
         }
    }

}
