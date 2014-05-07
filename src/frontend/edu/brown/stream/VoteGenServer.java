package edu.brown.stream;

import java.net.*;
import java.io.*;
import java.lang.System;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteGenServer implements Runnable 
{
    
    private final Socket clientSocket;
    private edu.brown.stream.VoteGenerator switchboard;

    public VoteGenServer(Socket clientsocket, edu.brown.stream.VoteGenerator switchboard)
    {
        this.clientSocket = clientsocket;
        this.switchboard = switchboard;
    }
    
    private synchronized String getNextVote()
    {
        String response;
        
        edu.brown.stream.PhoneCallGenerator.PhoneCall call = switchboard.nextVote();
        
        if(call != null)    
            response = call.getContent();
        else
            response = "0";
        
        return response;
    }
    
    public void run()
    {
       try{
           boolean beContinue = true;
    
           BufferedReader inFromClient = new BufferedReader(new InputStreamReader(this.clientSocket.getInputStream()));
           DataOutputStream outToClient = new DataOutputStream(this.clientSocket.getOutputStream());
    
           String request, response;
    
           response = "-1";
    
           while(beContinue==true)
           {
               request = inFromClient.readLine();
               //System.out.println("Received: " + request);
               if(request==null)
                   continue;
               
               if(request.equals("n")==true)
               {
                   response = getNextVote();
               }
               else
               {
                   if(request.equals("exit")==true)
                   {
                       response = "-1";
                       beContinue = false;
                   }
                   else
                       response = "-1"; // this is indicate invalid result
               }
               
               System.out.println("Response: " + response);
               outToClient.writeBytes(response + "\n");
           }
       }
       catch (Exception e)
       {
           e.printStackTrace();
       }
    }
    
    
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

         ServerSocket welcomeSocket = new ServerSocket(6789);

         while(true)
         {
            Socket connectionSocket = welcomeSocket.accept();
            new Thread(new VoteGenServer(connectionSocket, switchboard)).start();
         }

    }

}
