package edu.brown.stream;

import java.io.*;
import java.net.*;
import java.util.ArrayList;

public class VoteGenClient {
    
    public VoteGenClient() throws UnknownHostException, IOException
    {
    }
    
    public synchronized CurrentCall getNextCall() throws IOException
    {
        Socket clientSocket;
        DataOutputStream outToServer;
        BufferedReader inFromServer;

        String request;
        String response;
        CurrentCall result;

        clientSocket = new Socket("localhost", 6789);
        outToServer = new DataOutputStream(clientSocket.getOutputStream());
        inFromServer = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

        request = "n";//inFromUser.readLine();
        outToServer.writeBytes(request + "\n");
        response = inFromServer.readLine();
        //System.out.println("FROM SERVER: " + response);
        //outToServer.writeBytes("e" + "\n");
        clientSocket.close();
        
        if(response==null)
            result = null;
        
        if(response.equals("0") || response.equals("-1"))
            result = null;
        else
        {
            String[] info = response.split(" ");
            long voteId = Long.valueOf(info[0]);
            long phoneNumber = Long.valueOf(info[1]);
            int contestantNumber = Integer.valueOf(info[2]);
            int timestamp = Integer.valueOf(info[3]);
            result = new CurrentCall(voteId, contestantNumber, phoneNumber, timestamp );
        }

        return result;
    }
    
//    public void close() throws IOException
//    {
//        this.clientSocket.close();
//    }
    
    public class CurrentCall
    {
        public long voteId;
        public int contestantNumber;
        public long phoneNumber;
        public int timestamp;
        
        public CurrentCall(long voteId, int contestantNumber, long phoneNumber, int timestamp) {
            this.voteId = voteId;
            this.contestantNumber = contestantNumber;
            this.phoneNumber = phoneNumber;
            this.timestamp = timestamp;
        }
        
        public void debug() {
            System.out.println("call : " + this.voteId + "-" + this.phoneNumber + "-" + this.contestantNumber + "-" + this.timestamp);
        }
        
        public String getString()
        {
            return "call : " + this.voteId + "-" + this.phoneNumber + "-" + this.contestantNumber + "-" + this.timestamp;
        }
    }
    
   
    public static void main(String argv[]) throws Exception {
        
//        VoteGenClient client = new VoteGenClient();
//        for (int i=0; i<4000; i++)
//        {
//            CurrentCall call = client.getNextCall();
//            call.debug();
//        }
//        
//        client.close();
        for(int i=0; i<100; i++)
        {
            Thread thread = new Thread(){
                
                public void run() {
                    for (int i=0; i<10000; i++)
                    {
                        try {
                            VoteGenClient client = new VoteGenClient();
                            CurrentCall call = client.getNextCall();
                            //client.close();
                            if( call == null )
                                break;
                            System.out.println(" Thread: " + this.getName() + " - " + call.getString());

                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                        
                    }
                }
            };
            
            thread.start();
            
        }
        
    }
    
}