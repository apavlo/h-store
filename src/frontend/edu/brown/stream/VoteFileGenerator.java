package edu.brown.stream;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.System;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class VoteFileGenerator {
    
    // Initialize some common constants and variables
    public static final String CONTESTANT_NAMES_CSV = "Edwina Burnam,Tabatha Gehling,Kelly Clauss,Jessie Alloway," +
                                               "Alana Bregman,Jessie Eichman,Allie Rogalski,Nita Coster," +
                                               "Kurt Walser,Ericka Dieter,Loraine NygrenTania Mattioli";
    public static final int NUM_CONTESTANTS = 6; 

    public VoteFileGenerator() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] vargs) throws Exception {

//        generateTxtFile("voterdemohstoreanother-o-20000");
//        generateTxtFile("voterdemohstoreanother-d-20000");
//        generateTxtFile("voterdemohstoreanother-o-40000");
//        generateTxtFile("voterdemohstoreanother-d-40000");
        
        for(String argument : vargs){
            System.out.println(argument);
        }  
        
        AnotherArgumentsParser args = AnotherArgumentsParser.load( vargs , false);
        
        int shuffle_method = 0; // default
        if (args.hasParam(AnotherArgumentsParser.PARAM_SHUFFLE_METHOD)) {
            shuffle_method = args.getIntParam(AnotherArgumentsParser.PARAM_SHUFFLE_METHOD);
        }
        
        Long shuffle_size = 1000000l;
        if (args.hasParam(AnotherArgumentsParser.PARAM_SHUFFLE_SIZE)) {
            shuffle_size = args.getLongParam(AnotherArgumentsParser.PARAM_SHUFFLE_SIZE);
        }
        
        System.out.println("Shuffle method: " + shuffle_method);
        System.out.println("Shuffle size: " + shuffle_size);
        

        // Phone number generator
        int numContestants = getScaledNumContestants(1);
        PhoneCallGenerator switchboard = new PhoneCallGenerator(1, numContestants, shuffle_size);
        
        // generate the ordered and disordered votes based on parameters
        switchboard.generateVotes();
        
//        // test we have save the correct thing.
//        System.out.println("Testing PhoneCallAccessor.getPhoneCallsFromFile method - orderedcall.ser ... \n");
//        String filename = "orderedcall.ser";
//        ArrayList<PhoneCall> list = PhoneCallAccessor.getPhoneCallsFromFile(filename);
//        
//        for (PhoneCall call : list) 
//            call.debug();
//
//        System.out.println("Testing PhoneCallAccessor.getPhoneCallsFromFile method - disorderedcall.ser ... \n");
//        filename = "disorderedcall.ser";
//        list = PhoneCallAccessor.getPhoneCallsFromFile(filename);
//      
//        for (PhoneCall call : list) 
//            call.debug();
//        
//        System.out.println("Testing class VoteGenerator method with disorderedcall.ser ... ");
//        VoteGenerator vg = new VoteGenerator(filename);
//        
//        PhoneCall current_call = vg.nextVote();
//        while(current_call !=null )
//        {
//            current_call.debug();
//            current_call = vg.nextVote();
//        }
//        
//        while(vg.hasMoreVotes()==true)
//        {
//            PhoneCall current_call = vg.nextVote();
//            current_call.debug();
//        }
        
    }
    
    public static void generateTxtFile(String name) throws IOException
    {
        String filename = name + ".txt";
        BufferedWriter out;
        out = new BufferedWriter(new FileWriter(filename,true));
        
//        File file=new File(filename);
//        FileWriter fw = new FileWriter(file,true);
        System.out.println(filename);
        
        filename = name + ".ser";
        System.out.println(filename);
        VoteGenerator vg = new VoteGenerator(filename);
        //System.out.println(filename);
        
        PhoneCall current_call = vg.nextVote();
        while(current_call !=null )
        {
            current_call.debug();
            //fw.append(current_call.getString());
            out.write(current_call.getString());
            
            current_call = vg.nextVote();
        }
        
        out.close();
        
    }

    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoteFileGenerator.CONTESTANT_NAMES_CSV.split(",").length;
        
        int num_contestants = (int)Math.round(VoteFileGenerator.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;
        
        return (num_contestants);
    }

}
