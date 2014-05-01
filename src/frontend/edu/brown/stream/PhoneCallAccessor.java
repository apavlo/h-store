package edu.brown.stream;

import java.io.*;
import java.util.ArrayList;

import edu.brown.stream.PhoneCallGenerator.PhoneCall;

public class PhoneCallAccessor {

    public PhoneCallAccessor() {
        
    }
    
    static public ArrayList<PhoneCall> getPhoneCallsFromFile(String filename)
    {
        ArrayList<PhoneCall> list = new ArrayList<PhoneCall> ();
        
        try{    
            FileInputStream fis = new FileInputStream(filename);
            ObjectInputStream ois = new ObjectInputStream(fis);
            list =  (ArrayList<PhoneCall>) ois.readObject();
            ois.close();
            
        }
        catch (Exception e)
        {
            //System.out.println("File Not Found");
            System.out.println(e.getMessage());
        } 
        
        return list;
    }
    
    static public void savePhoneCallsToFile(ArrayList<PhoneCall> calls, String filename) throws FileNotFoundException, IOException
    {
        ObjectOutputStream out = new ObjectOutputStream (new FileOutputStream(filename));
        out.writeObject(calls);
        out.flush();
        out.close();
    }

}
