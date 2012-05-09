package edu.brown.hstore;

import jline.*;

import java.io.*;
import java.util.*;
import java.util.zip.*;

import edu.brown.protorpc.AbstractEventHandler;

/** MySQL Terminal **/
public class AdHocTerminalListener{ //extends AbstractEventHandler??
	
	ConsoleReader m_reader;
	
	public AdHocTerminalListener() throws IOException{
		//m_reader = new ConsoleReader();
		
		usage();
	}
	
	public static void usage() {
        System.out.println("Usage: ");
    }
	
	public String getQuery() throws IOException{
		String query = "";
		byte[] byte_query = new byte[200];
		//do {
			System.in.read(byte_query);//query = m_reader.readLine("enter command > ");            
        //} while(query != null && query.length() > 0);
		query = new String(byte_query);
		return query;
	}
	
	public void printResult(String result){
		System.out.println(result);
	}
	

}
