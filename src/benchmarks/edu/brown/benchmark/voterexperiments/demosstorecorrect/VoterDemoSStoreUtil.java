/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *                                                                         *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.voterexperiments.demosstorecorrect;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Random;

import org.voltdb.VoltTable;

import edu.brown.benchmark.voterexperiments.demohstorecorrect.VoterDemoHStoreConstants;
import edu.brown.rand.RandomDistribution.Zipf;

public abstract class VoterDemoSStoreUtil {

    public static final Random rand = new Random();
    public static Zipf zipf = null;
    public static final double zipf_sigma = 1.001d;

    /**
     * Return the number of contestants to use for the given scale factor
     * @param scaleFactor
     */
    public static int getScaledNumContestants(double scaleFactor) {
        int min_contestants = 1;
        int max_contestants = VoterDemoSStoreConstants.CONTESTANT_NAMES_CSV.split(",").length;
        
        int num_contestants = (int)Math.round(VoterDemoSStoreConstants.NUM_CONTESTANTS * scaleFactor);
        if (num_contestants < min_contestants) num_contestants = min_contestants;
        if (num_contestants > max_contestants) num_contestants = max_contestants;
        
        return (num_contestants);
    }
    
    public static int isActive() {
        return number(1, 100) < number(86, 100) ? 1 : 0;
    }

    // modified from tpcc.RandomGenerator
    /**
     * @returns a random alphabetic string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator
    /**
     * @returns a random numeric string with length in range [minimum_length,
     *          maximum_length].
     */
    public static String nstring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = number(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    // taken from tpcc.RandomGenerator
    public static Long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    }

    public static String padWithZero(Long n) {
        String meat = n.toString();
        char[] zeros = new char[15 - meat.length()];
        for (int i = 0; i < zeros.length; i++)
            zeros[i] = '0';
        return (new String(zeros) + meat);
    }

    /**
     * Returns sub array of arr, with length in range [min_len, max_len]. Each
     * element in arr appears at most once in sub array.
     */
    public static int[] subArr(int arr[], int min_len, int max_len) {
        assert min_len <= max_len && min_len >= 0;
        int sub_len = number(min_len, max_len).intValue();
        int arr_len = arr.length;

        assert sub_len <= arr_len;

        int sub[] = new int[sub_len];
        for (int i = 0; i < sub_len; i++) {
            int j = number(0, arr_len - 1).intValue();
            sub[i] = arr[j];
            // arr[j] put to tail
            int tmp = arr[j];
            arr[j] = arr[arr_len - 1];
            arr[arr_len - 1] = tmp;

            arr_len--;
        } // FOR

        return sub;
    }
    

    public static void waitForSignal()
    {
    	try {
			InetAddress host = InetAddress.getLocalHost();
			//System.out.println("Host: " + host);
			//System.out.println("Host Name: " + host.getHostName());
			String hostname;
			
			
			if(host.getHostName().startsWith(VoterDemoSStoreConstants.HOST_PREFIX) || 
					host.getHostName().startsWith(VoterDemoSStoreConstants.HOST_PREFIX_2))
			{
				hostname = VoterDemoSStoreConstants.SERVER_HOST_NAME;
			}
			else
			{
				hostname = VoterDemoSStoreConstants.JIANG_HOST;
			}

			Socket socket = new Socket(hostname, VoterDemoSStoreConstants.SERVER_PORT_NUM);
			BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			PrintWriter out = new PrintWriter(socket.getOutputStream());
			//ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			String response;
			
			
			
			String mes = "s-store ready";
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
			 
		}
		catch (UnknownHostException e){
			System.err.println("UnknownHostException");
			e.printStackTrace();
		}
		catch (IOException e) {
			System.err.println("IOException");
			e.printStackTrace();
		}
		catch (InterruptedException e) {
			System.err.println("InterruptedException");
			e.printStackTrace(); 
		}
    }
    
    public static void clearFiles() throws IOException
    {
    	ArrayList<PrintWriter> out = new ArrayList<PrintWriter>();
    	out.add(new PrintWriter(new BufferedWriter(new FileWriter(VoterDemoSStoreConstants.OUTPUT_FILE, false))));
    	out.add(new PrintWriter(new BufferedWriter(new FileWriter(VoterDemoSStoreConstants.OVERWRITE_FILE, false))));
    	closeAllFiles(out);
    }
    
    private static void writeToAllFiles(ArrayList<PrintWriter> p, String toWrite)
    {
    	for(int i = 0; i < p.size(); i++)
    	{
    		p.get(i).print(toWrite);
    	}
    }
    
    private static void closeAllFiles(ArrayList<PrintWriter> p)
    {
    	for(int i = 0; i < p.size(); i++)
    	{
    		p.get(i).close();
    	}
    }
    
    public static void writeToFile(VoltTable[] v, ArrayList<String> tableNames, int numVotes) throws IOException
    {
    	ArrayList<PrintWriter> out = new ArrayList<PrintWriter>();
    	out.add(new PrintWriter(new BufferedWriter(new FileWriter(VoterDemoSStoreConstants.OUTPUT_FILE, true))));
    	out.add(new PrintWriter(new BufferedWriter(new FileWriter(VoterDemoSStoreConstants.OVERWRITE_FILE, false))));
    	if(numVotes == VoterDemoHStoreConstants.DELETE_CODE)
    	{
    		writeToAllFiles(out,"####DELETECANDIDATE####\n");
    	}
    	else
    		writeToAllFiles(out,"####" + numVotes + "####\n");
    	
        for(int i = 0; i < v.length; i++)
        {
        	writeToAllFiles(out,"**" + tableNames.get(i) + "**\n");
        	for(int j = 0; j < v[i].getRowCount(); j++)
        	{
        		for(int k = 0; k < v[i].getColumnCount(); k++)
        		{
        			if(k > 0)
	        		{
	        			writeToAllFiles(out, ",");
	        		}
        			
        			writeToAllFiles(out, (v[i].fetchRow(j).get(k)).toString());
        		}
        		writeToAllFiles(out,"\n");
        	}
        	writeToAllFiles(out,"---------------------------\n");
        }
        closeAllFiles(out);
    }

}
