/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
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

package edu.brown.benchmark.tpce.generators;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.util.EGenRandom;


/**
 *   Description:        This class encapsulates customer tier distribution
 *                       functions and provides functionality to:
 *                       - Generate customer tier based on customer ID
 *                       - Generate non-uniform customer ID
 *                       - Generate customer IDs in a specified partition, and
 *                         outside the specified partition a set percentage of
 *                         the time.
 */

public class CustomerSelection {
    public enum TierId {
        eCustomerTierOne,
        eCustomerTierTwo,
        eCustomerTierThree;
        
    }
    
    private final EGenRandom rnd;
    private final boolean isPartition;
    private final int partPercent;
    
    private final long startCustomer;
    private long myStartCustomer;
    private final long customerCount;
    private long myCustomerCount;
     
    public CustomerSelection(EGenRandom rnd, long startCustomer, long customerCount, int partPercent,
            long myStartCustomer, long myCustomerCount) {
        this.rnd = rnd;
        this.startCustomer = startCustomer + TPCEConstants.IDENT_SHIFT;
        this.myStartCustomer = myStartCustomer + TPCEConstants.IDENT_SHIFT;
        this.customerCount = customerCount;
        this.myCustomerCount = myCustomerCount;
        this.partPercent = partPercent;
        
        if (startCustomer == myStartCustomer && customerCount == myCustomerCount) {
            isPartition = false;
        }
        else {
            isPartition = true;
        }
    }
  
    public CustomerSelection(EGenRandom rnd, long startCustomer, long customerCount){
    	this.rnd = rnd;
    	this.startCustomer = startCustomer + TPCEConstants.IDENT_SHIFT;
    	this.myStartCustomer = 0 + TPCEConstants.IDENT_SHIFT;
    	this.customerCount = customerCount;
    	this.partPercent = 0;
    	this.isPartition = false;
    }
    
    public void setPartitionRange(long startFromCustomer, long customerCount) {
        if (isPartition) {
            this.myStartCustomer = startFromCustomer;
            this.myCustomerCount = customerCount;
        }
    }

    /**
     * Generate a customer id with a tier.
     * All intra-function comments are from EGen.
     * 
     * @return Generated customer id and the tier
     */
    public Object[] genRandomCustomer() {
        double cw = rnd.doubleRange(0.0001, 2000);
        TierId tier;

        // Uniformly select the higher portion of the customer ID.
        long cHigh;
        if (isPartition && rnd.rndPercent(partPercent)) {
            // Generate a load unit inside the partition.
            cHigh = (rnd.int64Range(myStartCustomer, myStartCustomer + myCustomerCount - 1) - 1) / 1000; // minus 1 for the upper boundary case
        }
        else {
            // Generate a load unit across the entire range
            cHigh = (rnd.int64Range(startCustomer, startCustomer + customerCount - 1) - 1) / 1000; // minus 1 for the upper boundary case
        }

        // Non-uniformly select the lower portion of the customer ID and the tier
        int cLow;
        if (cw <= 200) {
            // tier one
            cLow = (int)Math.ceil(Math.sqrt(22500 + 500 * cw) - 151);
            tier = TierId.eCustomerTierOne;
        }
        else if (cw <= 1400) {
            // tier two
            cLow = (int)Math.ceil(Math.sqrt(290000 + 1000 * cw) - 501);
            tier = TierId.eCustomerTierTwo;
        }
        else {
            // tier three
            cLow = (int)Math.ceil(149 + Math.sqrt(500 * cw - 277500));
            tier = TierId.eCustomerTierThree;
        }
        
        Object[] res = new Object[2];
        res[0] = cHigh * 1000 + permute(cLow, cHigh) + 1;
        res[1] = tier;

        return res;
    }
    
    // lower 3 digits
    private static long lowDigits(long cid) {
        return (cid - 1) % 1000;
    }

    // higher 3 digits
    private static long highDigits(long cid) {
        return (cid - 1) / 1000;
    }
    
    public static long permute(long low, long high) {
        return  (677 * low + 33 * (high + 1)) % 1000;
    }

    // Inverse permutation.
    public static long inversePermute(long low, long high) {
        // Extra mod to make the result always positive
        return  (((613 * (low - 33 * (high + 1))) % 1000) + 1000) % 1000;
    }
    
    public static TierId getTier(long cid) {
        long revCid = inversePermute(lowDigits(cid), highDigits(cid));

        if (revCid < 200) {
            return TierId.eCustomerTierOne;
        }
        else {
            if (revCid < 800) {
                return TierId.eCustomerTierTwo;
            }
            else {
                return TierId.eCustomerTierThree;
            }
        }
    }
    
    /*
     *   Return scrambled inverse customer id in range of 0 to 999.
     */
    public static int getInverseCid(long cid) {
        int cHigh      =  (int)highDigits(cid);
        int inverseCid =  (int)inversePermute(lowDigits(cid), cHigh);

        if (inverseCid < 200) {     // Tier 1: value 0 to 199
            return ((3 * inverseCid + (cHigh + 1)) % 200);
        }
        else {
            if (inverseCid < 800) { // Tier 2: value 200 to 799
                return (((59 * inverseCid + 47 * (cHigh + 1)) % 600) + 200);
            }
            else {                   // Tier 3: value 800 to 999
                return (((23 * inverseCid + 17 * (cHigh + 1)) % 200) + 800);
            }
        }
    }
}
