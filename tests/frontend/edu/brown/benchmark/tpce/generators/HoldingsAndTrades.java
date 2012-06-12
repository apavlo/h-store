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
import edu.brown.benchmark.tpce.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class HoldingsAndTrades {
     /* These are used for picking the transaction type at load time.
      * NOTE: the corresponding "if" tests must be in the same order!
      */
     public static final int MARKET_BUY_LOAD_THRESHOLD   = 30;                               //  1% - 30%
     public static final int MARKET_SELL_LOAD_THRESHOLD  = MARKET_BUY_LOAD_THRESHOLD   + 30; // 31% - 60%
     public static final int LIMIT_BUY_LOAD_THRESHOLD    = MARKET_SELL_LOAD_THRESHOLD  + 20; // 61% - 80%
     public static final int LIMIT_SELL_LOAD_THRESHOLD   = LIMIT_BUY_LOAD_THRESHOLD    + 10; // 81% - 90%
     public static final int STOP_LOSS_LOAD_THRESHOLD    = LIMIT_SELL_LOAD_THRESHOLD   + 10; // 91% - 100%
     public static final  double fMinSecPrice = 20.00;
     public static final  double fMaxSecPrice = 30.00;
     /*
      * Used to generate trades
      */
     public static final int[] TRADE_QTY_SIZES = {100, 200, 400, 800};

    /*
     * Used at load and run time to determine which intial trades
     * simulate rollback by "aborting" - I.e. used to skip over a
     * trade ID.
     */
    private static final int ABORTED_TRADE_MOD_FACTOR = 51;
    
    /*
     * At what trade count multiple to abort trades.
     * One trade in every iAboutTrade block is aborted (trade id is thrown out).
     * NOTE: this really is 10 * Trade-Order mix percentage!
     */
    public static final int ABORT_TRADE = 101;
    
    // maximum number of securities in a customer account
    public static final int MAX_SECURITIES_PER_ACCOUNT = 18;
    
    /*
     * Arrays for min and max bounds on the security ranges for different tier accounts.
     * The indices into these arrays are
     *     1) the customer tier (zero based)
     *     2) the number of accounts for the customer (zero based)
     * Entries with 0 mean there cannot be that many accounts for a customer with that tier.
     */
     private final static int[][] MIN_SECURITIES_PER_ACCOUNT_RANGE =
         {{6, 4, 2, 2, 0, 0, 0, 0, 0, 0}
         ,{0, 7, 5, 4, 3, 2, 2, 2, 0, 0}
         ,{0, 0, 0, 0, 4, 4, 3, 3, 2, 2}};
     
     private final static int[][] MAX_SECURITIES_PER_ACCOUNT_RANGE =
         {{14, 16, 18, 18, 00, 00, 00, 00, 00, 00}
         ,{00, 13, 15, 16, 17, 18, 18, 18, 00, 00}
         ,{00, 00, 00, 00, 16, 16, 17, 17, 18, 18}};
    
    private final EGenRandom rnd;
    private final CustomerAccountsGenerator custAccGen;
    private final long secCount;
    private long[] secIds = new long[MAX_SECURITIES_PER_ACCOUNT];
    
    public HoldingsAndTrades(TPCEGenerator generator) {
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        custAccGen = (CustomerAccountsGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
        secCount = SecurityHandler.getSecurityNum(generator.getTotalCustomers());
    }
    
    public void initNextLoadUnit(long tradesSkip) {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, tradesSkip);
    }
    
    /**
     * Generates an account number, security account index and the security position for the file
     * 
     * @param cid Customer ID
     * @param tier Customer tier
     * @return (account number, security account index, security flat_in file position)
     */
    public long[] generateRandomAccSecurity(long cid, TierId tier) {
        long[] custAccAndCount = custAccGen.genRandomAccId(rnd, cid, tier);
        
        int secNum = getNumberOfSecurities(custAccAndCount[0], tier, (int)custAccAndCount[1]);
        int secAccIndex = rnd.intRange(1, secNum);
        long secFlatFileIndex = getSecurityFlatFileIndex(custAccAndCount[0], secAccIndex);
        
        long[] res = new long[3];
        res[0] = custAccAndCount[0];
        res[1] = secAccIndex;
        res[2] = secFlatFileIndex;
        
        return res;      
    }
    
    private int getNumberOfSecurities(long accId, TierId tier, int accCount) {
        int minRange = MIN_SECURITIES_PER_ACCOUNT_RANGE[tier.ordinal() - TierId.eCustomerTierOne.ordinal()][accCount - 1];
        int maxRange = MAX_SECURITIES_PER_ACCOUNT_RANGE[tier.ordinal() - TierId.eCustomerTierOne.ordinal()][accCount - 1];

        long oldSeed = rnd.getSeed();
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_NUMBER_OF_SECURITIES, accId);
        int numberOfSecurities = rnd.intRange(minRange, maxRange);
        rnd.setSeed(oldSeed);
        
        return numberOfSecurities;
    }
    
    /**
     * Converts security index within an account (1-18) into
     * the corresponding security index within the
     * SECURITY.txt input file (0-6849).
     *
     * Needed to be able to get the security symbol
     * and other information from the input file.
     * 
     * @param accId Customer account ID
     * @param secIndex Security account index
     * @return security index within the input file (0-based)
     */
    public long getSecurityFlatFileIndex(long accId, int secIndex) {
        long  secFlatFileIndex = 0; // index of the selected security in the input flat file

        long oldSeed = rnd.getSeed();
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_STARTING_SECURITY_ID, accId * MAX_SECURITIES_PER_ACCOUNT);

        /*
         * The main idea behind the loop below is that we want to generate secIndex _unique_ flat
         * file indexes. The array is used to keep track of them.
         */
        int generatedIndexCount = 0;
        while (generatedIndexCount < secIndex) {
            secFlatFileIndex = rnd.int64Range(0, secCount - 1);
                    
            int i;
            for (i = 0; i < generatedIndexCount; i++) {
                if (secIds[i] == secFlatFileIndex) {
                    break;
                }
            }

            // If a duplicate is found, overwrite it in the same location
            // so basically no changes are made.
            secIds[i] = secFlatFileIndex;

            // If no duplicate is found, increment the count of unique ids
            if (i == generatedIndexCount) {
                generatedIndexCount++;
            }
        }

        rnd.setSeed(oldSeed);

        return secFlatFileIndex;
    }
    
    public static boolean isAbortedTrade(long tradeId) {
        if (ABORTED_TRADE_MOD_FACTOR == tradeId % ABORT_TRADE) {
            return true;
        }
        else {
            return false;
        }
    }

}
