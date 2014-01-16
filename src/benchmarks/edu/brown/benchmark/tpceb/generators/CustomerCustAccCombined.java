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

package edu.brown.benchmark.tpceb.generators;

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpceb.generators.CustomerSelection.TierId;
import edu.brown.benchmark.tpceb.generators.TPCEGenerator.InputFile;

import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpceb.TPCEConstants;
import edu.brown.benchmark.tpceb.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpceb.util.EGenDate;
import edu.brown.benchmark.tpceb.util.EGenRandom;


public class CustomerCustAccCombined extends TableGenerator {
   /* public enum TaxStatus {
        eNonTaxable,
        eTaxableAndWithhold,
        eTaxableAndDontWithhold;
        
    }*/
    /**
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private static final int RNG_SKIP_ONE_ROW_CUSTOMER = 35;  // real max count in v3.5 of EGen: 29
 
    private final long customersNum;
    private final long startCustomerId;

    
    private long counter;
    
    
    private static final double accountInitialPositiveBalanceMax = 9999999.99;
    private static final double accountInitialNegativeBalanceMin = -9999999.99;

    private static final int percentAccountsWithPositiveInitialBalance = 80;
    
    private int accsToGenerate; // every customer can have a different number of accs
    private int accsGenerated; // the number of accs already generated for the current customer

    /*
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private final static int rngSkipOneRowCustomerAccount = 10;   // real max count in v3.5: 7
    
    private final static int[] minAccountsPerCustRange = {1, 2, 5};  // tier based
    private final static int[] maxAccountsPerCustRange = {4, 8, 10}; // tier based
    public final static int MAX_ACCOUNTS_PER_CUST = 10; // should not be more than the last number in the max array
    
    private static final int BROKERS_COUNT = TPCEConstants.DEFAULT_LOAD_UNIT / TPCEConstants.BROKERS_DIV;
    
    private long startingAccId;

    private final EGenRandom rnd;
    
    public CustomerCustAccCombined(Table catalog_tbl, TPCEGenerator generator){
        super(catalog_tbl, generator);
        
        customersNum = generator.getCustomersNum();
        startCustomerId = generator.getStartCustomer();
        
 
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
 
   
     //   statusType = generator.getInputFile(InputFile.STATUS);
   
        
    }

    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, (counter + startCustomerId - 1) * RNG_SKIP_ONE_ROW_CUSTOMER);
    }

    public long generateCustomerId() {
        if (counter % TPCEConstants.DEFAULT_LOAD_UNIT == 0) {
            initNextLoadUnit();
        }
        
        counter++;
        
        return counter + startCustomerId - 1 + TPCEConstants.IDENT_SHIFT;
    }
    
    public long getCurrentCId() {
        return counter + startCustomerId - 1 + TPCEConstants.IDENT_SHIFT;
    }
    
    public long generateAccountId() {
        if ( getCurrentCId() % TPCEConstants.DEFAULT_LOAD_UNIT == 0) {
            initNextLoadUnit();
        }
        
        if (accsToGenerate == accsGenerated) {
            long cid = generateCustomerId();
            
            accsGenerated = 0;
            accsToGenerate = getNumberofAccounts(cid, CustomerSelection.getTier(cid).ordinal() + 1);
            
            startingAccId = getStartingAccId(cid);
        }
        
        accsGenerated++;
        long id = startingAccId + accsGenerated - 1;
        //System.out.println("THIS IS THE GENERATED ID:" + id);
        return startingAccId + accsGenerated - 1;
    }
   
    /**
     * Generates a random account ID for the specified customer.
     * Used for external generators, like trade
     * 
     * @param rnd External random generator
     * @param custId Customer ID
     * @param tier Customer tier
     * @return Array of two: account ID, the number of accounts for the customer 
     */
    public long[] genRandomAccId(EGenRandom rnd, long custId, TierId tier) {
        long custAcc, accCount, startAcc;
        
        accCount = getNumberofAccounts(custId, tier.ordinal() + 1);
        startAcc = getStartingAccId(custId);
        
        custAcc = rnd.int64Range(startAcc, startAcc + accCount - 1); // using the external generator here
        
        long[] res = new long[2];
        res[0] = custAcc;
        res[1] = accCount;
        
        return res;
    }
    
    public long[] genRandomAccId(EGenRandom rnd, long custId, TierId tier, long acct_id, int accountCount) {
        long custAcc,  startAcc;
        int accCount;
        long[] res = new long[2];
        
        accCount = getNumberofAccounts(custId, tier.ordinal() + 1);
        startAcc = getStartingAccId(custId);
        
        custAcc = rnd.int64Range(startAcc, startAcc + accCount - 1); // using the external generator here
        
        if (acct_id != -1){
            res[0] = acct_id = custAcc;
        }
        if(accountCount != -1){
            res[1] = accountCount = accCount;
        }
        return res;
    }
   
    
    @Override
    public boolean hasNext() {
        return counter < customersNum;
    }
   
    
    private int getNumberofAccounts(long cid, int tier) {
        int minAccountCount = minAccountsPerCustRange[tier - 1];
        int mod = maxAccountsPerCustRange[tier - 1] - minAccountCount + 1;
        int inverseCid = CustomerSelection.getInverseCid(cid);
        
        // Note: the calculations below assume load unit contains 1000 customers.
        if (inverseCid < 200) {     // Tier 1
            return ((inverseCid % mod) + minAccountCount);
        }
        else if (inverseCid < 800) { // Tier 2
            return (((inverseCid - 200 + 1) % mod) + minAccountCount);
        }
        else {                       // Tier 3
            return (((inverseCid - 800 + 2) % mod) + minAccountCount);
        }
    }
    
    public static long getStartingAccId(long cid) {
        //start account ids on the next boundary for the new customer
        return ((cid - 1) * MAX_ACCOUNTS_PER_CUST + 1);
    }
    
    public static long getEndtingAccId(long cid) {
        //start account ids on the next boundary for the new customer
        return ((cid + 1) * MAX_ACCOUNTS_PER_CUST);
    }
     
    public long generateBrokerId(long accId) {
        //  Customer that own the account (actually, customer id minus 1)
        long  customerId = ((accId - 1) / MAX_ACCOUNTS_PER_CUST) - TPCEConstants.IDENT_SHIFT;

        // Set the starting broker to be the first broker for the current load unit of customers.
        long startFromBroker = (customerId / TPCEConstants.DEFAULT_LOAD_UNIT) * MAX_ACCOUNTS_PER_CUST +
                TPCEConstants.STARTING_BROKER_ID + TPCEConstants.IDENT_SHIFT;

        // Note: this depends on broker ids being integer numbers from contiguous range.
        // The method of generating broker ids should be in sync with the BrokerGenerator
        return rnd.rndNthInt64Range(EGenRandom.RNG_SEED_BASE_BROKER_ID, accId - (10 * TPCEConstants.IDENT_SHIFT),
 startFromBroker, startFromBroker + BROKERS_COUNT - 1);
    }
    
    
    private double generateBalance() {
        if (rnd.rndPercent(percentAccountsWithPositiveInitialBalance)) {
            return rnd.doubleIncrRange(0.00, accountInitialPositiveBalanceMax, 0.01);
        }
        else {
            return rnd.doubleIncrRange(accountInitialNegativeBalanceMin, 0.00, 0.01);
        }
    }

    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        long cid = generateCustomerId(); 
        long accId = generateAccountId();
       
 
        tuple[0] = cid;
        tuple[1] = accId; // ca_id
        tuple[2] = generateBrokerId(accId); // ca_b_id
       // System.out.println("Cust ID to be inserted:" + cid);
       // System.out.println("Acc ID to be inserted:" + accId);
        tuple[3] = (short)CustomerSelection.getTier(cid).ordinal() + 1; 
        tuple[4] = generateBalance(); // ca_bal
        
        return tuple;       
    }
    
  
 
    
}
