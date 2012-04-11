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

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class CustomerAccountsGenerator extends TableGenerator {
    enum TaxStatus {
        eNonTaxable(0),
        eTaxableAndWithhold(1),
        eTaxableAndDontWithhold(2);
        
        private final int id;
        
        private TaxStatus(int id) {
            this.id = id;
        }
        
        public int getValue() {
            return id;
        }
    }

    private static final int percentAccountTaxStatusNonTaxable = 20;
    private static final int percentAccountTaxStatusTaxableAndWithhold = 50;
    private static final int percentAccountTaxStatusTaxableAndDontWithhold = 30;
    private static final int percentAccountsWithPositiveInitialBalance = 80;
    
    private static final double accountInitialPositiveBalanceMax = 9999999.99;
    private static final double accountInitialNegativeBalanceMin = -9999999.99;

    private int accsToGenerate; // every customer can have a different number of accs
    private int accsGenerated; // the number of accs already generated for the current customer
    private final CustomerGenerator customerGenerator; // is used for geenrating just customer ids
    private final EGenRandom rnd;
    
    /*
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private final static int rngSkipOneRowCustomerAccount = 10;   // real max count in v3.5: 7
    
    private final static int[] minAccountsPerCustRange = {1, 2, 5};  // tier based
    private final static int[] maxAccountsPerCustRange = {4, 8, 10}; // tier based
    private final static int maxAccountsPerCust = 10; // should not be more than the last number in the max array
    
    private final static long startingBrokerID = 1;
    private static final int BROKERS_COUNT = TPCEConstants.DEFAULT_LOAD_UNIT / TPCEConstants.BROKERS_DIV;
    
    private long startingAccId;
    
    private final InputFileHandler taxableNames;
    private final InputFileHandler nonTaxableNames;
    private final PersonHandler person;
    
    public CustomerAccountsGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        // do not need the Table for this. Nasty, though. Have to use type casting since we need specific functions
        customerGenerator = (CustomerGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER, null);
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        
        taxableNames = generator.getInputFile(InputFile.TAXACC);
        nonTaxableNames = generator.getInputFile(InputFile.NONTAXACC);
        person = new PersonHandler(generator.getInputFile(InputFile.LNAME), generator.getInputFile(InputFile.FEMFNAME),
                generator.getInputFile(InputFile.MALEFNAME));
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT,
                customerGenerator.getCurrentCId() * maxAccountsPerCust * rngSkipOneRowCustomerAccount); 
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
    
    private long getStartingAccId(long cid) {
        //start account ids on the next boundary for the new customer
        return ((cid - 1) * maxAccountsPerCust + 1);
    }
    
    private long generateAccountId() {
        if (customerGenerator.getCurrentCId() % TPCEConstants.DEFAULT_LOAD_UNIT == 0) {
            initNextLoadUnit();
        }
        
        if (accsToGenerate == accsGenerated) {
            long cid = customerGenerator.generateCustomerId();
            
            accsGenerated = 0;
            accsToGenerate = getNumberofAccounts(cid, CustomerSelection.getTier(cid).getValue());
            
            startingAccId = getStartingAccId(cid);
        }
        
        accsGenerated++;
        
        return startingAccId + accsGenerated - 1;
    }
    
    private long generateBrokerId(long accId) {
        //  Customer that own the account (actually, customer id minus 1)
        long  customerId = ((accId - 1) / maxAccountsPerCust) - TPCEConstants.IDENT_SHIFT;

        // Set the starting broker to be the first broker for the current load unit of customers.
        long startFromBroker = (customerId / TPCEConstants.DEFAULT_LOAD_UNIT) * maxAccountsPerCust + startingBrokerID + TPCEConstants.IDENT_SHIFT;

        // Note: this depends on broker ids being integer numbers from contiguous range.
        // The method of generating broker ids should be in sync with the BrokerGenerator
        return rnd.rndNthInt64Range(EGenRandom.RNG_SEED_BASE_BROKER_ID, accId - (10 * TPCEConstants.IDENT_SHIFT),
                startFromBroker, startFromBroker + BROKERS_COUNT -1);
    }
    
    private TaxStatus getAccountTaxStatus(long accId)
    {
        long oldSeed = rnd.getSeed();

        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_ACCOUNT_TAX_STATUS, accId);

        int thr = rnd.rndPercentage();
        rnd.setSeed(oldSeed);
        
        if (thr <= percentAccountTaxStatusNonTaxable) {
            return TaxStatus.eNonTaxable;
        }
        else {
            if (thr <= percentAccountTaxStatusNonTaxable + percentAccountTaxStatusTaxableAndWithhold) {
                return TaxStatus.eTaxableAndWithhold;
            }
            else {
                return TaxStatus.eTaxableAndDontWithhold;
            }
        }
    }
    
    private String generateAccName(TaxStatus ts, long accId, long cid) {
        String res = person.getFirstName(cid) + " " + person.getLastName(cid) + " ";
        if (ts == TaxStatus.eNonTaxable) {
            int nameInd = (int)accId % nonTaxableNames.getRecordsNum();
            res += nonTaxableNames.getTupleByIndex(nameInd)[0];
        }
        else {
            int nameInd = (int)accId % taxableNames.getRecordsNum();
            res += taxableNames.getTupleByIndex(nameInd)[0];
        }
        
        return res;
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
    public boolean hasNext() {
        // we either have more customers or are still generating acounts for the last one
        return customerGenerator.hasNext() || accsGenerated < accsToGenerate;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[this.catalog_tbl.getColumns().size()];
        
        long accId = generateAccountId();
        long cid = customerGenerator.getCurrentCId();
        TaxStatus tax = getAccountTaxStatus(accId);
        
        tuple[0] = accId; // ca_id
        tuple[1] = generateBrokerId(accId); // ca_b_id
        tuple[2] = cid; // ca_c_id
        tuple[3] = generateAccName(tax, accId, cid); // ca_name
        tuple[4] = (short)tax.getValue(); // ca_tax_st
        tuple[5] = generateBalance(); // ca_bal
        
        return tuple;       
    }
}
