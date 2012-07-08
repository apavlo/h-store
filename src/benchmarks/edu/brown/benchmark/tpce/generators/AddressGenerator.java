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

public class AddressGenerator extends TableGenerator {
    // For generating address lines
    private static final int streetNumberMin = 100;
    private static final int streetNumberMax = 25000;
    private static final int pctCustomersWithNullAD_LINE_2 = 60; // % of customers that do not have a second address line
    private static final int pctCustomersWithAptAD_LINE_2 = 75; // % of customer that live in an apartment (others live in a suite)
    private static final int apartmentNumberMin = 1;
    private static final int apartmentNumberMax = 1000;
    private static final int suiteNumberMin = 1;
    private static final int suiteNumberMax = 500;

    private static final int USACtryCode = 1;     //must be the same as the code in country tax rates file
    private static final int CanadaCtryCode = 2;  //must be the same as the code in country tax rates file
    
    private final long customersNum;
    private final long customersStart;
    
    private long counter;
    private long totalAddresses; // not really "total", just the last row number for hasNext() purposes
    private boolean isCustomerAddress; // is the currently generated address for a customer?
    private final long exchangeCount;
    private final long companyCount;
    
    /* 
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private static final int rngSkipOneRowAddress = 10;   // real number in 3.5: 7
    private final EGenRandom rnd;
    
    private final InputFileHandler streetFile;
    private final InputFileHandler streetSuffFile;
    private final InputFileHandler zipFile;
    
    public AddressGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        customersNum = generator.getCustomersNum();
        customersStart = generator.getStartCustomer();
        
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        
        exchangeCount = generator.getInputFile(InputFile.EXCHANGE).getRecordsNum();
        companyCount = generator.getCompanyCount(generator.getTotalCustomers());
        
        streetFile = generator.getInputFile(InputFile.STNAME);
        streetSuffFile = generator.getInputFile(InputFile.STSUFFIX);
        zipFile = generator.getInputFile(InputFile.ZIPCODE);
        
        // if we are generating another portion of customers, do not generate exchange/company addresses
        setCounter(customersStart != TPCEConstants.DEFAULT_START_CUSTOMER_ID);
    }
    
    public void setCounter(boolean customersOnly) {
        if (customersOnly) {
            // assume we have already generated addresses for exchanges and companies
            counter = exchangeCount + companyCount + customersStart - 1;
            totalAddresses = counter + customersNum;
        }
        else {
            counter = customersStart - 1;
            totalAddresses = counter + exchangeCount + companyCount + customersNum;
        }
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowAddress);
    }

    public long generateAddrId() {
        // if we are generating customer addresses, then reset the generator if needed
        if (counter > (exchangeCount + companyCount) &&
                ((counter - (exchangeCount + companyCount)) % TPCEConstants.DEFAULT_LOAD_UNIT == 0)) {
            initNextLoadUnit();
        }
        
        counter++;
        
        isCustomerAddress = (counter >= exchangeCount + companyCount);
        
        return counter + TPCEConstants.IDENT_SHIFT;
    }
    
    public long getCurrentAddrId() {
        return counter + TPCEConstants.IDENT_SHIFT;
    }
    
    public long getAddrIdForCustomer(long custId) {
        return exchangeCount + companyCount + custId;
    }
    
    private String generateAddrLine1() {
        int streetNum = rnd.intRange(streetNumberMin, streetNumberMax);
        int streetNameKey = rnd.intRange(0, streetFile.getMaxKey() - 1); // -1? made this to be compatible with EGen
        int streetSuffKey = rnd.intRange(0, streetSuffFile.getMaxKey());
        
        return Integer.toString(streetNum) + " " + streetFile.getTupleByKey(streetNameKey)[0] + 
                " " + streetSuffFile.getTupleByKey(streetSuffKey)[0];
    }
    
    private String generateAddrLine2() {
        // companies do not have a second line, as some customers (60% by default)
        if (!isCustomerAddress || rnd.rndPercent(pctCustomersWithNullAD_LINE_2)) {
            return "";
        }
        else {
            if (rnd.rndPercent(pctCustomersWithAptAD_LINE_2)) {
                return "Apt. " + Integer.toString(rnd.intRange(apartmentNumberMin, apartmentNumberMax));
            }
            else {
                return "Suite " + Integer.toString(rnd.intRange(suiteNumberMin, suiteNumberMax));
            }
        }
    }
    
    private int getZipRecKey(long adId) {
        long oldSeed = rnd.getSeed();
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_TOWN_DIV_ZIP, adId);
        int key = rnd.intRange(0, zipFile.getMaxKey());
        rnd.setSeed(oldSeed);
        
        return key;
    }
    
    private int getCountryCode(String zipCode) {
        char c = zipCode.charAt(0);
        if (c >= '0' && c <= '9') {
            return USACtryCode;
        }
        else {
            return CanadaCtryCode;
        }
    }
    
    // returns division and country codes (for tax rate)
    public int[] getCountryAndDivCodes(long adId) {
        int[] res = new int[2];
        
        String[] zipRecord = zipFile.getTupleByKey(getZipRecKey(adId));
        
        res[0] = getCountryCode(zipRecord[1]);  // country tax code
        res[1] = Integer.valueOf(zipRecord[0]); // division tax code
        
        return res;
    }
    
    private String[] generateZipAndCountry(long adId) {
        String[] zip_country = new String[2];
        
        int zipKey = getZipRecKey(adId);
        zip_country[0] = zipFile.getTupleByKey(zipKey)[1]; // zip is as a second field
        
        int countryCode = getCountryCode(zip_country[0]);
        if (countryCode == USACtryCode) {
            zip_country[1] = "USA";
        }
        else {
            zip_country[1] = "CANADA";
        }
        
        return zip_country;
    }
    
    @Override
    public boolean hasNext() {
        return counter < totalAddresses; 
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        long adId = generateAddrId();
        String[] zip_cntr = generateZipAndCountry(adId);
        
        tuple[0] = adId; // ad_id
        tuple[1] = generateAddrLine1(); // ad_line1
        tuple[2] = generateAddrLine2(); // ad_line2
        tuple[3] = zip_cntr[0]; // ad_zc_code
        tuple[4] = zip_cntr[1]; // ad_ctry
        
        return tuple;
    }
}
