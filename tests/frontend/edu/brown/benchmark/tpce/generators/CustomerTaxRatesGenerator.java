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

import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class CustomerTaxRatesGenerator extends TableGenerator {
    private final static int TAXRATES_PER_CUSTOMER = 2; // two records per customer: one for the division, one for the country
    
    private final CustomerGenerator custGenerator; // for generating cids
    private final AddressGenerator addrGenerator; // for generating divs and countries by cids
    private final EGenRandom rnd;
    
    private final InputFileHandler divTaxRates;
    private final InputFileHandler countryTaxRates;
    
    private int recsGenerated;

    public CustomerTaxRatesGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        custGenerator = (CustomerGenerator)generator.getTableGen(TPCEConstants.TABLENAME_CUSTOMER, null);
        addrGenerator = (AddressGenerator)generator.getTableGen(TPCEConstants.TABLENAME_ADDRESS, null);
        addrGenerator.setCounter(true); // we need to generate addresses for customers only
        
        divTaxRates = generator.getInputFile(InputFile.TAXDIV);
        countryTaxRates = generator.getInputFile(InputFile.TAXCOUNTRY);
        
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    }
    
    /*
     * @param code Country or Division code
     */
    private String[] getTaxRow(long cid, int code, boolean isCountry) {
        // Don't have to save the old seed since the random generator is not used anywhere else in this class
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_TAX_RATE_ROW, cid);

        List<String[]> codeRecords;
        if (isCountry) {
            codeRecords = countryTaxRates.getTuplesByIndex(code - 1); // indexes in the file are 0-based, code is 1-based
        }
        else {
            codeRecords = divTaxRates.getTuplesByIndex(code - 1); // indexes in the file are 0-based, code is 1-based
        }

        int tupIndex = rnd.intRange(0, codeRecords.size() - 1);

        return codeRecords.get(tupIndex);
    }
    
    private String getTaxId(long cid, int code, boolean isCountry) {
        return getTaxRow(cid, code, isCountry)[0];
    }
    
    public double getTaxRate(long cid, int code, boolean isCountry) {
        return Double.valueOf(getTaxRow(cid, code, isCountry)[2]);
    }
    
    @Override
    public boolean hasNext() {
        return custGenerator.hasNext() || recsGenerated < TAXRATES_PER_CUSTOMER;
    }

    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        long cid, ad_id; 
        if (recsGenerated % TAXRATES_PER_CUSTOMER == 0) {
            cid = custGenerator.generateCustomerId();
            ad_id = addrGenerator.generateAddrId();
            recsGenerated = 0;
        }
        else {
            cid = custGenerator.getCurrentCId();
            ad_id = addrGenerator.getCurrentAddrId();
        }
        
        // maybe cache it in the object? the codes are the same for both records
        int[] codes = addrGenerator.getCountryAndDivCodes(ad_id);
        
        tuple[0] = getTaxId(cid, codes[recsGenerated], recsGenerated == 0); // cx_tx_id: first -- country, second -- division
        tuple[1] = cid;
        
        recsGenerated++;
        
        return tuple;
    }
}
