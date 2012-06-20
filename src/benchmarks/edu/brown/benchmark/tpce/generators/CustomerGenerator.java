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

import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.StatusTypeGenerator.StatusTypeId;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class CustomerGenerator extends TableGenerator {
    // percent of people in ages: <= 18, 19-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75-84, >=85
    private static final int[] agePercents  = {5, 16, 17, 19, 16, 11, 8, 7, 1};
    
    // min-max age brackets for corresponding percents
    private static final int[] ageBrackets = {10, 19, 25, 35, 45, 55, 65, 75, 85, 100};

    // phone country code US/Canada
    private static final String usCountryCode = "011";
            
    // email domains
    private static final String[] emailDomains = {"@msn.com", "@hotmail.com", "@rr.com", "@netzero.com", "@earthlink.com", "@attbi.com"};
    
    /* 
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private static final int RNG_SKIP_ONE_ROW_CUSTOMER = 35;  // real max count in v3.5 of EGen: 29
 
    private final long customersNum;
    private final long startCustomerId;
    
    private final long exchNum; // the number of exchange records
    private final long compNum; // the number of company records
    
    private final EGenRandom rnd;
    private final PersonHandler person;
    private final InputFileHandler statusType;
    private final InputFileHandler areaCodes;
    
    private long counter;

    public CustomerGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        customersNum = generator.getCustomersNum();
        startCustomerId = generator.getStartCustomer();
        
        rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
        person = new PersonHandler(generator.getInputFile(InputFile.LNAME), generator.getInputFile(InputFile.FEMFNAME),
                generator.getInputFile(InputFile.MALEFNAME));
        statusType = generator.getInputFile(InputFile.STATUS);
        areaCodes = generator.getInputFile(InputFile.AREA);
        
        exchNum = generator.getInputFile(InputFile.EXCHANGE).getRecordsNum();
        
        /*
         * The official EGen uses the number of companies in the input file.
         * That seems incorrect since the number of companies for the ADDRESS table
         * changes according to the total number of customers
         */
        compNum = generator.getCompanyCount(generator.getTotalCustomers());
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
    
    private Date getDOB() {
        // first, determine the age bracket
        int per = rnd.intRange(1, 100);
        int bracket = 0;
        
        int curr_per = agePercents[0];
        while (per > curr_per) {
            bracket++;
            curr_per += agePercents[bracket];
        }
        
        // current date
        int year = EGenDate.getYear();
        int month = EGenDate.getMonth();
        int day = EGenDate.getDay();
        
        if (month == 1 && day == 29) { // Feb 29 --> March 1
            month = 2;
            day = 1;
        }
        
        // random is based on the number of days from Jan 1, 0001AD
        int daysMin = EGenDate.getDayNo(year - ageBrackets[bracket + 1], month, day) - 1;
        int daysMax = EGenDate.getDayNo(year - ageBrackets[bracket], month, day);
        
        int dobInDays = rnd.intRange(daysMin, daysMax);
        
        return EGenDate.getDateFromDayNo(dobInDays);
    }
    
    private long getAddrID(long cid) {
        return exchNum + compNum + cid;
    }
    
    private String getAreaCode(int num, long cid) {
        long oldSeed = rnd.getSeed();
        
        switch (num) {
            case 1:
                rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_C_AREA_1, cid);
                break;
            case 2:
                rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_C_AREA_2, cid);
                break;
            case 3:
                rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_C_AREA_3, cid);
                break;
            default:
                assert(false);
        }
        
        int key = rnd.intRange(0, areaCodes.getMaxKey());
        
        rnd.setSeed(oldSeed);
        
        return areaCodes.getTupleByKey(key)[0];
    }
    
    private String getLocal() {
        return rnd.rndAlphaNumFormatted("nnnnnnn");
    }
    
    private String getExt(int num) {
        int threshold;
        
        switch (num) {
            case 1:
                threshold = 25;
                break;
            case 2:
                threshold = 15;
                break;
            case 3:
                threshold = 5;
                break;
            default:
                assert(false);
                threshold = 0;
        }
        
        int per = rnd.intRange(1, 100);
        
        if (per <= threshold) {
            return rnd.rndAlphaNumFormatted("nnn"); 
        }
        else {
            return "";
        }
    }
    
    private String[] getEmails(String lname, String fname) {
        int ind1 = rnd.intRange(0, emailDomains.length - 1);
        int ind2 = rnd.intRangeExclude(0, emailDomains.length - 1, ind1);
        
        String[] res = new String[2];
        res[0] = fname.charAt(0) + lname + emailDomains[ind1];
        res[1] = fname.charAt(0) + lname + emailDomains[ind2];
        
        return res;
    }
    
    @Override
    public boolean hasNext() {
        return counter < customersNum;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        long cid = generateCustomerId();
        String[] namesAndtaxId = person.getFirstNameLastNameTaxID(cid);
        
        tuple[0] = cid;                     // c_id
        tuple[1] = namesAndtaxId[2];    // c_tax_id
        tuple[2] = statusType.getTupleByIndex(StatusTypeId.E_ACTIVE.ordinal())[0]; // c_st_id
        tuple[3] = namesAndtaxId[1]; // c_l_name
        tuple[4] = namesAndtaxId[0]; // c_f_name
        tuple[5] = person.getMiddleName(cid); // c_m_name
        tuple[6] = person.getGender(cid); // c_gndr
        tuple[7] = (short)CustomerSelection.getTier(cid).ordinal() + 1; // c_tier
        tuple[8] = new TimestampType(getDOB()); // c_dob
        tuple[9] = getAddrID(cid); // c_ad_id
        
        tuple[10] = usCountryCode; // c_ctry_1
        tuple[11] = getAreaCode(1, cid); // c_area_1
        tuple[12] = getLocal(); // c_local_1
        tuple[13] = getExt(1); // c_ext_1
        
        tuple[14] = usCountryCode; // c_ctry_2
        tuple[15] = getAreaCode(2, cid); // c_area_2
        tuple[16] = getLocal(); // c_local_2
        tuple[17] = getExt(2); // c_ext_2
        
        tuple[18] = usCountryCode; // c_ctry_3
        tuple[19] = getAreaCode(3, cid); // c_area_3
        tuple[20] = getLocal(); // c_local_3
        tuple[21] = getExt(3); // c_ext_3
        
        String[] emails = getEmails(namesAndtaxId[1], namesAndtaxId[0]);
        tuple[22] = emails[0]; // c_email_1
        tuple[23] = emails[1]; // c_email_2
        
        return tuple;
    }
}
