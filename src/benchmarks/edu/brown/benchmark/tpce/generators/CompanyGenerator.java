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
import org.voltdb.types.TimestampType;

import edu.brown.benchmark.tpce.TPCEConstants;
import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenRandom;

public class CompanyGenerator extends TableGenerator {
    /*
     * Number of RNG calls to skip for one row in order
     * to not use any of the random values from the previous row.
     */
    private static final int rngSkipOneRowCompany = 2; // one for SP rate and one for CO_OPEN_DATE
    
    private static final int multCEO = 1000; // for generating a CEO name
    
    private final long startingCompany;
    private long companyAddrId;
    private final long companyCount;
         
    private long counter;
    
    private static final int dayJan1_1800 = EGenDate.getDayNo(1800, 0, 1);
    private static final int dayJan2_2000 = EGenDate.getDayNo(2000, 0, 2);
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    
    private final InputFileHandler companyFile;
    private final InputFileHandler spFile;
    private final int companyRecords;
    private String[] compRecord;
    
    private final PersonHandler person;
    
    
    public CompanyGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        companyCount = generator.getCompanyCount(generator.getCustomersNum());
        startingCompany = generator.getCompanyCount(generator.getStartCustomer());
        companyAddrId = generator.getInputFile(InputFile.EXCHANGE).getRecordsNum() + startingCompany + TPCEConstants.IDENT_SHIFT;
        
        counter = startingCompany;
        
        companyFile = generator.getInputFile(InputFile.COMPANY);
        spFile = generator.getInputFile(InputFile.COMPANYSP);
        companyRecords = companyFile.getRecordsNum();
        
        person = new PersonHandler(generator.getInputFile(InputFile.LNAME), generator.getInputFile(InputFile.FEMFNAME),
                generator.getInputFile(InputFile.MALEFNAME));
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowCompany);
    }
    
    /**
     * Generates the company time by the company Id
     */
    public String generateCompanyName(long coId) {
        return generateCompanyName(getCompanyRecord(coId)[2], coId);
    }
    
    private String[] getCompanyRecord(long index) {
        return companyFile.getTupleByIndex((int)(index % companyRecords));
    }
    
    private String generateCompanyName(String baseName, long index) {
        String res = baseName; // name from the row
        
        long add = (index - 1) / companyRecords; // need the previous counter value here
        
        if (add > 0) {
            res = res + " #" + Long.toString(add);
        }
        
        return res;
    }
    
    private String generateSP(long coId) {
        long oldSeed = rnd.getSeed();
        
        rnd.setSeedNth(EGenRandom.RNG_SEED_BASE_SP_RATE, coId);
        int key = rnd.intRange(0, spFile.getMaxKey());
        rnd.setSeed(oldSeed);
        
        return spFile.getTupleByKey(key)[0];
    }
    
    public long generateCompId() {
        if (counter % TPCEConstants.DEFAULT_COMPANIES_PER_UNIT == 0) {
            initNextLoadUnit();
        }
        
        /*
         * Note that the number of companies to generate may be more that the number in the file.
         * That is why it wraps around every 5000 records (the number of records in the file).
         */
        this.compRecord = getCompanyRecord(counter);
        long coId = Long.valueOf(compRecord[0]) + TPCEConstants.IDENT_SHIFT + counter / companyRecords * companyRecords;
        
        counter++;
        return coId;
    }
    
    public long getCompanyCount(){
    	return companyCount;
    }
    @Override
    public boolean hasNext() {
        return counter < startingCompany + companyCount;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        long coId = generateCompId();
        int openDay = rnd.intRange(dayJan1_1800, dayJan2_2000);
        
        tuple[0] = coId; // co_id
        tuple[1] = compRecord[1]; // co_st_id
        tuple[2] = generateCompanyName(compRecord[2], counter); // co_name
        tuple[3] = compRecord[3]; // co_in
        tuple[4] = generateSP(coId); // co_sp_rate
        tuple[5] = person.getFirstName(multCEO * coId) + " " + person.getLastName(multCEO * coId); // co_ceo
        tuple[6] = ++companyAddrId; // co_ad_id
        tuple[7] = compRecord[4]; // co_desc
        tuple[8] = new TimestampType(EGenDate.getDateFromDayNo(openDay)); // co_open_date
        
        return tuple;
    }
}
