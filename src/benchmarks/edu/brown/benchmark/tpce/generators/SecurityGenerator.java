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
import edu.brown.benchmark.tpce.util.EGenDate;
import edu.brown.benchmark.tpce.util.EGenRandom;

/**
 * @author akalinin
 *
 */
public class SecurityGenerator extends TableGenerator {
    private final double S_NUM_OUTMin = 4000000.0;
    private final double S_NUM_OUTMax = 9500000000.0;

    private final double S_PEMin = 1.0;
    private final double S_PEMax = 120.0;

    private final double S_DIVIDNonZeroMin = 0.01;
    private final double S_DIVIDMax = 10.0;

    private final double S_YIELDNonZeroMin = 0.01;
    private final double S_YIELDMax = 120.0;
    
    private final int percentCompaniesWithNonZeroDividend = 60;
    
    private final int rngSkipOneRowSecurity = 11; // number of RNG calls for one row
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    
    private final long startSecurity;
    private final long numSecurity;
    private long counter;
     
    private final SecurityHandler secHandle;
    private final CompanyGenerator compGenerator;
    
    private final int startDayMin;
    private final int startDayMax;
    private final int currDay;

    /**
     * @param catalog_tbl
     * @param generator
     */
    public SecurityGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        secHandle = new SecurityHandler(generator);
        startSecurity = SecurityHandler.getSecurityStart(generator.getStartCustomer());
        numSecurity = SecurityHandler.getSecurityNum(generator.getCustomersNum());
        counter = startSecurity;
        
        compGenerator = (CompanyGenerator)generator.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);
        
        startDayMin = EGenDate.getDayNo(1900, 0, 1); // 01/01/1900
        startDayMax = EGenDate.getDayNo(2000, 0, 2); // 01/02/2000
        currDay =  EGenDate.getDayNo(TPCEConstants.initialTradePopulationBaseYear, 
                                     TPCEConstants.initialTradePopulationBaseMonth, 
                                     TPCEConstants.initialTradePopulationBaseDay);
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowSecurity);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return counter < startSecurity + numSecurity;
    }
    
    public String createName(long index) {
        long coId = secHandle.getCompanyId(index);
        String[] secRow = secHandle.getSecRecord(index);
        
        return secRow[3] + " of " +
                compGenerator.generateCompanyName(coId - 1 - TPCEConstants.IDENT_SHIFT); // <issue> of <company name>
    }

    public long getNumSecurity(){
    	return numSecurity;
    }
    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        if (counter % TPCEConstants.DEFAULT_SECURITIES_PER_UNIT == 0) {
            initNextLoadUnit();
        }
        
        Object[] tuple = new Object[columnsNum];
        String[] secRow = secHandle.getSecRecord(counter);
        
        String secName = createName(counter);
        
        tuple[0] = secHandle.createSymbol(counter, 15); // s_symb; CHAR(15)
        tuple[1] = secRow[3]; // s_issue
        tuple[2] = secRow[1]; // s_st_id
        tuple[3] = secName; // s_name
        tuple[4] = secRow[4].trim(); // s_ex_id; it seems there are whitespace characters present in the file???
        tuple[5] = secHandle.getCompanyId(counter); // s_co_id
        tuple[6] = (long)rnd.doubleIncrRange(S_NUM_OUTMin, S_NUM_OUTMax, 1.0); // s_num_out
        
        // start date
        int startDay = rnd.intRange(startDayMin, startDayMax);
        tuple[7] = new TimestampType(EGenDate.getDateFromDayNo(startDay)); // s_start_date
        
        // exchange date
        int exDay = rnd.intRange(startDay, startDayMax);
        tuple[8] = new TimestampType(EGenDate.getDateFromDayNo(exDay)); // s_exch_date
        
        tuple[9] = rnd.doubleIncrRange(S_PEMin, S_PEMax, 0.01); // s_pe
        
        // s_52wk_high
        double wkHigh = rnd.doubleIncrRange(TPCEConstants.minSecPrice + ((TPCEConstants.maxSecPrice - TPCEConstants.minSecPrice) / 2),
                TPCEConstants.maxSecPrice, 0.01);
        int wkDay = rnd.intRange(currDay - 7 * 52, currDay); // 7 days per week, 52 weeks per year
        tuple[10] = wkHigh; // s_52wk_high
        tuple[11] = new TimestampType(EGenDate.getDateFromDayNo(wkDay)); // s_52wk_high_date
        
        // s_52wk_low
        double wkLow = rnd.doubleIncrRange(TPCEConstants.minSecPrice, wkHigh, 0.01);
        wkDay = rnd.intRange(currDay - 7 * 52, currDay); // 7 days per week, 52 weeks per year
        tuple[12] = wkLow; // s_52wk_low
        tuple[13] = new TimestampType(EGenDate.getDateFromDayNo(wkDay)); // s_52wk_low_date
        
        double yield, dividend;
        if (rnd.rndPercent(percentCompaniesWithNonZeroDividend)) {
            yield = rnd.doubleIncrRange(S_YIELDNonZeroMin, S_YIELDMax, 0.01);
            dividend = rnd.doubleIncrRange(yield * 0.2, yield * 0.3, 0.01);
            
        }
        else {
            yield = dividend = 0;
        }
        
        tuple[14] = dividend; // s_dividend
        tuple[15] = yield; // s_yield
        
        counter++;
        
        return tuple;
    }
}
