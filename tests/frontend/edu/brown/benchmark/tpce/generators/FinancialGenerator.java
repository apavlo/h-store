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

public class FinancialGenerator extends TableGenerator {
    // Multiplier to get the diluted number of shares from outstanding
    private static final double dilutedSharesMultiplier = 1.1;

    // Multipliers for previous quarter to get the current quarter data
    private static final double finDataDownMult = 0.9;
    private static final double finDataUpMult   = 1.15;

    /*
     * Parameters to generate random values for companies
     */
    private static final double financialRevenueMin = 100000.0;
    private static final double financialRevenueMax = 16000000000.0;

    private static final double financialEarningsMin = -300000000.0;
    private static final double financialEarningsMax = 3000000000.0;

    private static final double financialOutBasicMin = 400000.0;
    private static final double financialOutBasicMax = 9500000000.0;

    private static final double financialInventMin = 0.0;
    private static final double financialInventMax = 2000000000.0;

    private static final double financialAssetsMin = 100000.0;
    private static final double financialAssetsMax = 65000000000.0;

    private static final double financialLiabMin = 100000.0;
    private static final double financialLiabMax = 35000000000.0;
    
    
    private static final int yearsForFins = 5;
    private static final int quartersInYear = 4;
    
    private int finsGeneratedForCompany;
    private static final int finsPerCompany = yearsForFins * quartersInYear; // 5 years of 4 quarters for each company
    
    private long counter;
    private long coId;
    private int finYear;
    private int finQuarter;
    private double finRev, finEarn, finInvent, finAssets, finLiab, finOutBasic;
    
    private final EGenRandom rnd = new EGenRandom(EGenRandom.RNG_SEED_TABLE_DEFAULT);
    private final CompanyGenerator compGenerator;

    /*
     *  Number of RNG calls to skip for one row in order
     *  to not use any of the random values from the previous row.
     */
    private static final int rngSkipOneRowFinancial = 6 + finsPerCompany * 6;
       
    /**
     * @param catalog_tbl
     * @param generator
     */
    public FinancialGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        compGenerator = (CompanyGenerator)generator.getTableGen(TPCEConstants.TABLENAME_COMPANY, null);

        finsGeneratedForCompany = finsPerCompany;
        
        // the counter depends on the starting company number
        counter = generator.getCompanyCount(generator.getStartCustomer()) * finsPerCompany;
    }
    
    private void initNextLoadUnit() {
        rnd.setSeedNth(EGenRandom.RNG_SEED_TABLE_DEFAULT, counter * rngSkipOneRowFinancial);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
        return compGenerator.hasNext() || finsGeneratedForCompany < finsPerCompany;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        if (finsGeneratedForCompany == finsPerCompany) {
            // reset the random generator
            if (counter % (TPCEConstants.DEFAULT_COMPANIES_PER_UNIT * finsPerCompany) == 0) {
                initNextLoadUnit();
            }
            
            coId = compGenerator.generateCompId();
            finsGeneratedForCompany = 0;
            
            finYear = TPCEConstants.dailyMarketBaseYear;
            finQuarter = TPCEConstants.dailyMarketBaseMonth / 3;
            
            /*
             * Generate basic parameters for the new company
             */
            finRev  = rnd.doubleIncrRange(financialRevenueMin, financialRevenueMax, 0.01);
            finEarn = rnd.doubleIncrRange(financialEarningsMin, finRev < financialEarningsMax ? finRev : financialEarningsMax, 0.01);
            finOutBasic = rnd.doubleIncrRange(financialOutBasicMin, financialOutBasicMax, 0.01);
            finInvent = rnd.doubleIncrRange(financialInventMin, financialInventMax, 0.01);
            finAssets = rnd.doubleIncrRange(financialAssetsMin, financialAssetsMax, 0.01);
            finLiab = rnd.doubleIncrRange(financialLiabMin, financialLiabMax, 0.01);
        }
        
        tuple[0] = coId; // fi_co_id
        tuple[1] = finYear; // fi_year
        tuple[2] = finQuarter + 1; // fi_qtr
        tuple[3] = new TimestampType(EGenDate.getDateFromYMD(finYear, finQuarter * 3, 1)); // fi_qtr_start_date
        
        finRev *= rnd.doubleRange(finDataDownMult, finDataUpMult); // finRev is changed incrementally!
        tuple[4] = finRev; // fi_revenue
        
        finEarn *= rnd.doubleRange(finDataDownMult, finDataUpMult);
        if (finEarn > finRev) {
            finEarn = 0.9 * finRev;
        }
        tuple[5] = finEarn; // fi_net_earn
        
        finOutBasic *= rnd.doubleRange(finDataDownMult, finDataUpMult);
        tuple[6] = finEarn / finOutBasic; // fi_basic_eps
        tuple[7] = finEarn / (finOutBasic * dilutedSharesMultiplier); // fi_dilut_eps
        
        tuple[8] = finEarn / finRev; // fi_margin
        
        finInvent *= rnd.doubleRange(finDataDownMult, finDataUpMult);
        tuple[9] = finInvent; // fi_inventory
        
        finAssets *= rnd.doubleRange(finDataDownMult, finDataUpMult);
        tuple[10] = finAssets; // fi_assets
        
        finLiab *= rnd.doubleRange(finDataDownMult, finDataUpMult);
        tuple[11] = finLiab; // fi_liability
        
        tuple[12] = finOutBasic; // fi_out_basic
        tuple[13] = finOutBasic * dilutedSharesMultiplier; // fi_out_dilut
        
        finQuarter++;
        if (finQuarter == 4) {
            finQuarter = 0;
            finYear++;
        }
        
        counter++;
        finsGeneratedForCompany++;
        
        return tuple;
    }

}
