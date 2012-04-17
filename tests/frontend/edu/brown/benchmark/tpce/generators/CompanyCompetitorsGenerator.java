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

public class CompanyCompetitorsGenerator extends TableGenerator {
    
    private final long companyCompsTotal;
    private final long companyCompsStart;
    private long counter;
    
    private final InputFileHandler compCompFile;
    private final int compCompRecords; 

    public CompanyCompetitorsGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        companyCompsTotal = generator.getCompanyCompetitorCount(generator.getCustomersNum());
        companyCompsStart = generator.getCompanyCompetitorCount(generator.getStartCustomer());
        
        counter = companyCompsStart;
        
        compCompFile = generator.getInputFile(InputFile.COMPANYCOMP);
        compCompRecords = compCompFile.getRecordsNum();
    }
    
    @Override
    public boolean hasNext() {
        return counter < companyCompsStart + companyCompsTotal;
    }

    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        
        /*
         * Note that the number of company competitors to generate may be more that the number in the file.
         * That is why it wraps around every 15000 records (the number of records in the file).
         */
        String[] compCompRecord = compCompFile.getTupleByIndex((int)(counter % compCompRecords));
        long coId = Long.valueOf(compCompRecord[0]) + TPCEConstants.IDENT_SHIFT + counter / compCompRecords * compCompRecords;
        long coCompId = Long.valueOf(compCompRecord[1]) + TPCEConstants.IDENT_SHIFT + counter / compCompRecords * compCompRecords;
        
        tuple[0] = coId; // cp_co_id
        tuple[1] = coCompId; // cp_comp_id
        tuple[2] = compCompRecord[2]; // cp_in_id
        
        counter++;
        
        return tuple;
    }
}
