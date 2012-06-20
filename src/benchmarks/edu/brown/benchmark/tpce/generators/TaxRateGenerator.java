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

import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;

public class TaxRateGenerator extends TableGenerator {
    private final InputFileHandler tr_country_file;
    private final InputFileHandler tr_div_file;
    
    private final int countrySize;
    private final int divSize;
    private int countryCnt = 0;
    private int divCnt = 0;
    private int tuplesCnt = 0;
    
    public TaxRateGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        tr_country_file = generator.getInputFile(InputFile.TAXCOUNTRY);
        tr_div_file = generator.getInputFile(InputFile.TAXDIV);
        
        countrySize = tr_country_file.getRecordsNum();
        divSize = tr_div_file.getRecordsNum();
    }
    
    @Override
    public boolean hasNext() {
        return (countryCnt < countrySize || divCnt < divSize);
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        List<String[]> tuples;
        
        if (countryCnt < countrySize) {
            tuples = tr_country_file.getTuplesByIndex(countryCnt);
        }
        else {
            tuples = tr_div_file.getTuplesByIndex(divCnt);
        }
        
        String[] tr_record = tuples.get(tuplesCnt++);
        int col = 0;

        tuple[col++] = tr_record[0]; // tr_id
        tuple[col++] = tr_record[1]; // tr_name
        tuple[col++] = Double.valueOf(tr_record[2]); // tr_rate
        
        if (tuplesCnt == tuples.size()) {
            tuplesCnt = 0;
            if (countryCnt < countrySize) {
                countryCnt++;
            }
            else {
                divCnt++;
            }
        }
        
        return tuple;
    }
}
