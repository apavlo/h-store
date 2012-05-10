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

public class ExchangeGenerator extends TableGenerator {

    /*
     * It is assumed that there are 4 exchanges in total
     */
    private static final int EXCHANGE_NUM = 4;
    private static final int SECURITY_COUNTS[][] = { { 0, 153, 307, 491, 688, 859, 1028, 1203, 1360, 1532, 1704 },
                                                     { 0, 173, 344, 498, 658, 848, 1006, 1191, 1402, 1572, 1749 },
                                                     { 0, 189, 360, 534, 714, 875, 1023, 1174, 1342, 1507, 1666 },
                                                     { 0, 170, 359, 532, 680, 843, 1053, 1227, 1376, 1554, 1731 } };
    
    private int[] numSecurities = new int[EXCHANGE_NUM];
    private final InputFileHandler exchange_file;
    private int counter = 0;
    private final int table_size;
    
    
    
    public ExchangeGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        exchange_file = generator.getInputFile(InputFile.EXCHANGE);
        table_size = exchange_file.getRecordsNum();
        
        int loadUnits = (int)(generator.getTotalCustomers() / TPCEConstants.DEFAULT_LOAD_UNIT);
        int numTens = loadUnits / 10;
        int numOnes = loadUnits % 10;
        for (int i = 0; i < EXCHANGE_NUM; i++) {
            numSecurities[i] = SECURITY_COUNTS[i][10] * numTens + SECURITY_COUNTS[i][numOnes];
        }
    }
    
    @Override
    public boolean hasNext() {
        return counter < table_size;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        String ex_record[] = exchange_file.getTupleByIndex(counter++);
        int col = 0;

        tuple[col++] = ex_record[0]; // ex_id
        tuple[col++] = ex_record[1]; // ex_name
        tuple[col++] = numSecurities[counter - 1]; // ex_num_symb
        tuple[col++] = Integer.valueOf(ex_record[2]); // ex_open
        tuple[col++] = Integer.valueOf(ex_record[3]); // ex_close
        tuple[col++] = ex_record[4]; // ex_desc
        tuple[col++] = Long.valueOf(ex_record[5]) + TPCEConstants.IDENT_SHIFT; // ex_ad_id
        
        return tuple;
    }
}
