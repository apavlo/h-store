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

import edu.brown.benchmark.tpce.generators.TPCEGenerator.InputFile;

public class CommissionRateGenerator extends TableGenerator {

    private final InputFileHandler comm_file;
    private int counter = 0;
    private final int table_size;
    
    public CommissionRateGenerator(Table catalog_tbl, TPCEGenerator generator) {
        super(catalog_tbl, generator);
        
        comm_file = generator.getInputFile(InputFile.COMMRATE);
        table_size = comm_file.getRecordsNum();
    }
    
    @Override
    public boolean hasNext() {
        return counter < table_size;
    }
    
    @Override
    public Object[] next() {
        Object tuple[] = new Object[columnsNum];
        String comm_record[] = comm_file.getTupleByIndex(counter++);
        int col = 0;

        tuple[col++] = Integer.valueOf(comm_record[0]); // cr_c_tier
        tuple[col++] = comm_record[1]; // cr_tt_id
        tuple[col++] = comm_record[2]; // cr_ex_id
        tuple[col++] = Integer.valueOf(comm_record[3]); // cr_from_qty
        tuple[col++] = Integer.valueOf(comm_record[4]); // cr_to_qty
        tuple[col++] = Double.valueOf(comm_record[5]); // cr_rate

        return tuple;
    }

}
