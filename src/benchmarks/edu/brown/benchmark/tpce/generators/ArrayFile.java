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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ArrayFile extends InputFileHandler {
    
    protected List<List<String[]>> tuples_list = new ArrayList<List<String[]>>();
    
    public ArrayFile(File inputFile) {
        super(inputFile);
    }
    
    @Override
    protected void insertTuple(String[] tuple) {
        // treat first element as index
        Integer index = Integer.valueOf(tuple[0]) - 1; // indexes in the file are 1-based
        String[] new_tuple = Arrays.copyOfRange(tuple, 1, tuple.length);
        
        if (index >= tuples_list.size()) {
            List<String[]> tuples = new ArrayList<String[]>();
            tuples_list.add(tuples);
        }
        
        tuples_list.get(tuples_list.size() - 1).add(new_tuple);
    }

    /*
     * Strictly, we do not need this for this type of file, but why not?
     */
    @Override
    public String[] getTupleByIndex(int index) {
        int total_ind = tuples_list.get(0).size() - 1, list_ind = 0;
        
        while (total_ind < index) {
            list_ind++;
            total_ind += tuples_list.get(list_ind).size();
        }
        
        int ind = index - (total_ind - tuples_list.get(list_ind).size() + 1);
        
        return tuples_list.get(list_ind).get(ind);
    }
    
    @Override
    public List<String[]> getTuplesByIndex(int index) {
        return tuples_list.get(index);
    }
    
    @Override
    public int getRecordsNum() {
        return tuples_list.size();
    }
}
