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
import java.util.List;

public class FlatFile extends InputFileHandler {
    
    protected List<String[]> tuples = new ArrayList<String[]>();
    
    public FlatFile(File inputFile) {
        super(inputFile);
    }
    
    @Override
    protected void insertTuple(String[] tuple) {
        tuples.add(tuple);
    }

    @Override
    public String[] getTupleByIndex(int index) {
        return tuples.get(index);
    }
    
    /*
     * Strictly, we do not need this for this type of file, but why not?
     */
    @Override
    public List<String[]> getTuplesByIndex(int index) {
        List<String[]> tupleList = new ArrayList<String[]>();
        
        tupleList.add(tuples.get(index));
        return tupleList;
    }
    
    @Override
    public int getRecordsNum() {
        return tuples.size();
    }
}
