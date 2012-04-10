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

public class WeightedFile extends FlatFile {

    private List<Integer> keys = new ArrayList<Integer>();
    
    public WeightedFile(File inputFile) {
        super(inputFile);
    }
    
    @Override
    protected void insertTuple(String[] tuple) {
        // treat first element as weight
        Integer weight = Integer.valueOf(tuple[0]);
        String[] new_tuple = Arrays.copyOfRange(tuple, 1, tuple.length);
        
        // add value tuple
        tuples.add(new_tuple);
        int index = tuples.size() - 1;
        
        // add weight times
        for (int i = 0; i < weight; i++)
            keys.add(index);
    }
    
    @Override
    public String[] getTupleByKey(int key) {
        return getTupleByIndex(keys.get(key));
    }

    @Override
    public int getMaxKey() {
        return keys.size() - 1;
    }
}
