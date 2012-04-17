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

import java.util.Iterator;
import org.voltdb.catalog.Table;
import org.voltdb.utils.NotImplementedException;

import edu.brown.benchmark.tpce.generators.TPCEGenerator;

public abstract class TableGenerator implements Iterator<Object[]>{
    protected final Table catalog_tbl;
    protected final int columnsNum;
    protected final TPCEGenerator generator;
    
    public TableGenerator(Table catalog_tbl, TPCEGenerator generator) {
        this.catalog_tbl = catalog_tbl;
        
        if (catalog_tbl == null) {  // some generators are called without tables (e.g., for generating cids)
            this.columnsNum = 0;
        }
        else {
            this.columnsNum = catalog_tbl.getColumns().size();
        }
        this.generator = generator;
    }
    
    public void remove() {
        throw new NotImplementedException("Remove not implemented");
    }
}
