/***************************************************************************
 *  Copyright (C) 2011 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package edu.brown.benchmark.seats;

import java.io.File;
import java.util.Random;

import org.voltdb.catalog.Catalog;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.seats.util.SEATSHistogramUtil;
import edu.brown.utils.ProjectType;

public abstract class SEATSBaseTestCase extends BaseTestCase {

    // HACK
    protected static File AIRLINE_DATA_DIR; //  = System.getenv(SEATSConstants.AIRLINE_DATA_PARAM.toUpperCase());
//    protected static SEATSLoader loader;
    
    protected Random rand = new Random();
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.SEATS);
        if (isFirstSetup()) {
            AIRLINE_DATA_DIR = SEATSHistogramUtil.findDataDir();
            
//            final Catalog loader_catalog = catalog;
//            loader = new SEATSLoader(
//                new String[]{ "datadir" + "=" + AIRLINE_DATA_DIR }) {
//                public Catalog getCatalog() {
//                    return (loader_catalog);
//                };
//            }
        }
    }
    
}
