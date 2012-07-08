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
/**
 * 
 */
package edu.brown.benchmark.seats;

import edu.brown.rand.AbstractRandomGenerator;

/**
 * @author pavlo
 *
 */
public class RandomGenerator extends AbstractRandomGenerator {

    public RandomGenerator(Integer seed) {
        super(seed);
    }
    
    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#loadProfile(java.lang.String)
     */
    @Override
    public void loadProfile(String inputPath) throws Exception {
        // TODO Auto-generated method stub

    }
    
    @Override
    public int numberExcluding(int minimum, int maximum, int excluding, String sourceTable, String targetTable) {
        return (this.numberExcluding(minimum, maximum, excluding));
    }
    
    @Override
    public int numberAffinity(int minimum, int maximum, int base, String baseTable, String targetTable) {
        // TODO Auto-generated method stub
        return 0;
    }


    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberAffinity(int, int, int, java.lang.String)
     */
//    @Override
//    public int numberAffinity(int minimum, int maximum, int base, String table) {
//        // TODO Auto-generated method stub
//        return 0;
//    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberExcluding(int, int, int, java.lang.String)
     */
//    @Override
//    public int numberExcluding(int minimum, int maximum, int excluding,
//            String table) {
//        // TODO Auto-generated method stub
//        return 0;
//    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#numberSkewed(int, int, double)
     */
    @Override
    public int numberSkewed(int minimum, int maximum, double skewFactor) {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.voltdb.benchmark.AbstractRandomGenerator#saveProfile(java.lang.String)
     */
    @Override
    public void saveProfile(String outputPath) throws Exception {
        // TODO Auto-generated method stub

    }

}
