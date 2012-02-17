/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.markov;

public abstract class MarkovConstants {

    // ----------------------------------------------------------------
    // TABLE INFORMATION
    // ----------------------------------------------------------------

    public static final String TABLENAME_TABLEA = "TABLEA";
    public static final long TABLESIZE_TABLEA = 10000l;
    public static final long BATCHSIZE_TABLEA = 50l;

    public static final String TABLENAME_TABLEB = "TABLEB";
    public static final long TABLESIZE_TABLEB = Math.round(MarkovConstants.TABLESIZE_TABLEA * 3.0);
    public static final long BATCHSIZE_TABLEB = 50l;

    public static final String TABLENAME_TABLEC = "TABLEC";
    public static final long TABLESIZE_TABLEC = Math.round(MarkovConstants.TABLESIZE_TABLEA * 0.75);
    public static final long BATCHSIZE_TABLEC = 25l;

    public static final String TABLENAME_TABLED = "TABLED";
    public static final long TABLESIZE_TABLED = 100000l;
    public static final long BATCHSIZE_TABLED = 50l;

    public static final String[] TABLENAMES = { TABLENAME_TABLEA, TABLENAME_TABLEB, TABLENAME_TABLEC, TABLENAME_TABLED, };

    // ----------------------------------------------------------------
    // STORED PROCEDURE INFORMATION
    // ----------------------------------------------------------------

    public static final int FREQUENCY_DONE_AT_PARTITON = 10;
    public static final int FREQUENCY_EXECUTION_TIME = 10;
    public static final int FREQUENCY_SINGLE_PARTITION_WRITE = 20;
    public static final int FREQUENCY_SINGLE_PARTITION_READ = 20;
    public static final int FREQUENCY_MULTI_PARTITION_WRITE = 15;
    public static final int FREQUENCY_MULTI_PARTITION_READ = 15;
    public static final int FREQUENCY_USER_ABORT = 10;

}
