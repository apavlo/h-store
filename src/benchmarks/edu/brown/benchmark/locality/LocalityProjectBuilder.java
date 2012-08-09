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
package edu.brown.benchmark.locality;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.locality.procedures.Get;
import edu.brown.benchmark.locality.procedures.Set;

public class LocalityProjectBuilder extends AbstractProjectBuilder {

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_clientClass = LocalityClient.class;
    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = LocalityLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        Get.class, Set.class, };
    
    // Transaction Frequencies
    {
        addTransactionFrequency(Get.class, LocalityConstants.FREQUENCY_GET_LOCAL);
        addTransactionFrequency(Set.class, LocalityConstants.FREQUENCY_SET_LOCAL);
        // addTransactionFrequency(Get.class,
        // LocalityConstants.FREQUENCY_GET_REMOTE);
        // addTransactionFrequency(Set.class,
        // LocalityConstants.FREQUENCY_SET_REMOTE);
    }

    public static final String PARTITIONING[][] = new String[][] { { LocalityConstants.TABLENAME_TABLEA, "A_ID" }, { LocalityConstants.TABLENAME_TABLEB, "B_A_ID" }, };

    public LocalityProjectBuilder() {
        super("locality", LocalityProjectBuilder.class, PROCEDURES, PARTITIONING);
    }

}
