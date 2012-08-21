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
package edu.brown.benchmark.mapreduce;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.mapreduce.procedures.*;

public class MapReduceProjectBuilder extends AbstractProjectBuilder {

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_clientClass = MapReduceClient.class;
    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = MapReduceLoader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        MockMapReduce.class,
        NormalWordCount.class
    };
    
    // Transaction Frequencies
    {
        addTransactionFrequency(MockMapReduce.class, MapReduceConstants.FREQUENCY_MOCK_MAPREDUCE);
        addTransactionFrequency(NormalWordCount.class, MapReduceConstants.FREQUENCY_NORMAL_WORDCOUNT);
    }
    
    public static final String PARTITIONING[][] = 
        new String[][] {
            {MapReduceConstants.TABLENAME_TABLEA, "A_ID"},
            {MapReduceConstants.TABLENAME_TABLEB, "B_A_ID"},
        };

    public MapReduceProjectBuilder() {
        super("mapreduce", MapReduceProjectBuilder.class, PROCEDURES, PARTITIONING);
        
        this.addStmtProcedure("GetNameCount", "SELECT A_NAME, COUNT(*) FROM TABLEA WHERE A_AGE >= ? GROUP BY A_NAME");
    }
    
}
