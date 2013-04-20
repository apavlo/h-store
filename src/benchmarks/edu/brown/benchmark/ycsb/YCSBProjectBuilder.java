/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Coded By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)   *								   
 *                                                                         *
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

package edu.brown.benchmark.ycsb;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.ycsb.procedures.DeleteRecord;
import edu.brown.benchmark.ycsb.procedures.InsertRecord;
import edu.brown.benchmark.ycsb.procedures.ReadRecord;
import edu.brown.benchmark.ycsb.procedures.ScanRecord;
import edu.brown.benchmark.ycsb.procedures.UpdateRecord;

public class YCSBProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = YCSBClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = YCSBLoader.class;

    // a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[]) new Class<?>[] {
        DeleteRecord.class,
        InsertRecord.class,
        ReadRecord.class,
        ScanRecord.class,
        UpdateRecord.class
    };

    {
        addTransactionFrequency(InsertRecord.class, YCSBConstants.FREQUENCY_INSERT_RECORD);
        addTransactionFrequency(DeleteRecord.class, YCSBConstants.FREQUENCY_DELETE_RECORD);
        addTransactionFrequency(ReadRecord.class, YCSBConstants.FREQUENCY_READ_RECORD);
        addTransactionFrequency(ScanRecord.class, YCSBConstants.FREQUENCY_SCAN_RECORD);
        addTransactionFrequency(UpdateRecord.class, YCSBConstants.FREQUENCY_UPDATE_RECORD);
    }

    // a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "USERTABLE", "YCSB_KEY" }
    };

    public YCSBProjectBuilder() {
        super("ycsb", YCSBProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}
