/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tm1;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.GetAccessData;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.benchmark.tm1.procedures.GetSubscriberData;
import edu.brown.benchmark.tm1.procedures.GetTableCounts;
import edu.brown.benchmark.tm1.procedures.InsertCallForwarding;
import edu.brown.benchmark.tm1.procedures.UpdateLocation;
import edu.brown.benchmark.tm1.procedures.UpdateSubscriberData;

public class TM1ProjectBuilder extends AbstractProjectBuilder {

    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_clientClass = TM1Client.class;
    /**
     * Retrieved via reflection by BenchmarkController
     */
    public static final Class<? extends BenchmarkComponent> m_loaderClass = TM1Loader.class;

    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
            // Benchmark Specification
            DeleteCallForwarding.class,
            GetAccessData.class,
            GetNewDestination.class,
            GetSubscriberData.class,
            InsertCallForwarding.class,
            UpdateLocation.class,
            UpdateSubscriberData.class,

            // Testing Procedures
            // InsertSubscriber.class,
            GetTableCounts.class,
    };
    
    {
        // Transaction Frequencies
        addTransactionFrequency(DeleteCallForwarding.class, TM1Constants.FREQUENCY_DELETE_CALL_FORWARDING);
        addTransactionFrequency(GetAccessData.class, TM1Constants.FREQUENCY_GET_ACCESS_DATA);
        addTransactionFrequency(GetNewDestination.class, TM1Constants.FREQUENCY_GET_NEW_DESTINATION);
        addTransactionFrequency(GetSubscriberData.class, TM1Constants.FREQUENCY_GET_SUBSCRIBER_DATA);
        addTransactionFrequency(InsertCallForwarding.class, TM1Constants.FREQUENCY_INSERT_CALL_FORWARDING);
        addTransactionFrequency(UpdateLocation.class, TM1Constants.FREQUENCY_UPDATE_LOCATION);
        addTransactionFrequency(UpdateSubscriberData.class, TM1Constants.FREQUENCY_UPDATE_SUBSCRIBER_DATA);

        // Replicates Secondary Indexes
        addReplicatedSecondaryIndex(TM1Constants.TABLENAME_SUBSCRIBER, "S_ID", "SUB_NBR");
    }

    public static final String PARTITIONING[][] = new String[][] {
        { "SUBSCRIBER", "S_ID" },
        { "ACCESS_INFO", "S_ID" },
        { "SPECIAL_FACILITY", "S_ID" },
        { "CALL_FORWARDING", "S_ID" },
    };

    public TM1ProjectBuilder() {
        super("tm1", TM1ProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}