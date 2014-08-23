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

package edu.brown.benchmark.sstore4demuxopexp;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.api.BenchmarkComponent;

import edu.brown.benchmark.sstore4demuxopexp.procedures.*;  

public class SStore4DemuxOpExpProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SStore4DemuxOpExpClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SStore4DemuxOpExpLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
    	Throughput.class
        ,SP1Input.class
        ,Demux.class
        ,Move1.class
        ,Move2.class
        ,Move3.class
        ,Move4.class
        ,Move5.class
        ,Move6.class
        ,Move7.class
        ,Move8.class
        ,Move9.class
        ,Move10.class
        ,Move11.class
        ,Move12.class
        ,Move13.class
        ,Move14.class
        ,Move15.class
        ,Move16.class
        ,Move17.class
        ,Move18.class
        ,Move19.class
        ,Move20.class
        ,Move21.class
        ,Move22.class
        ,Move23.class
        ,Move24.class
        ,Move25.class
        ,Move26.class
        ,Move27.class
        ,Move28.class
        ,Move29.class
        ,Move30.class
        ,Move31.class
        ,SP2_1.class
        ,SP2_2.class
        ,SP2_3.class
        ,SP2_4.class
        ,SP2_5.class
        ,SP2_6.class
        ,SP2_7.class
        ,SP2_8.class
        ,SP2_9.class
        ,SP2_10.class
        ,SP2_11.class
        ,SP2_12.class
        ,SP2_13.class
        ,SP2_14.class
        ,SP2_15.class
        ,SP2_16.class
        ,SP2_17.class
        ,SP2_18.class
        ,SP2_19.class
        ,SP2_20.class
        ,SP2_21.class
        ,SP2_22.class
        ,SP2_23.class
        ,SP2_24.class
        ,SP2_25.class
        ,SP2_26.class
        ,SP2_27.class
        ,SP2_28.class
        ,SP2_29.class
        ,SP2_30.class
        ,SP2_31.class
        };
	
	{
		//addTransactionFrequency(Vote.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { "votes", "phone_number" },
        { "contestants", "contestant_number" },
        { "area_code_state", "area_code"},
        { "s1", "part_id" },
        { "s101", "part_id" },
        { "s102", "part_id" },
        { "s103", "part_id" },
        { "s104", "part_id" },
        { "s105", "part_id" },
        { "s106", "part_id" },
        { "s107", "part_id" },
        { "s108", "part_id" },
        { "s109", "part_id" },
        { "s110", "part_id" },
        { "s111", "part_id" },
        { "s112", "part_id" },
        { "s113", "part_id" },
        { "s114", "part_id" },
        { "s115", "part_id" },
        { "s116", "part_id" },
        { "s117", "part_id" },
        { "s118", "part_id" },
        { "s119", "part_id" },
        { "s120", "part_id" },
        { "s121", "part_id" },
        { "s122", "part_id" },
        { "s123", "part_id" },
        { "s124", "part_id" },
        { "s125", "part_id" },
        { "s126", "part_id" },
        { "s127", "part_id" },
        { "s128", "part_id" },
        { "s129", "part_id" },
        { "s130", "part_id" },
        { "s131", "part_id" },
        { "s201", "part_id" },
        { "s202", "part_id" },
        { "s203", "part_id" },
        { "s204", "part_id" },
        { "s205", "part_id" },
        { "s206", "part_id" },
        { "s207", "part_id" },
        { "s208", "part_id" },
        { "s209", "part_id" },
        { "s210", "part_id" },
        { "s211", "part_id" },
        { "s212", "part_id" },
        { "s213", "part_id" },
        { "s214", "part_id" },
        { "s215", "part_id" },
        { "s216", "part_id" },
        { "s217", "part_id" },
        { "s218", "part_id" },
        { "s219", "part_id" },
        { "s220", "part_id" },
        { "s221", "part_id" },
        { "s222", "part_id" },
        { "s223", "part_id" },
        { "s224", "part_id" },
        { "s225", "part_id" },
        { "s226", "part_id" },
        { "s227", "part_id" },
        { "s228", "part_id" },
        { "s229", "part_id" },
        { "s230", "part_id" },
        { "s231", "part_id" },
        { "s201prime", "part_id" },
        { "s202prime", "part_id" },
        { "s203prime", "part_id" },
        { "s204prime", "part_id" },
        { "s205prime", "part_id" },
        { "s206prime", "part_id" },
        { "s207prime", "part_id" },
        { "s208prime", "part_id" },
        { "s209prime", "part_id" },
        { "s210prime", "part_id" },
        { "s211prime", "part_id" },
        { "s212prime", "part_id" },
        { "s213prime", "part_id" },
        { "s214prime", "part_id" },
        { "s215prime", "part_id" },
        { "s216prime", "part_id" },
        { "s217prime", "part_id" },
        { "s218prime", "part_id" },
        { "s219prime", "part_id" },
        { "s220prime", "part_id" },
        { "s221prime", "part_id" },
        { "s222prime", "part_id" },
        { "s223prime", "part_id" },
        { "s224prime", "part_id" },
        { "s225prime", "part_id" },
        { "s226prime", "part_id" },
        { "s227prime", "part_id" },
        { "s228prime", "part_id" },
        { "s229prime", "part_id" },
        { "s230prime", "part_id" },
        { "s231prime", "part_id" },
        { "s2", "part_id" }
    };

    public SStore4DemuxOpExpProjectBuilder() {
        super("sstore4demuxopexp", SStore4DemuxOpExpProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}




