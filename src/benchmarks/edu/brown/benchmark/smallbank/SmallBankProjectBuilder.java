/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
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

package edu.brown.benchmark.smallbank;

import org.voltdb.VoltProcedure;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.smallbank.procedures.Amalgamate;
import edu.brown.benchmark.smallbank.procedures.Balance;
import edu.brown.benchmark.smallbank.procedures.DepositChecking;
import edu.brown.benchmark.smallbank.procedures.SendPayment;
import edu.brown.benchmark.smallbank.procedures.TransactSavings;
import edu.brown.benchmark.smallbank.procedures.WriteCheck;
import edu.brown.api.BenchmarkComponent;


/**
 * SmallBank Benchmark
 * 
 * @author Mohammad Alomari (miomari@it.usyd.edu.au)  21-04-2007
 * @author Michael Cahill (mjc@it.usyd.edu.au)  17-07-2007 (cleanup)
 * @author Andy Pavlo - Ported to H-Store
 */
public class SmallBankProjectBuilder extends AbstractProjectBuilder {

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_clientClass = SmallBankClient.class;

    // REQUIRED: Retrieved via reflection by BenchmarkController
    public static final Class<? extends BenchmarkComponent> m_loaderClass = SmallBankLoader.class;

	// a list of procedures implemented in this benchmark
    @SuppressWarnings("unchecked")
    public static final Class<? extends VoltProcedure> PROCEDURES[] = (Class<? extends VoltProcedure>[])new Class<?>[] {
        Amalgamate.class,
        Balance.class,
        DepositChecking.class,
        SendPayment.class,
        TransactSavings.class,
        WriteCheck.class
    };
	
	{
		addTransactionFrequency(Balance.class, 100);
	}
	
	// a list of tables used in this benchmark with corresponding partitioning keys
    public static final String PARTITIONING[][] = new String[][] {
        { SmallBankConstants.TABLENAME_ACCOUNTS,  "custid" },
        { SmallBankConstants.TABLENAME_SAVINGS,   "custid" },
        { SmallBankConstants.TABLENAME_CHECKING,  "custid" },
    };

    public SmallBankProjectBuilder() {
        super("smallbank", SmallBankProjectBuilder.class, PROCEDURES, PARTITIONING);
    }
}




