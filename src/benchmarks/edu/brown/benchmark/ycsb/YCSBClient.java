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

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.ycsb.distributions.CounterGenerator;
import edu.brown.benchmark.ycsb.distributions.ZipfianGenerator;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;


public class YCSBClient extends BenchmarkComponent {
	
	private ZipfianGenerator readRecord;
    private static CounterGenerator insertRecord;
    private ZipfianGenerator randScan;
	
	private List<String> value_list; 
	
	private final FlatHistogram<Transaction> txnWeights;
	
    public static void main(String args[]) {
        BenchmarkComponent.main(YCSBClient.class, args, false);
    }

    public YCSBClient(String args[]) {
        super(args);
		
		int init_record_count = 0;  // XXX: fix this 
		
		// initialize distribution generators 
		readRecord = new ZipfianGenerator(init_record_count);// pool for read keys
        randScan = new ZipfianGenerator(YCSBConstants.MAX_SCAN);
		
		value_list = new LinkedList<String>(); 
		
		synchronized (YCSBClient.class) {
            // We must know where to start inserting
            if (insertRecord == null) {
                insertRecord = new CounterGenerator(init_record_count);
            }
		}  // end SYNC
		
		// Initialize the sampling table
        Histogram<Transaction> txns = new Histogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : txns;
        Random rand = new Random(); // FIXME
        this.txnWeights = new FlatHistogram<Transaction>(rand, txns);
    }
	
	public static enum Transaction {
		INSERT_RECORD("Insert Record", YCSBConstants.FREQUENCY_INSERT_RECORD),
		
		DELETE_RECORD("Delete Record", YCSBConstants.FREQUENCY_DELETE_RECORD), 
		
		//READ_MODIFY_WRITE_RECORD("read Modify Write Record", YCSBConstants.FREQUENCY_READ_MODIFY_WRITE_RECORD), 
		
		READ_RECORD("Read Record", YCSBConstants.FREQUENCY_READ_RECORD), 
		
		SCAN_RECORD("Scan Record", YCSBConstants.FREQUENCY_SCAN_RECORD), 
		
		UPDATE_RECORD("Update Record", YCSBConstants.FREQUENCY_UPDATE_RECORD);
		
        /**
         * Constructor
         */
        private Transaction(String displayName, int weight) {
            this.displayName = displayName;
            this.callName = displayName.replace(" ", "");
            this.weight = weight;
        }
		
        public final String displayName;
        public final String callName;
        public final int weight; // probability (in terms of percentage) the transaction gets executed
	
	} // TRANSCTION ENUM
	

    @Override
    public void runLoop() {
        try {
            Client client = this.getClientHandle();
            Random rand = new Random();
			int key = -1; 
			int scan_count; 
			
            while (true) {
				
				runOnce(); 
            } 
        } 
		catch (IOException e) {
            
        }
    }
	
	@Override
    protected boolean runOnce() throws IOException {
		// pick random transaction to call, weighted by txnWeights
		final Transaction target = this.txnWeights.nextValue(); 
		int procIdx = target.ordinal(); 
		String procName = target.callName; 
		
		int key = 0; 
		int scan_count = 0; 
		
		if (procName.equals("DeleteRecord")) {
			
			key = readRecord.nextInt(); 
		} 
		else if (procName.equals("InsertRecord")) {
			
			key = insertRecord.nextInt(); 
			List<String> values = buildValues(10); 
		} 
		else if (procName.equals("ReadModifyWriteRecord")) {
			
			key = readRecord.nextInt(); 
			List<String> values = buildValues(10); 
		} 
		else if (procName.equals("ReadRecord")) {
			
			key = readRecord.nextInt(); 
		} 
		else if (procName.equals("ScanRecord")) {
			
			key = readRecord.nextInt(); 
			scan_count = randScan.nextInt(); 
		} 
		else if (procName.equals("UpdateRecord")) {
			
			key = readRecord.nextInt(); 
		}
		else {
			key = readRecord.nextInt();
		}
		
		Object procParams[] = new Object[]{ key };
		Callback callback = new Callback(procIdx);
		boolean response = this.getClientHandle().callProcedure(callback, procName, procParams);
				
		return response; 
	}
	
	private List<String> buildValues(int numVals) {
        this.value_list.clear();
        for (int i = 0; i < numVals; i++) {
            this.value_list.add(YCSBUtil.astring(1,100));
        }
        return this.value_list;
    }

    private class Callback implements ProcedureCallback {
        private final int idx;

        public Callback(int idx) {
            this.idx = idx;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            // Increment the BenchmarkComponent's internal counter on the
            // number of transactions that have been completed
            incrementTransactionCounter(clientResponse, this.idx);
        }
    } // END CLASS

    @Override
    public String[] getTransactionDisplayNames() {
        // Return an array of transaction names
        String procNames[] = new String[YCSBProjectBuilder.PROCEDURES.length];
        for (int i = 0; i < procNames.length; i++) {
            procNames[i] = YCSBProjectBuilder.PROCEDURES[i].getSimpleName();
        }
        return (procNames);
    }
}
