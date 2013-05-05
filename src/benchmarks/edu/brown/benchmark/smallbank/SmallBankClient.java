/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original By: VoltDB Inc.											   *
 *  Ported By:  Justin A. DeBrabant (http://www.cs.brown.edu/~debrabant/)  *								   
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

package edu.brown.benchmark.smallbank;

import java.io.IOException;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.StringUtil;

/**
 * SmallBank Client Driver
 * @author pavlo
 */
public class SmallBankClient extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(SmallBankClient.class);
    
    /**
     * Each Transaction element provides an ArgGenerator to create the proper
     * arguments used to invoke the stored procedure
     */
    private static interface ArgGenerator {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         * @param acct0
         * @param acct1
         * @return
         */
        public Object[] genArgs(long acct0, long acct1);
    }
    
    /**
     * Set of transactions structs with their appropriate parameters
     */
    public static enum Transaction {
        AMALGAMATE(SmallBankConstants.FREQUENCY_AMALGAMATE, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return (null);
            }
        }),
        BALANCE(SmallBankConstants.FREQUENCY_BALANCE, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return (null);
            }
        }),
        DEPOSIT_CHECKING(SmallBankConstants.FREQUENCY_DEPOSIT_CHECKING, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return (null);
            }
        }),
        TRANSACT_SAVINGS(SmallBankConstants.FREQUENCY_TRANSACT_SAVINGS, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return (null);
            }
        }),
        WRITE_CHECK(SmallBankConstants.FREQUENCY_WRITE_CHECK, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return (null);
            }
        });
        
        /**
         * Constructor
         */
        private Transaction(int weight, ArgGenerator ag) {
            this.displayName = StringUtil.title(this.name().replace("_", " ").toLowerCase());
            this.callName = this.displayName.replace(" ", "");
            this.weight = weight;
            this.ag = ag;
        }
        
        public Object[] generateParams(Random rand, int numAccounts) {
            long acct0, acct1;
            
            // Outside the hotspot
            if (rand.nextInt(100) < SmallBankConstants.HOTSPOT_PROBABILITY) {
                acct0 = rand.nextInt(numAccounts - SmallBankConstants.HOTSPOT_SIZE) + SmallBankConstants.HOTSPOT_SIZE;
                acct1 = rand.nextInt(numAccounts - SmallBankConstants.HOTSPOT_SIZE) + SmallBankConstants.HOTSPOT_SIZE;
                LOG.debug(String.format("Random number outside hotspot: %s [%d, %d]",
                          this, acct0, acct1));
            }
            // Inside the hotspot
            else { 
                acct0 = rand.nextInt(SmallBankConstants.HOTSPOT_SIZE);
                acct1 = rand.nextInt(SmallBankConstants.HOTSPOT_SIZE);
                LOG.debug(String.format("Random number inside hotspot: %s [%d, %d]",
                          this, acct0, acct1));
            }
            return (this.ag.genArgs(acct0, acct1));
        }

        private final String displayName;
        private final String callName;
        private final int weight;
        private final ArgGenerator ag;
    };
    
    /**
     * Callback Class
     */
    private class SmallBankCallback implements ProcedureCallback {
        private final Transaction txnType;

        public SmallBankCallback(Transaction txnType) {
            this.txnType = txnType;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, this.txnType.ordinal());
            // LOG.info(clientResponse);
        }
    } // END CLASS
    
    private final FlatHistogram<Transaction> txnWeights;
    private final SmallBankCallback callbacks[];
    private final int numAccounts;
    private final Random rand = new Random();
    
    public static void main(String args[]) {
        BenchmarkComponent.main(SmallBankClient.class, args, false);
    }

    public SmallBankClient(String args[]) {
        super(args);
        
        this.numAccounts = (int)Math.round(SmallBankConstants.NUM_ACCOUNTS * this.getScaleFactor());
        
        // Initialize the sampling table
        Histogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) {
                weight = t.weight;
            }
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : "Invalid txn percentage total: " + txns.getSampleCount() + "\n" + txns;
        Random rand = new Random(); // FIXME
        this.txnWeights = new FlatHistogram<Transaction>(rand, txns);
        if (LOG.isDebugEnabled())
            LOG.debug("Transaction Workload Distribution:\n" + txns);

        // Setup callbacks
        int num_txns = Transaction.values().length;
        this.callbacks = new SmallBankCallback[num_txns];
        for (Transaction txnType : Transaction.values()) {
            this.callbacks[txnType.ordinal()] = new SmallBankCallback(txnType);
        } // FOR
    }

    @Override
    protected void runLoop() throws IOException {
        // Not needed.
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        Transaction target = this.txnWeights.nextValue();

        this.startComputeTime(target.displayName);
        Object params[] = target.generateParams(this.rand, this.numAccounts);
        this.stopComputeTime(target.displayName);

        ProcedureCallback callback = this.callbacks[target.ordinal()];
        boolean ret = this.getClientHandle().callProcedure(callback, target.callName, params);
        LOG.debug("Executing txn " + target);
        return (ret);
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        int ii = 0;
        for (Transaction transaction : Transaction.values()) {
            names[ii++] = transaction.displayName;
        }
        return names;
    }

}
