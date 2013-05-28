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
import java.util.Arrays;
import java.util.Random;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.TheHashinator;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
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
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug);
    }
    
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
                return new Object[] {
                    acct0,  // acctId0
                    acct1,  // acctId1
                };
            }
        }),
        BALANCE(SmallBankConstants.FREQUENCY_BALANCE, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return new Object[] {
                    acct0,  // acctId
                };
            }
        }),
        DEPOSIT_CHECKING(SmallBankConstants.FREQUENCY_DEPOSIT_CHECKING, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return new Object[] {
                    acct0,  // acctId0
                    1.3     // amount (from original code)
                };
            }
        }),
        SEND_PAYMENT(SmallBankConstants.FREQUENCY_SEND_PAYMENT, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return new Object[] {
                    acct0,  // sendAcct
                    acct1,  // destAcct
                    5.00    // amount
                };
            }
        }),
        TRANSACT_SAVINGS(SmallBankConstants.FREQUENCY_TRANSACT_SAVINGS, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return new Object[] {
                    acct0,  // acctId
                    20.20   // amount (from original code)
                };
            }
        }),
        WRITE_CHECK(SmallBankConstants.FREQUENCY_WRITE_CHECK, new ArgGenerator() {
            public Object[] genArgs(long acct0, long acct1) {
                return new Object[] {
                    acct0,  // acctId
                    5.0     // amount (from original code)
                };
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
        
        public Object[] generateParams(SmallBankClient client) {
            final CatalogContext catalogContext = client.getCatalogContext();
            long acctIds[] = new long[2];
            int partitions[] = new int[acctIds.length];
            int sites[] = new int[acctIds.length];
            
            boolean is_hotspot = (client.rand.nextInt(100) < client.prob_account_hotspot);
            boolean is_dtxn = (client.rand.nextInt(100) < client.prob_multiaccount_dtxn);
            
            boolean retry = false;
            for (int i = 0; i < acctIds.length; i++) {
                // Outside the hotspot
                if (is_hotspot == false) {
                    acctIds[i] = client.rand.nextInt(client.numAccounts - SmallBankConstants.HOTSPOT_SIZE) + SmallBankConstants.HOTSPOT_SIZE;
                }
                // Inside the hotspot
                else { 
                    acctIds[i] = client.rand.nextInt(SmallBankConstants.HOTSPOT_SIZE);
                }
                
                // They can never be the same!
                if (i > 0 && acctIds[i-1] == acctIds[i]) {
                    continue;
                }
                
                partitions[i] = TheHashinator.hashToPartition(acctIds[i]);
                sites[i] = catalogContext.getSiteIdForPartitionId(partitions[i]);
                
                if (i == 0) continue;
                
                // DTXN
                if (is_dtxn) {
                    // Check whether the accounts need to be on different sites 
                    if (client.force_multisite_dtxns) {
                        retry = (sites[0] == sites[1]);
                    }
                    // Or they need to be on the same site
                    else if (client.force_singlesite_dtxns) {
                        retry = (sites[0] != sites[1] || partitions[0] == partitions[1]);
                    }
                    // Or at least on the same partition
                    else {
                        retry = (partitions[0] == partitions[1]);
                    }
                }
                // SINGLE-PARTITON
                else {
                    retry = (partitions[0] != partitions[1]);
                }
                if (retry) {
                    i -= 1;
                    continue;
                }
            } // FOR
            if (debug.val)
                LOG.debug(String.format("Accounts: %s [hotspot=%s, dtxn=%s]",
                          Arrays.toString(acctIds), is_hotspot, is_dtxn));

            return (this.ag.genArgs(acctIds[0], acctIds[1]));
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
            checkTransaction(txnType.callName, clientResponse, true, false);
        }
    } // END CLASS
    
    private final FlatHistogram<Transaction> txnWeights;
    private final SmallBankCallback callbacks[];
    private final int numAccounts;
    private final Random rand = new Random();
    private double prob_account_hotspot = 0d;
    private double prob_multiaccount_dtxn = 50d;
    private boolean force_multisite_dtxns = false;
    private boolean force_singlesite_dtxns = false;
    
    public static void main(String args[]) {
        BenchmarkComponent.main(SmallBankClient.class, args, false);
    }

    public SmallBankClient(String args[]) {
        super(args);
        CatalogContext catalogContext = this.getCatalogContext();
        TheHashinator.initialize(catalogContext.catalog);
        
        this.numAccounts = (int)Math.round(SmallBankConstants.NUM_ACCOUNTS * this.getScaleFactor());
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);
            
            // Probability that accounts are chosen from the hotspot
            if (key.equalsIgnoreCase("prob_account_hotspot")) {
                this.prob_account_hotspot = Double.parseDouble(value);
            }
            // Probability that multi-accounts will be on different partitions
            else if (key.equalsIgnoreCase("prob_multiaccount_dtxn")) {
                this.prob_multiaccount_dtxn = Double.parseDouble(value);
            }
            // Force all distributed txns to be multi-site
            else if (key.equalsIgnoreCase("force_multisite_dtxns")) {
                this.force_multisite_dtxns = Boolean.parseBoolean(value);
            }
            // Force all distributed txns to be single-sited
            else if (key.equalsIgnoreCase("force_singlesite_dtxns")) {
                this.force_singlesite_dtxns = Boolean.parseBoolean(value);
            }
        } // FOR
        if (catalogContext.numberOfPartitions == 1) {
            this.prob_multiaccount_dtxn = 0;
        }
        if (catalogContext.sites.size() == 1) {
            this.force_multisite_dtxns = false;
        }
        // Disable all multi-partition txns
        if (this.isSinglePartitionOnly()) {
            this.force_multisite_dtxns = false;
            this.prob_multiaccount_dtxn = 0;
        }
        
        // Initialize the sampling table
        Histogram<Transaction> txns = new ObjectHistogram<Transaction>(); 
        for (Transaction t : Transaction.values()) {
            Integer weight = this.getTransactionWeight(t.callName);
            if (weight == null) weight = t.weight;
            txns.put(t, weight);
        } // FOR
        assert(txns.getSampleCount() == 100) : "Invalid txn percentage total: " + txns.getSampleCount() + "\n" + txns;
        this.txnWeights = new FlatHistogram<Transaction>(this.rand, txns);
        if (debug.val)
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
        Object params[] = target.generateParams(this);
        this.stopComputeTime(target.displayName);

        ProcedureCallback callback = this.callbacks[target.ordinal()];
        boolean ret = this.getClientHandle().callProcedure(callback, target.callName, params);
        if (debug.val) LOG.debug("Executing txn " + target);
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
