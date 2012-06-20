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

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.rand.AbstractRandomGenerator;

public class MarkovClient extends BenchmarkComponent {

    // --------------------------------------------------------------------
    // DATA MEMBERS
    // --------------------------------------------------------------------

    private int m_scalefactor = 1;
    private final AbstractRandomGenerator m_rng;

    /**
     * If the lock is not null, then we will only submit txns one at a time
     */
    private final Object blockingLock = new Object();

    /**
     * Number of Records Per Table
     */
    private final Map<String, Long> table_sizes = new HashMap<String, Long>();

    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface MarkovParamGenerator {
        public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> table_sizes);
    }

    // --------------------------------------------------------------------
    // BENCHMARK CONTROLLER REQUIREMENTS
    // --------------------------------------------------------------------

    public static enum Transaction {
        DoneAtPartition(MarkovConstants.FREQUENCY_DONE_AT_PARTITON, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] { rng.number(0, tableSizes.get(MarkovConstants.TABLENAME_TABLEA).intValue()), // A_ID
                        rng.number(0, 1 << 30), // VALUE
                };
                return (params);
            }
        }), ExecutionTime(MarkovConstants.FREQUENCY_EXECUTION_TIME, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] { rng.number(0, tableSizes.get(MarkovConstants.TABLENAME_TABLEA).intValue()), // A_ID
                };
                return (params);
            }
        }), SinglePartitionWrite(MarkovConstants.FREQUENCY_SINGLE_PARTITION_WRITE, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] {
                // TODO
                };
                return (params);
            }
        }), SinglePartitionRead(MarkovConstants.FREQUENCY_SINGLE_PARTITION_READ, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] {
                // TODO
                };
                return (params);
            }
        }), MultiPartitionWrite(MarkovConstants.FREQUENCY_MULTI_PARTITION_WRITE, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] {
                // TODO
                };
                return (params);
            }
        }), MultiPartitionRead(MarkovConstants.FREQUENCY_MULTI_PARTITION_READ, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] {
                // TODO
                };
                return (params);
            }
        }), UserAbort(MarkovConstants.FREQUENCY_USER_ABORT, new MarkovParamGenerator() {
            @Override
            public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                Object params[] = new Object[] {
                // TODO
                };
                return (params);
            }
        });

        private Transaction(int weight, MarkovParamGenerator generator) {
            this.weight = weight;
            this.generator = generator;
            MarkovClient.TOTAL_WEIGHT += this.weight;
        }

        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
            }
        }

        public static Transaction get(int idx) {
            assert (idx >= 0);
            Transaction ret = Transaction.idx_lookup.get(idx);
            return (ret);
        }

        public Object[] params(AbstractRandomGenerator rng, Map<String, Long> table_sizes) {
            return (this.generator.generate(rng, table_sizes));
        }

        public int getWeight() {
            return weight;
        }

        private final MarkovParamGenerator generator;
        private final int weight;

    };

    private static int TOTAL_WEIGHT;

    /**
     * Transaction Execution Weights
     */
    private static final MarkovClient.Transaction XACT_WEIGHTS[] = new MarkovClient.Transaction[100];
    static {
        int i = 0;
        int sum = 0;
        for (Transaction t : MarkovClient.Transaction.values()) {
            for (int j = 0; j < t.weight; j++, i++) {
                XACT_WEIGHTS[i] = t;
            } // FOR
            sum += t.weight;
        } // FOR
        assert (100 == sum);
    }

    public static void main(String args[]) {
        edu.brown.api.BenchmarkComponent.main(MarkovClient.class, args, false);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public MarkovClient(String[] args) {
        super(args);

        // Sanity check
        assert (MarkovClient.TOTAL_WEIGHT == 100);

        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;

        for (String arg : args) {
            String[] parts = arg.split("=", 2);
            if (parts.length == 1)
                continue;

            if (parts[1].startsWith("${"))
                continue;

            if (parts[0].equals("scalefactor")) {
                m_scalefactor = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("randomseed")) {
                seed = Integer.parseInt(parts[1]);
            } else if (parts[0].equals("randomgenerator")) {
                randGenClassName = parts[1];
            } else if (parts[0].equals("randomprofile")) {
                randGenProfilePath = parts[1];
            }
        } // FOR

        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null)
                rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        m_rng = rng;

        // Number of Records Per Table
        this.table_sizes.put(MarkovConstants.TABLENAME_TABLEA, MarkovConstants.TABLESIZE_TABLEA / m_scalefactor);
        this.table_sizes.put(MarkovConstants.TABLENAME_TABLEB, MarkovConstants.TABLESIZE_TABLEB / m_scalefactor);
        this.table_sizes.put(MarkovConstants.TABLENAME_TABLEC, MarkovConstants.TABLESIZE_TABLEC / m_scalefactor);
        this.table_sizes.put(MarkovConstants.TABLENAME_TABLED, MarkovConstants.TABLESIZE_TABLED / m_scalefactor);
        for (String tableName : MarkovConstants.TABLENAMES) {
            assert (this.table_sizes.containsKey(tableName)) : "Missing table size entry for " + tableName;
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        for (int i = 0; i < names.length; i++) {
            names[i] = Transaction.values()[i].name();
        }
        return names;
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                MarkovClient.Transaction txn_type = XACT_WEIGHTS[m_rng.number(0, 99)];
                assert (txn_type != null);
                Object params[] = txn_type.params(m_rng, this.table_sizes);
                this.getClientHandle().callProcedure(new MarkovCallback(txn_type), txn_type.name(), params);
                this.getClientHandle().backpressureBarrier();
            } // WHILE
        } catch (NoConnectionsException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            /*
             * At shutdown an IOException is thrown for every connection to the
             * DB that is lost Ignore the exception here in order to not get
             * spammed, but will miss lost connections at runtime
             */
        }
    }

    /**
     * Basic Callback Class
     */
    protected class MarkovCallback implements ProcedureCallback {
        private final Transaction txn;

        public MarkovCallback(Transaction txn) {
            super();
            this.txn = txn;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) {
            incrementTransactionCounter(clientResponse, this.txn.ordinal());
            if (MarkovClient.this.blockingLock != null) {
                synchronized (MarkovClient.this.blockingLock) {
                    MarkovClient.this.blockingLock.notifyAll();
                }
            }
        }
    } // END CLASS
}
