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

import java.io.*;
import java.util.*;

import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.benchmark.*;

import edu.brown.rand.AbstractRandomGenerator;

public class LocalityClient extends ClientMain {

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends VoltProjectBuilder> m_projectBuilderClass = LocalityProjectBuilder.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final Class<? extends ClientMain> m_loaderClass = LocalityLoader.class;

    /** Retrieved via reflection by BenchmarkController */
    public static final String m_jarFileName = "locality.jar";
    
    // --------------------------------------------------------------------
    // DATA MEMBERS
    // --------------------------------------------------------------------

    private int m_scalefactor = 1;
    private final AbstractRandomGenerator m_rng;
    
    /**
     * Number of Records Per Table
     */
    private final Map<String, Long> table_sizes = new HashMap<String, Long>();

    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface LocalityParamGenerator {
        public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> table_sizes);
    }
    
    // --------------------------------------------------------------------
    // BENCHMARK CONTROLLER REQUIREMENTS
    // --------------------------------------------------------------------

    public static enum Transaction {
        GetLocal(LocalityConstants.FREQUENCY_GET_LOCAL,
            new LocalityParamGenerator() {
                @Override
                public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                	Object params[] = new Object[] {
                		rng.nextInt(tableSizes.get(LocalityConstants.TABLENAME_TABLEA).intValue())	
                	};
                    return (params);
                }
        }),
        SetLocal(LocalityConstants.FREQUENCY_SET_LOCAL,
            new LocalityParamGenerator() {
                @Override
                public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                    // TODO
                    return (null);
                }
        }),
        GetRemote(LocalityConstants.FREQUENCY_GET_REMOTE,
            new LocalityParamGenerator() {
                @Override
                public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                    // TODO
                    return (null);
                }
        }),
        SetRemote(LocalityConstants.FREQUENCY_SET_REMOTE,
            new LocalityParamGenerator() {
                @Override
                public Object[] generate(AbstractRandomGenerator rng, Map<String, Long> tableSizes) {
                    // TODO
                    return (null);
                }
        })
        ;
        
        private Transaction(int weight, LocalityParamGenerator generator) {
            this.weight = weight;
            this.generator = generator;
            LocalityClient.TOTAL_WEIGHT += this.weight;
        }
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
            }
        }
        
        public static Transaction get(int idx) {
            assert(idx >= 0);
            Transaction ret = Transaction.idx_lookup.get(idx);
            return (ret);
        }
        
        public Object[] params(AbstractRandomGenerator rng, Map<String, Long> table_sizes) {
            return (this.generator.generate(rng, table_sizes));
        }
        
        public int getWeight() {
            return weight;
        }
        
        private final LocalityParamGenerator generator;
        private final int weight;
        
    };
    private static int TOTAL_WEIGHT;
    
    /**
     * Transaction Execution Weights
     */
    private static final LocalityClient.Transaction XACT_WEIGHTS[] = new LocalityClient.Transaction[100];
    static {
        int i = 0;
        int sum  = 0;
        for (Transaction t : LocalityClient.Transaction.values()) {
            for (int j = 0; j < t.weight; j++, i++) {
                XACT_WEIGHTS[i] = t;
            } // FOR
            sum += t.weight;
        } // FOR
        assert (100 == sum);
    }

    public static void main(String args[]) {
        org.voltdb.benchmark.ClientMain.main(LocalityClient.class, args, false);
    }

    /**
     * Constructor
     * @param args
     */
    public LocalityClient(String[] args) {
        super(args);
        
        // Sanity check
        assert(LocalityClient.TOTAL_WEIGHT == 100);

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
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        m_rng = rng;
        
        // Number of Records Per Table
        this.table_sizes.put(LocalityConstants.TABLENAME_TABLEA, LocalityConstants.TABLESIZE_TABLEA / m_scalefactor);
        this.table_sizes.put(LocalityConstants.TABLENAME_TABLEB, LocalityConstants.TABLESIZE_TABLEB / m_scalefactor);
        for (String tableName : LocalityConstants.TABLENAMES) {
            assert(this.table_sizes.containsKey(tableName)) : "Missing table size entry for " + tableName;
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
            	runOnce();
            	m_voltClient.backpressureBarrier();
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
             * At shutdown an IOException is thrown for every connection to
             * the DB that is lost Ignore the exception here in order to not
             * get spammed, but will miss lost connections at runtime
             */
        }
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        LocalityClient.Transaction txn_type = XACT_WEIGHTS[m_rng.number(0, 99)];
        assert(txn_type != null);
        Object params[] = txn_type.params(m_rng, this.table_sizes);
        boolean ret = m_voltClient.callProcedure(new LocalityCallback(txn_type), txn_type.name(), params);
        return (ret);
    }
    
    /**
     * Basic Callback Class
     */
    protected class LocalityCallback implements ProcedureCallback {
        private final Transaction txn;
        
        public LocalityCallback(Transaction txn) {
            super();
            this.txn = txn;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            LocalityClient.this.m_counts[this.txn.ordinal()].incrementAndGet();
        }
    } // END CLASS
    
    @Override
    public String getApplicationName() {
        return "Locality Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Client";
    }
}
