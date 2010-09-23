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

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.log4j.Logger;
import org.voltdb.*;
import org.voltdb.benchmark.*;

import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.RandomDistribution;
import edu.brown.rand.WrappingRandomDistribution;
import edu.brown.statistics.Histogram;
import edu.brown.utils.StringUtil;

public class MarkovLoader extends ClientMain {
    private static final Logger LOG = Logger.getLogger(MarkovLoader.class.getSimpleName());
    
    // Composite Id
    private static final long COMPOSITE_ID_MASK = 4294967295l; // (2^32)-1
    private static final int COMPOSITE_ID_OFFSET = 32;
    
    // scale all table cardinalities by this factor
    private int m_scalefactor = 1;
    
    // used internally
    private final AbstractRandomGenerator m_rng;
    
    // When set to true, all operations will run single-threaded
    private boolean debug = true;
    
    // Histograms
    // TableName -> Histogram for A_ID
    private final Map<String, Histogram> histograms = new HashMap<String, Histogram>();
    
    // Data Generator Classes
    // TableName -> AbstactTableGenerator
    private final Map<String, AbstractTableGenerator> generators = new HashMap<String, AbstractTableGenerator>();
    
    // Table Sizes
    // TableName -> Tuple Count
    private final Map<String, AtomicLong> table_sizes = new HashMap<String, AtomicLong>();
    
    public static void main(String args[]) throws Exception {
        org.voltdb.benchmark.ClientMain.main(MarkovLoader.class, args, true);
    }
    
    /**
     * Constructor
     * @param args
     */
    public MarkovLoader(String[] args) {
        super(args);
        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;
        
        for (String arg : args) {
            String[] parts = arg.split("=",2);
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
            } else if (parts[0].equals("debug")) {
                this.debug = Boolean.getBoolean(parts[1]);
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
        
        // Histograms + Table Sizes + Generators
        for (String tableName : MarkovConstants.TABLENAMES) {
            this.histograms.put(tableName, new Histogram());
            this.table_sizes.put(tableName, new AtomicLong(0l));
            
            if (tableName.equals(MarkovConstants.TABLENAME_TABLEA)) {
                this.generators.put(tableName, new TABLEAGenerator());
            } else if (tableName.equals(MarkovConstants.TABLENAME_TABLEB)) {
                this.generators.put(tableName, new TABLEBGenerator());
            } else if (tableName.equals(MarkovConstants.TABLENAME_TABLEC)) {
                this.generators.put(tableName, new TABLECGenerator());
            } else if (tableName.equals(MarkovConstants.TABLENAME_TABLED)) {
                this.generators.put(tableName, new TABLEDGenerator());
            }
        } // FOR
    }
    
    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    /**
     * Main execution loop for invoking all the data generator threads
     */
    @Override
    public void runLoop() {
        List<Thread> load_threads = new ArrayList<Thread>();
        for (final String tableName : MarkovConstants.TABLENAMES) {
            load_threads.add(new Thread() { 
                @Override
                public void run() {
                    generateTableData(tableName);
                }
            });
        } // FOR
        
        try {
            for (Thread thread : load_threads) {
                thread.start();
                if (this.debug) thread.join();
            }
            if (!this.debug) {
                for (Thread thread : load_threads) thread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        System.err.println("Finished generating data for all tables");
    }
    
    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        System.out.println("Starting data generator for '" + tableName + "'");
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert(generator != null);
        long tableSize = generator.getTableSize();
        long batchSize = generator.getBatchSize();
        VoltTable table = generator.getVoltTable();
        
        System.out.println("Loading " + tableSize + " tuples for table '" + tableName + "'");
        while (generator.hasMore()) {
            generator.addRow();
            if (table.getRowCount() >= batchSize) {
                System.err.println(String.format(tableName + ": loading %d rows (id %d of %d)", table.getRowCount(), generator.getCount(), tableSize));
                loadTable(tableName, table);
                table.clearRowData();
            }
            this.table_sizes.get(tableName).incrementAndGet();
        } // WHILE
        if (table.getRowCount() > 0) {
            System.err.println(tableName + ": loading final " + table.getRowCount() + " rows.");
            loadTable(tableName, table);
            table.clearRowData();
        }
        System.out.println(tableName + ": Inserted " + this.table_sizes.get(tableName) + " tuples");
    }
    
    /**
     * 
     * @param debug
     */
    protected void setDebug(boolean debug) {
        this.debug = debug;
    }
    
    protected static Long encodeCompositeId(long a_id, long id) {
        return (a_id | id<<COMPOSITE_ID_OFFSET);
    }
    
    /**
     * Returns the pieces of a composite id
     * The first element of the returned array will be the A_ID portion of the 
     * composite and the second element will be the ID portion 
     * @param composite_id
     * @return
     */
    protected static long[] decodeCompositeId(long composite_id) {
        long values[] = { composite_id & COMPOSITE_ID_MASK,
                          composite_id>>COMPOSITE_ID_OFFSET };
        return (values);
    }

    // ----------------------------------------------------------------
    // DATA GENERATION
    // ----------------------------------------------------------------
    
    protected abstract class AbstractTableGenerator {
        protected final String tableName;
        protected final VoltTable table;
        protected final Histogram hist;
        protected Long tableSize;
        protected Long batchSize;
        
        protected final Object[] row;
        protected long count = 0;
        
        public AbstractTableGenerator(String tableName, VoltTable table) {
            this.tableName = tableName;
            this.table = table;
            this.hist = MarkovLoader.this.histograms.get(this.tableName);
            assert(hist != null);
            
            this.row = new Object[this.table.getColumnCount()];
            
            // Initialize dynamic parameters
            try {
                String field_name = "TABLESIZE_" + tableName;
                Field field_handle = MarkovConstants.class.getField(field_name);
                assert(field_handle != null);
                this.tableSize = (Long)field_handle.get(null) / MarkovLoader.this.m_scalefactor;

                field_name = "BATCHSIZE_" + tableName;
                field_handle = MarkovConstants.class.getField(field_name);
                assert(field_handle != null);
                this.batchSize = (Long)field_handle.get(null);
            } catch (Exception ex) {
                LOG.error(ex);
                System.exit(1);
            }
            System.out.println("Preparing to load " + this.tableSize + " tuples for '" + this.tableName + "' [batchSize=" + this.batchSize + "]");
        }
        
        public boolean hasMore() {
            return (this.count < this.tableSize);
        }
        
        public VoltTable getVoltTable() {
            return this.table;
        }
        
        public Long getTableSize() {
            return this.tableSize;
        }
        
        public Long getBatchSize() {
            return this.batchSize;
        }
        
        public String getTableName() {
            return this.tableName;
        }
        
        public long getCount() {
            return this.count;
        }
        
        public void addRow() {
            this.populateRow();
            this.count++;
            this.table.addRow(this.row);
        }
        
        protected abstract void populateRow();
    } // END CLASS
    
    /**
     * TABLEA Generator
     */
    protected class TABLEAGenerator extends AbstractTableGenerator {
        
        public TABLEAGenerator() {
            super(MarkovConstants.TABLENAME_TABLEA, MarkovTables.initializeTableA());
        }
        
        @Override
        protected void populateRow() {
            int col = 0;
            
            // A_ID
            row[col++] = new Integer((int)this.count);
            
            // A_SATTR##
            for (int j=0; j < 20; ++j) {
                row[col++] = m_rng.astring(6, 32);
            }
            // A_IATTR##
            for (int j=0; j < 20; ++j) {
                row[col++] = m_rng.number(0, 1<<30);
            }
            assert (col == this.table.getColumnCount());
            //System.err.println(this.tableName + "[" + this.count + "]: " + Arrays.toString(row));
        }
    } // END CLASS

    /**
     * TABLEB Generator
     */
    protected class TABLEBGenerator extends AbstractTableGenerator {
        private RandomDistribution.DiscreteRNG rands[];
        
        public TABLEBGenerator() {
            super(MarkovConstants.TABLENAME_TABLEB, MarkovTables.initializeTableB());
            
            // TABLEB has a Zipfian distribution on B_A_ID
            // We alternate between a curve starting at zero and a curve starting at the middle of A_ID
            // It's kind of lame but it's something for now
            long num_a_records = MarkovConstants.TABLESIZE_TABLEA / m_scalefactor;
            RandomDistribution.Zipf zipf = new RandomDistribution.Zipf(m_rng, 0, (int)num_a_records, 1.001d);

            this.rands = new WrappingRandomDistribution[] {
                new WrappingRandomDistribution(zipf, 0),
                new WrappingRandomDistribution(zipf, (int)(num_a_records / 2.0)),
            };
        }
        
        @Override
        protected void populateRow() {
            int col = 0;
            
            // Use a composite id containing both the A_ID and the index of
            // this B_ID record for said A_ID:  <B_ID><A_ID>
            long a_id = new Long(rands[(int)this.count % 2].nextInt());
            hist.put(a_id);
            long b_id = encodeCompositeId(a_id, hist.get(a_id)); //  - 1);
            
            // B_ID
            row[col++] = b_id;
            // B_A_ID
            row[col++] = a_id;
            // B_SATTR##
            for (int j=0; j < 16; ++j) {
                row[col++] = m_rng.astring(6, 32);
            }
            // B_IATTR##
            for (int j=0; j < 16; ++j) {
                row[col++] = m_rng.number(0, 1<<30);
            }
            assert (col == this.table.getColumnCount());
//            System.err.println(this.tableName + "[" + this.count + "]: " + Arrays.toString(row));
//            for (int i = 0; i < col; i++) {
//                System.err.println(" [" + i + "]: " + row[i].toString() + " (len=" + row[i].toString().length() + ")");
//            }
//            System.err.println(StringUtil.SINGLE_LINE);
            
        }
    } // END CLASS
    
    /**
     * TABLEC Generator
     */
    protected class TABLECGenerator extends AbstractTableGenerator {
        private RandomDistribution.DiscreteRNG rand;
        
        public TABLECGenerator() {
            super(MarkovConstants.TABLENAME_TABLEC, MarkovTables.initializeTableC());
            
            // TABLEC has a uniform distribution on C_A_ID
            long num_a_records = MarkovConstants.TABLESIZE_TABLEA / m_scalefactor;
            this.rand = new RandomDistribution.Flat(m_rng, 0, (int)num_a_records);
        }
        
        @Override
        protected void populateRow() {
            int col = 0;
            
            // Use a composite id containing both the A_ID and the index of
            // this C_ID record for said A_ID:  <C_ID><A_ID>
            long a_id = new Long(rand.nextInt());
            hist.put(a_id);
            
            // C_ID
            row[col++] = encodeCompositeId(a_id, hist.get(a_id)); // - 1);
            // C_A_ID
            row[col++] = a_id;
            // C_SATTR##
            for (int j=0; j < 16; ++j) {
                row[col++] = m_rng.astring(32, 128);
            }
            // C_IATTR##
            for (int j=0; j < 16; ++j) {
                row[col++] = m_rng.number(0, 1<<30);
            }
            assert (col == this.table.getColumnCount());
//            System.err.println(this.tableName + "[" + this.count + "]: " + Arrays.toString(row));
        }
    } // END CLASS

    /**
     * TABLED Generator
     */
    protected class TABLEDGenerator extends AbstractTableGenerator {
        private final long num_a_records;
        private final long num_b_records;
        private final long num_c_records;
        
        public TABLEDGenerator() {
            super(MarkovConstants.TABLENAME_TABLED, MarkovTables.initializeTableD());
            this.num_a_records = MarkovLoader.this.table_sizes.get(MarkovConstants.TABLENAME_TABLEA).get();
            this.num_b_records = MarkovLoader.this.table_sizes.get(MarkovConstants.TABLENAME_TABLEB).get();
            this.num_c_records = MarkovLoader.this.table_sizes.get(MarkovConstants.TABLENAME_TABLEC).get();
        }

        @Override
        protected void populateRow() {
            int col = 0;
            long id = this.count;

            // Pick a random values for the parent B record
            Long b_id = new Long(m_rng.number(0, (int)num_b_records));
            Long b_a_id = new Long(m_rng.number(0, (int)num_a_records));
            
            // Now generate a parent C record using the affinity generator on B_A_ID
            // The C_ID value can be random
            Long c_id = new Long(m_rng.number(0, (int)num_c_records));
            Long c_a_id = new Long(m_rng.numberAffinity(0, (int)num_a_records, (int)id, MarkovConstants.TABLENAME_TABLEC, MarkovConstants.TABLENAME_TABLEB));
            
            row[col++] = new Long(id);  // D_ID
            row[col++] = b_id;          // D_B_ID
            row[col++] = b_a_id;        // D_B_A_ID
            row[col++] = c_id;          // D_C_ID
            row[col++] = c_a_id;        // D_C_A_ID

            // D_SATTR##
            for (int j = 0; j < 16; ++j) {
                assert(col < row.length) : "Record got too big (" + col + " <=> " + row.length + ")";
                row[col++] = m_rng.astring(6, 32);
            } // FOR
            
            // D_IATTR##
            for (int j = 0; j < 16; ++j) {
                try {
                    assert(col < row.length) : "Record got too big (" + col + " <=> " + row.length + ")";
                    row[col++] = m_rng.number(0, 1<<30);
                } catch (ArrayIndexOutOfBoundsException ex) {
                    System.out.println(col + " ==> " + row.length);
                    ex.printStackTrace();
                    System.exit(1);
                }
            } // FOR
            assert (col == this.table.getColumnCount());
        }
    } // END CLASS
        
    /**
     * 
     * @param tablename
     * @param table
     */
    protected void loadTable(String tablename, VoltTable table) {
        // System.out.println("Loading " + table.getRowCount() + " tuples for table " + tablename + " [bytes=" + table.getUnderlyingBufferSize() + "]");
    
        // Load up this dirty mess...
        try {
            m_voltClient.callProcedure("@LoadMultipartitionTable", tablename, table);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    @Override
    public String getApplicationName() {
        return "Markov Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Loader";
    }
}

