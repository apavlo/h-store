package edu.brown.benchmark.simple;
 
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
 
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Table;

import edu.brown.api.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;
import edu.brown.api.Loader;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ThreadUtil;
 
public class SimpleLoader extends Loader {

    private static final Logger LOG = Logger.getLogger(SimpleClient.class);
    private static final LoggerBoolean debug = new LoggerBoolean(true);

    static {
        LoggerUtil.attachObserver(LOG, debug);
    }

    private final long init_record_count;
    private int loadthreads = ThreadUtil.availableProcessors();
    
    public static void main(String args[]) throws Exception {
        if (debug.val)
            LOG.debug("MAIN: " + SimpleLoader.class.getName());
        BenchmarkComponent.main(SimpleLoader.class, args, true);
    }

    public SimpleLoader(String[] args) {
        super(args);
        if (debug.val)
            LOG.debug("CONSTRUCTOR: " + SimpleLoader.class.getName());
       
        
        long size = -1;        
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);
            
            // TODO: Retrieve parameters
            if (key.equalsIgnoreCase("num_records")) {
                size = Long.valueOf(value);
            }
            // Multi-Threaded Loader
            else if (key.equalsIgnoreCase("loadthreads")) {
                this.loadthreads = Integer.valueOf(value);
            }

        } // FOR


        if(size > 0)
            this.init_record_count = size;
        else
            this.init_record_count = 100;  // Constant
        
        LOG.info("Initializing Simple database with " + init_record_count + " records.");
    }
 
    // UTILS
    
    public static final Random rand = new Random();

    public static String astring(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }
   
    // taken from tpcc.RandomGenerator
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = (int)number(minimum_length, maximum_length);
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + number(0, numCharacters - 1));
        }
        return new String(bytes);
    }
                         
    // taken from tpcc.RandomGenerator
    public static long number(long minimum, long maximum) {
        assert minimum <= maximum;
        long value = Math.abs(rand.nextLong()) % (maximum - minimum + 1) + minimum;
        assert minimum <= value && value <= maximum;
        return value;
    } 
 
    @Override
    public void load() {
        if (debug.val)
            LOG.debug("Starting SimpleLoader");

        final CatalogContext catalogContext = this.getCatalogContext(); 
        final Table catalog_tbl = catalogContext.getTableByName("STABLE");   // Constant
        final AtomicLong total = new AtomicLong(0);
        
        // Multi-threaded loader
        final int rows_per_thread = (int)Math.ceil(init_record_count / (double)this.loadthreads);
        final List<Runnable> runnables = new ArrayList<Runnable>();
        for (int i = 0; i < this.loadthreads; i++) {
            final int thread_id = i;
            final int start = rows_per_thread * i;
            final int stop = start + rows_per_thread;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    // Create an empty VoltTable handle and then populate it in batches
                    // to be sent to the DBMS
                    VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
                    Object row[] = new Object[table.getColumnCount()];

                    for (int i = start; i < stop; i++) {
                        row[0] = i;
                        row[1] = astring(4, 4);  // Constants
                        
                        table.addRow(row);

                        // insert this batch of tuples
                        if (table.getRowCount() >= 10) {   // BATCH SIZE  - Constant
                            loadVoltTable("STABLE", table);        // Constant
                            total.addAndGet(table.getRowCount());
                            table.clearRowData();
                            if (debug.val)
                                LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                          thread_id, total.get(), init_record_count));
                        }
                    } // FOR

                    // load remaining records
                    if (table.getRowCount() > 0) {
                        loadVoltTable("STABLE", table);     // Constant
                        total.addAndGet(table.getRowCount());
                        table.clearRowData();
                        if (debug.val)
                            LOG.debug(String.format("[%d] Records Loaded: %6d / %d",
                                      thread_id, total.get(), init_record_count));
                    }
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        if (debug.val)
            LOG.info("Finished loading " + catalog_tbl.getName());
    }   

}
