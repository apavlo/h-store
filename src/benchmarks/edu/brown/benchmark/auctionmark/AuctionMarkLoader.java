/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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
package edu.brown.benchmark.auctionmark;

import java.io.File;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.auctionmark.AuctionMarkConstants.ItemStatus;
import edu.brown.benchmark.auctionmark.util.AuctionMarkCategoryParser;
import edu.brown.benchmark.auctionmark.util.AuctionMarkUtil;
import edu.brown.benchmark.auctionmark.util.Category;
import edu.brown.benchmark.auctionmark.util.GlobalAttributeGroupId;
import edu.brown.benchmark.auctionmark.util.GlobalAttributeValueId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.LoaderItemInfo;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.benchmark.auctionmark.util.UserIdGenerator;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.CompositeId;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;

/**
 * 
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends BenchmarkComponent {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final AuctionMarkProfile profile;
    
    /**
     * Base catalog objects that we can reference to figure out how to access
     */
    protected final Catalog catalog;
    protected final Database catalog_db;

    
    /**
     * Data Generator Classes
     * TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = new ListOrderedMap<String, AbstractTableGenerator>();
    
    private final Collection<String> sub_generators = new HashSet<String>();

    /** The set of tables that we have finished loading **/
    private final transient Collection<String> finished = new HashSet<String>();

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        edu.brown.api.BenchmarkComponent.main(AuctionMarkLoader.class, args, true);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(String[] args) {
        super(args);

        if (debug.val)
            LOG.debug("AuctionMarkLoader::: numClients = " + this.getNumClients());
        
        int seed = 0;
        String randGenClassName = DefaultRandomGenerator.class.getName();
        String randGenProfilePath = null;
        File dataDir = null;
        Integer temporal_window = null;
        Integer temporal_total = null;
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Random Generator Seed
            if (key.equalsIgnoreCase("RANDOMSEED")) {
                seed = Integer.parseInt(value);
            }
            // Random Generator Class
            else if (key.equalsIgnoreCase("RANDOMGENERATOR")) {
                randGenClassName = value;
            }
            // Random Generator Profile File
            else if (key.equalsIgnoreCase("RANDOMPROFILE")) {
                randGenProfilePath = value;
            }
            // Data directory
            else if (key.equalsIgnoreCase("DATADIR")) {
                dataDir = new File(value);
            }
            // Temporal Skew
            else if (key.equalsIgnoreCase("TEMPORALWINDOW")) {
                assert(m_extraParams.containsKey("TEMPORALTOTAL")) : "Missing TEMPORALTOTAL parameter";
                temporal_window = Integer.valueOf(m_extraParams.get("TEMPORALWINDOW"));
                temporal_total = Integer.valueOf(m_extraParams.get("TEMPORALTOTAL"));
            }
        } // FOR
        
        // Random Generator
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        
        HStoreConf hstore_conf = this.getHStoreConf();
        
        // Data Directory Path
        if (dataDir == null) dataDir = AuctionMarkUtil.getDataDirectory();
        assert(dataDir != null);
        
        // BenchmarkProfile
        profile = new AuctionMarkProfile(rng, getNumClients());
        profile.setDataDirectory(dataDir);
        profile.setAndGetBenchmarkStartTime();
        profile.setScaleFactor(hstore_conf.client.scalefactor);
        
        // Temporal Skew
        if (temporal_window != null && temporal_window > 0) {
            profile.enableTemporalSkew(temporal_window, temporal_total);
            LOG.info(String.format("Enabling temporal skew [window=%d, total=%d]", temporal_window, temporal_total));
        }
        
        // Catalog
        CatalogContext _catalog = null;
        try {
            _catalog = this.getCatalogContext();
        } catch (Exception ex) {
            LOG.error("Failed to retrieve already compiled catalog", ex);
            System.exit(1);
        }
        this.catalog = _catalog.catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
        
        // ---------------------------
        // Fixed-Size Table Generators
        // ---------------------------
        
        this.registerGenerator(new RegionGenerator());
        this.registerGenerator(new CategoryGenerator(new File(profile.data_directory.getAbsolutePath() + "/categories.txt")));
        this.registerGenerator(new GlobalAttributeGroupGenerator());
        this.registerGenerator(new GlobalAttributeValueGenerator());

        // ---------------------------
        // Scaling-Size Table Generators
        // ---------------------------
        
        // USER TABLES
        this.registerGenerator(new UserGenerator());
        this.registerGenerator(new UserAttributesGenerator());
        this.registerGenerator(new UserItemGenerator());
        this.registerGenerator(new UserWatchGenerator());
        this.registerGenerator(new UserFeedbackGenerator());
        
        // ITEM TABLES
        this.registerGenerator(new ItemGenerator());
        this.registerGenerator(new ItemAttributeGenerator());
        this.registerGenerator(new ItemBidGenerator());
        this.registerGenerator(new ItemMaxBidGenerator());
        this.registerGenerator(new ItemCommentGenerator());
        this.registerGenerator(new ItemImageGenerator());
        this.registerGenerator(new ItemPurchaseGenerator());
    }
    
    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubTableGenerators()) {
            this.registerGenerator(sub_generator);
            this.sub_generators.add(sub_generator.getTableName());
        } // FOR
    }
    
    protected AbstractTableGenerator getGenerator(String table_name) {
        return (this.generators.get(table_name));
    }

    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    /**
     * Call by the benchmark framework to load the table data
     */
    @Override
    public void runLoop() {
        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        final List<Thread> threads = new ArrayList<Thread>();
        for (AbstractTableGenerator generator : this.generators.values()) {
            // if (isSubGenerator(generator)) continue;
            Thread t = new Thread(generator);
            t.setUncaughtExceptionHandler(handler);
            threads.add(t);
            // Make sure we call init first before starting any thread
            generator.init();
        } // FOR
        assert(threads.size() > 0);
        handler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> t) {
                for (Thread thread : threads)
                    thread.interrupt();
            }
        });
        
        // Construct a new thread to load each table
        // Fire off the threads and wait for them to complete
        // If debug is set to true, then we'll execute them serially
        try {
            for (Thread t : threads) {
                t.start();
            } // FOR
            for (Thread t : threads) {
                t.join();
            } // FOR
        } catch (InterruptedException e) {
            LOG.fatal("Unexpected error", e);
        } finally {
            if (handler.hasError()) {
                throw new RuntimeException("Error while generating table data.", handler.getError());
            }
        }
        
        profile.saveProfile(this);
        LOG.info("Finished generating data for all tables");
        if (debug.val) LOG.debug("Table Sizes:\n" + this.getTableTupleCounts());
    }

    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        LOG.info("*** START " + tableName);
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);

        // Generate Data
        final VoltTable volt_table = generator.getVoltTable();
        while (generator.hasMore()) {
            generator.generateBatch();
            this.loadVoltTable(generator.getTableName(), volt_table);
            volt_table.clearRowData();
        } // WHILE
        
        // Mark as finished
        generator.markAsFinished();
        this.finished.add(tableName);
        LOG.info(String.format("*** FINISH %s - %d tuples - [%d / %d]", tableName, this.getTableTupleCount(tableName), this.finished.size(), this.generators.size()));
        if (debug.val) {
            LOG.debug("Remaining Tables: " + CollectionUtils.subtract(this.generators.keySet(), this.finished));
        }
    }

    /**********************************************************************************************
     * AbstractTableGenerator
     **********************************************************************************************/
    protected abstract class AbstractTableGenerator implements Runnable {
        private final String tableName;
        private final Table catalog_tbl;
        protected final VoltTable table;
        protected Long tableSize;
        protected Long batchSize;
        protected final CountDownLatch latch = new CountDownLatch(1);
        protected final List<String> dependencyTables = new ArrayList<String>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator. 
         * The queues we need to update every time we generate a new LoaderItemInfo
         */
        protected final Set<SubTableGenerator<?>> sub_generators = new HashSet<SubTableGenerator<?>>();  

        protected final Object[] row;
        protected long count = 0;
        
        /** Any column with the name XX_SATTR## will automatically be filled with a random string */
        protected final List<Column> random_str_cols = new ArrayList<Column>();
        protected final Pattern random_str_regex = Pattern.compile("[\\w]+\\_SATTR[\\d]+", Pattern.CASE_INSENSITIVE);
        
        /** Any column with the name XX_IATTR## will automatically be filled with a random integer */
        protected List<Column> random_int_cols = new ArrayList<Column>();
        protected final Pattern random_int_regex = Pattern.compile("[\\w]+\\_IATTR[\\d]+", Pattern.CASE_INSENSITIVE);

        /**
         * Constructor
         * @param catalog_tbl
         */
        public AbstractTableGenerator(String tableName, String...dependencies) {
            this.tableName = tableName;
            this.catalog_tbl = catalog_db.getTables().get(tableName);
            assert(catalog_tbl != null) : "Invalid table name '" + tableName + "'";
            
            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName());

            // Generate a VoltTable instance we can use
            this.table = CatalogUtil.getVoltTable(catalog_tbl);
            this.row = new Object[this.table.getColumnCount()];

            // Add the dependencies so that we know what we need to block on
            CollectionUtil.addAll(this.dependencyTables, dependencies);
            
            try {
                String field_name = "BATCHSIZE_" + catalog_tbl.getName();
                Field field_handle = AuctionMarkConstants.class.getField(field_name);
                assert (field_handle != null);
                this.batchSize = (Long) field_handle.get(null);
            } catch (Exception ex) {
                throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
            } 

            // Initialize dynamic parameters for tables that are not loaded from data files
            if (!data_file && !dynamic_size && tableName.equalsIgnoreCase(AuctionMarkConstants.TABLENAME_ITEM) == false) {
                try {
                    String field_name = "TABLESIZE_" + catalog_tbl.getName();
                    Field field_handle = AuctionMarkConstants.class.getField(field_name);
                    assert (field_handle != null);
                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize = Math.round(this.tableSize * profile.getScaleFactor());
                    }
                } catch (NoSuchFieldException ex) {
                    if (debug.val) LOG.warn("No table size field for '" + tableName + "'", ex);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
                } 
            } 
            
            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() == VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_str_cols.add(catalog_col);
                    if (trace.val) LOG.trace("Random String Column: " + catalog_col.fullName());
                }
                else if (random_int_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() != VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_int_cols.add(catalog_col);
                    if (trace.val) LOG.trace("Random Integer Column: " + catalog_col.fullName());
                }
            } // FOR
            if (debug.val) {
                if (this.random_str_cols.size() > 0) LOG.debug(String.format("%s Random String Columns: %s", tableName, CatalogUtil.debug(this.random_str_cols)));
                if (this.random_int_cols.size() > 0) LOG.debug(String.format("%s Random Integer Columns: %s", tableName, CatalogUtil.debug(this.random_int_cols)));
            }
        }

        /**
         * Initiate data that need dependencies
         */
        public abstract void init();
        
        /**
         * All sub-classes must implement this. This will enter new tuple data into the row
         */
        protected abstract int populateRow();
        
        public void run() {
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && debug.val)
                LOG.debug(String.format("%s: Table generator is blocked waiting for %d other tables: %s",
                                        this.tableName, this.dependencyTables.size(), this.dependencyTables));
            for (String dependency : this.dependencyTables) {
                AbstractTableGenerator gen = AuctionMarkLoader.this.generators.get(dependency);
                assert(gen != null) : "Missing table generator for '" + dependency + "'";
                try {
                    gen.latch.await();
                } catch (InterruptedException ex) {
                    throw new RuntimeException("Unexpected interruption for '" + this.tableName + "' waiting for '" + dependency + "'", ex);
                }
            } // FOR
            
            // Then invoke the loader generation method
            try {
                AuctionMarkLoader.this.generateTableData(this.tableName);
            } catch (Throwable ex) {
                throw new RuntimeException("Unexpected error while generating table data for '" + this.tableName + "'", ex);
            }
        }
        
        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubTableGenerator(SubTableGenerator<?> sub_item) {
            this.sub_generators.add(sub_item);
            return ((T)this);
        }
        @SuppressWarnings("unchecked")
        public void updateSubTableGenerators(Object obj) {
            // Queue up this item for our multi-threaded sub-generators
            if (trace.val)
                LOG.trace(String.format("%s: Updating %d sub-generators with %s: %s",
                                        this.tableName, this.sub_generators.size(), obj, this.sub_generators));
            for (@SuppressWarnings("rawtypes") SubTableGenerator sub_generator : this.sub_generators) {
                sub_generator.queue(obj);
            } // FOR
        }
        public boolean hasSubTableGenerators() {
            return (!this.sub_generators.isEmpty());
        }
        public Collection<SubTableGenerator<?>> getSubTableGenerators() {
            return (this.sub_generators);
        }
        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<String>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName());
            }
            return (names);
        }
        
        protected int populateRandomColumns(Object row[]) {
            int cols = 0;
            
            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = profile.rng.astring(profile.rng.nextInt(size - 1), size);
                cols++;
            } // FOR
            
            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = profile.rng.number(0, 1<<30);
                cols++;
            } // FOR
            
            return (cols);
        }

        /**
         * Returns true if this generator has more tuples that it wants to add
         * @return
         */
        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }
        /**
         * Return the table's catalog object for this generator
         * @return
         */
        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }
        /**
         * Return the VoltTable handle
         * @return
         */
        public VoltTable getVoltTable() {
            return this.table;
        }
        /**
         * Returns the number of tuples that will be loaded into this table
         * @return
         */
        public Long getTableSize() {
            return this.tableSize;
        }
        /**
         * Returns the number of tuples per batch that this generator will want loaded
         * @return
         */
        public Long getBatchSize() {
            return this.batchSize;
        }
        /**
         * Returns the name of the table this this generates
         * @return
         */
        public String getTableName() {
            return this.tableName;
        }
        /**
         * Returns the total number of tuples generated thusfar
         * @return
         */
        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            int cols = this.populateRow();
            // RANDOM COLS
            cols += populateRandomColumns(this.row);
            
            assert(cols == this.table.getColumnCount()) : String.format("Invalid number of columns for %s [expected=%d, actual=%d]",
                                                                        this.tableName, this.table.getColumnCount(), cols);
            
            // Convert all CompositeIds into their long encodings
            for (int i = 0; i < cols; i++) {
                if (this.row[i] != null && this.row[i] instanceof CompositeId) {
                    this.row[i] = ((CompositeId)this.row[i]).encode();
                }
            } // FOR
            
            this.count++;
            this.table.addRow(this.row);
        }
        /**
         * 
         */
        public void generateBatch() {
            if (trace.val) LOG.trace(String.format("%s: Generating new batch", this.getTableName()));
            long batch_count = 0;
            while (this.hasMore() && this.table.getRowCount() < this.batchSize) {
                this.addRow();
                batch_count++;
            } // WHILE
            if (debug.val) LOG.debug(String.format("%s: Finished generating new batch of %d tuples", this.getTableName(), batch_count));
        }

        public void markAsFinished() {
            this.latch.countDown();
            for (SubTableGenerator<?> sub_generator : this.sub_generators) {
                sub_generator.stopWhenEmpty();
            } // FOR
        }
        
        public boolean isFinish(){
            return (this.latch.getCount() == 0);
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }
        
        @Override
        public String toString() {
            return String.format("Generator[%s]", this.tableName);
        }
    } // END CLASS

    /**********************************************************************************************
     * SubUserTableGenerator
     * This is for tables that are based off of the USER table
     **********************************************************************************************/
    protected abstract class SubTableGenerator<T> extends AbstractTableGenerator {
        
        private final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<T>();
        private T current;
        private short currentCounter;
        private boolean stop = false;
        private final String sourceTableName;

        public SubTableGenerator(String tableName, String sourceTableName, String...dependencies) {
            super(tableName, dependencies);
            this.sourceTableName = sourceTableName;
        }
        
        protected abstract short getElementCounter(T t);
        protected abstract int populateRow(T t, short remaining);
        
        public void queue(T t) {
            assert(this.queue.contains(t) == false) : "Trying to queue duplicate element for '" + this.getTableName() + "'";
            this.queue.add(t);
        }
        public void stopWhenEmpty() {
            this.stop = true;
        }
        
        @Override
        public void init() {
            // Get the AbstractTableGenerator that will feed into this generator
            AbstractTableGenerator parent_gen = AuctionMarkLoader.this.generators.get(this.sourceTableName);
            assert(parent_gen != null) : "Unexpected source TableGenerator '" + this.sourceTableName + "'";
            parent_gen.addSubTableGenerator(this);
            
            this.current = null;
            this.currentCounter = 0;
        }
        @Override
        public final boolean hasMore() {
            return (this.getNext() != null);
        }
        @Override
        protected final int populateRow() {
            T t = this.getNext();
            assert(t != null);
            this.currentCounter--;
            return (this.populateRow(t, this.currentCounter));
        }
        private final T getNext() {
            T last = this.current;
            if (this.current == null || this.currentCounter == 0) {
                while (this.currentCounter == 0) {
                    try {
                        this.current = this.queue.poll(1000, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException ex) {
                        return (null);
                    }
                    // Check whether we should stop
                    if (this.current == null) {
                        if (this.stop) break;
                        continue;
                    }
                    this.currentCounter = this.getElementCounter(this.current);
                } // WHILE
            }
            if (last != this.current) {
                if (last != null) this.finishElementCallback(last);
                if (this.current != null) this.newElementCallback(this.current);
            }
            return this.current;
        }
        protected void finishElementCallback(T t) {
            // Nothing...
        }
        protected void newElementCallback(T t) {
            // Nothing... 
        }
    } // END CLASS
    
    /**********************************************************************************************
     * REGION Generator
     **********************************************************************************************/
    protected class RegionGenerator extends AbstractTableGenerator {

        public RegionGenerator() {
            super(AuctionMarkConstants.TABLENAME_REGION);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            // R_ID
            this.row[col++] = new Integer((int) this.count);
            // R_NAME
            this.row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        private final Map<String, Category> categoryMap;
        private final LinkedList<Category> categories = new LinkedList<Category>();

        public CategoryGenerator(File data_file) {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.data_file = data_file;
            assert(this.data_file.exists()) : 
                "The data file for the category generator does not exist: " + this.data_file;

            this.categoryMap = (new AuctionMarkCategoryParser(data_file)).getCategoryMap();
            this.tableSize = (long)this.categoryMap.size();
        }

        @Override
        public void init() {
            for (Category category : this.categoryMap.values()) {
                if (category.isLeaf()) {
                    profile.item_category_histogram.put((long)category.getCategoryID(), category.getItemCount());
                }
                this.categories.add(category);
            } // FOR
        }
        @Override
        protected int populateRow() {
            int col = 0;

            Category category = this.categories.poll();
            assert(category != null);
            
            // C_ID
            this.row[col++] = category.getCategoryID();
            // C_NAME
            this.row[col++] = category.getName();
            // C_PARENT_ID
            this.row[col++] = category.getParentCategoryID();
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;
        private final ObjectHistogram<Integer> category_groups = new ObjectHistogram<Integer>();
        private final LinkedList<GlobalAttributeGroupId> group_ids = new LinkedList<GlobalAttributeGroupId>();

        public GlobalAttributeGroupGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = getTableTupleCount(AuctionMarkConstants.TABLENAME_CATEGORY);
            
            for (int i = 0; i < this.tableSize; i++) {
                int category_id = profile.rng.number(0, (int)this.num_categories);
                this.category_groups.put(category_id);
                int id = this.category_groups.get(category_id).intValue();
                int count = (int)profile.rng.number(1, AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_VALUE_PER_GROUP);
                GlobalAttributeGroupId gag_id = new GlobalAttributeGroupId(category_id, id, count);
                assert(profile.gag_ids.contains(gag_id) == false);
                profile.gag_ids.add(gag_id);
                this.group_ids.add(gag_id);
            } // FOR
        }
        @Override
        protected int populateRow() {
            int col = 0;

            GlobalAttributeGroupId gag_id = this.group_ids.poll();
            assert(gag_id != null);
            
            // GAG_ID
            this.row[col++] = gag_id.encode();
            // GAG_C_ID
            this.row[col++] = gag_id.getCategoryId();
            // GAG_NAME
            this.row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private ObjectHistogram<GlobalAttributeGroupId> gag_counters = new ObjectHistogram<GlobalAttributeGroupId>(true);
        private Iterator<GlobalAttributeGroupId> gag_iterator;
        private GlobalAttributeGroupId gag_current;
        private int gav_counter = -1;

        public GlobalAttributeValueGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        }

        @Override
        public void init() {
            this.tableSize = 0l;
            for (GlobalAttributeGroupId gag_id : profile.gag_ids) {
                this.gag_counters.set(gag_id, 0);
                this.tableSize += gag_id.getCount();
            } // FOR
            this.gag_iterator = profile.gag_ids.iterator();
        }
        @Override
        protected int populateRow() {
            int col = 0;
            
            if (this.gav_counter == -1 || ++this.gav_counter == this.gag_current.getCount()) {
                this.gag_current = this.gag_iterator.next();
                assert(this.gag_current != null);
                this.gav_counter = 0;
            }

            GlobalAttributeValueId gav_id = new GlobalAttributeValueId(this.gag_current.encode(),
                                                                     this.gav_counter);
            
            // GAV_ID
            this.row[col++] = gav_id.encode();
            // GAV_GAG_ID
            this.row[col++] = this.gag_current.encode();
            // GAV_NAME
            this.row[col++] = profile.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Zipf randomRating;
        private UserIdGenerator idGenerator;
        
        public UserGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(profile.rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_RATING,
                                                                     AuctionMarkConstants.USER_MAX_RATING, 1.0001);
            this.randomBalance = new Zipf(profile.rng, AuctionMarkConstants.USER_MIN_BALANCE,
                                                                      AuctionMarkConstants.USER_MAX_BALANCE, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            Zipf randomNumItems = new Zipf(profile.rng,
                                           AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
                                           Math.round(AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER * profile.getScaleFactor()),
                                           1.001);
            for (long i = 0; i < this.tableSize; i++) {
                long num_items = randomNumItems.nextInt();
                profile.users_per_item_count.put(num_items);
            } // FOR
            if (trace.val)
                LOG.trace("Users Per Item Count:\n" + profile.users_per_item_count);
            this.idGenerator = new UserIdGenerator(profile.users_per_item_count, getNumClients());
            assert(this.idGenerator.hasNext());
        }
        @Override
        public synchronized boolean hasMore() {
            return this.idGenerator.hasNext();
        }
        @Override
        protected int populateRow() {
            int col = 0;

            UserId u_id = this.idGenerator.next();
            
            // U_ID
            this.row[col++] = u_id;
            // U_RATING
            this.row[col++] = this.randomRating.nextInt();
            // U_BALANCE
            this.row[col++] = (this.randomBalance.nextInt()) / 10.0;
            // U_COMMENTS
            this.row[col++] = 0;
            // U_R_ID
            this.row[col++] = this.randomRegion.nextInt();
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            // U_UPDATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            
            this.updateSubTableGenerators(u_id);
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ATTRIBUTES Generator
     **********************************************************************************************/
    protected class UserAttributesGenerator extends SubTableGenerator<UserId> {
        private final Zipf randomNumUserAttributes;
        
        public UserAttributesGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES,
                  AuctionMarkConstants.TABLENAME_USER);
            
            this.randomNumUserAttributes = new Zipf(profile.rng,
                                                    AuctionMarkConstants.USER_MIN_ATTRIBUTES,
                                                    AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(randomNumUserAttributes.nextInt());
        }
        @Override
        protected int populateRow(UserId user_id, short remaining) {
            int col = 0;
            
            // UA_ID
            this.row[col++] = this.count;
            // UA_U_ID
            this.row[col++] = user_id;
            // UA_NAME
            this.row[col++] = profile.rng.astring(5, 32);
            // UA_VALUE
            this.row[col++] = profile.rng.astring(5, 32);
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends SubTableGenerator<UserId> {
        
        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<Long, Pair<Zipf,Zipf>>();
        
        /** End date distribution */
        private final ObjectHistogram<String> endDateHistogram = new ObjectHistogram<String>();
        private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");
        
        public ItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }
        
        @Override
        protected short getElementCounter(UserId user_id) {
            return (short)(user_id.getItemCount());
        }

        @Override
        public void init() {
            super.init();
            this.tableSize = 0l;
            for (Long size : profile.users_per_item_count.values()) {
                this.tableSize += size.intValue() * profile.users_per_item_count.get(size);
            } // FOR
        }
        @Override
        protected int populateRow(UserId seller_id, short remaining) {
            int col = 0;
            
            ItemId itemId = new ItemId(seller_id, remaining);
            TimestampType endDate = this.getRandomEndTimestamp();
            TimestampType startDate = this.getRandomStartTimestamp(endDate); 
            
            //LOG.info("endDate = " + endDate + " : startDate = " + startDate);
            long bidDurationDay = ((endDate.getTime() - startDate.getTime()) / AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
            if (this.item_bid_watch_zipfs.containsKey(bidDurationDay) == false) {
                Zipf randomNumBids = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY * (int)bidDurationDay,
                        1.001);
                Zipf randomNumWatches = new Zipf(profile.rng,
                        AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY * (int)bidDurationDay,
                        (int)Math.ceil(AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY * (int)bidDurationDay * profile.getScaleFactor()), 1.001);
                this.item_bid_watch_zipfs.put(bidDurationDay, Pair.of(randomNumBids, randomNumWatches));
            }
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            assert(p != null);

            // Calculate the number of bids and watches for this item
            short numBids = (short)p.getFirst().nextInt();
            short numWatches = (short)p.getSecond().nextInt();
            
            // Create the ItemInfo object that we will use to cache the local data 
            // for this item. This will get garbage collected once all the derivative
            // tables are done with it.
            LoaderItemInfo itemInfo = new LoaderItemInfo(itemId, endDate, numBids);
            itemInfo.sellerId = seller_id;
            itemInfo.startDate = endDate;
            itemInfo.initialPrice = profile.randomInitialPrice.nextInt();
            assert(itemInfo.initialPrice > 0) : "Invalid initial price for " + itemId;
            itemInfo.numImages = (short) profile.randomNumImages.nextInt();
            itemInfo.numAttributes = (short) profile.randomNumAttributes.nextInt();
            itemInfo.numBids = numBids;
            itemInfo.numWatches = numWatches;
            
            // The auction for this item has already closed
            if (itemInfo.endDate.getTime() <= profile.getBenchmarkStartTime().getTime()) {
                if (itemInfo.numBids > 0) {
                    // Somebody won a bid and bought the item
                    itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
                    //System.out.println("@@@ z last_bidder_id = " + itemInfo.last_bidder_id);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.endDate);
                    itemInfo.numComments = (short) profile.randomNumComments.nextInt();
                }
                itemInfo.status = ItemStatus.CLOSED;
            }
            // Item is still available
            else if (itemInfo.numBids > 0) {
                itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
            }
            profile.addItemToProperQueue(itemInfo, true);

            // I_ID
            this.row[col++] = itemInfo.itemId;
            // I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // I_C_ID
            this.row[col++] = profile.getRandomCategoryId();
            // I_NAME
            this.row[col++] = profile.rng.astring(6, 32);
            // I_DESCRIPTION
            this.row[col++] = profile.rng.astring(50, 255);
            // I_USER_ATTRIBUTES
            this.row[col++] = profile.rng.astring(20, 255);
            // I_INITIAL_PRICE
            this.row[col++] = itemInfo.initialPrice;

            // I_CURRENT_PRICE
            if (itemInfo.numBids > 0) {
                itemInfo.currentPrice = itemInfo.initialPrice + (itemInfo.numBids * itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP);
                this.row[col++] = itemInfo.currentPrice;
            } else {
                this.row[col++] = itemInfo.initialPrice;
            }

            // I_NUM_BIDS
            this.row[col++] = itemInfo.numBids;
            // I_NUM_IMAGES
            this.row[col++] = itemInfo.numImages;
            // I_NUM_GLOBAL_ATTRS
            this.row[col++] = itemInfo.numAttributes;
            // I_START_DATE
            this.row[col++] = itemInfo.startDate;
            // I_END_DATE
            this.row[col++] = itemInfo.endDate;
            // I_STATUS
            this.row[col++] = itemInfo.status.ordinal();
            // I_UPDATED
            this.row[col++] = itemInfo.startDate;

            if (debug.val)
                this.endDateHistogram.put(sdf.format(itemInfo.endDate.asApproximateJavaDate()));
            
            this.updateSubTableGenerators(itemInfo);
            return (col);
        }
        
        @Override
        public void markAsFinished() {
            super.markAsFinished();
            if (debug.val) LOG.debug("Item End Date Distribution:\n" + this.endDateHistogram);
        }

        private TimestampType getRandomStartTimestamp(TimestampType endDate) {
            long duration = ((long)profile.randomDuration.nextInt()) * AuctionMarkConstants.MICROSECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            TimestampType startTimestamp = new TimestampType(lStartTimestamp);
            return startTimestamp;
        }
        private TimestampType getRandomEndTimestamp() {
            long timeDiff = profile.randomTimeDiff.nextLong();
            TimestampType time = new TimestampType(profile.getBenchmarkStartTime().getTime() + (timeDiff * 1000000));
//            LOG.info(timeDiff + " => " + sdf.format(time.asApproximateJavaDate()));
            return time;
        }
        private TimestampType getRandomPurchaseTimestamp(TimestampType endDate) {
            long duration = profile.randomPurchaseDuration.nextInt();
            return new TimestampType(endDate.getTime() + duration * AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
        }
    }
    
    /**********************************************************************************************
     * ITEM_IMAGE Generator
     **********************************************************************************************/
    protected class ItemImageGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemImageGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numImages;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;

            // II_ID
            this.row[col++] = this.count;
            // II_I_ID
            this.row[col++] = itemInfo.itemId;
            // II_U_ID
            this.row[col++] = itemInfo.sellerId;

            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemAttributeGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE,
                  AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return itemInfo.numAttributes;
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            GlobalAttributeValueId gav_id = profile.getRandomGlobalAttributeValue();
            assert(gav_id != null);
            
            // IA_ID
            this.row[col++] = this.count;
            // IA_I_ID
            this.row[col++] = itemInfo.itemId;
            // IA_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IA_GAV_ID
            this.row[col++] = gav_id.encode();
            // IA_GAG_ID
            this.row[col++] = gav_id.getGlobalAttributeGroup().encode();

            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemCommentGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.purchaseDate != null ? itemInfo.numComments : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;

            // IC_ID
            this.row[col++] = new Integer((int) this.count);
            // IC_I_ID
            this.row[col++] = itemInfo.itemId;
            // IC_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IC_BUYER_ID
            this.row[col++] = itemInfo.lastBidderId;
            // IC_QUESTION
            this.row[col++] = profile.rng.astring(10, 128);
            // IC_RESPONSE
            this.row[col++] = profile.rng.astring(10, 128);
            // IC_CREATED
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);
            // IC_UPDATED
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            int start = Math.round(startDate.getTime() / 1000000);
            int end = Math.round(endDate.getTime() / 1000000);
            return new TimestampType((profile.rng.number(start, end)) * 1000 * 1000);
        }
    }

    /**********************************************************************************************
     * ITEM_BID Generator
     **********************************************************************************************/
    protected class ItemBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        private LoaderItemInfo.Bid bid = null;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private boolean new_item;
        
        public ItemBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_BID,
                  AuctionMarkConstants.TABLENAME_ITEM);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return ((short)itemInfo.numBids);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            assert(itemInfo.numBids > 0);
            
            UserId bidderId = null;
            
            // Figure out the UserId for the person bidding on this item now
            if (this.new_item) {
                // If this is a new item and there is more than one bid, then
                // we'll choose the bidder's UserId at random.
                // If there is only one bid, then it will have to be the last bidder
                bidderId = (itemInfo.numBids == 1 ? itemInfo.lastBidderId :
                                                    profile.getRandomBuyerId(itemInfo.sellerId));
                TimestampType endDate;
                if (itemInfo.status == ItemStatus.OPEN) {
                    endDate = profile.getBenchmarkStartTime();
                } else {
                    endDate = itemInfo.endDate;
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.startDate.getTime()) / (remaining + 1);
                this.currentBidPriceAdvanceStep = itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
            }
            // The last bid must always be the item's lastBidderId
            else if (remaining == 0) {
                bidderId = itemInfo.lastBidderId; 
            }
            // The first bid for a two-bid item must always be different than the lastBidderId
            else if (itemInfo.numBids == 2) {
                assert(remaining == 1);
                bidderId = profile.getRandomBuyerId(itemInfo.lastBidderId, itemInfo.sellerId);
            } 
            // Since there are multiple bids, we want randomly select one based on the previous bidders
            // We will get the histogram of bidders so that we are more likely to select
            // an existing bidder rather than a completely random one
            else {
                assert(this.bid != null);
                ObjectHistogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.bidderId, itemInfo.sellerId);
            }
            assert(bidderId != null);

            float last_bid = (this.new_item ? itemInfo.initialPrice : this.bid.maxBid);
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.createDate = new TimestampType(itemInfo.startDate.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            
            if (remaining == 0) {
                this.bid.maxBid = itemInfo.currentPrice;
                if (itemInfo.purchaseDate != null) {
                    assert(itemInfo.getBidCount() == itemInfo.numBids) : String.format("%d != %d\n%s", itemInfo.getBidCount(), itemInfo.numBids, itemInfo);
                }
            } else {
                this.bid.maxBid = last_bid + this.currentBidPriceAdvanceStep;
            }
            
            // IB_ID
            this.row[col++] = new Long(this.bid.id);
            // IB_I_ID
            this.row[col++] = itemInfo.itemId;
            // IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IB_BUYER_ID
            this.row[col++] = this.bid.bidderId;
            // IB_BID
            this.row[col++] = this.bid.maxBid - (remaining > 0 ? (this.currentBidPriceAdvanceStep/2.0f) : 0);
            // IB_MAX_BID
            this.row[col++] = this.bid.maxBid;
            // IB_CREATED
            this.row[col++] = this.bid.createDate;
            // IB_UPDATED
            this.row[col++] = this.bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
            return (col);
        }
        @Override
        protected void newElementCallback(LoaderItemInfo itemInfo) {
            this.new_item = true;
            this.bid = null;
        }
    }

    /**********************************************************************************************
     * ITEM_MAX_BID Generator
     **********************************************************************************************/
    protected class ItemMaxBidGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemMaxBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID,
                AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : "No bids?\n" + itemInfo;

            // IMB_I_ID
            this.row[col++] = itemInfo.itemId;
            // IMB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_IB_ID
            this.row[col++] = bid.id;
            // IMB_IB_I_ID
            this.row[col++] = itemInfo.itemId;
            // IMB_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_CREATED
            this.row[col++] = bid.createDate;
            // IMB_UPDATED
            this.row[col++] = bid.updateDate;

            if (remaining == 0) this.updateSubTableGenerators(itemInfo);
            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubTableGenerator<LoaderItemInfo> {

        public ItemPurchaseGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // IP_ID
            this.row[col++] = this.count;
            // IP_IB_ID
            this.row[col++] = bid.id;
            // IP_IB_I_ID
            this.row[col++] = itemInfo.itemId;
            // IP_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IP_DATE
            this.row[col++] = itemInfo.purchaseDate;

            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                bid.buyer_feedback = true;
            }
            if (profile.rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                bid.seller_feedback = true;
            }
            
            if (remaining == 0) this.updateSubTableGenerators(bid);
            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends SubTableGenerator<LoaderItemInfo.Bid> {

        public UserFeedbackGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_FEEDBACK,
                  AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        protected short getElementCounter(LoaderItemInfo.Bid bid) {
            return (short)((bid.buyer_feedback ? 1 : 0) + (bid.seller_feedback ? 1 : 0));
        }
        @Override
        protected int populateRow(LoaderItemInfo.Bid bid, short remaining) {
            int col = 0;

            boolean is_buyer = false;
            if (bid.buyer_feedback && bid.seller_feedback == false) {
                is_buyer = true;
            } else if (bid.seller_feedback && bid.buyer_feedback == false) {
                is_buyer = false;
            } else if (remaining > 1) {
                is_buyer = true;
            }
            LoaderItemInfo itemInfo = bid.getLoaderItemInfo();
            
            // UF_ID
            this.row[col++] = this.count;
            // UF_I_ID
            this.row[col++] = itemInfo.itemId;
            // UF_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UF_TO_ID
            this.row[col++] = (is_buyer ? bid.bidderId : itemInfo.sellerId);
            // UF_FROM_ID
            this.row[col++] = (is_buyer ? itemInfo.sellerId : bid.bidderId);
            // UF_RATING
            this.row[col++] = 1; // TODO
            // UF_DATE
            this.row[col++] = profile.getBenchmarkStartTime(); // Does this matter?

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ITEM Generator
     **********************************************************************************************/
    protected class UserItemGenerator extends SubTableGenerator<LoaderItemInfo> {

        public UserItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ITEM,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null ? 1 : 0);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            LoaderItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            
            // UI_U_ID
            this.row[col++] = bid.bidderId;
            // UI_I_ID
            this.row[col++] = itemInfo.itemId;
            // UI_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UI_IP_ID
            this.row[col++] = null;
            // UI_IP_IB_ID
            this.row[col++] = null;
            // UI_IP_IB_I_ID
            this.row[col++] = null;
            // UI_IP_IB_U_ID
            this.row[col++] = null;
            // UI_CREATED
            this.row[col++] = itemInfo.endDate;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubTableGenerator<LoaderItemInfo> {

        public UserWatchGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_WATCH,
                  AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getElementCounter(LoaderItemInfo itemInfo) {
            return (itemInfo.numWatches);
        }
        @Override
        protected int populateRow(LoaderItemInfo itemInfo, short remaining) {
            int col = 0;
            
            // Make it more likely that a user that has bid on an item is watching it
            ObjectHistogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
            UserId buyerId = null;
            try {
                profile.getRandomBuyerId(bidderHistogram, itemInfo.sellerId);
            } catch (NullPointerException ex) {
                LOG.error("Busted Bidder Histogram:\n" + bidderHistogram);
                throw ex;
            }
            
            // UW_U_ID
            this.row[col++] = buyerId;
            // UW_I_ID
            this.row[col++] = itemInfo.itemId;
            // UW_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UW_CREATED
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            int start = Math.round(startDate.getTime() / 1000000);
            int end = Math.round(endDate.getTime() / 1000000);
            long offset = profile.rng.number(start, end);
            return new TimestampType(offset * 1000000l);
        }
    } // END CLASS
} // END CLASS