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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Observable;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Pair;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.auctionmark.util.AuctionMarkCategoryParser;
import edu.brown.benchmark.auctionmark.util.Category;
import edu.brown.benchmark.auctionmark.util.CompositeId;
import edu.brown.benchmark.auctionmark.util.ItemId;
import edu.brown.benchmark.auctionmark.util.ItemInfo;
import edu.brown.benchmark.auctionmark.util.UserId;
import edu.brown.benchmark.auctionmark.util.UserIdGenerator;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;
import edu.brown.statistics.Histogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * 
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends AuctionMarkBaseClient {
    private static final Logger LOG = Logger.getLogger(AuctionMarkLoader.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Data Generator Classes
     * TableName -> AbstactTableGenerator
     */
    private final Map<String, AbstractTableGenerator> generators = new ListOrderedMap<String, AbstractTableGenerator>();
    
    private final Collection<String> sub_generators = new HashSet<String>();

    /** The set of tables that we have finished loading **/
    private final transient Collection<String> finished = new HashSet<String>();
    
    /** Temporary storage of user ids */
    private final List<UserId> user_ids = new ArrayList<UserId>();

    /** Temporary storage of the number of bids that we need to create per item */
    private final List<ItemInfo> item_infos = new ArrayList<ItemInfo>();

    /** These are the item purchases that we need to create feedback from the buyer */
    private final Queue<ItemInfo.Bid> pending_buyer_feedback = new LinkedList<ItemInfo.Bid>();
    
    /** These are the item purchases that we need to create feedback from the seller */
    private final Queue<ItemInfo.Bid> pending_seller_feedback = new LinkedList<ItemInfo.Bid>();
    
    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        edu.brown.benchmark.BenchmarkComponent.main(AuctionMarkLoader.class, args, true);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(String[] args) {
        super(AuctionMarkLoader.class, args);

        assert(AuctionMarkConstants.MAXIMUM_NUM_CLIENTS > this.getNumClients());
        if (debug.get()) LOG.debug("AuctionMarkLoader::: numClients = " + this.getNumClients());
        
        // ---------------------------
        // Fixed-Size Table Generators
        // ---------------------------
        
        // REGION
        this.registerGenerator(new RegionGenerator());

        // CATEGORY
        this.registerGenerator(new CategoryGenerator(new File(this.data_directory, "categories.txt")));

        // GLOBAL_ATTRIBUTE_GROUP
        this.registerGenerator(new GlobalAttributeGroupGenerator());

        // GLOBAL_ATTRIBUTE_VALUE
        this.registerGenerator(new GlobalAttributeValueGenerator());

        // ---------------------------
        // Scaling-Size Table Generators
        // ---------------------------
        
        // USER TABLES
        this.registerGenerator(
            new UserGenerator().addSubGenerator(
                new UserAttributesGenerator())
        );

        // ITEM TABLES
        this.registerGenerator(
            new ItemGenerator().addSubGenerator(
                new ItemBidGenerator().addSubGenerator(
                    new ItemMaxBidGenerator(),
                    new ItemPurchaseGenerator().addSubGenerator(
                        new UserFeedbackGenerator()
                    ),
                    new UserItemGenerator(),
                    new UserWatchGenerator()
                ),
                new ItemAttributeGenerator(),
                new ItemImageGenerator(),
                new ItemCommentGenerator()
            )
        );
    }
    
    private void registerGenerator(AbstractTableGenerator generator) {
        // Register this one as well as any sub-generators
        this.generators.put(generator.getTableName(), generator);
        for (AbstractTableGenerator sub_generator : generator.getSubGenerators()) {
            this.registerGenerator(sub_generator);
            this.sub_generators.add(sub_generator.getTableName());
        } // FOR
    }
    
    private boolean isSubGenerator(AbstractTableGenerator generator) {
        return (this.sub_generators.contains(generator.getTableName()));
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
            if (isSubGenerator(generator)) continue;
            Thread t = new Thread(generator);
            t.setUncaughtExceptionHandler(handler);
            threads.add(t);
        }
        assert(threads.size() > 0);
        handler.addObserver(new EventObserver() {
            @Override
            public void update(Observable o, Object obj) {
                for (Thread t : threads)
                    t.interrupt();
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
        
        this.saveProfile();
        LOG.info("Finished generating data for all tables");
        if (debug.get()) LOG.debug("Table Sizes:\n" + profile.table_sizes);
    }

    /**
     * Load the tuples for the given table name
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);

        // Generate Data
        generator.init();
        long expectedTotal = generator.getTableSize();
        if (debug.get()) LOG.debug("Loading " + expectedTotal + " tuples for table '" + tableName + "'");
        this.generateTableDataLoop(generator);
        
        // Mark as finished
        Collection<String> finishedTables = this.generateTableDataFinished(generator);
        for (String t : finishedTables) {
            this.finished.add(t);
            LOG.info(String.format("Finished loading %d tuples for %s [%d / %d]", this.profile.getTableSize(t), t, this.finished.size(), catalog_db.getTables().size()));
        } // FOR
        if (debug.get()) {
            LOG.debug("Remaining Tables: " + CollectionUtils.subtract(CatalogUtil.getDisplayNames(catalog_db.getTables()), this.finished));
        }
        
    }
    
    private Collection<String> generateTableDataFinished(AbstractTableGenerator generator) {
        // Mark as Finished
        Collection<String> finishedTables = CollectionUtil.addAll(new ArrayList<String>(), generator.getTableName());
        generator.markAsFinished();
        if (generator.hasSubGenerators()) {
            for (AbstractTableGenerator sub_generator : generator.sub_generators) {
                finishedTables.addAll(this.generateTableDataFinished(sub_generator));
            } // FOR
        }
        return (finishedTables);
    }
    
    private void generateTableDataLoop(AbstractTableGenerator generator) {
        final VoltTable volt_table = generator.getVoltTable();
        while (generator.hasMore()) {
            generator.generateBatch();
            this.loadTable(generator.getTableName(), volt_table, generator.getTableSize());
            volt_table.clearRowData();

            // Dependent Tables
            if (generator.hasSubGenerators()) {
                if (debug.get())
                    LOG.debug("Invoking " + generator.sub_generators.size() + " sub-generators for " + generator.getTableName());
                for (AbstractTableGenerator sub_generator : generator.sub_generators) {
                    sub_generator.init();
                    this.generateTableDataLoop(sub_generator);
                } // FOR
            }
        } // WHILE
        return;
    }
    
    /**
     * The method that actually loads the VoltTable into the database Can be overridden for testing purposes.
     * 
     * @param tableName
     * @param table
     */
    protected void loadTable(String tableName, VoltTable table, Long expectedTotal) {
        long count = table.getRowCount();
        long current = this.profile.getTableSize(tableName);
        
        if (debug.get()) 
            LOG.debug(String.format("%s: Loading %d rows - %d / %s [bytes=%d]",
                                    tableName, count, current, (expectedTotal != null ? expectedTotal.toString() : "-----"),
                                    table.getUnderlyingBufferSize()));

        // Load up this dirty mess...
        try {
            this.getClientHandle().callProcedure("@LoadMultipartitionTable", tableName, table);
        } catch (Exception e) {
            throw new RuntimeException("Error when trying load data for '" + tableName + "'", e);
        }
        this.profile.addToTableSize(tableName, count);
    }

    // ----------------------------------------------------------------
    // DATA GENERATION
    // ----------------------------------------------------------------

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
         * Some generators have children tables that we want to load tuples for each batch of this generator. For
         * example, for each ITEM batch, we want to generate ITEM_BID tuples.
         */
        protected final List<AbstractTableGenerator> sub_generators = new ArrayList<AbstractTableGenerator>();

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
         * 
         * @param catalog_tbl
         */
        public AbstractTableGenerator(String tableName, String...dependencies) {
            this.tableName = tableName;
            this.catalog_tbl = AuctionMarkLoader.this.getTableCatalog(tableName);
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
                        this.tableSize = Math.round(this.tableSize / AuctionMarkLoader.this.profile.getScaleFactor());
                    }
                } catch (NoSuchFieldException ex) {
                    if (debug.get()) LOG.warn("No table size field for '" + tableName + "'", ex);
                } catch (Exception ex) {
                    throw new RuntimeException("Missing field needed for '" + tableName + "'", ex);
                } 
            } 
            
            for (Column catalog_col : this.catalog_tbl.getColumns()) {
                if (random_str_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() == VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_str_cols.add(catalog_col);
                    if (trace.get()) LOG.trace("Random String Column: " + catalog_col.fullName());
                }
                else if (random_int_regex.matcher(catalog_col.getName()).matches()) {
                    assert(catalog_col.getType() != VoltType.STRING.getValue()) : catalog_col.fullName();
                    this.random_int_cols.add(catalog_col);
                    if (trace.get()) LOG.trace("Random Integer Column: " + catalog_col.fullName());
                }
            } // FOR
            if (debug.get()) {
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
         * @return TODO
         */
        protected abstract int populateRow();
        
        public void run() {
            // Sub-generators should not be run in separate threads
            assert(isSubGenerator(this) == false) : "Unexpected threaded execution for '" + tableName + "'";
            
            // First block on the CountDownLatches of all the tables that we depend on
            if (this.dependencyTables.size() > 0 && debug.get())
                LOG.debug(String.format("Table generator for '%s' is blocked waiting for %d other tables: %s",
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
        
        protected int populateRandomColumns(Object row[]) {
            int cols = 0;
            
            // STRINGS
            for (Column catalog_col : this.random_str_cols) {
                int size = catalog_col.getSize();
                row[catalog_col.getIndex()] = rng.astring(rng.nextInt(size - 1), size);
                cols++;
            } // FOR
            
            // INTEGER
            for (Column catalog_col : this.random_int_cols) {
                row[catalog_col.getIndex()] = rng.number(0, 1<<30);
                cols++;
            } // FOR
            
            return (cols);
        }
        
        
        @SuppressWarnings("unchecked")
        public <T extends AbstractTableGenerator> T addSubGenerator(AbstractTableGenerator...generators) {
            CollectionUtil.addAll(this.sub_generators, generators);
            return ((T)this);
        }

        public boolean hasSubGenerators() {
            return (!this.sub_generators.isEmpty());
        }
        public Collection<AbstractTableGenerator> getSubGenerators() {
            return (this.sub_generators);
        }
        public Collection<String> getSubGeneratorTableNames() {
            List<String> names = new ArrayList<String>();
            for (AbstractTableGenerator gen : this.sub_generators) {
                names.add(gen.catalog_tbl.getName());
            }
            return (names);
        }

        /**
         * Returns true if this generator has more tuples that it wants to add
         * 
         * @return
         */
        public synchronized boolean hasMore() {
            return (this.count < this.tableSize);
        }

        /**
         * Return the table's catalog object for this generator
         * 
         * @return
         */
        public Table getTableCatalog() {
            return (this.catalog_tbl);
        }

        /**
         * Return the VoltTable handle
         * 
         * @return
         */
        public VoltTable getVoltTable() {
            return this.table;
        }

        /**
         * Returns the number of tuples that will be loaded into this table
         * 
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
            if (debug.get()) LOG.debug("Generating new batch for " + this.getTableName());
            long batch_count = 0;
            try {
                while (this.hasMore() && this.table.getRowCount() < this.batchSize) {
                    this.addRow();
                    batch_count++;
                } // WHILE
            } catch (ClassCastException ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
            if (debug.get()) LOG.debug("Finished generating new batch of " + batch_count + " tuples for " + this.getTableName());
        }

        public void markAsFinished(){
        	this.latch.countDown();
        }
        
        public boolean isFinish(){
        	return (this.latch.getCount() == 0);
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }
    } // END CLASS

    /**********************************************************************************************
     * SubItemTableGenerator
     * This is for tables that are based off of the ITEM table
     **********************************************************************************************/
    protected abstract class SubItemTableGenerator extends AbstractTableGenerator {
        
        private Iterator<ItemInfo> itemInfoItr;
        private ItemInfo currentItemInfo;
        private short currentNumItemElements;

        public SubItemTableGenerator(String tableName) {
            super(tableName, AuctionMarkConstants.TABLENAME_ITEM);
        }
        
        protected abstract short getItemInfoField(ItemInfo itemInfo);
        protected abstract int populateRow(ItemInfo itemInfo, short remaining);
        
        @Override
        public void init() {
            long numElements = 0;
            this.itemInfoItr = AuctionMarkLoader.this.item_infos.iterator();
            while (this.itemInfoItr.hasNext()) {
                ItemInfo itemInfo = this.itemInfoItr.next();
                numElements += this.getItemInfoField(itemInfo);
            } // WHILE
            this.currentItemInfo = null;
            this.currentNumItemElements = 0;

            this.tableSize = numElements;
            this.itemInfoItr = AuctionMarkLoader.this.item_infos.iterator();
        }
        @Override
        public final boolean hasMore() {
            return (this.getNextItemInfo() != null);
        }
        @Override
        protected final int populateRow() {
            ItemInfo itemInfo = this.getNextItemInfo();
            assert(itemInfo != null);
            this.currentNumItemElements--;
            return (this.populateRow(itemInfo, this.currentNumItemElements));
        }
        private final ItemInfo getNextItemInfo() {
            ItemInfo last = this.currentItemInfo;
            if (this.currentItemInfo == null || this.currentNumItemElements == 0) {
                assert(this.itemInfoItr != null) : "Missing ItemInfo Iterator?";
                while (this.itemInfoItr.hasNext() && this.currentNumItemElements == 0) {
                    this.currentItemInfo = this.itemInfoItr.next();
                    this.currentNumItemElements = this.getItemInfoField(this.currentItemInfo);
                } // WHILE
                if (this.currentNumItemElements == 0) return (null);
            }
            if (last != this.currentItemInfo) this.newItemInfoCallback(this.currentItemInfo);
            return this.currentItemInfo;
        }
        protected void newItemInfoCallback(ItemInfo itemInfo) {
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
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * CATEGORY Generator
     **********************************************************************************************/
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        // private final Map<String, Long> name_id_xref = new HashMap<String, Long>();
        private final Map<String, Category> categoryMap;
        private Iterator<String> categoryKeyItr;

        public CategoryGenerator(File data_file) {
            super(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.data_file = data_file;

            assert (this.data_file.exists()) : "The data file for the category generator does not exist: " + this.data_file;

            this.categoryMap = (new AuctionMarkCategoryParser(data_file)).getCategoryMap();
            this.categoryKeyItr = this.categoryMap.keySet().iterator();
            this.tableSize = new Long(this.categoryMap.size());
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            String category_key = this.categoryKeyItr.next();
            Category category = this.categoryMap.get(category_key);

            Long category_id = new Long(category.getCategoryID());
            String category_name = category.getName();
            Long parent_id = new Long(category.getParentCategoryID());
            Long item_count = new Long(category.getItemCount());
            // this.name_id_xref.put(category_name, this.count);

            boolean leaf_node = category.isLeaf();
            if (leaf_node) {
                AuctionMarkLoader.this.profile.item_category_histogram.put(category_id, item_count.intValue());
            }

            // C_ID
            this.row[col++] = category_id;
            // C_NAME
            this.row[col++] = category_name;
            // C_PARENT_ID
            this.row[col++] = parent_id;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_GROUP Generator
     **********************************************************************************************/
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;

        public GlobalAttributeGroupGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP,
                  AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_CATEGORY);
        }
        @Override
        protected int populateRow() {
            int col = 0;

            // GAG_ID
            this.row[col++] = new Integer((int) this.count);
            // GAG_C_ID
            this.row[col++] = AuctionMarkLoader.this.rng.number(0, (int)this.num_categories);
            // GAG_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * GLOBAL_ATTRIBUTE_VALUE Generator
     **********************************************************************************************/
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private Zipf zipf;

        public GlobalAttributeValueGenerator() {
            super(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE,
                  AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
            this.zipf = new Zipf(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_GROUP, 1.001);
        }

        @Override
        public void init() {
            // Nothing to do
        }
        @Override
        protected int populateRow() {
            int col = 0;

            long GAV_ID = new Integer((int) this.count);
            long GAV_GAG_ID = this.zipf.nextInt(); 
            
            profile.addGAGIdGAVIdPair(GAV_GAG_ID, GAV_ID);
            
            // GAV_ID
            this.row[col++] = GAV_ID;
            // GAV_GAG_ID
            this.row[col++] = GAV_GAG_ID;
            // GAV_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER Generator
     **********************************************************************************************/
    protected class UserGenerator extends AbstractTableGenerator {
        private final Zipf randomBalance;
        private final Flat randomRegion;
        private final Gaussian randomRating;
        private UserIdGenerator idGenerator;
        
        public UserGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER,
                  AuctionMarkConstants.TABLENAME_REGION);
            this.randomRegion = new Flat(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Gaussian(AuctionMarkLoader.this.rng, 0, 6);
            this.randomBalance = new Zipf(AuctionMarkLoader.this.rng, 0, 501, 1.001);
        }

        @Override
        public void init() {
            // Populate the profile's users per item count histogram so that we know how many
            // items that each user should have. This will then be used to calculate the
            // the user ids by placing them into numeric ranges
            Zipf randomNumItems = new Zipf(rng,
                                           AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
                                           AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER,
                                           1.001);
            for (long i = 0; i < this.tableSize; i++) {
                int num_items = randomNumItems.nextInt();
                profile.users_per_item_count.put(num_items);
            } // FOR
            if (trace.get())
                LOG.trace("Users Per Item Count:\n" + profile.users_per_item_count);
            this.idGenerator = new UserIdGenerator(profile.users_per_item_count, getNumClients());
            assert(this.idGenerator.hasNext());
        }
        @Override
        public void generateBatch() {
            super.generateBatch();
            AuctionMarkLoader.this.user_ids.clear();
        }
        @Override
        public synchronized boolean hasMore() {
            return this.idGenerator.hasNext();
        }
        @Override
        protected int populateRow() {
            int col = 0;

            UserId u_id = this.idGenerator.next();
            AuctionMarkLoader.this.user_ids.add(u_id);
            
            // U_ID
            this.row[col++] = u_id;
            // U_RATING
            this.row[col++] = this.randomRating.nextInt();
            // U_BALANCE
            this.row[col++] = (this.randomBalance.nextInt()) / 10.0;
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            // U_R_ID
            this.row[col++] = this.randomRegion.nextInt();
            
            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ATTRIBUTES Generator
     **********************************************************************************************/
    protected class UserAttributesGenerator extends AbstractTableGenerator {
        private final Zipf randomNumUserAttributes;
        private Iterator<UserId> userAttributeItr;
        private UserId currentUserId;
        private int currentNumUserAttributes;
        
        public UserAttributesGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES,
                  AuctionMarkConstants.TABLENAME_USER);
            
            this.randomNumUserAttributes = new Zipf(rng, AuctionMarkConstants.USER_MIN_ATTRIBUTES, AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);
        }

        @Override
        public void init() {
            // Loop through all the users from the current batch and generate attributes
            this.userAttributeItr = AuctionMarkLoader.this.user_ids.iterator();
            this.currentUserId = null;
            this.currentNumUserAttributes = 0;
        }
        @Override
        public synchronized boolean hasMore() {
            return (this.currentNumUserAttributes > 0 || this.userAttributeItr.hasNext());
        }
        @Override
        protected int populateRow() {
            int col = 0;

            if (this.currentNumUserAttributes == 0) {
                this.currentUserId = this.userAttributeItr.next();
                this.currentNumUserAttributes = randomNumUserAttributes.nextInt();
            }
            assert(this.currentUserId != null);
            
            // UA_ID
            this.row[col++] = new Integer((int) this.count);
            // UA_U_ID
            this.row[col++] = this.currentUserId;
            // UA_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // UA_VALUE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);
            
            this.currentNumUserAttributes--;
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM Generator
     **********************************************************************************************/
    protected class ItemGenerator extends AbstractTableGenerator {

        private UserIdGenerator sellerItemsItr;
        private int sellerNumItems = 0;
        private UserId currentSeller = null;
        
        // Current time in milliseconds
        private long currentTimestamp;

        /**
         * BidDurationDay -> Pair<NumberOfBids, NumberOfWatches>
         */
        private final Map<Long, Pair<Zipf, Zipf>> item_bid_watch_zipfs = new HashMap<Long, Pair<Zipf,Zipf>>();
        
        public ItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM,
                  AuctionMarkConstants.TABLENAME_CATEGORY, AuctionMarkConstants.TABLENAME_USER);
            
            this.tableSize = 0l;
            for (Integer size : profile.users_per_item_count.values()) {
                this.tableSize += size.intValue() * profile.users_per_item_count.get(size);
            } // FOR
        }
        
        @Override
        public void generateBatch() {
            // IMPORTANT: Make sure we clear out the ItemInfo collection so that our
            // sub-generators only work with the new ITEM records that we insert in this batch
            AuctionMarkLoader.this.item_infos.clear();
            super.generateBatch();
        }
        @Override
        public synchronized boolean hasMore() {
            return sellerItemsItr.hasNext();
        }
        @Override
        public void init() {
            long ct1 = Calendar.getInstance().getTimeInMillis();
            long ct2 = Math.round((double)ct1 / 1000);
            long ct3 = ct2 * 1000;
            this.currentTimestamp = ct3;
            this.sellerItemsItr = new UserIdGenerator(profile.users_per_item_count, getNumClients());
            this.sellerNumItems = 1;
            this.sellerItemsItr.setCurrentItemCount(this.sellerNumItems);
            this.currentSeller = null;
        }
        @Override
        protected int populateRow() {
            int col = 0;
            
            if (this.currentSeller == null || this.sellerNumItems == 0) {
                this.currentSeller = this.sellerItemsItr.next();
                this.sellerNumItems = (int)this.currentSeller.getItemCount();
            }
            assert(this.currentSeller != null);

            ItemInfo itemInfo = new ItemInfo(new ItemId(this.currentSeller, this.sellerNumItems));
            itemInfo.sellerId = this.currentSeller;
            itemInfo.end_date = this.getRandomEndTimestamp();
            itemInfo.start_date = this.getRandomStartTimestamp(itemInfo.end_date);
            itemInfo.initialPrice = profile.randomInitialPrice.nextInt();
            itemInfo.numImages = (short) profile.randomNumImages.nextInt();
            itemInfo.numAttributes = (short) profile.randomNumAttributes.nextInt();
            
            //LOG.info("endDate = " + endDate + " : startDate = " + startDate);
            long bidDurationDay = ((itemInfo.end_date.getTime() - itemInfo.start_date.getTime()) / AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
            if (this.item_bid_watch_zipfs.containsKey(bidDurationDay) == false) {
                Zipf randomNumBids = new Zipf(AuctionMarkLoader.this.rng,
                        AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY * (int)bidDurationDay,
                        1.001);
                Zipf randomNumWatches = new Zipf(AuctionMarkLoader.this.rng,
                        AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY * (int)bidDurationDay,
                        AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY * (int)bidDurationDay, 1.001);
                this.item_bid_watch_zipfs.put(bidDurationDay, Pair.of(randomNumBids, randomNumWatches));
            }
            Pair<Zipf, Zipf> p = this.item_bid_watch_zipfs.get(bidDurationDay);
            assert(p != null);
            
            // Calculate the number of bids and watches for this item
            itemInfo.numBids = (short)p.getFirst().nextInt();
            itemInfo.num_watches = (short)p.getSecond().nextInt();
            
            // The auction for this item has already closed
            if (itemInfo.end_date.getTime() <= this.currentTimestamp * 1000l) {
                itemInfo.still_available = false;
                if (itemInfo.numBids > 0) {
                	// Somebody won a bid and bought the item
                    itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
                    //System.out.println("@@@ z last_bidder_id = " + itemInfo.last_bidder_id);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.end_date);
                    itemInfo.numComments = (short) profile.randomNumComments.nextInt();
                    profile.addCompleteItem(itemInfo.id);
                }
            }
            // Item is still available
            else {
            	itemInfo.still_available = true;
            	profile.addAvailableItem(itemInfo.id);
            	if (itemInfo.numBids > 0) {
            		itemInfo.lastBidderId = profile.getRandomBuyerId(itemInfo.sellerId);
            	}
            }

            // I_ID
            this.row[col++] = itemInfo.id;

            // I_U_ID
            this.row[col++] = itemInfo.sellerId;

            // I_C_ID
            this.row[col++] = profile.getRandomCategoryId();

            // I_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);

            // I_DESCRIPTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(50, 255);

            // I_USER_ATTRIBUTES
            this.row[col++] = AuctionMarkLoader.this.rng.astring(20, 255);

            // I_INITIAL_PRICE
            this.row[col++] = itemInfo.initialPrice;

            // I_CURRENT_PRICE ?? reserve price
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
            this.row[col++] = itemInfo.start_date;

            // I_END_DATE
            this.row[col++] = itemInfo.end_date;

            // I_STATUS
            this.row[col++] = (itemInfo.still_available ? AuctionMarkConstants.STATUS_ITEM_OPEN : AuctionMarkConstants.STATUS_ITEM_CLOSED);

            AuctionMarkLoader.this.item_infos.add(itemInfo);
            return (col);
        }

        private TimestampType getRandomStartTimestamp(TimestampType endDate) {
            long duration = ((long)profile.randomDuration.nextInt()) * AuctionMarkConstants.MICROSECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            TimestampType startTimestamp = new TimestampType(lStartTimestamp);
            return startTimestamp;
        }
        private TimestampType getRandomEndTimestamp() {
            int timeDiff = profile.randomTimeDiff.nextInt();
            return new TimestampType(this.currentTimestamp * 1000 + timeDiff * 1000 * 1000);
        }
        private TimestampType getRandomPurchaseTimestamp(TimestampType endDate) {
            long duration = profile.randomPurchaseDuration.nextInt();
            return new TimestampType(endDate.getTime() + duration * AuctionMarkConstants.MICROSECONDS_IN_A_DAY);
        }
    }
    
    /**********************************************************************************************
     * ITEM_IMAGE Generator
     **********************************************************************************************/
    protected class ItemImageGenerator extends SubItemTableGenerator {

        public ItemImageGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_IMAGE);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return itemInfo.numImages;
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;

            // II_ID
            this.row[col++] = this.count;
            // II_I_ID
            this.row[col++] = itemInfo.id;
            // II_U_ID
            this.row[col++] = itemInfo.sellerId;

            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * ITEM_ATTRIBUTE Generator
     **********************************************************************************************/
    protected class ItemAttributeGenerator extends SubItemTableGenerator {

        public ItemAttributeGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_ATTRIBUTE);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return itemInfo.numAttributes;
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            Pair<Long, Long> gag_gav = profile.getRandomGAGIdGAVIdPair();
            assert(gag_gav != null);
            
            // IA_ID
            this.row[col++] = this.count;
            // IA_I_ID
            this.row[col++] = itemInfo.id;
            // IA_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IA_GAV_ID
            this.row[col++] = gag_gav.getSecond();
            // IA_GAG_ID
            this.row[col++] = gag_gav.getFirst();

            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * ITEM_COMMENT Generator
     **********************************************************************************************/
    protected class ItemCommentGenerator extends SubItemTableGenerator {

        public ItemCommentGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_COMMENT);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (itemInfo.purchaseDate != null ? itemInfo.numComments : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;

            // IC_ID
            this.row[col++] = new Integer((int) this.count);
            // IC_I_ID
            this.row[col++] = itemInfo.id;
            // IC_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IC_BUYER_ID
            this.row[col++] = itemInfo.lastBidderId;
            // IC_DATE
            this.row[col++] = this.getRandomCommentDate(itemInfo.start_date, itemInfo.end_date);
            // IC_QUESTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);
            // IC_RESPONSE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            int start = Math.round(startDate.getTime() / 1000000);
            int end = Math.round(endDate.getTime() / 1000000);
            return new TimestampType((rng.number(start, end)) * 1000 * 1000);
        }
    }

    /**********************************************************************************************
     * ITEM_BID Generator
     **********************************************************************************************/
    protected class ItemBidGenerator extends SubItemTableGenerator {

        private ItemInfo.Bid bid = null;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private boolean new_item;
        
        public ItemBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_BID);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (itemInfo.numBids);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
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
                if (itemInfo.still_available) {
                    endDate = new TimestampType();
                } else {
                    endDate = itemInfo.end_date;
                }
                this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.start_date.getTime()) / (remaining + 1);
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
                Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
                bidderId = profile.getRandomBuyerId(bidderHistogram, this.bid.bidderId, itemInfo.sellerId);
            }
            assert(bidderId != null);

            float last_bid = (this.new_item ? itemInfo.initialPrice : this.bid.maxBid);
            this.bid = itemInfo.getNextBid(this.count, bidderId);
            this.bid.maxBid = last_bid + this.currentBidPriceAdvanceStep;    
            this.bid.createDate = new TimestampType(itemInfo.start_date.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            
            if (remaining == 0 && itemInfo.purchaseDate != null) {
                assert(itemInfo.getBidCount() == itemInfo.numBids) : String.format("%d != %d\n%s",
                                                                                   itemInfo.getBidCount(), itemInfo.numBids, itemInfo);
                profile.addWaitForPurchaseItem(itemInfo.id);
            }
            
            // IB_ID
            this.row[col++] = new Long(this.bid.id);
            // IB_I_ID
            this.row[col++] = itemInfo.id;
            // IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IB_BUYER_ID
            this.row[col++] = this.bid.bidderId;
            // IB_BID
            this.row[col++] = this.bid.maxBid - (this.currentBidPriceAdvanceStep/2.0f);
            // IB_MAX_BID
            this.row[col++] = this.bid.maxBid;
            // IB_CREATED
            this.row[col++] = this.bid.createDate;
            // IB_UPDATED
            this.row[col++] = this.bid.updateDate;

            return (col);
        }
        @Override
        protected void newItemInfoCallback(ItemInfo itemInfo) {
            this.new_item = true;
            this.bid = null;
        }
    }

    /**********************************************************************************************
     * ITEM_MAX_BID Generator
     **********************************************************************************************/
    protected class ItemMaxBidGenerator extends SubItemTableGenerator {

        public ItemMaxBidGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.getLastBid().set_maxbid == false ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : "No bids?\n" + itemInfo;
            bid.set_maxbid = true;

            // IMB_I_ID
            this.row[col++] = itemInfo.id;
            // IMB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_IB_ID
            this.row[col++] = bid.id;
            // IMB_IB_I_ID
            this.row[col++] = itemInfo.id;
            // IMB_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IMB_CREATED
            this.row[col++] = bid.createDate;
            // IMB_UPDATED
            this.row[col++] = bid.updateDate;

            return (col);
        }
    }

    /**********************************************************************************************
     * ITEM_PURCHASE Generator
     **********************************************************************************************/
    protected class ItemPurchaseGenerator extends SubItemTableGenerator {

        public ItemPurchaseGenerator() {
            super(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null && itemInfo.getLastBid().set_purchase == false ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            bid.set_purchase = true;
            
            // IP_ID
            this.row[col++] = this.count;
            // IP_IB_ID
            this.row[col++] = bid.id;
            // IP_IB_I_ID
            this.row[col++] = itemInfo.id;
            // IP_IB_U_ID
            this.row[col++] = itemInfo.sellerId;
            // IP_DATE
            this.row[col++] = itemInfo.purchaseDate;

            if (rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_BUYER_LEAVES_FEEDBACK) {
                pending_buyer_feedback.add(bid);
            }
            if (rng.number(1, 100) <= AuctionMarkConstants.PROB_PURCHASE_SELLER_LEAVES_FEEDBACK) {
                pending_seller_feedback.add(bid);
            }
            
            return (col);
        }
    } // END CLASS
    
    /**********************************************************************************************
     * USER_FEEDBACK Generator
     **********************************************************************************************/
    protected class UserFeedbackGenerator extends AbstractTableGenerator {

        public UserFeedbackGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_FEEDBACK,
                  AuctionMarkConstants.TABLENAME_ITEM_PURCHASE);
        }

        @Override
        public void init() {
            this.tableSize = (long)(pending_buyer_feedback.size() + pending_seller_feedback.size());
        }
        @Override
        protected int populateRow() {
            int col = 0;

            boolean is_buyer = true;
            ItemInfo.Bid bid = pending_buyer_feedback.poll();
            if (bid == null) {
                bid = pending_seller_feedback.poll();
                is_buyer = false;
            }
            assert(bid != null);
            ItemInfo itemInfo = bid.getItemInfo();
            
            // UF_ID
            this.row[col++] = this.count;
            // UF_I_ID
            this.row[col++] = itemInfo.id;
            // UF_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UF_TO_ID
            this.row[col++] = (is_buyer ? bid.bidderId : itemInfo.sellerId);
            // UF_FROM_ID
            this.row[col++] = (is_buyer ? itemInfo.sellerId : bid.bidderId);
            // UF_RATING
            this.row[col++] = 1; // TODO
            // UF_DATE
            this.row[col++] = new TimestampType(); // Does this matter?

            return (col);
        }
    }

    /**********************************************************************************************
     * USER_ITEM Generator
     **********************************************************************************************/
    protected class UserItemGenerator extends SubItemTableGenerator {

        public UserItemGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_ITEM);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (short)(itemInfo.getBidCount() > 0 && itemInfo.purchaseDate != null && itemInfo.getLastBid().set_useritem == false ? 1 : 0);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            ItemInfo.Bid bid = itemInfo.getLastBid();
            assert(bid != null) : itemInfo;
            bid.set_useritem = true; 
            
            // UI_U_ID
            this.row[col++] = bid.bidderId;
            // UI_I_ID
            this.row[col++] = itemInfo.id;
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
            this.row[col++] = itemInfo.end_date;
            
            return (col);
        }
    } // END CLASS

    /**********************************************************************************************
     * USER_WATCH Generator
     **********************************************************************************************/
    protected class UserWatchGenerator extends SubItemTableGenerator {

        public UserWatchGenerator() {
            super(AuctionMarkConstants.TABLENAME_USER_WATCH);
        }
        @Override
        public short getItemInfoField(ItemInfo itemInfo) {
            return (itemInfo.num_watches);
        }
        @Override
        protected int populateRow(ItemInfo itemInfo, short remaining) {
            int col = 0;
            
            // Make it more likely that a user that has bid on an item is watching it
            Histogram<UserId> bidderHistogram = itemInfo.getBidderHistogram();
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
            this.row[col++] = itemInfo.id;
            // UW_I_U_ID
            this.row[col++] = itemInfo.sellerId;
            // UW_CREATED
            TimestampType endDate = (itemInfo.still_available ? new TimestampType() : itemInfo.end_date);
            this.row[col++] = this.getRandomCommentDate(itemInfo.start_date, endDate);

            return (col);
        }
        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            long start = (startDate.getTime() / 1000000);
            long end = (endDate.getTime() / 1000000);
            return new TimestampType((AuctionMarkLoader.this.rng.number(start, end) * 1000 * 1000));
        }
    } // END CLASS
} // END CLASS