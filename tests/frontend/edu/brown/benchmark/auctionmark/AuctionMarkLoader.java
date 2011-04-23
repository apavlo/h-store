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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.auctionmark.model.Bid;
import edu.brown.benchmark.auctionmark.model.AuctionMarkCategory;
import edu.brown.benchmark.auctionmark.model.ItemInfo;
import edu.brown.benchmark.auctionmark.util.AuctionMarkCategoryParser;
import edu.brown.rand.RandomDistribution.Flat;
import edu.brown.rand.RandomDistribution.FlatHistogram;
import edu.brown.rand.RandomDistribution.Gaussian;
import edu.brown.rand.RandomDistribution.Zipf;

/**
 * 
 * @author pavlo
 * @author visawee
 */
public class AuctionMarkLoader extends AuctionMarkBaseClient {

    // Data Generator Classes
    // TableName -> AbstactTableGenerator
    private final Map<String, AbstractTableGenerator> generators = new ListOrderedMap<String, AbstractTableGenerator>();

    final Lock dependencyLock = new ReentrantLock();
    final Condition dependencyCondition = this.dependencyLock.newCondition();

    // Temporary storage of the number of bids that we need to create per item
    private final Map<Long, ItemInfo> item_info = new HashMap<Long, ItemInfo>();

    // Temporary storage of the max bids generated from bid generator
    private final List<Bid> max_bids = new ArrayList<Bid>();
    
    // Counter used to determine when to print debug messages
    private final transient Map<String, Long> load_table_count = new HashMap<String, Long>();
    
    private static final long SECONDS_IN_A_DAY = 24 * 60 * 60;
    private static final long MILLISECONDS_IN_A_DAY = SECONDS_IN_A_DAY * 1000;
    private static final long MICROSECONDS_IN_A_DAY = MILLISECONDS_IN_A_DAY * 1000;
    
    private int numClients;

    /**
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String args[]) throws Exception {
        org.voltdb.benchmark.ClientMain.main(AuctionMarkLoader.class, args, true);
    }

    /**
     * Constructor
     * 
     * @param args
     */
    public AuctionMarkLoader(String[] args) {
        super(AuctionMarkLoader.class, args);

        numClients = this.getNumClients();
        assert(AuctionMarkConstants.MAXIMUM_NUM_CLIENTS > numClients);
        LOG.debug("AuctionMarkLoader::: numClients = " + numClients);
        
        // Data Generators
        this.generators.put(AuctionMarkConstants.TABLENAME_REGION, new RegionGenerator());

        // CATEGORY
        File category_file = new File(this.data_directory, "categories.txt");
        this.generators.put(AuctionMarkConstants.TABLENAME_CATEGORY, new CategoryGenerator(category_file));

        GlobalAttributeGroupGenerator globalAttributeGroupGenerator = new GlobalAttributeGroupGenerator();
        this.generators.put(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP, globalAttributeGroupGenerator);

        GlobalAttributeValueGenerator globalAttributeValueGenerator = new GlobalAttributeValueGenerator();
        this.generators.put(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE, globalAttributeValueGenerator);

        // USER TABLES
        UserGenerator userGenerator = new UserGenerator();
        this.generators.put(AuctionMarkConstants.TABLENAME_USER, userGenerator);

        UserAttributesGenerator userAttributesGenerator = new UserAttributesGenerator();
        this.generators.put(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES, userAttributesGenerator);

        // ITEM TABLES
        ItemGenerator itemGenerator = new ItemGenerator(this.profile);
        itemGenerator.addSubGenerator(new ItemImageGenerator());
        itemGenerator.addSubGenerator(new ItemCommentGenerator());
        itemGenerator.addSubGenerator(new ItemBidGenerator());
        itemGenerator.addSubGenerator(new ItemMaxBidGenerator());
        itemGenerator.addSubGenerator(new ItemPurchaseGenerator());
        itemGenerator.addSubGenerator(new UserItemGenerator());
        this.generators.put(AuctionMarkConstants.TABLENAME_ITEM, itemGenerator);

        // USER+ITEM TABLES

        UserWatchGenerator userWatchGenerator = new UserWatchGenerator();
        this.generators.put(AuctionMarkConstants.TABLENAME_USER_WATCH, userWatchGenerator);
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
        // Construct a new thread to load each table
        // TODO: Need to think about how to handle tables that may need to wait for earlier
        // tables to be constructed
        List<Thread> load_threads = new ArrayList<Thread>();
        for (final String tableName : this.generators.keySet()) {
            load_threads.add(new Thread() {
                @Override
                public void run() {
                    AuctionMarkLoader.this.generateTableData(tableName);
                }
            });
        } // FOR
        
        // Fire off the threads and wait for them to complete
        // If debug is set to true, then we'll execute them serially
        try {
            for (Thread thread : load_threads) {
                thread.start();
                if (this.debug) thread.join();
            } // FOR
            for (Thread thread : load_threads) {
                if (!this.debug) thread.join();
            } // FOR
        } catch (InterruptedException e) {
            LOG.fatal("Unexpected error", e);
            System.exit(-1);
        }
        
        this.saveProfile();
        LOG.info("Finished generating data for all tables");
    }

    /**
     * Load the tuples for the given table name
     * 
     * @param tableName
     */
    protected void generateTableData(String tableName) {
        final boolean debug = LOG.isDebugEnabled();
        
        // LOG.setLevel(Level.DEBUG);
        final AbstractTableGenerator generator = this.generators.get(tableName);
        assert (generator != null);
        VoltTable volt_table = generator.getVoltTable();

        for (String dependeningTable : generator.getDependencies()) {
            AbstractTableGenerator dependingGenerator = this.generators.get(dependeningTable);
            assert(dependingGenerator != null) : "The TableGenerator for " + dependeningTable + " is null!";
            try {
                this.dependencyLock.lock();
                while (!dependingGenerator.isFinish()) {
                    if (debug) LOG.debug("The generator for " + tableName + " is blocked waiting for "
                                         + dependingGenerator.getTableName() + " [" + this + "]");
                    this.dependencyCondition.await();
                } // WHILE
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                this.dependencyLock.unlock();
            }
        }

        generator.init();

        if (debug) LOG.debug("Loading " + generator.getTableSize() + " tuples for table '" + tableName + "'");
        while (generator.hasMore()) {
            generator.generateBatch();
            this.loadTable(tableName, volt_table);
            volt_table.clearRowData();

            // Dependent Tables
            if (generator.hasSubGenerators()) {
                if (debug) LOG.debug("Invoking " + generator.sub_generators.size() + " sub-generators for " + tableName);
                for (AbstractTableGenerator sub_generator : generator.sub_generators) {
                    // Always call init() for the sub-generator in each new round
                    sub_generator.init();
                    while (sub_generator.hasMore()) {
                        sub_generator.generateBatch();
                        this.loadTable(sub_generator.getTableName(), sub_generator.getVoltTable());
                        sub_generator.getVoltTable().clearRowData();
                    } // WHILE
                } // FOR
            }
        } // WHILE

        generator.setFinish(true);
        if(generator.hasSubGenerators()){
        	for (AbstractTableGenerator sub_generator : generator.sub_generators) {
        		sub_generator.setFinish(true);
        	}
        }
        
        this.dependencyLock.lock();
        this.dependencyCondition.signalAll();
        this.dependencyLock.unlock();
        LOG.debug("Finished loading all data for " + tableName + " [" + this + "]");
    }

    // ----------------------------------------------------------------
    // DATA GENERATION
    // ----------------------------------------------------------------

    protected abstract class AbstractTableGenerator {
        protected final Table catalog_tbl;
        protected final VoltTable table;
        protected Long tableSize;
        protected Long batchSize;
        protected boolean finish;
        
        protected final List<String> dependencyTables = new ArrayList<String>();

        /**
         * Some generators have children tables that we want to load tuples for each batch of this generator. For
         * example, for each ITEM batch, we want to generate ITEM_BID tuples.
         */
        protected final List<AbstractTableGenerator> sub_generators = new ArrayList<AbstractTableGenerator>();

        protected final Object[] row;
        protected long count = 0;

        /**
         * Constructor
         * 
         * @param catalog_tbl
         */
        public AbstractTableGenerator(Table catalog_tbl) {
            this.catalog_tbl = catalog_tbl;
            AuctionMarkLoader.this.load_table_count.put(catalog_tbl.getName(), 0l);
        
            this.finish = false;
            
            boolean fixed_size = AuctionMarkConstants.FIXED_TABLES.contains(catalog_tbl.getName());
            boolean dynamic_size = AuctionMarkConstants.DYNAMIC_TABLES.contains(catalog_tbl.getName());
            boolean data_file = AuctionMarkConstants.DATAFILE_TABLES.contains(catalog_tbl.getName());

            // Generate a VoltTable instance we can use
            this.table = CatalogUtil.getVoltTable(catalog_tbl);
            this.row = new Object[this.table.getColumnCount()];

            try {
                String field_name;
                Field field_handle;

                // Initialize dynamic parameters for tables that are not loaded
                // from data files
                if (!data_file && !dynamic_size) {
                    field_name = "TABLESIZE_" + catalog_tbl.getName();
                    field_handle = AuctionMarkConstants.class.getField(field_name);
                    assert (field_handle != null);
                    this.tableSize = (Long) field_handle.get(null);
                    if (!fixed_size) {
                        this.tableSize /= AuctionMarkLoader.this.profile.getScaleFactor();
                    }
                }

                field_name = "BATCHSIZE_" + catalog_tbl.getName();
                field_handle = AuctionMarkConstants.class.getField(field_name);
                assert (field_handle != null);
                this.batchSize = (Long) field_handle.get(null);
            } catch (Exception ex) {
                LOG.error(ex);
                System.exit(1);
            }
        }

        public void addSubGenerator(AbstractTableGenerator generator) {
            this.sub_generators.add(generator);
        }

        public boolean hasSubGenerators() {
            return (!this.sub_generators.isEmpty());
        }

        /**
         * Initiate data that need dependencies
         */
        public abstract void init();

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
         * 
         * @return
         */
        public Long getBatchSize() {
            return this.batchSize;
        }

        public String getTableName() {
            return this.catalog_tbl.getName();
        }

        /**
         * Returns the total number of tuples generated thusfar
         * 
         * @return
         */
        public synchronized long getCount() {
            return this.count;
        }

        /**
         * When called, the generator will populate a new row record and append it to the underlying VoltTable
         */
        public synchronized void addRow() {
            this.populateRow();
            this.count++;
            this.table.addRow(this.row);
        }

        /**
         * 
         */
        public void generateBatch() {
            LOG.debug("Generating new batch for " + this.getTableName());
            long batch_count = 0;
            while (this.hasMore() && this.table.getRowCount() < this.batchSize) {
                this.addRow();
                batch_count++;
            } // WHILE
            LOG.debug("Finished generating new batch of " + batch_count + " tuples for " + this.getTableName());
        }

        public void addDependency(String tableName) {
            for (String name : AuctionMarkConstants.TABLENAMES) {
                if (name.equals(tableName)) {
                    if (!this.dependencyTables.contains(tableName)) {
                        this.dependencyTables.add(tableName);
                    }
                    break;
                }
            }
        }

        public void setFinish(boolean finish){
        	this.finish = finish;
        }
        
        public boolean isFinish(){
        	return this.finish;
        }
        
        public List<String> getDependencies() {
            return this.dependencyTables;
        }

        /**
         * All sub-classes must implement this. This will enter new tuple data into the row
         */
        protected abstract void populateRow();
    } // END CLASS

    /**
     * REGION Generator
     */
    protected class RegionGenerator extends AbstractTableGenerator {

        public RegionGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_REGION));
        }

        @Override
        protected void populateRow() {
            int col = 0;

            // R_ID
            this.row[col++] = new Integer((int) this.count);
            // R_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);

            assert (col == this.table.getColumnCount());
        }

        @Override
        public void init() {
            // Nothing to do
        }
    } // END CLASS

    /**
     * CATEGORY Generator
     */
    protected class CategoryGenerator extends AbstractTableGenerator {
        private final File data_file;
        // private final Map<String, Long> name_id_xref = new HashMap<String, Long>();
        private final Map<String, AuctionMarkCategory> categoryMap;
        private Iterator<String> categoryKeyItr;

        public CategoryGenerator(File data_file) {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_CATEGORY));
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
        protected void populateRow() {
            int col = 0;

            String category_key = this.categoryKeyItr.next();
            AuctionMarkCategory category = this.categoryMap.get(category_key);

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

            assert (col == this.table.getColumnCount());
        }
    } // END CLASS

    /**
     * GLOBAL_ATTRIBUTE_GROUP Generator
     */
    protected class GlobalAttributeGroupGenerator extends AbstractTableGenerator {
        private long num_categories = 0l;

        public GlobalAttributeGroupGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP));
            this.addDependency(AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        public void init() {
            // Grab the number of CATEGORY items that we have inserted
            this.num_categories = AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_CATEGORY);
        }

        @Override
        protected void populateRow() {
            int col = 0;

            // GAG_ID
            this.row[col++] = new Integer((int) this.count);
            // GAG_C_ID
            this.row[col++] = AuctionMarkLoader.this.rng.number(0, (int)this.num_categories);
            // GAG_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);

            assert (col == this.table.getColumnCount());

        }
    }

    /**
     * GLOBAL_ATTRIBUTE_VALUE Generator
     */
    protected class GlobalAttributeValueGenerator extends AbstractTableGenerator {

        private Zipf zipf;

        public GlobalAttributeValueGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_VALUE));
            this.zipf = new Zipf(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_GLOBAL_ATTRIBUTE_GROUP, 1.001);
            this.addDependency(AuctionMarkConstants.TABLENAME_GLOBAL_ATTRIBUTE_GROUP);
        }

        @Override
        public void init() {
            // Nothing to do
        }

        @Override
        protected void populateRow() {
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

            assert (col == this.table.getColumnCount());

        }

    }

    /**
     * USER Generator
     */
    protected class UserGenerator extends AbstractTableGenerator {

        private Zipf randomBalance;
        private Flat randomRegion;
        private Gaussian randomRating;
        private PartitionIdGenerator idGenerator;
        
        public UserGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_USER));
            this.addDependency(AuctionMarkConstants.TABLENAME_REGION);
            this.idGenerator = new PartitionIdGenerator(numClients, 0, AuctionMarkConstants.MAXIMUM_CLIENT_IDS);
            this.randomRegion = new Flat(AuctionMarkLoader.this.rng, 0, (int) AuctionMarkConstants.TABLESIZE_REGION);
            this.randomRating = new Gaussian(AuctionMarkLoader.this.rng, 0, 6);
            this.randomBalance = new Zipf(AuctionMarkLoader.this.rng, 0, 501, 1.001);
        }

        @Override
        public void init() {
            // Nothing to do
        }

        @Override
        protected void populateRow() {
            int col = 0;

            long u_id = this.idGenerator.getNextId();
            profile.addUserId(u_id);
            
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
            // U_SATTR##
            for (int i = 0; i < 8; i++) {
                this.row[col++] = AuctionMarkLoader.this.rng.astring(16, 64);
            }

            assert (col == this.table.getColumnCount());
        }

    }

    /**
     * USER_ATTRIBUTES Generator
     */
    protected class UserAttributesGenerator extends AbstractTableGenerator {

        private Iterator<Map.Entry<Integer, Short>> userAttributeItr;
        private int currentUserIDIndex;
        private short currentNumUserAttributes;

        public UserAttributesGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_USER_ATTRIBUTES));
            this.addDependency(AuctionMarkConstants.TABLENAME_USER);
        }

        @Override
        public void init() {
            long numAttributes = 0;

            Zipf randomNumAttributes = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.USER_MIN_ATTRIBUTES,
                    AuctionMarkConstants.USER_MAX_ATTRIBUTES, 1.001);

            Map<Integer, Short> userAttributeMap = new HashMap<Integer, Short>();

            long numUsers = AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_USER);

            for (int i = 0; i < numUsers; i++) {
                short n = (short) (randomNumAttributes.nextInt());
                numAttributes += n;
                userAttributeMap.put(i, n);
            }

            this.currentUserIDIndex = -1;
            this.currentNumUserAttributes = 0;

            LOG.debug("Number of user attributes = " + numAttributes);

            this.tableSize = numAttributes;
            this.userAttributeItr = userAttributeMap.entrySet().iterator();
        }

        @Override
        protected void populateRow() {
            int col = 0;

            long userID = this.getNextUserID();
            assert (-1 != userID);

            // UA_ID
            this.row[col++] = new Integer((int) this.count);
            // UA_U_ID
            this.row[col++] = userID;
            // UA_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // UA_VALUE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(5, 32);
            // U_CREATED
            this.row[col++] = VoltTypeUtil.getRandomValue(VoltType.TIMESTAMP);

            assert (col == this.table.getColumnCount());
        }

        private long getNextUserID() {

            if (this.currentUserIDIndex == -1 || this.currentNumUserAttributes == 0) {
                while (this.userAttributeItr.hasNext() && this.currentNumUserAttributes == 0) {
                    Map.Entry<Integer, Short> entry = this.userAttributeItr.next();
                    this.currentUserIDIndex = entry.getKey();
                    this.currentNumUserAttributes = entry.getValue();
                }

                if (this.currentNumUserAttributes == 0) {
                    return -1;
                }
            }

            this.currentNumUserAttributes--;
            return profile.getUserId(this.currentUserIDIndex);
        }
    }

    /**
     * ITEM Generator
     */
    protected class ItemGenerator extends AbstractTableGenerator {

        private FlatHistogram<String> randomCategory;

        private Iterator<Map.Entry<Integer, Integer>> sellerItemsItr;
        // Entry for <user id index in the profiler, number of items this user sell>
        private Map.Entry<Integer, Integer> sellerItems;

        // Current time in milliseconds
        private long currentTimestamp;
        // Random time different in seconds
        private Gaussian randomTimeDiff;
        // Random duration in days
        private Gaussian randomDuration;

        private Zipf randomInitialPrice, randomNumImages, randomNumAttrs, randomBuyer, randomNumItems,
                randomPurchaseDuration, randomNumComments;

        public ItemGenerator(AuctionMarkBenchmarkProfile profile) {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM));

            this.addDependency(AuctionMarkConstants.TABLENAME_CATEGORY);
            this.addDependency(AuctionMarkConstants.TABLENAME_USER);
        }
        
        @Override
        public void generateBatch() {
            // IMPORTANT: Make sure we clear out the ItemInfo collection so that our
            // sub-generators only work with the new ITEM records that we insert in this batch
            AuctionMarkLoader.this.item_info.clear();
            AuctionMarkLoader.this.max_bids.clear();
            
            super.generateBatch();
        }

        @Override
        public void init() {
        	
            this.randomCategory = new FlatHistogram(AuctionMarkLoader.this.rng, AuctionMarkLoader.this.profile.item_category_histogram);
            long ct1 = Calendar.getInstance().getTimeInMillis();
            long ct2 = Math.round((double)ct1 / 1000);
            long ct3 = ct2 * 1000;
            this.currentTimestamp = ct3;

            // Random time difference in a second scale
            this.randomTimeDiff = new Gaussian(AuctionMarkLoader.this.rng, -AuctionMarkConstants.ITEM_PRESERVE_DAYS * 24 * 60 * 60,
                    AuctionMarkConstants.ITEM_MAX_DURATION_DAYS * 24 * 60 * 60);
            this.randomDuration = new Gaussian(AuctionMarkLoader.this.rng, 1, AuctionMarkConstants.ITEM_MAX_DURATION_DAYS);

            this.randomPurchaseDuration = new Zipf(AuctionMarkLoader.this.rng, 0,
                    AuctionMarkConstants.ITEM_MAX_PURCHASE_DURATION_DAYS, 1.001);

            this.randomInitialPrice = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.ITEM_MIN_INITIAL_PRICE,
                    AuctionMarkConstants.ITEM_MAX_INITIAL_PRICE, 1.001);

            this.randomNumItems = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.ITEM_MIN_ITEMS_PER_SELLER,
                    AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER, 1.500);

            this.randomNumImages = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.ITEM_MIN_IMAGES,
                    AuctionMarkConstants.ITEM_MAX_IMAGES, 1.001);
            this.randomNumAttrs = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.ITEM_MIN_GLOBAL_ATTRS,
                    AuctionMarkConstants.ITEM_MAX_GLOBAL_ATTRS, 1.001);

            this.randomNumComments = new Zipf(AuctionMarkLoader.this.rng, AuctionMarkConstants.ITEM_MIN_COMMENTS,
                    AuctionMarkConstants.ITEM_MAX_COMMENTS, 1.001);

            int numUsers = (int) AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_USER);
            this.randomBuyer = new Zipf(AuctionMarkLoader.this.rng, 0, numUsers, 1.001);

            // Map from (Number of items a person hold) -> (Number of persons in this group)
            Map<Integer, Integer> numItemDistribution = new HashMap<Integer, Integer>();
            // Map from seller id index -> # items hold by that seller
            Map<Integer, Integer> itemDistribution = new HashMap<Integer, Integer>();

            long numItems = this.tableSize.longValue();

            LOG.debug("ITEM : numItems = " + numItems);
            
            long tempNumItems = numItems;

            for (int i = 1; i < AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER; i++) {
                numItemDistribution.put(i, 0);
            }

            LOG.debug(numItemDistribution.containsKey(172));

            int numSellers;

            for (numSellers = 1; numSellers <= numUsers; numSellers++) {
                int n = this.randomNumItems.nextInt();
                if (tempNumItems > n) {
                    numItemDistribution.put(n, numItemDistribution.remove(n) + 1);
                    tempNumItems -= n;
                } else {
                    numItemDistribution.put((int) tempNumItems, numItemDistribution.remove((int) tempNumItems)
                            .intValue() + 1);
                    break;
                }
            }

            /*
             * try{ // Create file FileWriter fstream = new FileWriter("/home/visawee/numItemStat.txt"); BufferedWriter
             * out = new BufferedWriter(fstream);
             * 
             * for(int i=1; i<AuctionMarkConstants.ITEM_MAX_ITEMS_PER_SELLER; i++){ out.write(numItemDistribution.get(i) +
             * "\n"); }
             * 
             * //Close the output stream out.close(); } catch(Exception e){ e.printStackTrace(); }
             */

            LOG.trace("Seller = " + ((float) numSellers / (float) numUsers) * 100 + "%");

            int sellerIDIndex = 0;
            
            for (int i = 1; i < numItemDistribution.size(); i++) {
                int numSellersInGroup = numItemDistribution.get(i);
                for (int j = 0; j < numSellersInGroup; j++) {
                    itemDistribution.put(sellerIDIndex, i);
                    sellerIDIndex++;
                }
            }

            int n=0;
            for (Map.Entry<Integer, Integer> entry : itemDistribution.entrySet()){
            	n += entry.getValue();
            }

            LOG.trace("r num items = " + n);
            
            /*
             * 
             * Map<Integer, Integer> m = new HashMap<Integer, Integer>(); for(int i=0; i<1000; i++){ m.put(i, 0); }
             * 
             * Zipf z = new Zipf(rng,0,1000,1.001); for(int i=0; i<1000000; i++){ int r = z.nextInt(); m.put(r,
             * m.remove(r) + 1); }
             * 
             * for(int i=0; i<1000; i++){ LOG.info(i + ", " + m.get(i)); }
             */

            this.sellerItemsItr = itemDistribution.entrySet().iterator();
        }

        @Override
        protected void populateRow() {
            int col = 0;
            long sellerID = this.getNextUserID();

            ItemInfo itemInfo = new ItemInfo();

            TimestampType endDate = this.getRandomEndTimestamp();
            itemInfo.endDate = endDate;
            TimestampType startDate = this.getRandomStartTimestamp(endDate);
            itemInfo.startDate = startDate;
            //LOG.info("endDate = " + endDate + " : startDate = " + startDate);
            long bidDurationDay = ((endDate.getTime() - startDate.getTime()) / MICROSECONDS_IN_A_DAY);
            //LOG.info("bidDurationDay = " + bidDurationDay);

            Zipf randomNumBids = new Zipf(AuctionMarkLoader.this.rng,
                    AuctionMarkConstants.ITEM_MIN_BIDS_PER_DAY * (int)bidDurationDay,
                    AuctionMarkConstants.ITEM_MAX_BIDS_PER_DAY * (int)bidDurationDay,
                    1.001);
            itemInfo.num_bids = (short) randomNumBids.nextInt();
            
            Long itemID = new Long((int) this.count);
            
            int itemStatus;
            
            // The auction for this item has already closed
            if (itemInfo.endDate.getTime() <= this.currentTimestamp * 1000l) {
                itemInfo.still_available = false;
                if (itemInfo.num_bids > 0) {
                	// Somebody won a bid and bought the item
                    itemInfo.last_bidder_id = new Long(this.randomBuyer.nextInt());
                    //System.out.println("@@@ z last_bidder_id = " + itemInfo.last_bidder_id);
                    itemInfo.purchaseDate = this.getRandomPurchaseTimestamp(itemInfo.endDate);
                    itemInfo.num_comments = (short) this.randomNumComments.nextInt();
                    profile.addCompleteItem(sellerID, itemID);
                }
                itemStatus = 2;
            } else {
            	// Item is still available
            	itemInfo.still_available = true;
            	profile.addAvailableItem(sellerID, itemID);
            	itemStatus = 0;
            	
            	if (itemInfo.num_bids > 0) {
            		itemInfo.last_bidder_id = new Long(this.randomBuyer.nextInt());
            	}
            }

            // I_ID
            this.row[col++] = itemID;

            // I_U_ID
            itemInfo.seller_id = sellerID;
            assert (itemInfo.seller_id > -1);
            this.row[col++] = new Long(itemInfo.seller_id);

            // I_C_ID
            this.row[col++] = this.randomCategory.nextValue();

            // I_NAME
            this.row[col++] = AuctionMarkLoader.this.rng.astring(6, 32);

            // I_DESCRIPTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(50, 255);

            // I_USER_ATTRIBUTES
            this.row[col++] = AuctionMarkLoader.this.rng.astring(20, 255);

            // I_INITIAL_PRICE
            itemInfo.initialPrice = this.randomInitialPrice.nextInt();
            this.row[col++] = itemInfo.initialPrice;

            Zipf randomNumWatches = new Zipf(AuctionMarkLoader.this.rng,
                    AuctionMarkConstants.ITEM_MIN_WATCHES_PER_DAY * (int)bidDurationDay,
                    AuctionMarkConstants.ITEM_MAX_WATCHES_PER_DAY * (int)bidDurationDay, 1.001);
            itemInfo.num_watches = (short) randomNumWatches.nextInt();

            // I_CURRENT_PRICE ?? reserve price
            if (itemInfo.num_bids > 0) {
                itemInfo.currentPrice = itemInfo.initialPrice + (itemInfo.num_bids * itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP);
                this.row[col++] = itemInfo.currentPrice;
            } else {
                this.row[col++] = itemInfo.initialPrice;
            }

            // I_NUM_BIDS
            this.row[col++] = itemInfo.num_bids;

            // I_NUM_IMAGES
            itemInfo.num_images = (short) this.randomNumImages.nextInt();
            this.row[col++] = itemInfo.num_images;

            // I_NUM_GLOBAL_ATTRS
            itemInfo.num_attributes = (short) this.randomNumAttrs.nextInt();
            this.row[col++] = itemInfo.num_attributes;

            // I_START_DATE
            this.row[col++] = startDate;

            // I_END_DATE
            this.row[col++] = endDate;

            // I_STATUS
            this.row[col++] = itemStatus;
            
            AuctionMarkLoader.this.item_info.put(itemID, itemInfo);

            assert (col == this.table.getColumnCount());
        }
        
        @Override
        public synchronized boolean hasMore() {
            if (this.sellerItems == null || this.sellerItems.getValue() == 0) {
                assert(this.sellerItemsItr != null) : "The seller items iterator is null for " + this.getTableName();
                if (this.sellerItemsItr.hasNext()) {
                    return (true);
                } else {
                	return (false);
                }
            } else {
            	return (true);
            }
        }

        private long getNextUserID() {
            if (this.sellerItems == null || this.sellerItems.getValue() == 0) {
                if (this.sellerItemsItr.hasNext()) {
                    this.sellerItems = this.sellerItemsItr.next();
                } else {
                    return -1;
                }
            }
            this.sellerItems.setValue(this.sellerItems.getValue() - 1);
            return profile.getUserId(this.sellerItems.getKey());
        }

        private TimestampType getRandomStartTimestamp(TimestampType endDate) {
            long duration = ((long)this.randomDuration.nextInt()) * MICROSECONDS_IN_A_DAY;
            long lStartTimestamp = endDate.getTime() - duration;
            TimestampType startTimestamp = new TimestampType(lStartTimestamp);
            return startTimestamp;
        }

        private TimestampType getRandomEndTimestamp() {
            int timeDiff = this.randomTimeDiff.nextInt();
            return new TimestampType(this.currentTimestamp * 1000 + timeDiff * 1000 * 1000);
        }

        private TimestampType getRandomPurchaseTimestamp(TimestampType endDate) {
            long duration = this.randomPurchaseDuration.nextInt();
            return new TimestampType(endDate.getTime() + duration * MICROSECONDS_IN_A_DAY);
        }

    }

    /**
     * ITEM_IMAGE Generator
     */
    protected class ItemImageGenerator extends AbstractTableGenerator {

        private Iterator<Map.Entry<Long, ItemInfo>> itemInfoItr;
        private long currentItemInfoID;
        private short currentNumItemImages;

        public ItemImageGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM_IMAGE));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM);
        }

        @Override
        public void init() {
            long numImages = 0;

            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
            while (this.itemInfoItr.hasNext()) {
                ItemInfo itemInfo = this.itemInfoItr.next().getValue();
                numImages += itemInfo.num_images;
            }

            this.currentItemInfoID = -1;
            this.currentNumItemImages = 0;

            LOG.debug("numImages = " + numImages);

            this.tableSize = numImages;
            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
        }
        
        @Override
        public synchronized boolean hasMore() {
            return (this.getNextItemInfoID() != -1);
        }

        @Override
        protected void populateRow() {
            int col = 0;

            long itemInfoID = this.getNextItemInfoID();
            assert (-1 != itemInfoID);
            this.currentNumItemImages--;
            ItemInfo itemInfo = AuctionMarkLoader.this.item_info.get(itemInfoID);
            assert (itemInfo != null);

            // II_ID
            this.row[col++] = new Integer((int) this.count);
            // II_I_ID
            this.row[col++] = itemInfoID;
            // II_U_ID
            this.row[col++] = itemInfo.seller_id;
            // II_PATH
            this.row[col++] = AuctionMarkLoader.this.rng.astring(20, 100);

            assert (col == this.table.getColumnCount());
        }

        private long getNextItemInfoID() {
            if (this.currentItemInfoID == -1 || this.currentNumItemImages == 0) {
                while (this.itemInfoItr.hasNext() && this.currentNumItemImages == 0) {
                    Map.Entry<Long, ItemInfo> entry = this.itemInfoItr.next();
                    this.currentItemInfoID = entry.getKey();
                    this.currentNumItemImages = entry.getValue().num_images;
                }
                if (this.currentNumItemImages == 0) {
                    return -1;
                }
            }
            return this.currentItemInfoID;
        }
    } // END CLASS

    /**
     * ITEM_COMMENT Generator
     */
    protected class ItemCommentGenerator extends AbstractTableGenerator {

        private Iterator<Map.Entry<Long, ItemInfo>> itemInfoItr;
        private long currentItemInfoID;
        private short currentNumItemComments;

        public ItemCommentGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM_COMMENT));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM);
            this.addDependency(AuctionMarkConstants.TABLENAME_USER);
        }

        @Override
        public void init() {
            long numComments = 0;

            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
            while (this.itemInfoItr.hasNext()) {
                ItemInfo itemInfo = this.itemInfoItr.next().getValue();
                if (null != itemInfo.purchaseDate) {
                    numComments += itemInfo.num_comments;
                }
            }

            LOG.debug("numComments = " + numComments);

            this.currentItemInfoID = -1;
            this.currentNumItemComments = 0;

            this.tableSize = numComments;
            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
        }
        
        @Override
        public synchronized boolean hasMore() {
            return (this.getNextItemInfoID() != -1);
        }

        @Override
        protected void populateRow() {
            int col = 0;

            long itemInfoID = this.getNextItemInfoID();
            assert (-1 != itemInfoID);
            this.currentNumItemComments--;

            ItemInfo itemInfo = AuctionMarkLoader.this.item_info.get(itemInfoID);

            // IC_ID
            this.row[col++] = new Integer((int) this.count);
            // IC_I_ID
            this.row[col++] = itemInfoID;
            // IC_U_ID
            this.row[col++] = itemInfo.seller_id;
            // IC_BUYER_ID
            this.row[col++] = itemInfo.last_bidder_id;
            // IC_DATE
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, itemInfo.endDate);
            // IC_QUESTION
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);
            // IC_RESPONSE
            this.row[col++] = AuctionMarkLoader.this.rng.astring(10, 128);

            assert (col == this.table.getColumnCount());
        }

        private long getNextItemInfoID() {

            if (this.currentItemInfoID == -1 || this.currentNumItemComments == 0) {
                while (this.itemInfoItr.hasNext() && this.currentNumItemComments == 0) {
                    Map.Entry<Long, ItemInfo> entry = this.itemInfoItr.next();
                    this.currentItemInfoID = entry.getKey();
                    ItemInfo itemInfo = entry.getValue();
                    this.currentNumItemComments = itemInfo.num_comments;
                }

                if (this.currentNumItemComments == 0) {
                    return -1;
                }
            }
            return this.currentItemInfoID;
        }

        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            return new TimestampType((AuctionMarkLoader.this.rng.number((int) (startDate.getTime() / 1000000),
                    (int) (endDate.getTime() / 1000000)) * 1000 * 1000));
        }
    }

    /**
     * ITEM_BID Generator
     * 
     * CreateDate VS. UpdateDate
     * 
     */
    protected class ItemBidGenerator extends AbstractTableGenerator {

        private Zipf randomBuyerIndex;

        private Iterator<Map.Entry<Long, ItemInfo>> itemInfoItr;

        private Bid bid;
        private short currentNumBids;
        private float currentBidPriceAdvanceStep;
        private long currentCreateDateAdvanceStep;
        private long currentCount;
        
        public ItemBidGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM_BID));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM);
            this.addDependency(AuctionMarkConstants.TABLENAME_USER);
        }

        @Override
        public void init() {
            int numUsers = (int) AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_USER);
            this.randomBuyerIndex = new Zipf(AuctionMarkLoader.this.rng, 0, numUsers, 1.001);
            
            long numBids = 0;

            Map<Long, ItemInfo> itemInfoWithBid = new HashMap<Long, ItemInfo>();
            
            //this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
            
            for(Map.Entry<Long, ItemInfo> entry: AuctionMarkLoader.this.item_info.entrySet()){
                ItemInfo itemInfo = entry.getValue();
                numBids += itemInfo.num_bids;
                if(itemInfo.num_bids > 0){
                	itemInfoWithBid.put(entry.getKey(), itemInfo);
                }
            }
            
            /*
            while (this.itemInfoItr.hasNext()) {
            	Map.Entry<Long, V>
            	
                ItemInfo itemInfo = this.itemInfoItr.next().getValue();
                numBids += itemInfo.num_bids;
                if(itemInfo.num_bids > 0){
                	itemInfoWithBid.put(key, value)
                }
            }
            */
            
            

            LOG.debug("total number of bids = " + numBids);
            LOG.debug("total number of items = " + AuctionMarkLoader.this.item_info.size());
            LOG.debug("threshold = " + ((float) numBids / (float) AuctionMarkLoader.this.item_info.size()));

            this.currentNumBids = 0;
            this.currentCount = 0;

            this.tableSize = numBids;
            //this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
            this.itemInfoItr = itemInfoWithBid.entrySet().iterator();

            this.bid = null;
        }
        
        @Override
        public synchronized boolean hasMore() {
        	
        	if (LOG.isDebugEnabled() && this.currentCount > 0 && this.currentCount % 100 == 0){
            	LOG.debug("ITEM BID : currentCount = " + this.currentCount + " - tableSize = " + this.tableSize);
        	}
            return (this.currentCount < this.tableSize);
        }

        @Override
        protected void populateRow() {
            int col = 0;

            this.populateNextBid();
            assert (null != this.bid);
            
            // IB_ID
            this.row[col++] = new Long(this.bid.id);
            // IB_I_ID
            this.row[col++] = this.bid.itemId;
            // IB_U_ID
            this.row[col++] = this.bid.sellerId;
            // IB_BUYER_ID
            this.row[col++] = this.bid.bidderId;
            // IB_BID
            this.row[col++] = this.bid.maxBid - (this.currentBidPriceAdvanceStep/2);
            // IB_MAX_BID
            this.row[col++] = this.bid.maxBid;
            // IB_CREATED
            this.row[col++] = this.bid.createDate;
            // IB_UPDATED
            this.row[col++] = this.bid.updateDate;

            this.currentCount++;
            
            assert (col == this.table.getColumnCount());
        }

        private boolean populateNextBid() {

            if (null == this.bid || this.currentNumBids == 0) {
                while (this.itemInfoItr.hasNext() && this.currentNumBids == 0) {
                    Map.Entry<Long, ItemInfo> entry = this.itemInfoItr.next();
                    ItemInfo itemInfo = entry.getValue();
                    this.currentNumBids = itemInfo.num_bids;
                    if (this.currentNumBids > 0) {
                        this.bid = new Bid();
                        this.bid.itemId = entry.getKey();
                        this.bid.sellerId = itemInfo.seller_id;
                        
                        this.bid.maxBid = itemInfo.initialPrice;
                        this.bid.createDate = itemInfo.startDate;

                        TimestampType endDate;
                        if (itemInfo.still_available) {
                            endDate = new TimestampType();
                        } else {
                            endDate = itemInfo.endDate;
                        }
                        this.currentCreateDateAdvanceStep = (endDate.getTime() - itemInfo.startDate.getTime())
                                / this.currentNumBids;
                        this.currentBidPriceAdvanceStep = itemInfo.initialPrice * AuctionMarkConstants.ITEM_BID_PERCENT_STEP;
                    }
                }

                if (this.currentNumBids == 0) {
                    this.bid = null;
                    return false;
                }
            }

            this.bid.id = this.count;
            this.bid.maxBid += this.currentBidPriceAdvanceStep;
            this.bid.createDate = new TimestampType(this.bid.createDate.getTime() + this.currentCreateDateAdvanceStep);
            this.bid.updateDate = this.bid.createDate; 
            this.currentNumBids--;

            this.bid = this.bid.clone();

            if (0 == this.currentNumBids) {
                ItemInfo itemInfo = AuctionMarkLoader.this.item_info.get(this.bid.itemId);
                this.bid.bidderId = itemInfo.last_bidder_id;
                profile.addWaitForPurchaseItem(this.bid.sellerId, this.bid.itemId, this.bid.bidderId, this.bid.bidderId);
                if (null != itemInfo.purchaseDate) {
                    this.bid.won = true;
                }
                AuctionMarkLoader.this.max_bids.add(this.bid);
            } else {
                this.bid.bidderId = profile.getUserId(this.randomBuyerIndex.nextInt());
            }

            return true;
        }
    }

    /**
     * ITEM_MAX_BID Generator
     * 
     * Needs review
     */
    protected class ItemMaxBidGenerator extends AbstractTableGenerator {

        private Iterator<Bid> bidItr;

        public ItemMaxBidGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM_MAX_BID));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public void init() {
            this.bidItr = AuctionMarkLoader.this.max_bids.iterator();
            this.tableSize = new Long(AuctionMarkLoader.this.max_bids.size());
        }
        
        @Override
        public synchronized boolean hasMore() {
            return (this.bidItr.hasNext());
        }

        @Override
        protected void populateRow() {
            int col = 0;

            assert (this.bidItr.hasNext());
            Bid bid = this.bidItr.next();

            // IMB_I_ID
            this.row[col++] = bid.itemId;
            // IMB_U_ID
            this.row[col++] = bid.sellerId;
            // IMB_IB_ID
            this.row[col++] = bid.id;
            // IMB_IB_I_ID
            this.row[col++] = bid.itemId;
            // IMB_IB_U_ID
            this.row[col++] = bid.sellerId;
            // IMB_CREATED
            this.row[col++] = bid.createDate;
            // IMB_UPDATED
            this.row[col++] = bid.updateDate;

            assert (col == this.table.getColumnCount());
        }
    }

    /**
     * ITEM_PURCHASE Generator
     * 
     */
    protected class ItemPurchaseGenerator extends AbstractTableGenerator {

        private Iterator<Bid> bidItr;

        public ItemPurchaseGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_ITEM_PURCHASE));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public void init() {
            this.bidItr = AuctionMarkLoader.this.max_bids.iterator();

            long purchaseCount = 0;
            
            while (this.bidItr.hasNext()) {
                if (this.bidItr.next().won) {
                    purchaseCount++;
                }
            }
            
            this.bidItr = AuctionMarkLoader.this.max_bids.iterator();
            this.tableSize = purchaseCount;
        }

        @Override
        protected void populateRow() {
            int col = 0;

            Bid bid = null;

            while (null == bid && this.bidItr.hasNext()) {
                bid = this.bidItr.next();
                if (bid.won) {
                    break;
                } else {
                    bid = null;
                }
            }
            assert (null != bid);
            
            // IP_ID
            this.row[col++] = new Integer((int) this.count);
            // IP_IB_ID
            this.row[col++] = bid.id;
            // IP_IB_I_ID
            this.row[col++] = bid.itemId;
            // IP_IB_U_ID
            this.row[col++] = bid.sellerId;
            // IP_DATE
            this.row[col++] = AuctionMarkLoader.this.item_info.get(bid.itemId).purchaseDate;

            assert (col == this.table.getColumnCount());
        }
    }

    /**
     * USER_ITEM Generator
     * 
     */
    protected class UserItemGenerator extends AbstractTableGenerator {

        private Iterator<Bid> bidItr;

        public UserItemGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_USER_ITEM));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM_BID);
        }

        @Override
        public void init() {
            this.bidItr = AuctionMarkLoader.this.max_bids.iterator();

            long purchaseCount = 0;

            while (this.bidItr.hasNext()) {
                if (this.bidItr.next().won) {
                    purchaseCount++;
                }
            }

            this.bidItr = AuctionMarkLoader.this.max_bids.iterator();
            
            this.tableSize = purchaseCount;
        }

        @Override
        protected void populateRow() {
            int col = 0;

            Bid bid = null;

            while (null == bid && this.bidItr.hasNext()) {
                bid = this.bidItr.next();
                if (bid.won) {
                    break;
                } else {
                    bid = null;
                }
            }
            assert (null != bid);
            
            // UI_U_ID
            this.row[col++] = bid.bidderId;
            // UI_I_ID
            this.row[col++] = bid.itemId;
            // UI_I_U_ID
            this.row[col++] = bid.sellerId;
            // UI_CREATED
            this.row[col++] = AuctionMarkLoader.this.item_info.get(bid.itemId).endDate;

            assert (col == this.table.getColumnCount());
        }
    }

    /**
     * USER_WATCH Generator
     * 
     */
    protected class UserWatchGenerator extends AbstractTableGenerator {
        private Iterator<Map.Entry<Long, ItemInfo>> itemInfoItr;
        private long currentItemInfoID;
        private short currentNumWatches;

        private Zipf randomBuyerIndex;

        public UserWatchGenerator() {
            super(AuctionMarkLoader.this.getTableCatalog(AuctionMarkConstants.TABLENAME_USER_WATCH));
            this.addDependency(AuctionMarkConstants.TABLENAME_ITEM);
            this.addDependency(AuctionMarkConstants.TABLENAME_USER);
        }

        @Override
        public void init() {
            int numUsers = (int) AuctionMarkLoader.this.profile.getTableSize(AuctionMarkConstants.TABLENAME_USER);
            this.randomBuyerIndex = new Zipf(AuctionMarkLoader.this.rng, 0, numUsers, 1.001);

            long numWatches = 0;

            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
            while (this.itemInfoItr.hasNext()) {
                ItemInfo itemInfo = this.itemInfoItr.next().getValue();
                numWatches += itemInfo.num_watches;
            }

            this.currentItemInfoID = -1;
            this.currentNumWatches = 0;

            this.tableSize = numWatches;
            this.itemInfoItr = AuctionMarkLoader.this.item_info.entrySet().iterator();
        }

        @Override
        protected void populateRow() {
            int col = 0;

            long itemInfoID = this.getNextItemInfoID();
            assert (-1 != itemInfoID);

            ItemInfo itemInfo = AuctionMarkLoader.this.item_info.get(itemInfoID);

            long buyerId = profile.getUserId(this.randomBuyerIndex.nextInt());
            
            // UW_U_ID
            this.row[col++] = buyerId;
            // UW_I_ID
            this.row[col++] = itemInfoID;
            // UW_I_U_ID
            this.row[col++] = itemInfo.seller_id;
            // UW_CREATED
            TimestampType endDate;
            if (itemInfo.still_available) {
                endDate = new TimestampType();
            } else {
                endDate = itemInfo.endDate;
            }
            this.row[col++] = this.getRandomCommentDate(itemInfo.startDate, endDate);

            assert (col == this.table.getColumnCount());
        }

        private long getNextItemInfoID() {

            if (this.currentItemInfoID == -1 || this.currentNumWatches == 0) {
                while (this.itemInfoItr.hasNext() && this.currentNumWatches == 0) {
                    Map.Entry<Long, ItemInfo> entry = this.itemInfoItr.next();
                    this.currentItemInfoID = entry.getKey();
                    ItemInfo itemInfo = entry.getValue();
                    this.currentNumWatches = itemInfo.num_watches;
                }

                if (this.currentNumWatches == 0) {
                    return -1;
                }
            }

            this.currentNumWatches--;
            return this.currentItemInfoID;
        }

        private TimestampType getRandomCommentDate(TimestampType startDate, TimestampType endDate) {
            return new TimestampType((AuctionMarkLoader.this.rng.number((int) (startDate.getTime() / 1000000),
                    (int) (endDate.getTime() / 1000000)) * 1000 * 1000));
        }
    }

    /**
     * The method that actually loads the VoltTable into the database Can be overridden for testing purposes.
     * 
     * @param tableName
     * @param table
     */
    protected void loadTable(String tableName, VoltTable table) {
        long count = table.getRowCount();
        long total = this.profile.getTableSize(tableName);
        long last_reported = this.load_table_count.get(tableName);
        if ((total - last_reported) > 1000) {
            LOG.debug(String.format(tableName + ": loading %d rows (total %d)", count, total));
            this.load_table_count.put(tableName, total);
        }

        // Load up this dirty mess...
        try {
            this.m_voltClient.callProcedure("@LoadMultipartitionTable", tableName, table);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
        this.profile.addToTableSize(tableName, count);
    }
    
    @Override
    public String getApplicationName() {
        return "AuctionMark Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Loader";
    }
} // END CLASS