package edu.brown.utils;

import java.util.*;

import org.junit.Ignore;
import org.junit.Test;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.benchmark.tpcc.TPCCConstants;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.catalog.*;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.hashing.*;
import edu.brown.hstore.HStoreConstants;

/**
 * Simple PartitionEstimator Tests
 * @author pavlo
 */
public class TestPartitionEstimator extends BaseTestCase {

    protected static AbstractHasher hasher;
    protected static final int NUM_PARTITIONS = 100;
    protected static final int BASE_PARTITION = 1;
    
    private final PartitionSet partitions = new PartitionSet();
    
    private final TPCCProjectBuilder builder = new TPCCProjectBuilder() {
        {
            addAllDefaults();
            addStmtProcedure("SinglePartitionOR",
                             "SELECT * FROM " + TPCCConstants.TABLENAME_DISTRICT +
                             " WHERE D_W_ID = ? AND (D_STREET_1 = ? OR D_STREET_2 = ?)");
            addStmtProcedure("MultiPartitionOR",
                             "SELECT * FROM " + TPCCConstants.TABLENAME_DISTRICT +
                             " WHERE D_W_ID = ? OR D_W_ID = ?");
            addStmtProcedure("ConstantOR",
                             "SELECT * FROM " + TPCCConstants.TABLENAME_DISTRICT +
                             " WHERE D_W_ID = " + BASE_PARTITION + " OR D_W_ID = ?");
            addStmtProcedure("ConstantRange",
                             "SELECT * FROM " + TPCCConstants.TABLENAME_DISTRICT +
                             " WHERE D_W_ID > " + BASE_PARTITION);
        }
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(builder);
        this.addPartitions(NUM_PARTITIONS);
        if (hasher == null) {
            hasher = new DefaultHasher(catalogContext, NUM_PARTITIONS); // CatalogUtil.getNumberOfPartitions(catalog_db));
        }
        
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_WAREHOUSE);
        Column catalog_col = this.getColumn(catalog_tbl, "W_ID");
        catalog_tbl.setPartitioncolumn(catalog_col);
        this.partitions.clear();
    }
    
    /**
     * testSinglePartitionOR
     */
    public void testSinglePartitionOR() throws Exception {
        // Check that if we have a query that does a look up on a partitioning column
        // but has an OR in it, it's still a single-partition query.
        Procedure catalog_proc = this.getProcedure("SinglePartitionOR");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        
        Object params[] = new Object[] { BASE_PARTITION, "XXX", "YYY" };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertEquals(1, partitions.size());
        assertEquals(BASE_PARTITION, partitions.get());
        
    }
    
    /**
     * testMultiPartitionOR
     */
    public void testMultiPartitionOR() throws Exception {
        // Check that if we have a query that does a look up on a partitioning column
        // but has an OR in it, then it has to be sent to exactly the number of partitions
        // that we expected
        Procedure catalog_proc = this.getProcedure("MultiPartitionOR");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        
        Object params[] = new Object[] { BASE_PARTITION, BASE_PARTITION+1 };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertEquals(2, partitions.size());
    }
    
    /**
     * testConstantOR
     */
    public void testConstantOR() throws Exception {
        // Check that if we have a query that does a look up on a partitioning column
        // using a constant but then it has an OR in it, then it has to be sent to 
        // exactly the number of partitions that we expected
        Procedure catalog_proc = this.getProcedure("ConstantOR");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        
//        System.err.println(PlanNodeUtil.debug(PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true)));
        
        Object params[] = new Object[] { BASE_PARTITION };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertEquals(1, partitions.size());
        assertEquals(BASE_PARTITION, partitions.get());
        
        partitions.clear();
        params = new Object[] { BASE_PARTITION+1 };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertEquals(2, partitions.size());
    }
    
    /**
     * testConstantRange
     */
    public void testConstantRange() throws Exception {
        // Check that if we have a query that does a look up on a partitioning column
        // using a constant value with a range expression, the it should be sent
        // to all partitions in the cluster
        Procedure catalog_proc = this.getProcedure("ConstantRange");
        Statement catalog_stmt = CollectionUtil.first(catalog_proc.getStatements());
        assertNotNull(catalog_stmt);
        
        Object params[] = new Object[] { };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertEquals(catalogContext.getAllPartitionIds(), partitions);
    }
    
    /**
     * testMultiAttributePartitioning
     */
    public void testMultiAttributePartitioning() throws Exception {
        // This checks that the ProcParameters and the StmtParameters get mapped to
        // the same partition for a multi-attribute partitioned Procedure+Table
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);

        // Procedure
        Procedure catalog_proc = this.getProcedure(clone_db, paymentByCustomerId.class);
        ProcParameter catalog_params[] = new ProcParameter[] {
            this.getProcParameter(clone_db, catalog_proc, 1),   // D_ID
            this.getProcParameter(clone_db, catalog_proc, 0),   // W_ID
        };
        MultiProcParameter mpp = MultiProcParameter.get(catalog_params);
        assertNotNull(mpp);
        assert(mpp.getIndex() >= 0);
        catalog_proc.setPartitionparameter(mpp.getIndex());

        // Table
        Table catalog_tbl = this.getTable(clone_db, TPCCConstants.TABLENAME_DISTRICT);
        String table_key = CatalogKey.createKey(catalog_tbl);
        Column catalog_cols[] = new Column[] {
            this.getColumn(clone_db, catalog_tbl, "D_ID"),
            this.getColumn(clone_db, catalog_tbl, "D_W_ID"),
        };
        MultiColumn mc = MultiColumn.get(catalog_cols);
        assertNotNull(mc);
        catalog_tbl.setPartitioncolumn(mc);
        p_estimator.initCatalog(clone_catalogContext);
        
        // Procedure Partition
        Long proc_params[] = new Long[catalog_proc.getParameters().size()];
        proc_params[0] = new Long(NUM_PARTITIONS-1); // W_ID
        proc_params[1] = new Long(BASE_PARTITION);   // D_ID
        int proc_partition = p_estimator.getBasePartition(catalog_proc, proc_params, true);
        assert(proc_partition != HStoreConstants.NULL_DEPENDENCY_ID);
        assert(proc_partition >= 0);
        assert(proc_partition < NUM_PARTITIONS);
        
        // Statement Partition
        Statement catalog_stmt = this.getStatement(clone_db, catalog_proc, "getDistrict");
        Long stmt_params[] = new Long[] {
            new Long(proc_params[0].longValue()), // W_ID
            new Long(proc_params[1].longValue()), // D_ID
        };
        Map<String, PartitionSet> stmt_partitions = p_estimator.getTablePartitions(catalog_stmt, stmt_params, proc_partition);
        System.err.println(StringUtil.formatMaps(stmt_partitions));
        assertNotNull(stmt_partitions);
        assertEquals(1, stmt_partitions.size());
        assert(stmt_partitions.containsKey(table_key));
        assertEquals(1, stmt_partitions.get(table_key).size());
        assertFalse(stmt_partitions.get(table_key).contains(HStoreConstants.NULL_PARTITION_ID));
        assertEquals(stmt_partitions.get(table_key).toString(), proc_partition, CollectionUtil.first(stmt_partitions.get(table_key)).intValue());
    }
    
    /**
     * testMultiProcParameter
     */
    public void testMultiProcParameter() throws Exception {
        //  Try to partition TPC-C's neworder on W_ID and D_ID parameters
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        Procedure catalog_proc = this.getProcedure(clone_db, neworder.class);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);

        ProcParameter catalog_params[] = new ProcParameter[] {
            this.getProcParameter(clone_db, catalog_proc, 0),   // W_ID
            this.getProcParameter(clone_db, catalog_proc, 1),   // D_ID
        };
        MultiProcParameter mpp = MultiProcParameter.get(catalog_params);
        assertNotNull(mpp);
        assert(mpp.getIndex() >= 0);
        catalog_proc.setPartitionparameter(mpp.getIndex());
        p_estimator.initCatalog(clone_catalogContext);
        
        // Case #1: Both parameters have values in the input
        Long params[] = new Long[catalog_proc.getParameters().size()];
        params[0] = new Long(NUM_PARTITIONS-1); // W_ID
        params[1] = new Long(BASE_PARTITION);   // D_ID
        int partition0 = p_estimator.getBasePartition(catalog_proc, params, true);
        assert(partition0 != HStoreConstants.NULL_DEPENDENCY_ID);
        assert(partition0 >= 0);
//        System.err.println("partition0=" + partition0);
        assert(partition0 < NUM_PARTITIONS);
        
        // Case #2: The second parameter is null
        params = new Long[catalog_proc.getParameters().size()];
        params[0] = new Long(NUM_PARTITIONS-1); // W_ID
        params[1] = null; // D_ID
        int partition1 = p_estimator.getBasePartition(catalog_proc, params, true);
        assert(partition1 != HStoreConstants.NULL_DEPENDENCY_ID);
        assert(partition1 >= 0);
        assert(partition1 < NUM_PARTITIONS);
//        System.err.println("partition1=" + partition1);
        assertFalse(partition0 == partition1);
        
        // Case #3: The first parameter is null
        params = new Long[catalog_proc.getParameters().size()];
        params[0] = null; // W_ID
        params[1] = new Long(BASE_PARTITION);   // D_ID
        int partition2 = p_estimator.getBasePartition(catalog_proc, params, true);
        assert(partition2 != HStoreConstants.NULL_DEPENDENCY_ID);
        assert(partition2 >= 0);
        assert(partition2 < NUM_PARTITIONS);
//        System.err.println("partition2=" + partition2);
        assertNotSame(partition0, partition2);
    }
    
    
    /**
     * testMultiColumn
     */
    public void testMultiColumn() throws Exception {
        // Try to use multi-column partitioning on DISTRICT and see whether we can
        // actually get the right answer for neworder.getDistrict
        Database clone_db = CatalogCloner.cloneDatabase(catalogContext.database);
        CatalogContext clone_catalogContext = new CatalogContext(clone_db.getCatalog());
        Table catalog_tbl = this.getTable(clone_db, TPCCConstants.TABLENAME_DISTRICT);
        String table_key = CatalogKey.createKey(catalog_tbl);
        
        Column catalog_col0 = this.getColumn(clone_db, catalog_tbl, "D_ID");
        Column catalog_col1 = this.getColumn(clone_db, catalog_tbl, "D_W_ID");
        MultiColumn mc = MultiColumn.get(catalog_col0, catalog_col1);
        catalog_tbl.setPartitioncolumn(mc);
//        System.err.println("MUTLI COLUMN: " + mc);
        
        Procedure catalog_proc = this.getProcedure(clone_db, neworder.class);
        Statement catalog_stmt = this.getStatement(clone_db, catalog_proc, "getDistrict");

        Long params[] = new Long[] {
            new Long(BASE_PARTITION), // D_ID
            new Long(NUM_PARTITIONS - 1), // D_W_ID
        };
        PartitionEstimator p_estimator = new PartitionEstimator(clone_catalogContext);
        Map<String, PartitionSet> p = p_estimator.getTablePartitions(catalog_stmt, params, BASE_PARTITION);
        assertNotNull(p);
        
        // Just check to make sure that we get back exactly one partition
        assert(p.containsKey(table_key));
        assertEquals("Unexpected result: " + p.get(table_key), 1, p.get(table_key).size());
        int stmt_partition = CollectionUtil.first(p.get(table_key));
        assert(stmt_partition >= 0) : "Invalid Partition: " + p.get(table_key);
        
        // Now create VoltTable and test that
        VoltTable vt = new VoltTable(new ColumnInfo[] {
            new ColumnInfo(catalog_col0.getName(), VoltType.get(catalog_col0.getType())),   // D_ID 
            new ColumnInfo(catalog_col1.getName(), VoltType.get(catalog_col1.getType())),   // D_W_ID
        });
        vt.addRow(params[0], params[1]);
        VoltTableRow vt_row = vt.fetchRow(0);
        int vt_partition = p_estimator.getTableRowPartition(catalog_tbl, vt_row);
        assert(vt_partition >= 0) : "Invalid Partition: " + vt_partition;
        assertEquals(stmt_partition, vt_partition);
    }
    
    /**
     * testArrayParameters
     */
    public void testArrayParameters() throws Exception {
        // Grab TPC-C's neworder and pass an array of W_ID's to getWarehouseTaxRate
        // Check that we get back all of the partitions that we expect from the array
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_WAREHOUSE);
        String table_key = CatalogKey.createKey(catalog_tbl);
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getWarehouseTaxRate");
        
        int num_warehouses = 5;
        Long params_object[] = new Long[num_warehouses];
        long params_primitive[] = new long[num_warehouses];
        PartitionSet expected = new PartitionSet();
        for (int i = 0; i < num_warehouses; i++) {
            long w_id = NUM_PARTITIONS - i - 1;
            params_object[i] =  new Long(w_id);
            params_primitive[i] = w_id;
            expected.add((int)w_id);
        } // FOR
        assertEquals(params_object.length, expected.size());
        assertEquals(params_primitive.length, expected.size());
//        System.err.println("EXPECTED: " + expected);
        
        // OBJECT
        Map<String, PartitionSet> p = p_estimator.getTablePartitions(catalog_stmt, new Object[]{ params_object }, BASE_PARTITION);
        assertNotNull(p);
        assert(p.containsKey(table_key));
//        System.err.println("OBJECT: " + p.get(table_key));
        assertEquals(expected.size(), p.get(table_key).size());
        assertEquals(expected, p.get(table_key));
        
        // PRIMITIVE
        p = p_estimator.getTablePartitions(catalog_stmt, new Object[]{ params_object }, BASE_PARTITION);
        assertNotNull(p);
        assert(p.containsKey(table_key));
//        System.err.println("PRIMITIVE: " + p.get(table_key));
        assertEquals(expected.size(), p.get(table_key).size());
        assertEquals(expected, p.get(table_key));
        
    }
    
    /**
     * testSelect
     */
    public void testSelect() throws Exception {
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "getDistrict");
        assertNotNull(catalog_stmt);
//        System.err.println(CatalogUtil.getDisplayName(this.getTable(TPCCConstants.TABLENAME_DISTRICT).getPartitioncolumn()));
        
        for (int w_id = 1; w_id < NUM_PARTITIONS; w_id++) {
            Object params[] = new Integer[]{ 2, w_id }; // d_id, d_w_id
            PartitionEstimator estimator = new PartitionEstimator(catalogContext, hasher);
            partitions.clear();
            estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
            assertEquals(w_id, (int)CollectionUtil.first(partitions));
        } // FOR
    }
    
    /**
     * testGetPartitionsFragments
     */
    @Ignore
    public void testFragments() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        assertNotNull(catalog_tbl);
        Procedure catalog_proc = this.getProcedure("neworder");
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("getDistrict");
        assertNotNull(catalog_stmt);
        
        for (int w_id = 1; w_id < NUM_PARTITIONS; w_id++) {
            Object params[] = new Integer[]{ 2, w_id }; // d_id, d_w_id
            PartitionEstimator estimator = new PartitionEstimator(catalogContext, hasher);
            partitions.clear();
            estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
            assertEquals(w_id, (int)CollectionUtil.first(partitions));
        } // FOR
    }
    
    /**
     * testGetPartitionsInsert
     */
    public void testInsert() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_ORDERS);
        assertNotNull(catalog_tbl);
        Procedure catalog_proc = this.getProcedure("neworder");
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get("createOrder");
        assertNotNull(catalog_stmt);
        
        PartitionEstimator estimator = new PartitionEstimator(catalogContext, hasher);
        Object params[] = new Object[catalog_stmt.getParameters().size()];
        int w_id = 9;
        //  3001, 1, 9, 376, Mon Aug 10 00:28:54 EDT 2009, 0, 13, 1]
        for (int i = 0; i < params.length; i++) {
            StmtParameter catalog_param = catalog_stmt.getParameters().get(i);
            VoltType type = VoltType.get((byte)catalog_param.getJavatype());
            if (i == 2) {
                params[i] = w_id;
            } else {
                params[i] = VoltTypeUtil.getRandomValue(type);
            }
            //System.out.print((i != 0 ? ", " : "[") + params[i].toString() + (i + 1 == params.length ? "\n" : "")); 
        } // FOR
        
        estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
//        System.out.println(catalog_stmt.getName() + " Partitions: " + partitions);
        assertFalse(partitions.isEmpty());
        assertEquals(1, partitions.size());
        assertEquals(w_id, (int)CollectionUtil.first(partitions));
    }
    
    /**
     * testReplicatedSelect
     */
    @Test
    public void testReplicatedSelect() throws Exception {
        Catalog new_catalog = new Catalog();
        new_catalog.execute(catalogContext.catalog.serialize());
        CatalogContext new_catalogContext = new CatalogContext(new_catalog);
        final Table new_table = new_catalogContext.database.getTables().get(TPCCConstants.TABLENAME_WAREHOUSE);
        assertNotNull(new_table);
        new_table.setIsreplicated(true);
        new_table.setPartitioncolumn(new Column() {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends CatalogType> T getParent() {
                return ((T)new_table);
            }
        });
        
        PartitionEstimator p_estimator = new PartitionEstimator(new_catalogContext, hasher);
        Procedure catalog_proc = new_catalogContext.database.getProcedures().get(neworder.class.getSimpleName());
        Statement catalog_stmt = catalog_proc.getStatements().get("getWarehouseTaxRate");
        assertNotNull(catalog_stmt);
        
        // We should get back exactly one partition id (base_partition)
        Object params[] = new Object[] { new Long(1234) };
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        assertNotNull(partitions);
        assertEquals(1, partitions.size());
        assertEquals(BASE_PARTITION, CollectionUtil.first(partitions).intValue());
    }
    
    /**
     * testInvalidateCache
     */
    @Test
    public void testInvalidateCache() throws Exception {
        Catalog new_catalog = new Catalog();
        new_catalog.execute(catalogContext.catalog.serialize());
        CatalogContext new_catalogContext = new CatalogContext(new_catalog);
        
        final Table new_table = new_catalogContext.database.getTables().get(TPCCConstants.TABLENAME_WAREHOUSE);
        assertNotNull(new_table);
        new_table.setPartitioncolumn(new Column() {
            @SuppressWarnings("unchecked")
            @Override
            public <T extends CatalogType> T getParent() {
                return ((T)new_table);
            }
        });
        
        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext, hasher);
        Procedure catalog_proc = this.getProcedure(neworder.class);
        Statement catalog_stmt = catalog_proc.getStatements().get("getWarehouseTaxRate");
        assertNotNull(catalog_stmt);
        
        // First calculate the partitions for the query using the original catalog
        // We should get back exactly one partition id (base_partition)
        Object params[] = new Object[] { new Long(BASE_PARTITION) };
        for (int i = 0; i < 10000; i++) {
            String debug = String.format("Attempt #%05d", i); 
            this.partitions.clear();
            p_estimator.getAllPartitions(this.partitions, catalog_stmt, params, BASE_PARTITION);
            assertEquals(debug + " -> " + this.partitions.toString(), 1, this.partitions.size());
            assertEquals(debug, BASE_PARTITION, CollectionUtil.first(this.partitions).intValue());
            assertFalse(debug, this.partitions.contains(HStoreConstants.NULL_PARTITION_ID));
        } // FOR
        
        // Then reset the catalog in p_estimator and run the estimation again
        // The new catalog has a different partition column for WAREHOUSE, so we should get
        // back all the partitions
        p_estimator.initCatalog(new_catalogContext);
        catalog_proc = new_catalogContext.database.getProcedures().get(catalog_proc.getName());
        catalog_stmt = catalog_proc.getStatements().get("getWarehouseTaxRate");
        
        partitions.clear();
        p_estimator.getAllPartitions(partitions, catalog_stmt, params, BASE_PARTITION);
        PartitionSet all_partitions = new_catalogContext.getAllPartitionIds();
        assertNotNull(partitions);
        assertEquals(all_partitions.size(), partitions.size());
        assertFalse(partitions.contains(HStoreConstants.NULL_PARTITION_ID));
    }
    
    /**
     * testPopulateColumnJoinsAll
     */
    @Ignore
    public void testPopulateColumnJoinsAll() {
        Map<Column, Set<Column>> column_joins = new TreeMap<Column, Set<Column>>();
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        
        Column last = null;
        List<Column> order = new ArrayList<Column>();
        for (Column catalog_col : catalog_tbl.getColumns()) {
            column_joins.put(catalog_col, new TreeSet<Column>());
            if (last != null) {
                column_joins.get(last).add(catalog_col);
                column_joins.get(catalog_col).add(last);
            }
            last = catalog_col;
            order.add(catalog_col);
        } // FOR
        assertNotNull(last);
        
        PartitionEstimator.populateColumnJoinSets(column_joins);
        for (Column catalog_col : order) {
            assert(column_joins.containsKey(catalog_col));
            assertEquals(order.size() - 1, column_joins.get(catalog_col).size());
        } // FOR
    }
    
    /**
     * testPopulateColumnJoinsSplit
     */
    @SuppressWarnings("unchecked")
    public void testPopulateColumnJoinsSplit() {
        Map<Column, Set<Column>> column_joins = new TreeMap<Column, Set<Column>>();
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_DISTRICT);
        
        Column last[] = { null, null };
        @SuppressWarnings("rawtypes")
        HashSet split[] = {
            new HashSet<Column>(),
            new HashSet<Column>(),
        };
        
        int ctr = 0;
        for (Column catalog_col : catalog_tbl.getColumns()) {
            column_joins.put(catalog_col, new TreeSet<Column>());
            
            int idx = ctr++ % 2;
            if (last[idx] != null) {
                column_joins.get(last[idx]).add(catalog_col);
                column_joins.get(catalog_col).add(last[idx]);
            }
            last[idx] = catalog_col;
            split[idx].add(catalog_col);
        } // FOR
        assertNotNull(last);
//        System.err.println("split[0]: " + split[0]);
//        System.err.println("split[1]: " + split[1] + "\n");
        
        PartitionEstimator.populateColumnJoinSets(column_joins);
        ctr = 0;
        for (Column catalog_col : catalog_tbl.getColumns()) {
            assert(column_joins.containsKey(catalog_col));
            int idx = ctr++ % 2;
//            System.err.println(catalog_col + ": " + column_joins.get(catalog_col));
            assertEquals(split[idx].size() - 1, column_joins.get(catalog_col).size());
            
            column_joins.get(catalog_col).add(catalog_col);
            assert(column_joins.get(catalog_col).containsAll(split[idx]));
        } // FOR
    }
    
    /**
     * testGetTableRowPartitionNumeric
     */
    public void testGetTableRowPartitionNumeric() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_WAREHOUSE);
        Column catalog_col = this.getColumn(catalog_tbl, "W_ID");
        catalog_tbl.setPartitioncolumn(catalog_col);
        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext);
        
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_rows = 1000;
        Map<Long, Integer> expected = new HashMap<Long, Integer>();
        for (int i = 0; i < num_rows; i++) {
            Object row[] = new Object[catalog_tbl.getColumns().size()];
            for (int j = 0; j < row.length; j++) {
                Column col = catalog_tbl.getColumns().get(j);
                assertNotNull(col);
                VoltType vtype = VoltType.get(col.getType());
                if (col.equals(catalog_col)) {
                    long w_id = (i % NUM_PARTITIONS);
                    row[j] = w_id;
                    expected.put(w_id, p_estimator.getHasher().hash(w_id));
                }
                else row[j] = VoltTypeUtil.getRandomValue(vtype);
            } // FOR
            vt.addRow(row);
        } // FOR
        assertEquals(num_rows, vt.getRowCount());
        
        vt.resetRowPosition();
        while (vt.advanceRow()) {
            VoltTableRow row = vt.getRow();
            long w_id = row.getLong(catalog_col.getIndex());
            int p = p_estimator.getTableRowPartition(catalog_tbl, row);
            assert(p >= 0 && p <= NUM_PARTITIONS);
            
            Integer last = expected.get(w_id);
            assertNotNull(last);
            assertEquals(last.intValue(), p);
        } // WHILE
    }
    
    /**
     * testGetTableRowPartitionString
     */
    public void testGetTableRowPartitionString() throws Exception {
        Table catalog_tbl = this.getTable(TPCCConstants.TABLENAME_WAREHOUSE);
        Column catalog_col = this.getColumn(catalog_tbl, "W_NAME");
        catalog_tbl.setPartitioncolumn(catalog_col);
        PartitionEstimator p_estimator = new PartitionEstimator(catalogContext);
        
        Map<String, Integer> expected = new HashMap<String, Integer>();
        VoltTable vt = CatalogUtil.getVoltTable(catalog_tbl);
        int num_rows = 1000;
        for (int i = 0; i < num_rows; i++) {
            Object row[] = new Object[catalog_tbl.getColumns().size()];
            for (int j = 0; j < row.length; j++) {
                Column col = catalog_tbl.getColumns().get(j);
                assertNotNull(col);
                VoltType vtype = VoltType.get(col.getType());
                if (col.equals(catalog_col)) {
                    String name = "WAREHOUSE-" + (i % NUM_PARTITIONS);
                    row[j] = name;
                    expected.put(name, p_estimator.getHasher().hash(name));
                }
                else row[j] = VoltTypeUtil.getRandomValue(vtype);
            } // FOR
            vt.addRow(row);
        } // FOR
        assertEquals(num_rows, vt.getRowCount());
        
        vt.resetRowPosition();
        while (vt.advanceRow()) {
            VoltTableRow row = vt.getRow();
            
            String w_name = row.getString(catalog_col.getIndex());
            int name_p = p_estimator.getHasher().hash(w_name);
            assert(name_p >= 0 && name_p <= NUM_PARTITIONS);
            
            int row_p = p_estimator.getTableRowPartition(catalog_tbl, row);
            assert(row_p >= 0 && row_p <= NUM_PARTITIONS);
            assertEquals(w_name, name_p, row_p);

//            System.err.println(w_name + " => " + row_p);
            Integer last = expected.get(w_name);
            assertNotNull(last);
            assertEquals(last.intValue(), row_p);
        } // WHILE
        
//        System.err.println(StringUtil.formatMaps(expected));
//        System.err.println(actual);
    }
    
}