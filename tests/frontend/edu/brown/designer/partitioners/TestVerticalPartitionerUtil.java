package edu.brown.designer.partitioners;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.InsertCallForwarding;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.TestAbstractPartitioner.MockPartitioner;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 */
public class TestVerticalPartitionerUtil extends BasePartitionerTestCase {

    private MockPartitioner partitioner;
    private AccessGraph agraph;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalog_db, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(MockPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (MockPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
    }
    
    private Map<String, Object> generateFieldMap(Statement catalog_stmt) {
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (String f : catalog_stmt.getFields()) {
            Object val = catalog_stmt.getField(f);
            if (f.endsWith("exptree") || f.endsWith("fullplan")) {
                val = StringUtil.md5sum(val.toString());
            }
            m.put(f, val);
        } // FOR
        return (m);
    }
    
    /**
     * testCatalogUpdates
     */
    public void testCatalogUpdates() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        MultiColumn target_col = MultiColumn.get(this.getColumn(catalog_tbl, "S_ID"));
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, target_col, hints);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        // HACK: Clear out the query plans that could have been generated from other tests
        for (VerticalPartitionColumn c : candidates) c.clear();
        
        VerticalPartitionerUtil.compileOptimizedStatements(catalog_db, candidates);
        VerticalPartitionColumn vpc = CollectionUtil.getFirst(candidates);
        assertNotNull(vpc);
        
        // BEFORE!
        assertNull(CatalogUtil.getVerticalPartition(catalog_tbl));
        Map<Statement, Map<String, Object>> fields_before = new ListOrderedMap<Statement, Map<String, Object>>();
        for (Statement catalog_stmt : vpc.getStatements()) {
            fields_before.put(catalog_stmt, this.generateFieldMap(catalog_stmt));
        } // FOR
//        System.err.println("BEFORE:\n" + StringUtil.formatMaps(fields_before));
        
        // AFTER!
        MaterializedViewInfo catalog_view = vpc.updateCatalog();
        assertNotNull(catalog_view);
        assertEquals(CatalogUtil.getVerticalPartition(catalog_tbl), catalog_view);
        for (Statement catalog_stmt : vpc.getStatements()) {
            Map<String, Object> before_m = fields_before.get(catalog_stmt);
            assertNotNull(before_m);
            Map<String, Object> after_m = this.generateFieldMap(catalog_stmt);
            assertEquals(before_m.keySet(), after_m.keySet());
            System.err.println(StringUtil.columns(StringUtil.formatMaps(before_m),
                               StringUtil.formatMaps(after_m)));
            
            for (String f : before_m.keySet()) {
                // Use the MD5 checksum to make sure that these fields have changed
                // Yes I could just compare the original strings but... well, uh... I forget why I did this...
                if (f.endsWith("fullplan")) {
                    assertThat(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), not(equalTo(after_m.get(f))));
                // Sometimes the Expression tree will be different, sometimes it will be the same
                // So just make sure it's not null/empty
                } else if (f.endsWith("exptree")) {
                    assertNotNull(after_m.get(f));
                    assertFalse(catalog_stmt.fullName() +" ["+f+"]", after_m.get(f).toString().isEmpty());
                // All the other fields should be the same
                } else {
                    assertEquals(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), after_m.get(f));
                }
            } // FOR
        } // FOR
        System.err.println(StringUtil.SINGLE_LINE);
        
        // REVERT!
        vpc.revertCatalog();
        assertNull(CatalogUtil.getVerticalPartition(catalog_tbl));
        for (Statement catalog_stmt : vpc.getStatements()) {
            Map<String, Object> before_m = fields_before.get(catalog_stmt);
            assertNotNull(before_m);
            Map<String, Object> revert_m = this.generateFieldMap(catalog_stmt);
            assertEquals(before_m.keySet(), revert_m.keySet());
            System.err.println(StringUtil.columns(StringUtil.formatMaps(before_m),
                               StringUtil.formatMaps(revert_m)));
            
            // Now everything should be the same again
            for (String f : before_m.keySet()) {
                assertEquals(catalog_stmt.fullName() +" ["+f+"]", before_m.get(f), revert_m.get(f));
            } // FOR
        } // FOR
    }
    
    /**
     * testCompileOptimizedStatements
     */
    public void testCompileOptimizedStatements() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        if (CatalogUtil.getVerticalPartition(catalog_tbl) != null) {
            catalog_tbl.getViews().clear();
            assert(catalog_tbl.getViews().isEmpty());
        }
        Column catalog_cols[] = {
            this.getColumn(catalog_tbl, "S_ID"),
//            this.getColumn(catalog_tbl, "SUB_NBR"),
//            this.getColumn(catalog_tbl, "VLR_LOCATION"),
        };
        Set<VerticalPartitionColumn> candidates = new HashSet<VerticalPartitionColumn>();
        for (Column catalog_col : catalog_cols) {
            MultiColumn hp_col = MultiColumn.get(catalog_col);
            Collection<VerticalPartitionColumn> col_candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, hp_col, hints);
            assertNotNull(col_candidates);
            candidates.addAll(col_candidates);
        } // FOR
        assertFalse(candidates.isEmpty());
        
        for (VerticalPartitionColumn c : candidates) {
            // HACK: Clear out the query plans that could have been generated from other tests
            c.clear();
            assertFalse(c.getStatements().isEmpty());
            for (Statement catalog_stmt : c.getStatements()) {
                assertNull(catalog_stmt.fullName(), c.getOptimizedQuery(catalog_stmt));
            } // FOR
        } // FOR
        
        VerticalPartitionerUtil.compileOptimizedStatements(catalog_db, candidates);
        for (VerticalPartitionColumn c : candidates) {
            System.err.println(c);
            assertFalse(c.getStatements().isEmpty());
            for (Statement catalog_stmt : c.getStatements()) {
                assertNotNull(c.getOptimizedQuery(catalog_stmt));
            } // FOR
        } // FOR
        
        // Lastly, our table should not still have a vertical partition
        assert(CatalogUtil.getVerticalPartition(catalog_tbl) == null);
    }
    
    /**
     * testGenerateCandidates
     */
    public void testGenerateCandidates() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        MultiColumn target_col = MultiColumn.get(this.getColumn(catalog_tbl, "S_ID"));
        
        Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, target_col, hints);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionColumn vpc = CollectionUtil.getFirst(candidates);
        assertNotNull(vpc);

        Collection<Column> expected_cols = CollectionUtil.addAll(new HashSet<Column>(), this.getColumn(catalog_tbl, "SUB_NBR"),
                                                                                        this.getColumn(catalog_tbl, "S_ID"));
        assertEquals(expected_cols.size(), vpc.getVerticalMultiColumn().size());
        assertTrue(expected_cols + " <=> " + vpc.getVerticalPartitionColumns(), expected_cols.containsAll(vpc.getVerticalPartitionColumns()));
        
        Collection<Statement> expected_stmts = new HashSet<Statement>();
        expected_stmts.add(this.getStatement(this.getProcedure(DeleteCallForwarding.class), "query"));
        expected_stmts.add(this.getStatement(this.getProcedure(InsertCallForwarding.class), "query1"));
        assertEquals(expected_stmts.size(), vpc.getStatements().size());
        assert(expected_stmts.containsAll(vpc.getStatements()));
    }
    
    /**
     * testGenerateCandidatesAllColumns
     */
    public void testGenerateCandidatesAllColumns() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column target_col = catalog_tbl.getPartitioncolumn();
        assertNotNull(target_col);
        
        for (Column catalog_col : catalog_tbl.getColumns()) {
            Collection<VerticalPartitionColumn> candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, MultiColumn.get(catalog_col), hints);
            assertEquals(candidates.toString(), catalog_col.equals(target_col), candidates.size() > 0);
        } // FOR
    }
    
}
