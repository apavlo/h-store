package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.benchmark.tm1.procedures.InsertCallForwarding;
import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.TestAbstractPartitioner.MockPartitioner;
import edu.brown.designer.partitioners.VerticalPartitionerUtil.Candidate;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

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
    
    /**
     * testCompileOptimizedStatements
     */
    public void testCompileOptimizedStatements() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        Column catalog_cols[] = {
            this.getColumn(catalog_tbl, "S_ID"),
//            this.getColumn(catalog_tbl, "SUB_NBR"),
//            this.getColumn(catalog_tbl, "VLR_LOCATION"),
        };
        Set<Candidate> candidates = new HashSet<Candidate>();
        for (Column catalog_col : catalog_cols) {
            Collection<VerticalPartitionerUtil.Candidate> col_candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, catalog_col, hints);
            assertNotNull(col_candidates);
            candidates.addAll(col_candidates);
        } // FOR
        assertFalse(candidates.isEmpty());
        
        for (Candidate c : candidates) {
            assertFalse(c.getStatements().isEmpty());
            for (Statement catalog_stmt : c.getStatements()) {
                assertNull(c.getOptimizedQuery(catalog_stmt));
            } // FOR
        } // FOR
        
        VerticalPartitionerUtil.compileOptimizedStatements(catalog_db, candidates);
        for (Candidate c : candidates) {
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
        Column target_col = this.getColumn(catalog_tbl, "S_ID");
        
        Collection<VerticalPartitionerUtil.Candidate> candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, target_col, hints);
        assertNotNull(candidates);
        assertFalse(candidates.isEmpty());
        VerticalPartitionerUtil.Candidate vpc = CollectionUtil.getFirst(candidates);
        assertNotNull(vpc);

        Collection<Column> expected_cols = new HashSet<Column>();
        expected_cols.add(this.getColumn(catalog_tbl, "SUB_NBR"));
        expected_cols.add(this.getColumn(catalog_tbl, "S_ID"));
        assertEquals(expected_cols.size(), vpc.getColumns().size());
        assert(expected_cols.containsAll(vpc.getColumns()));
        
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
            Collection<Candidate> candidates = VerticalPartitionerUtil.generateCandidates(info, agraph, catalog_col, hints);
            assertEquals(candidates.toString(), catalog_col.equals(target_col), candidates.size() > 0);
        } // FOR
    }
    
}
