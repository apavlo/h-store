package edu.brown.designer;

import java.util.Collection;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.ExpressionType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogPair;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ProjectType;

public class TestColumnSet extends BaseTestCase {

    private PredicatePairs cset;
    private Procedure catalog_proc;
    private Table catalog_tbl0;
    private Table catalog_tbl1;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
        this.cset = new PredicatePairs();
        this.catalog_proc = this.getProcedure(neworder.class); 
        this.catalog_tbl0 = this.getTable("CUSTOMER");
        this.catalog_tbl1 = this.getTable("ORDERS");
        
        cset.add(this.getColumn(catalog_tbl0, "C_W_ID"), this.getColumn(catalog_tbl1, "O_W_ID"));
        cset.add(this.getColumn(catalog_tbl0, "C_W_ID"), this.getProcParameter(catalog_proc, 0));
        cset.add(this.getColumn(catalog_tbl0, "C_D_ID"), this.getColumn(catalog_tbl1, "O_D_ID"));
        cset.add(this.getColumn(catalog_tbl0, "C_ID"), this.getColumn(catalog_tbl1, "O_C_ID"));
    }
    
    /**
     * testAddEntry
     */
    public void testAddEntry() throws Exception {
        // Add a new entry where the NEW_ORDER table comes before ORDERS and make sure it
        // automatically gets flipped!
        Table catalog_tbl = this.getTable("NEW_ORDER");
        cset.add(this.getColumn(catalog_tbl, "NO_O_ID"), this.getColumn(this.catalog_tbl1, "O_ID"));
        // And that two columns from the same table are sorted
        cset.add(this.getColumn(catalog_tbl1, "O_ID"), this.getColumn(this.catalog_tbl1, "O_D_ID"));
        for (CatalogPair e : cset) {
            assertNotNull(e);
            CatalogType first = e.getFirst();
            assertNotNull(first);
            CatalogType second = e.getSecond();
            assertNotNull(second);
            assert(first.compareTo(second) <= 0) : first + " > " + second;
        } // FOR
        
        // Now check that if we add duplicate entries they don't get added again
        int expected = this.cset.size();
        Column catalog_col0 = this.getColumn(this.catalog_tbl1, "O_ID");
        Column catalog_col1 = this.getColumn(this.catalog_tbl1, "O_D_ID");
        cset.add(catalog_col0, catalog_col1);
        cset.add(catalog_col1, catalog_col0);
        assertEquals(expected, this.cset.size());
        
        // But if we change the ComparisonExp, then it's not the same entry
        cset.add(catalog_col0, catalog_col1, ExpressionType.COMPARE_NOTEQUAL);
        assertEquals(expected + 1, this.cset.size());
        
//        System.err.println(cset);
    }
    
    /**
     * testFindAll
     */
    public void testFindAll() throws Exception {
        // Just search for C_W_ID
        Column expected = this.getColumn(catalog_tbl0, "C_W_ID");
        Collection<Column> match = cset.findAll(Column.class, expected);
        assertNotNull(match);
        assertFalse(match.isEmpty());
        Column catalog_col = CollectionUtil.first(match);
        assertNotNull(catalog_col);
        assertEquals(expected, catalog_col);
    }
    
    /**
     * testFindAllForOther
     */
    public void testFindAllForOther() throws Exception {
        // Search for C_W_ID and we should get back two elements (one Column and one ProcParameter)
        Column catalog_col = this.getColumn(catalog_tbl0, "C_W_ID");
        Collection<CatalogType> match0 = cset.findAllForOther(CatalogType.class, catalog_col);
        assertNotNull(match0);
        assertFalse(match0.isEmpty());
        assertEquals(2, match0.size());
        
        CatalogType expected0 = this.getColumn(catalog_tbl1, "O_W_ID");
        assert(match0.contains(expected0)) : "Missing " + expected0;
        CatalogType expected1 = this.getProcParameter(catalog_proc, 0);
        assert(match0.contains(expected1)) : "Missing " + expected1;
        
        // Search again, but this type specify that we only want ProcParameters
        // Now we should only get back one result
        Collection<ProcParameter> match1 = cset.findAllForOther(ProcParameter.class, catalog_col);
        assertNotNull(match1);
        assertFalse(match1.isEmpty());
        assertEquals(1, match1.size());
        assert(match1.contains(expected1)) : "Missing " + expected1;
    }
    
    /**
     * testFindAllForOther
     */
    public void testFindAllForParent() throws Exception {
        // Search for CUSTOMER and make sure we get back our three columns
        String expected[] = { "C_W_ID", "C_D_ID", "C_ID" };
        Collection<Column> match = cset.findAllForParent(Column.class, catalog_tbl0);
        assertNotNull(match);
        assertFalse(match.isEmpty());
        assertEquals(expected.length, match.size());
        
        for (String col_name : expected) {
            Column catalog_col = this.getColumn(catalog_tbl0, col_name);
            assert(match.contains(catalog_col)) : "Missing " + catalog_col;
        } // FOR
    }
    
    /**
     * testFindAllForOtherParent
     */
    public void testFindAllForOtherParent() throws Exception {
        // Search against the neworder Procedure and check that we get back C_W_ID
        Collection<Column> match = cset.findAllForOtherParent(Column.class, catalog_proc);
        assertNotNull(match);
        assertFalse(match.isEmpty());
        
        Column catalog_col = CollectionUtil.first(match);
        assertNotNull(catalog_col);
        assertEquals(this.getColumn(catalog_tbl0, "C_W_ID"), catalog_col);
    }
    
}
