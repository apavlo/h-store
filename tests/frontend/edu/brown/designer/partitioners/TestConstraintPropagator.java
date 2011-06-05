package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.ColumnSet;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.utils.ProjectType;

public class TestConstraintPropagator extends BasePartitionerTestCase {
    
    private static AccessGraph agraph;
    private ConstraintPropagator cp;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        if (agraph == null) {
            agraph = AccessGraphGenerator.generateGlobal(this.info);
            agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, agraph);
            agraph.setVerbose(true);
//            System.err.println("Dumping AccessGraph to " + FileUtil.writeStringToFile("/tmp/tpcc.dot", GraphvizExport.export(agraph, "tpcc")));
        }
        assertNotNull(agraph);
        this.cp = new ConstraintPropagator(info, hints, agraph);
    }
    
    /**
     * testInitialize
     */
    public void testInitialize() throws Exception {
        for (Vertex v : agraph.getVertices()) {
            assertNotNull(cp.getEdgeColumns(v));
            // System.err.println(v + " => " + cp.getEdgeColumns(v));
        }
        
        boolean has_multi_vertex_edge = false;
        for (Edge e : agraph.getEdges()) {
            Set<Vertex> vertices = new HashSet<Vertex>(agraph.getIncidentVertices(e));
            assertFalse(vertices.isEmpty());
            has_multi_vertex_edge = has_multi_vertex_edge || (vertices.size() > 1);
//            if (vertices.size() > 1) System.err.println(e);
        }
        assert(has_multi_vertex_edge);
    }
    
    /**
     * testUpdateTwice
     */
    public void testUpdateTwice() throws Exception {
        Table catalog_tbl = this.getTable("WAREHOUSE");
        cp.update(catalog_tbl);
        try {
            cp.update(catalog_tbl);
        } catch (AssertionError ex) {
            return;
        }
        assert(false) : "Was allowed to update twice!";
    }
    
    /**
     * testUpdateTable
     */
    @SuppressWarnings("unchecked")
    public void testUpdateTable() throws Exception {
        Set<Table> targets = new HashSet<Table>();
        for (String table_name : new String[]{ "WAREHOUSE", "DISTRICT", "CUSTOMER" }) {
            Table catalog_tbl = this.getTable(table_name);
            cp.update(catalog_tbl);
            targets.add(catalog_tbl);    
        } // FOR
        targets.add(this.getTable("ITEM"));
        
        // Make sure that the columns that remain for the edges to our target tables
        // are only connected through the column that the target tables were partitioned on
        Collection<Column> columns;
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (targets.contains(catalog_tbl)) continue;
            Vertex v0 = agraph.getVertex(catalog_tbl);
            
            try {
                columns = (Collection<Column>)cp.getPossibleValues(catalog_tbl, Column.class);
            } catch (IllegalArgumentException ex) {
                continue;
            }
            assertNotNull(columns);
            assertFalse(columns.isEmpty());
            
//            System.err.println(String.format("Examining %d columns for %s", columns.size(), catalog_tbl));
            
            // For each column that is still available for this table, check to make sure it's 
            // not pointing to one of our target tables or that it is pointing to one of target tables
            // but uses the column that the table is partitioned on
            for (Column catalog_col : columns) {
                Collection<Edge> edges = agraph.findEdgeSet(v0, catalog_col);
                assertNotNull(catalog_col.fullName(), edges);
                assertFalse(catalog_col.fullName(), edges.isEmpty());
                
                for (Edge e : edges) {
                    if (cp.isMarked(e)) continue;
                    
                    Vertex v1 = agraph.getOpposite(v0, e);
                    assertNotNull(e.toString(), v1);
                    Table other_tbl = v1.getCatalogItem();
                    assertNotNull(other_tbl);

                    // Skip if not one of our target tables
                    if (targets.contains(other_tbl) == false || other_tbl.getName().equals("ITEM")) continue;
                    
                    // Get the ColumnSet, which should only have one entry
                    ColumnSet cset = e.getAttribute(EdgeAttributes.COLUMNSET);
                    assertNotNull(cset);
                    assertEquals(cset.toString(), 1, cset.size());
                    ColumnSet.Entry entry = cset.get(0);
                    assertNotNull(entry);
                    
                    // Get the element from the entry that is not our column
                    CatalogType other = entry.getOther(catalog_col);
                    assertNotNull(other);
                    if ((other instanceof Column) == false) continue;
                    Column other_col = (Column)other;
                    assertEquals(e.toString(), other_tbl, other_col.getParent());
                    
                    // Make sure that the table's partitioning column matches  
                    assertEquals(String.format("%s<-->%s\n%s", catalog_tbl, other_tbl, e.toString()), other_tbl.getPartitioncolumn(), other_col);
                } // FOR (Edge)
            } // FOR (Column)
        } // FOR
    }
    
    /**
     * testResetTable
     */
    @SuppressWarnings("unchecked")
    public void testResetTable() throws Exception {
        // First update all of the tables
        for (Table catalog_tbl : catalog_db.getTables()) {
            try {
                cp.update(catalog_tbl);
            } catch (IllegalArgumentException ex) {
                continue;
            }
        } // FOR
        
        // Get the list of columns that we can use for ORDERS
        Table catalog_tbl0 = this.getTable("ORDERS");
        Set<Column> orig_columns = new HashSet<Column>((Collection<Column>)cp.getPossibleValues(catalog_tbl0, Column.class));
        assertFalse(catalog_tbl0.toString(), orig_columns.isEmpty());
        
        // Reset both ORDERS and CUSTOMER
        // This should give us more possible columns for ORDERS
        Table catalog_tbl1 = this.getTable("CUSTOMER");
        cp.reset(catalog_tbl0);
        cp.reset(catalog_tbl1);
        
        // Then grab the list of colunms for ORDERS again. It should be larger than our original list
        // And the original list should be a subset
        Collection<Column> new_columns = (Collection<Column>)cp.getPossibleValues(catalog_tbl0, Column.class);
        assert(new_columns.size() > orig_columns.size()) : orig_columns;
        assert(new_columns.containsAll(orig_columns)) : String.format("ORIG: %s\nNEW: %s", orig_columns, new_columns);
    }
    
    /**
     * testResetTwice
     */
    public void testResetTwice() throws Exception {
        Table catalog_tbl = this.getTable("WAREHOUSE");
        cp.update(catalog_tbl);
        cp.reset(catalog_tbl);
        try {
            cp.reset(catalog_tbl);
        } catch (AssertionError ex) {
            return;
        }
        assert(false) : "Was allowed to reset twice!";
    }
}
