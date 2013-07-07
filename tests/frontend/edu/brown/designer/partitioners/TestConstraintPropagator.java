package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.catalog.CatalogPair;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ProjectType;

public class TestConstraintPropagator extends BasePartitionerTestCase {
    
    private static AccessGraph agraph;
    private ConstraintPropagator cp;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
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
//        System.err.println(this.cp.toString());
        
        for (DesignerVertex v : agraph.getVertices()) {
            assertNotNull(cp.getEdgeColumns(v));
            // System.err.println(v + " => " + cp.getEdgeColumns(v));
        }
        
        boolean has_multi_vertex_edge = false;
        for (DesignerEdge e : agraph.getEdges()) {
            Set<DesignerVertex> vertices = new HashSet<DesignerVertex>(agraph.getIncidentVertices(e));
            assertFalse(vertices.isEmpty());
            has_multi_vertex_edge = has_multi_vertex_edge || (vertices.size() > 1);
//            if (vertices.size() > 1) System.err.println(e);
        } // FOR
        assert(has_multi_vertex_edge);
    }
    
    /**
     * testInitializeVerticalPartitioning
     */
    public void testInitializeVerticalPartitioning() throws Exception {
        hints.enable_vertical_partitioning = true;
        cp = new ConstraintPropagator(info, hints, agraph);
        
        // Check that a vertical partition candidate column for SUBSCRIBER
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
        DesignerVertex v = agraph.getVertex(catalog_tbl);
        assertNotNull(v);
        Collection<VerticalPartitionColumn> vp_columns = new HashSet<VerticalPartitionColumn>();
        for (Collection<Column> catalog_cols : cp.getEdgeColumns(v).values()) {
            for (Column catalog_col : catalog_cols) {
                if (catalog_col instanceof VerticalPartitionColumn) {
                    vp_columns.add((VerticalPartitionColumn)catalog_col);
                }
            } // FOR
        } // FOR
        assertFalse(vp_columns.isEmpty());
        
        // Make sure that each of these have a horizontal partition column
        // We expected to at least have a VerticalPartition with SUB_NBR in it
        Column expected_hp = this.getColumn(catalog_tbl, "S_ID");
        Column expected_vp = this.getColumn(catalog_tbl, "SUB_NBR");
        boolean found = false;
        for (VerticalPartitionColumn vp_col : vp_columns) {
            assertNotNull(vp_col);
            assertNotNull(vp_col.toString(), vp_col.getHorizontalColumn());
            assertNotNull(vp_col.toString(), vp_col.getVerticalMultiColumn());
            
            if (vp_col.getVerticalMultiColumn().contains(expected_vp) &&
                vp_col.getHorizontalColumn().equals(expected_hp)) {
                assert(vp_col.getVerticalMultiColumn().contains(expected_hp));
                assert(vp_col.hasOptimizedQueries());
                found = true;
            }
//            System.err.println(vp_col.toString() + "\n");
        } // FOR
        assert(found);
    }
    
    /**
     * testInitializeMultiAttribute
     */
    public void testInitializeMultiAttribute() throws Exception {
        hints.enable_multi_partitioning = true;
        cp = new ConstraintPropagator(info, hints, agraph);
        
        // Check that we have multi-attribute candidates for ACCESS_INFO
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_ACCESS_INFO);
        DesignerVertex v = agraph.getVertex(catalog_tbl);
        assertNotNull(v);
        Collection<MultiColumn> multi_columns = new HashSet<MultiColumn>();
        for (Collection<Column> catalog_cols : cp.getEdgeColumns(v).values()) {
            for (Column catalog_col : catalog_cols) {
                if (catalog_col instanceof MultiColumn) {
                    multi_columns.add((MultiColumn)catalog_col);
                }
            } // FOR
        } // FOR
        assertFalse(multi_columns.isEmpty());
    }
    
    /**
     * testUpdateTwice
     */
    public void testUpdateTwice() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
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
    public void testUpdateTable() throws Exception {
        Set<Table> targets = new HashSet<Table>();
        for (String table_name : new String[]{ TM1Constants.TABLENAME_SUBSCRIBER, TM1Constants.TABLENAME_ACCESS_INFO }) {
            Table catalog_tbl = this.getTable(table_name);
            cp.update(catalog_tbl);
            targets.add(catalog_tbl);    
        } // FOR
        
        // Make sure that the columns that remain for the edges to our target tables
        // are only connected through the column that the target tables were partitioned on
        Collection<Column> columns;
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (targets.contains(catalog_tbl) || catalog_tbl.getSystable()) continue;
            DesignerVertex v0 = agraph.getVertex(catalog_tbl);
            
            try {
                columns = cp.getCandidateValues(catalog_tbl, Column.class);
            } catch (IllegalArgumentException ex) {
                continue;
            }
            assertNotNull(columns);
            assertFalse(columns.isEmpty());
            
            System.err.println(String.format("Examining %d columns for %s", columns.size(), catalog_tbl));
            
            // For each column that is still available for this table, check to make sure that:
            //  (1) It does not have an unmarked edge to one of our target tables
            //  (2) If it is does have an edge to one of target tables then that edge uses the column that the table is partitioned on
            for (Column catalog_col : columns) {
                if (catalog_col instanceof ReplicatedColumn) continue;
                Collection<DesignerEdge> edges = agraph.findEdgeSet(v0, catalog_col);
                assertNotNull(catalog_col.fullName(), edges);
                assertFalse(catalog_col.fullName(), edges.isEmpty());
                
                for (DesignerEdge e : edges) {
                    if (cp.isMarked(e)) continue;
                    
                    DesignerVertex v1 = agraph.getOpposite(v0, e);
                    assertNotNull(e.toString(), v1);
                    Table other_tbl = v1.getCatalogItem();
                    assertNotNull(other_tbl);

                    // Skip if not one of our target tables
                    if (targets.contains(other_tbl) == false || other_tbl.getPartitioncolumn() == null) continue;
                    
                    // Get the ColumnSet, which should only have one entry
                    PredicatePairs cset = e.getAttribute(EdgeAttributes.COLUMNSET);
                    assertNotNull(cset);
                    assertEquals(cset.toString(), 1, cset.size());
                    CatalogPair entry = cset.get(0);
                    assertNotNull(entry);
                    
                    // Get the element from the entry that is not our column
                    CatalogType other = entry.getOther(catalog_col);
                    assertNotNull(other);
                    if ((other instanceof Column) == false) continue;
                    Column other_col = (Column)other;
                    System.err.println(String.format("catalog_col=%s, other_tbl=%s, other_col=%s",
                                                     catalog_col.fullName(), other_tbl.fullName(), other_col.fullName()));
                    assertEquals(e.toString(), other_tbl, other_col.getParent());
                    
                    // Make sure that the table's partitioning column matches  
                    assertEquals(String.format("%s<-->%s\n%s", catalog_tbl, other_tbl, e.toString()), other_tbl.getPartitioncolumn(), other_col);
                } // FOR (Edge)
            } // FOR (Column)
        } // FOR
    }
    
  /**
  * testUpdateTableVerticalPartitioning
  */
 public void testUpdateTableVerticalPartitioning() throws Exception {
     hints.enable_vertical_partitioning = true;
     cp = new ConstraintPropagator(info, hints, agraph);
     
     // We're going to mark SUBSCRIBER as partitioned on S_ID and get the candidates for ACCESS_INFO  
     Table catalog_tbl0 = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
     Column catalog_col = this.getColumn(catalog_tbl0, "S_ID");
     catalog_tbl0.setPartitioncolumn(catalog_col);
     cp.update(catalog_tbl0);
     Table catalog_tbl1 = this.getTable(TM1Constants.TABLENAME_ACCESS_INFO);
     Collection<Column> expected_cols = cp.getCandidateValues(catalog_tbl1, Column.class); 
     assertNotNull(expected_cols);
     assertFalse(expected_cols.isEmpty());
     
     // Get the VerticalPartitionColumn that uses S_ID as the horizontal partitioning Column
     // Make sure that the optimized query plans have not been applied yet
     Collection<VerticalPartitionColumn> vp_columns = cp.getVerticalPartitionColumns(catalog_tbl0);
     assertNotNull(vp_columns);
     VerticalPartitionColumn vp_col = null;
     for (VerticalPartitionColumn c : vp_columns) {
         if (c.getHorizontalColumn().equals(catalog_col)) {
             vp_col = c;
             break;
         }
     } // FOR
     assertNotNull(vp_col);
     assertFalse(vp_col.isUpdateApplied());
     
     // Now partition SUBSCRIBER using its VerticalPartitionColumn
     cp.reset(catalog_tbl0);
     catalog_tbl0.setPartitioncolumn(vp_col);
     cp.update(catalog_tbl0);
     
     // And make sure that we get back the Columns we expect and that the Statements
     // have been updated
     Collection<Column> actual_cols = cp.getCandidateValues(catalog_tbl1, Column.class); 
     assertNotNull(actual_cols);
     assertEquals(expected_cols.size(), actual_cols.size());
     assertEquals(expected_cols, actual_cols);
     assertFalse(vp_col.isUpdateApplied());
     
     // Revert
     catalog_tbl0.setPartitioncolumn(catalog_col);
     cp.reset(catalog_tbl0);
 }
    
//    /**
//     * testUpdateTableMultiAttribute
//     */
//    public void testUpdateTableMultiAttribute() throws Exception {
//        Database clone_db = CatalogCloner.cloneDatabase(catalog_db);
//        info = this.generateInfo(clone_db);
//        hints.enable_multi_partitioning = true;
//        
//        designer = new Designer(info, hints, info.getArgs());
//        RandomPartitioner partitioner = new RandomPartitioner(designer, info);
//        AccessGraph agraph = AccessGraphGenerator.convertToSingleColumnEdges(clone_db, partitioner.generateAccessGraph());
//        cp = new ConstraintPropagator(info, hints, agraph);
//        
//        Table catalog_tbl0 = this.getTable(clone_db, TM1Constants.TABLENAME_CALL_FORWARDING);
//        Collection<MultiColumn> multi_columns = new HashSet<MultiColumn>();
//        DesignerVertex v = agraph.getVertex(catalog_tbl0);
//        assertNotNull(catalog_tbl0.fullName(), v);
//        for (Collection<Column> catalog_cols : cp.getEdgeColumns(v).values()) {
//            for (Column catalog_col : catalog_cols) {
//                String catalog_key = CatalogKey.createKey(catalog_col);
//                assertNotNull(catalog_key);
//                catalog_col = CatalogKey.getFromKey(clone_db, catalog_key, Column.class);
//                assertNotNull(catalog_col);
//                
//                if (catalog_col instanceof MultiColumn) {
//                    multi_columns.add((MultiColumn)catalog_col);
//                }
//            } // FOR
//        } // FOR
//        assertFalse(multi_columns.isEmpty());
//        
//        // We're going to mark SPECIAL_FACILITY as updated using a particular Column that 
//        // should cause all the MultiColumns for CALL_FORWADING to get invalidated
//        Table catalog_tbl1 = this.getTable(clone_db, TM1Constants.TABLENAME_SPECIAL_FACILITY);
//        catalog_tbl0.setPartitioncolumn(this.getColumn(catalog_tbl1, 2));
//        cp.update(catalog_tbl1);
//        
//        // Make sure that the columns that remain for the edges to our target tables
//        // are only connected through the column that the target tables were partitioned on
//        Collection<Column> expected = CollectionUtil.addAll(new HashSet<Column>(),
//                                                            this.getColumn(catalog_tbl0, "S_ID"),
//                                                            this.getColumn(catalog_tbl0, "SF_TYPE"));
//        
//        Collection<Column> columns = cp.getCandidateValues(catalog_tbl0, Column.class);
//        assertNotNull(columns);
//        assertFalse(columns.isEmpty());
//        for (Column catalog_col : columns) {
//            if (catalog_col instanceof MultiColumn) {
//                MultiColumn mc = (MultiColumn)catalog_col;
//                assertTrue(catalog_col.toString(), CollectionUtils.intersection(mc, expected).isEmpty());
//            }
//        } // FOR
//    }
    
    /**
     * testResetTable
     */
    public void testResetTable() throws Exception {
        // First update all of the tables
        for (Table catalog_tbl : catalog_db.getTables()) {
            try {
                cp.update(catalog_tbl);
            } catch (IllegalArgumentException ex) {
                continue;
            }
        } // FOR
        
        // Get the list of columns that we can use for CALL_FORWARDING
        Table catalog_tbl0 = this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING);
        Collection<Column> orig_columns = new HashSet<Column>((Collection<Column>)cp.getCandidateValues(catalog_tbl0, Column.class));
        assertFalse(catalog_tbl0.toString(), orig_columns.isEmpty());
        
        // Reset both ACCESS_INFO and CALL_FORWARDING
        // This should give us more possible columns for CALL_FORWARDING
        Table catalog_tbl1 = this.getTable(TM1Constants.TABLENAME_ACCESS_INFO);
        cp.reset(catalog_tbl0);
        cp.reset(catalog_tbl1);
        
        // Then grab the list of colunms for ORDERS again. It should be larger than our original list
        // And the original list should be a subset
        Collection<Column> new_columns = cp.getCandidateValues(catalog_tbl0, Column.class);
        assert(new_columns.size() > orig_columns.size()) : orig_columns;
        assert(new_columns.containsAll(orig_columns)) : String.format("ORIG: %s\nNEW: %s", orig_columns, new_columns);
    }
    
    /**
     * testResetTwice
     */
    public void testResetTwice() throws Exception {
        Table catalog_tbl = this.getTable(TM1Constants.TABLENAME_SUBSCRIBER);
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
