package edu.brown.designer.generators;

import java.util.*;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;
import org.voltdb.types.ConstraintType;

import edu.brown.designer.*;
import edu.brown.graphs.*;
import edu.brown.workload.Workload;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * 
 * @author pavlo
 *
 */
public class DependencyGraphGenerator extends AbstractGenerator<AbstractDirectedGraph<Vertex, Edge>> {
    private static final Logger LOG = Logger.getLogger(DependencyGraphGenerator.class);
    
    public DependencyGraphGenerator(DesignerInfo info) {
        super(info);
    }
    
    /**
     * Convenience method for generating a DependencyGraph
     * @param catalog_db
     * @return
     */
    public static DependencyGraph generate(Database catalog_db) {
        DependencyGraph dgraph = new DependencyGraph(catalog_db);
        DesignerInfo info = new DesignerInfo(catalog_db, new Workload(catalog_db.getCatalog()));
        try {
            new DependencyGraphGenerator(info).generate(dgraph);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        return (dgraph);
    }
    
    public void generate(AbstractDirectedGraph<Vertex, Edge> graph) throws Exception {
        for (Table catalog_table : this.info.catalog_db.getTables()) {
            graph.addVertex(new Vertex(catalog_table));
        } // FOR

        //
        // We first need to generate the list of candidate root tables
        // These are the tables that do not have any foreign key references
        //
        Map<Vertex, List<Column>> fkey_ref_cols = new HashMap<Vertex, List<Column>>();
        Map<Vertex, List<Constraint>> fkey_ref_consts = new HashMap<Vertex, List<Constraint>>();
        for (Table catalog_table : this.info.catalog_db.getTables()) {
            Vertex vertex = graph.getVertex(catalog_table);
            fkey_ref_cols.put(vertex, new ArrayList<Column>());
            fkey_ref_consts.put(vertex, new ArrayList<Constraint>());

            LOG.debug("Inspecting table '" + catalog_table.getName() + "'");

            //
            // Constraints
            //
            for (Constraint catalog_const : catalog_table.getConstraints()) {
                //
                // Foreign Key Constraint
                //
                if (catalog_const.getType() == ConstraintType.FOREIGN_KEY.getValue()) {
                    if (catalog_const.getForeignkeytable() == null) {
                        throw new Exception("ERROR: The table is null for foreign key constraint '" + catalog_const + "' for table '" + catalog_table + "'");
                    } else if (catalog_const.getForeignkeycols().isEmpty()) {
                        throw new Exception("ERROR: The list of columns are empty for foreign key constraint '" + catalog_const + "' for table '" + catalog_table + "'");
                    }
                    for (ColumnRef catalog_col_ref : catalog_const.getForeignkeycols()) {
                        fkey_ref_cols.get(vertex).add(catalog_col_ref.getColumn());
                    } // FOR
                    fkey_ref_consts.get(vertex).add(catalog_const);
                }
            } // FOR
            //
            // Columns ????????
            // 
//            for (Column catalog_col : catalog_table.getColumns()) {
//                LOG.debug("Inspecting column '" + catalog_col.getName() + "' in table '" + catalog_table.getName() + "'");
//            } // FOR
        } // FOR
        //
        // Build the dependency graph's edges
        //
        for (Table catalog_table : this.info.catalog_db.getTables()) {
            //
            // For each vertex, get the list of foreign key references to point to it and
            // make a new edge between the parent and child
            //
            Vertex vertex = graph.getVertex(catalog_table);
            for (int ctr = 0, cnt = fkey_ref_consts.get(vertex).size(); ctr < cnt; ctr++) {
                Constraint catalog_const = fkey_ref_consts.get(vertex).get(ctr);
                Column catalog_col = fkey_ref_cols.get(vertex).get(ctr);

                //
                // Grab the table object used in this foreign key constraint
                // We then get the vertex that we're using to represent it
                //
                Table catalog_fkey_table = catalog_const.getForeignkeytable();
                Vertex other_vertex = graph.getVertex(catalog_fkey_table);
                if (other_vertex == null) {
                    throw new Exception("ERROR: The constraint '" + catalog_const + "' on '" + vertex + "' uses an unknown table '" + catalog_fkey_table.getName() + "'");
                }
                ColumnSet cset = new ColumnSet();
                // FIXME cset.add(catalog_const.getFkeycolumn, catalog_col);
                Edge edge = new Edge(graph);
                edge.setAttribute(DependencyGraph.EdgeAttributes.CONSTRAINT.name(), catalog_const);
                edge.setAttribute(DependencyGraph.EdgeAttributes.COLUMNSET.name(), cset);
                graph.addEdge(edge, other_vertex, vertex, EdgeType.DIRECTED);
            } // FOR
        } // FOR
    }
}
