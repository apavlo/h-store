package edu.brown.designer.generators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;

import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.utils.PredicatePairs;
import edu.brown.workload.Workload;
import edu.uci.ics.jung.graph.util.EdgeType;

/**
 * @author pavlo
 */
public class DependencyGraphGenerator extends AbstractGenerator<AbstractDirectedGraph<DesignerVertex, DesignerEdge>> {
    private static final Logger LOG = Logger.getLogger(DependencyGraphGenerator.class);

    public DependencyGraphGenerator(DesignerInfo info) {
        super(info);
    }

    /**
     * Convenience method for generating a DependencyGraph
     * 
     * @param catalog_db
     * @return
     */
    public static DependencyGraph generate(CatalogContext catalogContext) {
        DependencyGraph dgraph = new DependencyGraph(catalogContext.database);
        DesignerInfo info = new DesignerInfo(catalogContext, new Workload(catalogContext.catalog));
        try {
            new DependencyGraphGenerator(info).generate(dgraph);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        return (dgraph);
    }

    public void generate(AbstractDirectedGraph<DesignerVertex, DesignerEdge> graph) throws Exception {
        Collection<Table> catalog_tables = info.catalogContext.getDataTables();
        for (Table catalog_table : catalog_tables) {
            assert (catalog_table.getSystable() == false) : "Unexpected " + catalog_table;
            graph.addVertex(new DesignerVertex(catalog_table));
        } // FOR

        //
        // We first need to generate the list of candidate root tables
        // These are the tables that do not have any foreign key references
        //
        Map<DesignerVertex, List<Column>> fkey_ref_cols = new HashMap<DesignerVertex, List<Column>>();
        Map<DesignerVertex, List<Constraint>> fkey_ref_consts = new HashMap<DesignerVertex, List<Constraint>>();
        for (Table catalog_table : catalog_tables) {
            DesignerVertex vertex = graph.getVertex(catalog_table);
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
              // for (Column catalog_col : catalog_table.getColumns()) {
            // LOG.debug("Inspecting column '" + catalog_col.getName() +
            // "' in table '" + catalog_table.getName() + "'");
            // } // FOR
        } // FOR
          //
          // Build the dependency graph's edges
          //
        for (Table catalog_table : catalog_tables) {
            // For each vertex, get the list of foreign key references to point
            // to it and
            // make a new edge between the parent and child
            DesignerVertex vertex = graph.getVertex(catalog_table);
            for (int ctr = 0, cnt = fkey_ref_consts.get(vertex).size(); ctr < cnt; ctr++) {
                Constraint catalog_const = fkey_ref_consts.get(vertex).get(ctr);
                Column catalog_col = fkey_ref_cols.get(vertex).get(ctr);

                //
                // Grab the table object used in this foreign key constraint
                // We then get the vertex that we're using to represent it
                //
                Table catalog_fkey_table = catalog_const.getForeignkeytable();
                DesignerVertex other_vertex = graph.getVertex(catalog_fkey_table);
                if (other_vertex == null) {
                    throw new Exception("ERROR: The constraint '" + catalog_const + "' on '" + vertex + "' uses an unknown table '" + catalog_fkey_table.getName() + "'");
                }
                PredicatePairs cset = new PredicatePairs();
                // FIXME cset.add(catalog_const.getFkeycolumn, catalog_col);
                DesignerEdge edge = new DesignerEdge(graph);
                edge.setAttribute(DependencyGraph.EdgeAttributes.CONSTRAINT.name(), catalog_const);
                edge.setAttribute(DependencyGraph.EdgeAttributes.COLUMNSET.name(), cset);
                graph.addEdge(edge, other_vertex, vertex, EdgeType.DIRECTED);
            } // FOR
        } // FOR
    }
}
