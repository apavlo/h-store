/**
 * 
 */
package edu.brown.designer.indexselectors;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;
import org.voltdb.utils.CatalogUtil;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerEdge.Members;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.IndexPlan;
import edu.brown.designer.PartitionTree;
import edu.brown.designer.partitioners.plan.PartitionEntry;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.utils.PredicatePairs;

/**
 * @author pavlo
 */
public class SimpleIndexSelector extends AbstractIndexSelector {

    public SimpleIndexSelector(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    /*
     * (non-Javadoc)
     * @see edu.brown.designer.indexselectors.AbstractIndexSelector#generate()
     */
    @Override
    public IndexPlan generate(PartitionPlan plan) throws Exception {
        IndexPlan indexPlan = new IndexPlan(info.catalogContext.database);

        //
        // Go through and count up all the attribute sets that aren't used for
        // partitioning
        //
        PartitionTree ptree = null; // FIXME designer.getPartitionTree();
        for (Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            AccessGraph agraph = designer.getAccessGraph(catalog_proc);
            if (agraph == null)
                continue;

            for (DesignerEdge edge : agraph.getEdges()) {
                ArrayList<DesignerVertex> vertices = new ArrayList<DesignerVertex>();
                vertices.addAll(agraph.getIncidentVertices(edge));
                // FIXME
                if (true || !(ptree.getPath(vertices.get(0), vertices.get(1)).isEmpty() && ptree.getPath(vertices.get(1), vertices.get(0)).isEmpty())) {
                    PredicatePairs cset = (PredicatePairs) (edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name()));
                    for (DesignerVertex vertex : vertices) {
                        Table catalog_tbl = vertex.getCatalogItem();
                        Collection<Column> edge_columns = cset.findAllForParent(Column.class, catalog_tbl);

                        //
                        // Exclusion: Check whether this table is already
                        // partitioned on these columns
                        //
                        PartitionEntry pentry = plan.getTableEntries().get(catalog_tbl);
                        if (pentry == null) {
                            LOG.warn("PartitionEntry is null for " + catalog_tbl);
                            continue;
                            // } else if (pentry.getMethod() !=
                            // PartitionMethodType.REPLICATION &&
                            // pentry.getAttributes().equals(edge_columns)) {
                            // LOG.info(catalog_tbl +
                            // " is already partitioned on " + edge_columns +
                            // ". Skipping...");
                            // continue;
                        }
                        //
                        // Exclusion: Check whether this is the table's primary
                        // key
                        //
                        Collection<Column> pkeys = CatalogUtil.getPrimaryKeyColumns(catalog_tbl);
                        if (pkeys.containsAll(edge_columns) && edge_columns.containsAll(pkeys)) {
                            LOG.info(catalog_tbl + "'s primary key already contains " + edge_columns + ". Skipping...");
                            continue;
                        }
                        //
                        // Exclusion: These columns are only used in INSERTS
                        //
                        Map<QueryType, Integer> query_counts = cset.getQueryCounts();
                        if (query_counts.get(QueryType.SELECT) == 0 && query_counts.get(QueryType.UPDATE) == 0 && query_counts.get(QueryType.DELETE) == 0) {
                            LOG.info("The columns " + edge_columns + " are only used in INSERT operations on " + catalog_tbl + ". Skipping...");
                            continue;
                        }

                        //
                        // Check whether we already have a candidate index for
                        // this set of columns
                        //
                        IndexPlan.Entry found = null;
                        for (IndexPlan.Entry index : indexPlan.get(catalog_tbl)) {
                            if (index.getColumns().containsAll(edge_columns) && edge_columns.containsAll(index.getColumns())) {
                                found = index;
                                break;
                            }
                        } // FOR

                        //
                        // We have a match, so we need to add this edge's weight
                        // to it
                        //
                        Double weight = (Double) edge.getAttribute(Members.WEIGHTS.name());
                        if (found != null) {
                            weight += found.getWeight();
                        } else {
                            found = indexPlan.new Entry(catalog_tbl);
                            found.getColumns().addAll(edge_columns);
                        }
                        found.setWeight(weight);
                        found.getProcedures().add(catalog_proc);
                        indexPlan.get(catalog_tbl).add(found);
                    } // FOR
                } // IF
            } // FOR
        } // FOR

        //
        // We now need to consolidate overlapping indexes for each table if they
        // are
        // used in the same procedure
        //
        for (Table catalog_tbl : indexPlan.keySet()) {
            Iterator<IndexPlan.Entry> it0 = indexPlan.get(catalog_tbl).iterator();
            while (it0.hasNext()) {
                IndexPlan.Entry index0 = it0.next();
                //
                // Look for another index that has all our columns
                //
                for (IndexPlan.Entry index1 : indexPlan.get(catalog_tbl)) {
                    if (index0 == index1)
                        continue;
                    if (index1.getColumns().containsAll(index0.getColumns())
                            && (index1.getProcedures().containsAll(index0.getProcedures()) || index0.getProcedures().containsAll(index1.getProcedures()))) {
                        //
                        // Merge the one index into the other
                        //
                        index1.merge(index0);
                        it0.remove();
                        break;
                    }
                } // FOR
            } // WHILE
        } // FOR

        //
        // Now that we have our candidate indexes, we need to go through and
        // multiple
        // the index weights by the number of times the procedures are executed
        // that would
        // use that index
        //
        /*
         * for (Table catalog_tbl : this.indexes.keySet()) {
         * Set<IndexPlan.Entry> remove = new HashSet<IndexPlan.Entry>(); for
         * (IndexPlan.Entry index : this.indexes.get(catalog_tbl)) { Double
         * weight = index.getWeight(); // // Important! Some procedures may have
         * never been executed, so we need // to make sure we don't multiply the
         * weight if the count is zero // for (Procedure catalog_proc :
         * index.getProcedures()) { int count =
         * this.info.stats.get(catalog_proc).proc_counts; if (count > 0) weight
         * *= count; } // FOR if (weight == 0) {
         * LOG.info("Removing candidate index " + index +
         * " because its weight is zero"); remove.add(index); }
         * index.setWeight(weight); } // FOR if (!remove.isEmpty())
         * this.indexes.get(catalog_tbl).removeAll(remove); } // FOR
         */

        //
        // Ah-ha! We can now sort the indexes by their weights
        //
        // for (IndexPlan.Entry index : sorted) {
        // System.out.println("[" + index.getWeight() + "] " + index + " - " +
        // index.getProcedures());
        // }
        return (indexPlan);
    }

}
