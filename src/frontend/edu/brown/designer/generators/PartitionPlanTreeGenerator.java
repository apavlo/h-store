/**
 * 
 */
package edu.brown.designer.generators;

import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.designer.*;
import edu.brown.designer.partitioners.PartitionEntry;
import edu.brown.designer.partitioners.PartitionPlan;
import edu.brown.graphs.*;
import edu.uci.ics.jung.graph.util.*;

/**
 * @author pavlo
 *
 */
public class PartitionPlanTreeGenerator extends AbstractGenerator<PartitionTree> {
    private static final Logger LOG = Logger.getLogger(PartitionPlanTreeGenerator.class);

    private final PartitionPlan pplan;
    
    public PartitionPlanTreeGenerator(DesignerInfo info, PartitionPlan plan) {
        super(info);
        this.pplan = plan;
    }
    
    /**
     * Convenience method for generating a PartitionTree from a PartitionPlan
     * @param catalog_db
     * @return
     */
    public static PartitionTree generate(Database catalog_db, PartitionPlan pplan) {
        PartitionTree ptree = new PartitionTree(catalog_db);
        DesignerInfo info = new DesignerInfo(catalog_db, null, null);
        try {
            new PartitionPlanTreeGenerator(info, pplan).generate(ptree);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        return (ptree);
    }
    
    @Override
    public void generate(final PartitionTree ptree) throws Exception {
        for (Table catalog_tbl : pplan.getRoots()) {
            Vertex root = info.dgraph.getVertex(catalog_tbl);
            LOG.debug("ROOT: " + root);
            
            // Walk down the paths in the plans and create the partition tree that represents the PartitionPlan
            new VertexTreeWalker<Vertex, Edge>(info.dgraph) {
                protected void populate_children(VertexTreeWalker.Children<Vertex> children, Vertex element) {
                    Set<Table> element_children = pplan.getChildren((Table)element.getCatalogItem());
                    if (element_children != null) {
                        for (Table child_tbl : element_children) {
                            Vertex child = info.dgraph.getVertex(child_tbl);
                            children.addAfter(child);
                        } // FOR
                    }
                    return;
                }
                
                @Override
                protected void callback(Vertex element) {
                    PartitionEntry entry = PartitionPlanTreeGenerator.this.pplan.getTableEntries().get((Table) element.getCatalogItem());
                    // Bad Mojo!
                    if (entry == null) {
                        LOG.warn("ERROR: No PartitionPlan entry for '" + element + "'");
                    // Non-Root
                    } else if (entry.getParent() != null) {
                        LOG.debug("Trying to create: " + entry.getParent() + "->" + element);
                        Vertex parent = info.dgraph.getVertex(entry.getParent());
                        
                        element.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), entry.getAttribute());
                        element.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), entry.getMethod());
                        
                        if (parent != null && !ptree.containsVertex(element)) {
                            if (!ptree.containsVertex(parent)) ptree.addVertex(parent);
//                            System.out.println("FINAL GRAPH: " + parent + "->" + element);
                            Edge edge = new Edge(ptree);
                            ptree.addEdge(edge, parent, element, EdgeType.DIRECTED);
                            element.setAttribute(ptree, PartitionTree.VertexAttributes.PARENT_ATTRIBUTE.name(), entry.getParentAttribute());
                        }
                    // Root
                    } else if (!ptree.containsVertex(element)) {
                        ptree.addVertex(element);
                        element.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), entry.getAttribute());
                        element.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), entry.getMethod());
                    }
                }
            }.traverse(root);
        } // FOR
        return;
    }
}
