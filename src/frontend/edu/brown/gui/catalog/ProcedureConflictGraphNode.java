package edu.brown.gui.catalog;

import java.util.Collection;

import javax.swing.JPanel;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractUndirectedGraph;
import edu.brown.graphs.AbstractVertex;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.CollectionUtil;

public class ProcedureConflictGraphNode {
    
    final Database catalog_db;
    final Collection<Procedure> procs;
    
    final ConflictGraph graph;
    GraphVisualizationPanel<ConflictVertex, ConflictEdge> vizPanel;
    
    public ProcedureConflictGraphNode(Collection<Procedure> procs) {
        this.catalog_db = CatalogUtil.getDatabase(CollectionUtil.first(procs));
        this.procs = procs;
        
        this.graph = new ConflictGraph(this.catalog_db);
        this.init();
    }
    
    private void init() {
        // First we need to construct the graph
        for (Procedure proc0 : this.procs) {
            ConflictVertex v0 = this.graph.getVertex(proc0);
            if (v0 == null) v0 = new ConflictVertex(proc0);
            for (Procedure proc1 : CatalogUtil.getConflictProcedures(proc0)) {
                ConflictVertex v1 = this.graph.getVertex(proc1);
                if (v1 == null) v1 = new ConflictVertex(proc1);
                if (this.graph.findEdge(v0, v1) == null) {
                    ConflictEdge e = new ConflictEdge(this.graph);
                    this.graph.addEdge(e, v0, v1);
                }
            }
        }
        
        this.vizPanel = GraphVisualizationPanel.factory(this.graph);
    }
    
    public JPanel getVisualization() {
        return (this.vizPanel);
    }
    
    @Override
    public String toString() {
        return ("Conflict Graph");
    }
    
    // ----------------------------------------------------------------------------
    // CONFLICT GRAPH
    // ----------------------------------------------------------------------------
    
    protected class ConflictVertex extends AbstractVertex {
        public ConflictVertex(Procedure catalog_proc) {
            super(catalog_proc);
        }
    }
    
    protected class ConflictEdge extends AbstractEdge {
        public ConflictEdge(ConflictGraph graph) {
            super(graph);
            this.setVerbose(false);
        }
        @Override
        public String toString() {
            return ("");
        }
    }
    
    protected class ConflictGraph extends AbstractUndirectedGraph<ConflictVertex, ConflictEdge> {
        private static final long serialVersionUID = 1L;
        public ConflictGraph(Database catalog_db) {
            super(catalog_db);
        }
    }
    
}
