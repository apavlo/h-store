package edu.brown.gui.catalog;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Paint;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.Stroke;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JPanel;

import org.apache.commons.collections15.Transformer;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.types.ConflictType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractUndirectedGraph;
import edu.brown.graphs.AbstractVertex;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.CollectionUtil;
import edu.uci.ics.jung.graph.util.EdgeType;

public class ProcedureConflictGraphNode {
    private static final Logger LOG = Logger.getLogger(ProcedureConflictGraphNode.class);
    
    static final Map<ConflictType, Paint> CONFLICT_COLORS = new HashMap<ConflictType, Paint>();
    static {
        CONFLICT_COLORS.put(ConflictType.READ_WRITE, Color.GREEN.darker());
        CONFLICT_COLORS.put(ConflictType.WRITE_WRITE, Color.BLUE.darker());
    }
    
    final Database catalog_db;
    final Collection<Procedure> procs;
    
    final ConflictGraph graph;
    GraphVisualizationPanel<ConflictVertex, ConflictEdge> vizPanel;
    
    final Transformer<ConflictEdge, Paint> edgeColor = new Transformer<ConflictEdge, Paint>() {
        @Override
        public Paint transform(ConflictEdge e) {
            return CONFLICT_COLORS.get(e.type);
        }
    };
    final Transformer<ConflictEdge, Stroke> edgeStroke = new Transformer<ConflictEdge, Stroke>() {
        final Stroke s = new BasicStroke(2.0f);
        @Override
        public Stroke transform(ConflictEdge e) {
            return (s);
        }
    };
    
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
            assert(v0 != null);
            
            // READ-WRITE
            for (Procedure proc1 : CatalogUtil.getReadWriteConflicts(proc0)) {
                ConflictVertex v1 = this.graph.getVertex(proc1);
                if (v1 == null) v1 = new ConflictVertex(proc1);
                ConflictEdge e = new ConflictEdge(this.graph, ConflictType.READ_WRITE);
                this.graph.addEdge(e, v0, v1);
                LOG.debug(String.format("%s %s [%s/%s]", e.type, e.toStringPath(graph), proc0.getName(), proc1.getName()));
            } // FOR
            
            // WRITE-WRITE
            for (Procedure proc1 : CatalogUtil.getWriteWriteConflicts(proc0)) {
                ConflictVertex v1 = this.graph.getVertex(proc1);
                if (v1 == null) v1 = new ConflictVertex(proc1);
                ConflictEdge e = new ConflictEdge(this.graph, ConflictType.WRITE_WRITE);
                this.graph.addEdge(e, v0, v1);
                LOG.debug(String.format("%s %s [%s/%s]", e.type, e.toStringPath(graph), proc0.getName(), proc1.getName()));
            } // FOR
        }
        
        this.vizPanel = GraphVisualizationPanel.factory(this.graph);
        this.vizPanel.getRenderContext().setEdgeDrawPaintTransformer(this.edgeColor);
        this.vizPanel.getRenderContext().setArrowDrawPaintTransformer(this.edgeColor);
        this.vizPanel.getRenderContext().setArrowFillPaintTransformer(this.edgeColor);
        this.vizPanel.getRenderContext().setEdgeStrokeTransformer(this.edgeStroke);
        
        this.vizPanel.getRenderContext().setVertexShapeTransformer(
            new Transformer<ConflictVertex, Shape>() {
                @Override
                public Shape transform(ConflictVertex v) {
                    return new Rectangle(-20, -10, 40, 20);
                }
            }
        );
        
        
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
        final ConflictType type;
        public ConflictEdge(ConflictGraph graph, ConflictType type) {
            super(graph);
            this.type = type;
            this.setVerbose(false);
        }
        @Override
        public String toString() {
            switch (this.type) {
                case READ_WRITE:
                    return ("RW");
                case WRITE_READ:
                    return ("WR");
                case WRITE_WRITE:
                    return ("WW");
                default:
                    return ("??");
            }
        }
    }
    
    protected class ConflictGraph extends AbstractUndirectedGraph<ConflictVertex, ConflictEdge> {
        private static final long serialVersionUID = 1L;
        public ConflictGraph(Database catalog_db) {
            super(catalog_db);
        }
        @Override
        public EdgeType getEdgeType(ConflictEdge e) {
            return EdgeType.DIRECTED;
        }
    }
    
}
