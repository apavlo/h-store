package edu.brown.gui.common;

import java.awt.*;
import java.awt.event.*;
import java.awt.geom.Point2D;
import java.lang.reflect.Method;

import javax.swing.JFrame;

import edu.brown.designer.PartitionTree;
import edu.brown.graphs.*;
import edu.brown.markov.MarkovGraph;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

import edu.uci.ics.jung.algorithms.layout.*;
import edu.uci.ics.jung.graph.*;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.*;
import edu.uci.ics.jung.visualization.decorators.*;
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;

public class GraphVisualizationPanel<V, E> extends VisualizationViewer<V, E> {
    private static final long serialVersionUID = 1L;
    
    public final EventObservable EVENT_SELECT_VERTEX = new EventObservable();
    public final EventObservable EVENT_SELECT_EDGE = new EventObservable();
    
    protected final Graph<V, E> graph;
    
    DefaultModalGraphMouse<V, E> visualizer_mouse;
    MouseListener<E> edge_listener;
    MouseListener<V> vertex_listener;

    /**
     * Convenience method for creating a JFrame that displays the graph
     * @param <V>
     * @param <E>
     * @param graph
     * @param observers
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <V, E> JFrame createFrame(Graph<V, E> graph, EventObserver...observers) {
        GraphVisualizationPanel<V, E> panel = factory(graph);
        for (EventObserver eo : observers) panel.EVENT_SELECT_VERTEX.addObserver(eo);
        
        // Check for a graph name
        Class clazz = graph.getClass();
        String title = clazz.getCanonicalName();
        try {
            Method handle = clazz.getMethod("getName");
            if (handle != null) {
                title = handle.invoke(graph).toString();
            }
        } catch (Exception ex) {
            // Ignore anything that gets thrown at us...
        }
        
        JFrame ret = new JFrame(title);
        ret.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        ret.setLayout(new BorderLayout());
        ret.setContentPane(panel);
        ret.setSize(650, 650);
        return (ret);
    }
    
    public static <V, E> GraphVisualizationPanel<V, E> factory(Graph<V, E> graph, EventObserver v_observer, EventObserver e_observer) {
        GraphVisualizationPanel<V, E> ret = GraphVisualizationPanel.factory(graph);
        if (v_observer != null) ret.EVENT_SELECT_VERTEX.addObserver(v_observer);
        if (e_observer != null) ret.EVENT_SELECT_EDGE.addObserver(e_observer);
        return (ret);
    }
    
    @SuppressWarnings("unchecked")
    public static <V, E> GraphVisualizationPanel<V, E> factory(Graph<V, E> graph) {
        Layout<V, E> layout = null;
        if (graph instanceof DelegateForest) { 
            layout = new TreeLayout<V, E>((Forest<V, E>) graph);
        } else if (graph instanceof MarkovGraph){
            layout = new FRLayout<V,E>( graph);
        }else if (graph instanceof AbstractDirectedGraph) {
            layout = new DAGLayout<V, E>(graph);
        } else
        {
            layout = new CircleLayout<V, E>(graph);
        }
        return (new GraphVisualizationPanel<V, E>(layout, graph));
    }
    
    private GraphVisualizationPanel(Layout<V, E> layout, Graph<V, E> graph) {
        super(layout);
        this.graph = graph;
        this.init();
    }
    
    public Graph<V, E> getGraph() {
        return this.graph;
    }
    
    /**
     * Map one of our graphs to 
     * @param agraph
     */
    @SuppressWarnings("unchecked")
    private void init() {
        this.setBackground(Color.white);
        this.visualizer_mouse = new DefaultModalGraphMouse<V, E>();
        this.visualizer_mouse.setMode(ModalGraphMouse.Mode.TRANSFORMING);
        
        this.setBackground(Color.white);
        
        // Auto-resize
        this.addComponentListener(new ComponentAdapter() {
            private Dimension last_size = null;
            
            @Override
            public void componentResized(ComponentEvent evt) {
                Dimension new_size = evt.getComponent().getSize();
                if (this.last_size == null || !new_size.equals(this.last_size)) {
                    Layout<V, E> layout = GraphVisualizationPanel.this.getGraphLayout();
                    try {
                        layout.setSize(new_size);
                        GraphVisualizationPanel.this.setGraphLayout(layout);
                        this.last_size = new_size;
                    } catch (UnsupportedOperationException ex) {
                        // Ignore...
                    }
                }
            }
        });

        this.getRenderContext().setEdgeLabelTransformer(new ToStringLabeller<E>());
        this.getRenderContext().setVertexLabelTransformer(new ToStringLabeller<V>());
        this.getRenderer().getVertexLabelRenderer().setPosition(Position.S);
        
        if (this.graph instanceof PartitionTree) {
            this.getRenderContext().setEdgeShapeTransformer(new EdgeShape.Line<V, E>());
        }
        if (this.graph instanceof AbstractDirectedGraph) {
            Dimension d = this.getGraphLayout().getSize(); 
            Point2D center = new Point2D.Double(d.width/2, d.height/2); 
            this.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT).rotate(-Math.PI, center); 
        }

        this.setGraphMouse(this.visualizer_mouse);
        this.edge_listener = new MouseListener<E>(this.getPickedEdgeState());
        this.vertex_listener = new MouseListener<V>(this.getPickedVertexState());
        this.addGraphMouseListener(this.vertex_listener);
        
        this.getPickedVertexState().addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                V vertex = CollectionUtil.getFirst(GraphVisualizationPanel.this.getPickedVertexState().getPicked());
                if (vertex != null) GraphVisualizationPanel.this.EVENT_SELECT_VERTEX.notifyObservers(vertex);
            }
        });
        this.getPickedEdgeState().addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                E edge = CollectionUtil.getFirst(GraphVisualizationPanel.this.getPickedEdgeState().getPicked());
                if (edge != null) GraphVisualizationPanel.this.EVENT_SELECT_EDGE.notifyObservers(edge);
            }
        });
        //this.visualizer.setBorder(BorderFactory.createLineBorder(Color.blue));
        this.repaint();
        
        //
        // Zoom in a little bit
        //
        new ViewScalingControl().scale(this, 0.85f, this.getCenter());
        //new LayoutScalingControl().scale();
    }
    
    @Override
    public void setVisible(boolean arg0) {
        // Hide UnsupportedOperationException
        try {
            super.setVisible(arg0);
        } catch (UnsupportedOperationException ex) {
            // Ignore...
        }
    }
    
    public void selectVertex(V vertex) {
        this.vertex_listener.graphClicked(vertex, null);
        this.EVENT_SELECT_VERTEX.notifyObservers(vertex);
    }
    
    public void selectEdge(E edge) {
        this.edge_listener.graphClicked(edge, null);
        this.EVENT_SELECT_EDGE.notifyObservers(edge);
    }
}