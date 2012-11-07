package edu.brown.gui.common;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Shape;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.geom.Point2D;
import java.lang.reflect.Method;
import java.util.concurrent.CountDownLatch;

import javax.swing.JFrame;

import org.apache.commons.collections15.Transformer;

import edu.brown.catalog.conflicts.ConflictGraph;
import edu.brown.designer.PartitionTree;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.markov.MarkovGraph;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.DAGLayout;
import edu.uci.ics.jung.algorithms.layout.FRLayout;
import edu.uci.ics.jung.algorithms.layout.KKLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.algorithms.layout.TreeLayout;
import edu.uci.ics.jung.graph.DelegateForest;
import edu.uci.ics.jung.graph.Forest;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.DefaultModalGraphMouse;
import edu.uci.ics.jung.visualization.control.ModalGraphMouse;
import edu.uci.ics.jung.visualization.control.ViewScalingControl;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;
import edu.uci.ics.jung.visualization.renderers.Renderer.VertexLabel.Position;
import edu.uci.ics.jung.visualization.transform.MutableTransformer;

public class GraphVisualizationPanel<V, E> extends VisualizationViewer<V, E> {
    private static final long serialVersionUID = 1L;
    
    public final EventObservable<V> EVENT_SELECT_VERTEX = new EventObservable<V>();
    public final EventObservable<E> EVENT_SELECT_EDGE = new EventObservable<E>();
    
    protected final Graph<V, E> graph;
    
    DefaultModalGraphMouse<V, E> visualizer_mouse;
    MouseListener<E> edge_listener;
    MouseListener<V> vertex_listener;
    final ViewScalingControl viewScalingControl = new ViewScalingControl();

    public static <V, E> EventObserver<V> makeVertexObserver(final Graph<V, E> graph) {
        return new EventObserver<V>() {
            @Override
            public void update(EventObservable<V> o, V v) {
                System.out.println(v);
            }
        };
    }
    
    /**
     * VertexFontTransformer
     * Copied from http://jung.sourceforge.net/site/jung-samples/
     * @param <V>
     */
    public final static class VertexFontTransformer<V> implements Transformer<V, Font> {
        private boolean bold;
        private final Font f = new Font("Helvetica", Font.PLAIN, 12);
        private final Font b = new Font("Helvetica", Font.BOLD, 12);

        public VertexFontTransformer(boolean bold) {
            this.bold = bold;
        }
        public VertexFontTransformer() {
            this(false);
        }
        
        public void setBold(boolean bold) {
            this.bold = bold;
        }
        
        public Font transform(V v) {
            return (bold ? b : f);
        }
    }

    /**
     * EdgeFontTransformer
     * Copied from http://jung.sourceforge.net/site/jung-samples/
     * @param <E>
     */
    public final static class EdgeFontTransformer<E> implements Transformer<E, Font> {
        private boolean bold;
        private final Font f = new Font("Helvetica", Font.PLAIN, 12);
        private final Font b = new Font("Helvetica", Font.BOLD, 12);

        public EdgeFontTransformer(boolean bold) {
            this.bold = bold;
        }
        public EdgeFontTransformer() {
            this(false);
        }
        
        public void setBold(boolean bold) {
            this.bold = bold;
        }

        public Font transform(E e) {
            return (bold ? b : f);
        }
    }
    
    public static <V, E> void show(final Graph<V, E> graph) throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread() {
            @SuppressWarnings("unchecked")
            public void run() {
                try {
                    GraphVisualizationPanel.createFrame(graph).setVisible(true);
                } finally {
                    latch.countDown();
                }
            }
        };
        t.setDaemon(true);
        t.start();
        latch.await();
    }
        
    /**
     * Convenience method for creating a JFrame that displays the graph
     * @param <V>
     * @param <E>
     * @param graph
     * @param observers
     * @return
     */
    public static <V, E> JFrame createFrame(Graph<V, E> graph, EventObserver<V>...observers) {
        GraphVisualizationPanel<V, E> panel = factory(graph);
        for (EventObserver<V> eo : observers) panel.EVENT_SELECT_VERTEX.addObserver(eo);
        
        // Check for a graph name
        Class<?> clazz = graph.getClass();
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
    
    public static <V, E> GraphVisualizationPanel<V, E> factory(Graph<V, E> graph, EventObserver<V> v_observer, EventObserver<E> e_observer) {
        GraphVisualizationPanel<V, E> ret = GraphVisualizationPanel.factory(graph);
        if (v_observer != null) ret.EVENT_SELECT_VERTEX.addObserver(v_observer);
        if (e_observer != null) ret.EVENT_SELECT_EDGE.addObserver(e_observer);
        return (ret);
    }
    
    public static <V, E> GraphVisualizationPanel<V, E> factory(Graph<V, E> graph) {
        Layout<V, E> layout = null;
        if (graph instanceof DelegateForest) { 
            layout = new TreeLayout<V, E>((Forest<V, E>) graph);
        } else if (graph instanceof MarkovGraph){
            layout = new FRLayout<V,E>(graph);
        } else if (graph instanceof ConflictGraph){
            layout = new KKLayout<V, E>(graph);
        } else if (graph instanceof AbstractDirectedGraph) {
            layout = new DAGLayout<V, E>(graph);
        } else {
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
    private void init() {
        this.setBackground(Color.white);
        this.visualizer_mouse = new DefaultModalGraphMouse<V, E>();
        this.visualizer_mouse.setMode(ModalGraphMouse.Mode.TRANSFORMING);
        
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
                V vertex = CollectionUtil.first(GraphVisualizationPanel.this.getPickedVertexState().getPicked());
                if (vertex != null) GraphVisualizationPanel.this.EVENT_SELECT_VERTEX.notifyObservers(vertex);
            }
        });
        this.getPickedEdgeState().addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                E edge = CollectionUtil.first(GraphVisualizationPanel.this.getPickedEdgeState().getPicked());
                if (edge != null) GraphVisualizationPanel.this.EVENT_SELECT_EDGE.notifyObservers(edge);
            }
        });
        //this.visualizer.setBorder(BorderFactory.createLineBorder(Color.blue));
        this.repaint();
        
        // Zoom in a little bit
        this.zoom(0.85);
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

    /**
     * 
     * @param scale
     */
    public void zoom(Double scale) {
        this.viewScalingControl.scale(this, scale.floatValue(), this.getCenter());
    }
    
    /**
     * Center the visualization panel on the given vertex
     * @param vertex
     */
    public void centerVisualization(V vertex) {
        this.centerVisualization(vertex, false);
    }
    
    public void centerVisualization(V vertex, final boolean immediate) {
        if (vertex != null) {
            Layout<V,E> layout = this.getGraphLayout();
            Point2D q = layout.transform(vertex);
            Point2D lvc = this.getRenderContext().getMultiLayerTransformer().inverseTransform(this.getCenter());
            final int steps = (immediate ? 1 : 10);
            final double dx = (lvc.getX() - q.getX()) / steps;
            final double dy = (lvc.getY() - q.getY()) / steps;
    
            new Thread() {
                public void run() {
                    MutableTransformer transformer = GraphVisualizationPanel.this.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT); 
                    for (int i = 0; i < steps; i++) {
                        transformer.translate(dx, dy);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException ex) {
                        }
                    }
                }
            }.start();
        }
    }
    
    /**
     * Return the position of the given vertex on the canvas
     * @param vertex
     * @return
     */
    public Point2D getPosition(V vertex) {
        Point2D pos = null; 
        if (vertex != null) {
            Layout<V,E> layout = this.getGraphLayout();
            pos = layout.transform(vertex);
//            MutableTransformer transformer = GraphVisualizationPanel.this.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT);
//            pos = transformer.inverseTransform(pos);
        }
        return (pos); 
    }
    
    public Point2D transform(Point2D p) {
        MutableTransformer transformer = GraphVisualizationPanel.this.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT);
        return (transformer.transform(p));
    }
    
    public Shape transform(Shape s) {
        MutableTransformer transformer = GraphVisualizationPanel.this.getRenderContext().getMultiLayerTransformer().getTransformer(Layer.LAYOUT);
        return (transformer.transform(s));
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