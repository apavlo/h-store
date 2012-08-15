package edu.brown.gui.catalog;

import java.awt.BasicStroke;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.Rectangle;
import java.awt.Shape;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.Collection;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;

import org.voltdb.catalog.PlanFragment;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.utils.Pair;

import edu.brown.gui.AbstractViewer;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.plannodes.PlanNodeGraph;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.uci.ics.jung.visualization.RenderContext;
import edu.uci.ics.jung.visualization.VisualizationServer.Paintable;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;

public class PlanTreeCatalogNode {

    private final String label;
    private final JPanel mainPanel;
    private JTextArea nodeField;
    private JTabbedPane tabbedPane;
    private final Collection<PlanFragment> fragments;
    private final AbstractPlanNode root;
    private final PlanNodeGraph graph;
    private final GraphVisualizationPanel<AbstractPlanNode, PlanNodeGraph.Edge> visualizationPanel;
    
    private boolean zoomed = false;
    
    private EventObserver<AbstractPlanNode> vertex_observer = new EventObserver<AbstractPlanNode>() {
        @Override
        public void update(EventObservable<AbstractPlanNode> o, AbstractPlanNode arg) {
            PlanTreeCatalogNode.this.selectNode((AbstractPlanNode)arg);
        }
    };
    
    public PlanTreeCatalogNode(String label, Collection<PlanFragment> fragments, AbstractPlanNode root) {
        this.label = label;
        this.fragments = fragments;
        this.root = root;
        
        this.graph = new PlanNodeGraph(this.root);
        this.mainPanel = new JPanel(new BorderLayout());
        this.visualizationPanel = GraphVisualizationPanel.factory(this.graph, this.vertex_observer, null);
        this.init();
    }
    
    private void init() {
        // GraphVisualization
        RenderContext<AbstractPlanNode, PlanNodeGraph.Edge> context = this.visualizationPanel.getRenderContext();
        context.setEdgeShapeTransformer(new EdgeShape.Line<AbstractPlanNode, PlanNodeGraph.Edge>());
        context.setVertexFontTransformer(new GraphVisualizationPanel.VertexFontTransformer<AbstractPlanNode>(true));

//        PlanFragmentBoundaries boundaryPainter = new PlanFragmentBoundaries();
//        this.visualizationPanel.addPostRenderPaintable(boundaryPainter);
        
        // Full Plan Tab
        JPanel textInfoPanel = new JPanel();
        textInfoPanel.setLayout(new BorderLayout());
        JTextArea textInfoTextArea = new JTextArea();
        textInfoTextArea.setEditable(false);
        textInfoTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        textInfoTextArea.setText(PlanNodeUtil.debug(this.root));
        textInfoPanel.add(new JScrollPane(textInfoTextArea), BorderLayout.CENTER);

        // Node Field Tab
        this.nodeField = new JTextArea();
        this.nodeField.setEditable(false);
        this.nodeField.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        this.nodeField.setText("");
        JPanel textInfoPanel2 = new JPanel(new BorderLayout());
        textInfoPanel2.add(new JScrollPane(this.nodeField), BorderLayout.CENTER);
        
        this.tabbedPane = new JTabbedPane();
        this.tabbedPane.add("Full Plan", textInfoPanel);
        this.tabbedPane.add("Selected Node", textInfoPanel2);
        
        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, visualizationPanel, this.tabbedPane);
        splitPane.setDividerLocation(AbstractViewer.DEFAULT_WINDOW_HEIGHT - 500);
        this.mainPanel.add(splitPane, BorderLayout.CENTER);
    }
    
    /**
     * 
     */
    public void centerOnRoot() {
        if (this.zoomed == false) {
            this.visualizationPanel.zoom(1.2);
            this.zoomed = true;
        }
        
        final int depth = PlanNodeUtil.getDepth(this.root) / 2;
        new PlanNodeTreeWalker(false) {
            @Override
            protected void callback(AbstractPlanNode element) {
                if (depth == this.getDepth()) {
                    PlanTreeCatalogNode.this.visualizationPanel.centerVisualization(element, true);
                    this.stop();
                }
            }
        }.traverse(this.root);
    }
    
    /**
     * 
     * @param node
     */
    private void selectNode(AbstractPlanNode node) {
        this.nodeField.setText(PlanNodeUtil.debugNode(node));
        this.tabbedPane.setSelectedIndex(1);
//        this.visualizationPanel.centerVisualization(node);
    }
    
    @Override
    public String toString() {
        return (this.label);
    }
    
    public JPanel getPanel() {
        return (this.mainPanel);
    }
    
    // -----------------------------------------------------------------
    // PLANFRAGMENT BOUNDARY BOXES
    // -----------------------------------------------------------------
    
    /**
     * An attempt to draw PlanFragment boundaries
     * @author pavlo
     */
    @SuppressWarnings("unused")
    private class PlanFragmentBoundaries implements Paintable {
        private PlanFragment fragments[];
        private Pair<AbstractPlanNode, AbstractPlanNode> fragment_boundaries[];
        private AbstractPlanNode nodes[];
        private int nodes_frags[];
        
        final Font pfFont = new Font("Serif", Font.BOLD, 10);
        final BasicStroke boundaryStroke = new BasicStroke(1.0f,
                                                           BasicStroke.CAP_BUTT,
                                                           BasicStroke.JOIN_MITER,
                                                           10.0f,
                                                           new float[]{5.0f},
                                                           0.0f);
        
        @SuppressWarnings("unchecked")
        PlanFragmentBoundaries() {
            this.fragments = PlanTreeCatalogNode.this.fragments.toArray(new PlanFragment[0]);
            this.fragment_boundaries = (Pair<AbstractPlanNode, AbstractPlanNode>[])new Pair<?,?>[this.fragments.length];
            int num_nodes = graph.getVertexCount();
            this.nodes = new AbstractPlanNode[num_nodes];
            this.nodes_frags = new int[num_nodes];
            int node_idx = 0;
            for (AbstractPlanNode n : graph.getVertices()) {
                this.nodes[node_idx] = n;
                
                // Now figure out what fragment each AbstractPlanNode belongs to
                this.nodes_frags[node_idx] = -1;
                for (int frag_idx = 0; frag_idx < this.fragments.length; frag_idx++) {
                    if (PlanNodeUtil.containsPlanNode(this.fragments[frag_idx], n)) {
                        this.nodes_frags[node_idx] = frag_idx;
                        break;
                    }
                } // FOR
                assert(this.nodes_frags[node_idx] != -1);
                node_idx++;
            } // FOR
            
            for (int frag_idx = 0; frag_idx < this.fragments.length; frag_idx++) {
                AbstractPlanNode max = null;
                int max_depth = -1;
                
                AbstractPlanNode min = null;
                int min_depth = Integer.MAX_VALUE;
                
                for (node_idx = 0; node_idx < nodes.length; node_idx++) {
                    if (this.nodes_frags[node_idx] != frag_idx) continue;
                    int depth = PlanNodeUtil.getDepth(this.nodes[node_idx]);
                    if (depth > max_depth) {
                        max_depth = depth;
                        max = this.nodes[node_idx];
                    }
                    if (depth < min_depth) {
                        min_depth = depth;
                        min = this.nodes[node_idx];
                    }
//                    System.err.println(this.nodes[node_idx] + " => " + depth);
                } // FOR
                if (max == null) max = min;
                if (min == null) min = max;
                this.fragment_boundaries[frag_idx] = Pair.of(max, min);
//                System.err.println(this.fragment_boundaries[frag_idx] + "\n");
            } // FOR (fragments)
            
        }
        
        @Override
        public void paint(Graphics g) {
            Graphics2D g2 = (Graphics2D)g;
            g2.setStroke(boundaryStroke);
            
            int offsetLeft = 100;
            int offsetRight = 50;
            int box_y = 0;
            
            float text_x = 5f;
            float text_y = 10f;
            
            Rectangle nodeSize = null;
            
//            for (AbstractPlanNode node : this.nodes) {
//                System.err.println(node + " -> " + visualizationPanel.getPosition(node));
//            }
            
            Font origFont = g2.getFont();
            g2.setFont(pfFont);
            for (int frag_idx = 0; frag_idx < this.fragments.length; frag_idx++) {
                g2.setColor(frag_idx == 0 ? Color.BLUE : Color.BLACK);
//                System.err.println(this.fragment_boundaries[frag_idx] + " => " + this.nodes_frags[frag_idx]);
                
                Pair<AbstractPlanNode, AbstractPlanNode> p = this.fragment_boundaries[frag_idx]; 
                AbstractPlanNode topNode = p.getFirst();
                Point2D topPos = visualizationPanel.getPosition(topNode);
//                System.err.println(topNode + " -> " + topPos);
                AbstractPlanNode botNode = p.getSecond();
                Point2D botPos = visualizationPanel.getPosition(botNode);
//                System.err.println(botNode + " -> " + botPos);
                
                if (nodeSize == null) {
                    Shape s = visualizationPanel.getRenderContext().getVertexShapeTransformer().transform(topNode);
                    nodeSize = s.getBounds();
//                    System.err.println("NodeSize: " + nodeSize.getBounds2D());
                }
                
                Point2D abs_topCorner = new Point2D.Double(topPos.getX() - offsetLeft,
                                                           topPos.getY() - 10);
//                Point2D draw_topCorner = visualizationPanel.transform(abs_topCorner);
                
                Point2D abs_botCorner = new Point2D.Double(botPos.getX() + offsetLeft,
                                                           botPos.getY() + nodeSize.getHeight());
//                Point2D draw_botCorner = visualizationPanel.transform(abs_botCorner);
                
                Rectangle2D.Double rect = new Rectangle2D.Double(abs_topCorner.getX(),
                                                                 abs_topCorner.getY(),
                                                                 Math.abs(abs_topCorner.getX() - abs_botCorner.getX()),
                                                                 Math.abs(abs_topCorner.getY() - abs_botCorner.getY()));
                Shape s = visualizationPanel.transform(rect); 
                g2.draw(s);
//                System.err.println(s.getBounds2D());
                
                // PlanFragment Label
                Point2D text_pos = new Point2D.Float((float)abs_topCorner.getX() + text_x,
                                                     (float)abs_topCorner.getY() + text_y);
                text_pos = visualizationPanel.transform(text_pos);
                g2.setColor(Color.BLACK);
                g2.drawString("#" + this.fragments[frag_idx].getName(),
                              (float)text_pos.getX(),
                              (float)text_pos.getY());
//                System.err.println();
            }
            g2.setFont(origFont);
        }

        @Override
        public boolean useTransform() {
            return false;
        }
    } // END CLASS
    
}
