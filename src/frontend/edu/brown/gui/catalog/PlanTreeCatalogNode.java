package edu.brown.gui.catalog;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.Graphics;
import java.awt.geom.Point2D;
import java.util.Collection;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;

import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;

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

        PlanFragmentBoundaries boundaryPainter = new PlanFragmentBoundaries();
        this.visualizationPanel.addPreRenderPaintable(boundaryPainter);
        
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
    
    private class PlanFragmentBoundaries implements Paintable {
        private PlanFragment fragments[];
        private AbstractPlanNode nodes[];
        private int nodes_frags[];
        
        PlanFragmentBoundaries() {
            this.fragments = PlanTreeCatalogNode.this.fragments.toArray(new PlanFragment[0]);
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
                if (this.nodes_frags[node_idx] == -1) {
                    System.err.println("???");
                }
                assert(this.nodes_frags[node_idx] != -1);
                node_idx++;
            } // FOR
        }
        
        @Override
        public void paint(Graphics g) {
            for (int i = 0; i < this.nodes.length; i++) {
                Point2D pos = visualizationPanel.getPosition(this.nodes[i]);
                int x = (int)pos.getX();
                int y = (int)pos.getY();
                
                g.drawLine(x, y, x + 100, y);
            } // FOR
        }

        @Override
        public boolean useTransform() {
            return false;
        }
    } // END CLASS

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
    
}
