package edu.brown.gui.catalog;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Font;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.geom.Point2D;
import java.util.Observable;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;

import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.gui.AbstractViewer;
import edu.brown.gui.CatalogViewer;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.plannodes.PlanNodeGraph;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.EventObserver;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.visualization.Layer;
import edu.uci.ics.jung.visualization.RenderContext;
import edu.uci.ics.jung.visualization.decorators.EdgeShape;

public class PlanTreeCatalogNode {

    private final String label;
    private final JPanel mainPanel;
    private JTextArea nodeField;
    private JTabbedPane tabbedPane;
    private final Statement catalog_stmt;
    private final AbstractPlanNode root;
    private final PlanNodeGraph graph;
    private final GraphVisualizationPanel<AbstractPlanNode, PlanNodeGraph.Edge> visualizationPanel;
    
    private boolean zoomed = false;
    
    private EventObserver vertex_observer = new EventObserver() {
        @Override
        public void update(Observable o, Object arg) {
            if (arg instanceof AbstractPlanNode) {
                PlanTreeCatalogNode.this.selectNode((AbstractPlanNode)arg);
            }
        }
    };
    
    public PlanTreeCatalogNode(String label, Statement catalog_stmt, AbstractPlanNode root) {
        this.label = label;
        this.catalog_stmt = catalog_stmt;
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
    
}
