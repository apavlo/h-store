package edu.brown.gui.designer;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.voltdb.catalog.CatalogType;

import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.IGraph;
import edu.brown.gui.AbstractInfoPanel;
import edu.brown.gui.AbstractViewer;
import edu.brown.gui.DesignerVisualization;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.StringUtil;

public class VertexInfoPanel extends AbstractInfoPanel<DesignerVertex> {
    private static final long serialVersionUID = -7696030449965056519L;
    
    protected final DesignerVisualization parent;
    JLabel relationLabel;
    JComboBox edgesCombo;
    protected JPanel attributesPanel;
    protected Map<String, JLabel> attributeLabels = new HashMap<String, JLabel>();
    
    public VertexInfoPanel(DesignerVisualization parent) {
        super();
        this.parent = parent;
    }
    
    protected void init() {
        this.setLayout(new BorderLayout());
        JPanel panel = new JPanel();
        this.add(panel, BorderLayout.NORTH);

    }
    
    public void update(DesignerVertex vertex) {
        this.setEnabled(true);
        this.setLayout(new BorderLayout());
        JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        this.add(panel, BorderLayout.NORTH);
        
        GridBagConstraints c = AbstractViewer.getConstraints();
        JLabel label = null;
        
        c.gridx = 0;
        label = new JLabel("Relation:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        c.gridx = 1;
        this.relationLabel = new JLabel("");
        this.relationLabel.setFont(AbstractViewer.value_font);
        panel.add(this.relationLabel, c);

        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Edges:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        
        this.edgesCombo = new JComboBox();
        this.edgesCombo.setFont(AbstractViewer.value_font);
        this.edgesCombo.addItemListener(new ItemListener() {
            @Override
            public void itemStateChanged(ItemEvent e) {
                if (e.getStateChange() == ItemEvent.SELECTED) {
                    DesignerEdge edge = (DesignerEdge)VertexInfoPanel.this.edgesCombo.getSelectedItem();
                    VertexInfoPanel.this.parent.getCurrentVisualizer().selectEdge(edge);
                }
            }
        });
        c.gridx = 1;
        panel.add(this.edgesCombo, c);

        
        GraphVisualizationPanel<DesignerVertex, DesignerEdge> visualizer = this.parent.getCurrentVisualizer();
        IGraph<DesignerVertex, DesignerEdge> graph = (IGraph<DesignerVertex, DesignerEdge>)visualizer.getGraph();
        
        this.relationLabel.setText(vertex.getCatalogItem().getName());
        
        /*
        Set<String> attributes = v.getAttributes(graph);
        if (attributes != null) {
            for (String key : attributes) {
                System.out.println(key + ": " + v.getAttribute(graph, key));
            }
        }*/
        
        //
        // Edges
        //
        this.edgesCombo.removeAllItems();
        boolean enabled = false;
        for (DesignerEdge edge : graph.getIncidentEdges(vertex)) {
            this.edgesCombo.addItem(edge);
            enabled = true;
        } // FOR
        this.edgesCombo.setEnabled(enabled);
        
        //
        // Attributes
        //
        c.gridwidth = 1;
        c.fill = GridBagConstraints.NONE;
        Set<String> attributes = vertex.getAttributes(graph);
        if (attributes != null) {
            for (String attr : attributes) {
                c.gridx = 0;
                c.gridy++;
                label = new JLabel(StringUtil.title(attr) + ":");
                label.setFont(AbstractViewer.key_font);
                panel.add(label, c);
                
                c.gridx = 1;
                Object value = vertex.getAttribute(graph, attr);
                String text = null;
                if (value instanceof CatalogType) {
                    text = ((CatalogType)value).getName();
                } else if (value != null) {
                    text = value.toString();
                } else {
                    text = "-";
                }
                label = new JLabel(text);
                label.setFont(AbstractViewer.value_font);
                panel.add(label, c);
            } // FOR
        }
        
        this.removeAll();
        this.add(panel, BorderLayout.CENTER);
        this.revalidate();
    }
}
