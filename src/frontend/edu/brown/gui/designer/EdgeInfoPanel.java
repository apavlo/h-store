package edu.brown.gui.designer;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ScrollPaneConstants;
import javax.swing.table.AbstractTableModel;

import org.voltdb.catalog.CatalogType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.IGraph;
import edu.brown.gui.AbstractInfoPanel;
import edu.brown.gui.AbstractViewer;
import edu.brown.gui.DesignerVisualization;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;
import edu.uci.ics.jung.graph.util.EdgeType;

public class EdgeInfoPanel extends AbstractInfoPanel<DesignerEdge> {
    private static final long serialVersionUID = 1L;
    
    protected final DesignerVisualization parent;
    protected JLabel idLabel;
    protected JLabel sourceVertexLabel;
    protected JLabel destVertexLabel;
    protected JPanel attributesPanel;
    protected Map<String, JLabel> attributeLabels = new HashMap<String, JLabel>();
    protected JPanel columnSetPanel;
    protected JTable columnSetTable;
    
    public EdgeInfoPanel(DesignerVisualization parent) {
        super();
        this.parent = parent;
    }
    
    protected void init() {
        this.setLayout(new BorderLayout());
        JPanel panel = new JPanel();
        this.add(panel, BorderLayout.NORTH);
    }
    
    public void update(DesignerEdge edge) {
        this.setEnabled(true);
        this.element = edge;
        
        GraphVisualizationPanel<DesignerVertex, DesignerEdge> visualizer = this.parent.getCurrentVisualizer();
        IGraph<DesignerVertex, DesignerEdge> graph = (IGraph<DesignerVertex, DesignerEdge>)visualizer.getGraph();
        DesignerVertex source = null;
        DesignerVertex dest = null;
        if (graph.getEdgeType(edge) == EdgeType.DIRECTED) {
            source = graph.getSource(edge);
            dest = graph.getDest(edge); 
        } else {
            ArrayList<DesignerVertex> vertices = new ArrayList<DesignerVertex>(graph.getIncidentVertices(edge));
            source = vertices.get(0);
            dest = vertices.get(1);
        }
        
        //
        // Reinitialize Edge Information
        //
        JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        JLabel label = null;
        
        GridBagConstraints c = AbstractViewer.getConstraints();

        c.gridx = 0;
        c.gridy = 0;
        label = new JLabel("ID:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);

        c.gridx = 1;
        this.idLabel = new JLabel(source != null ? Integer.toString(edge.hashCode()) : "null");
        this.idLabel.setFont(AbstractViewer.value_font);
        panel.add(this.idLabel, c);
        
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Source:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);

        c.gridx = 1;
        this.sourceVertexLabel = new JLabel(source != null ? source.toString() : "null");
        this.sourceVertexLabel.setFont(AbstractViewer.value_font);
        panel.add(this.sourceVertexLabel, c);
        
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Destination:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        
        c.gridx = 1;
        this.destVertexLabel = new JLabel(dest != null ? dest.toString() : "null");
        this.destVertexLabel.setFont(AbstractViewer.value_font);
        panel.add(this.destVertexLabel, c);

        /*
        c.gridx = 0;
        c.gridy++;
        c.gridwidth = 2;
        c.fill = GridBagConstraints.BOTH;
        this.columnSetPanel = new JPanel(new BorderLayout());
        panel.add(this.columnSetPanel, c);
         */

        //
        // Attributes
        //
        c.gridwidth = 1;
        c.fill = GridBagConstraints.NONE;
        Set<String> attributes = edge.getAttributes();
        if (attributes != null) {
            for (String attr : attributes) {
                if (attr.equals(AccessGraph.EdgeAttributes.COLUMNSET.name())) continue;
    
                c.gridx = 0;
                c.gridy++;
                label = new JLabel(StringUtil.title(attr) + ":");
                label.setFont(AbstractViewer.key_font);
                panel.add(label, c);
                
                c.gridx = 1;
                Object value = edge.getAttribute(attr);
                String text = null;
                if (value instanceof CatalogType) {
                    text = ((CatalogType)value).getName();
                } else {
                    text = value.toString();
                }
                label = new JLabel(text);
                label.setFont(AbstractViewer.value_font);
                panel.add(label, c);
            } // FOR
        }
        
        //
        // ColumnSet
        //
        JPanel columnSetPanel = new JPanel(new BorderLayout());
        if (edge.hasAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name())) {
            PredicatePairs cset = (PredicatePairs)edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
            
            this.columnSetTable = new JTable(new ColumnSetTableModel(cset));
            this.columnSetTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
            this.columnSetTable.setFillsViewportHeight(true);
            this.columnSetTable.setDragEnabled(false);
            this.columnSetTable.setColumnSelectionAllowed(false);
            this.columnSetTable.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
            this.columnSetTable.setFont(AbstractViewer.value_font);
            
            //Set the column widths
            /*
            TableColumn column = null;
            column = this.columnSetTable.getColumnModel().getColumn(0);
            column.setPreferredWidth((int)Math.round(this.parent.WINDOW_LEFT_SIZE / 2.25));
            column = this.columnSetTable.getColumnModel().getColumn(1);
            column.setPreferredWidth((int)Math.round(this.parent.WINDOW_LEFT_SIZE / 2.25));
            */
            
            JScrollPane scrollPane = new JScrollPane(this.columnSetTable);
            //scrollPane.setMaximumSize(this.columnSetTable.getPreferredScrollableViewportSize());
            scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
            columnSetPanel.add(scrollPane, BorderLayout.CENTER);
        } // COLUMNSET
        
        //this.columnSetPanel.removeAll();
        //this.columnSetPanel.add(scrollPane, BorderLayout.CENTER);
        //this.columnSetPanel.revalidate();
        
        this.removeAll();
        this.add(panel, BorderLayout.NORTH);
        this.add(columnSetPanel, BorderLayout.CENTER);
        this.revalidate();
    }
    
    /**
     * 
     * @author pavlo
     *
     */
    protected class ColumnSetTableModel extends AbstractTableModel {
        private static final long serialVersionUID = 6296162033763655458L;
        protected String columns[] = { "From", "To" };
        protected PredicatePairs cset = null;
        
        public ColumnSetTableModel(PredicatePairs cset) {
            this.cset = cset;
        }
        
        public String getColumnName(int col) { return (this.columns[col]); }
        public int getColumnCount() { return (this.columns.length); }
        public int getRowCount() { return (this.cset != null ? this.cset.size() : 0); }
        public Object getValueAt(int row, int col) {
            String ret = null;
            if (this.cset != null) {
                // FIXME
                //col = (col == 1 ? 0 : 1);
                CatalogType item = (CatalogType)this.cset.get(row).get(col);
                ret = CatalogUtil.getDisplayName(item, true);
            }
            return (ret);
        }
        public boolean isCellEditable(int row, int col) {
            return (false);
        }
        public Class<?> getColumnClass(int c) {
            return getValueAt(0, c).getClass();
        }
    };
}
