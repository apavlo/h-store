package edu.brown.gui.designer;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.util.ArrayList;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ScrollPaneConstants;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;

import org.voltdb.catalog.Table;

import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.graphs.IGraph;
import edu.brown.gui.DesignerVisualization;
import edu.brown.gui.common.GraphVisualizationPanel;

public class PartitionPlanPanel extends JPanel {
    private static final long serialVersionUID = 1L;

    private final DesignerVisualization parent;
    private final Designer designer;
    
    public PartitionPlanPanel(DesignerVisualization parent) {
        super();
        this.parent = parent;
        this.designer = this.parent.getDesigner();
        this.init();
    }
    
    private void init() {
        this.setLayout(new BorderLayout());
        
        final PartitionPlan plan = this.designer.getPartitionPlan();
        final ArrayList<Table> tables = new ArrayList<Table>();
        tables.addAll(plan.getTableEntries().keySet());
        
        final JTable partitonTable = new JTable(new AbstractTableModel() {
            private static final long serialVersionUID = 1L;
            protected final String columns[] = { "Table", "Method", "Partition Attribute", "Parent", "Parent Attribute" };
            
            public String getColumnName(int col) { return (this.columns[col]); }
            public int getColumnCount() { return (this.columns.length); }
            public int getRowCount() { return (tables.size()); }
            public Object getValueAt(int row, int col) {
                String ret = null;
                
                Table catalog_tbl = tables.get(row);
                TableEntry entry = plan.getTableEntries().get(catalog_tbl);
                switch (col) {
                    case 0:
                        ret = catalog_tbl.getName();
                        break;
                    case 1:
                        ret = entry.getMethod().toString();
                        break;
                    case 2:
                        ret = (entry.getAttribute() != null ? entry.getAttribute().getName() : "-");
                        break;
                    case 3:
                        ret = (entry.getParent() != null ? entry.getParent().getName() : "-");
                        break;
                    case 4:
                        ret = (entry.getParentAttribute() != null ? entry.getParentAttribute().getName() : "-");
                        break;
                } // SWITCH
                return (ret);
            }
            public boolean isCellEditable(int row, int col) {
                return (false);
            }
            public Class<?> getColumnClass(int c) {
                return getValueAt(0, c).getClass();
            }
        });
        
        partitonTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        partitonTable.setFillsViewportHeight(false);
        partitonTable.setDragEnabled(false);
        partitonTable.setColumnSelectionAllowed(false);
        partitonTable.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        
        //
        // Select the vertex in the graph when they select its corresponding row
        //
        partitonTable.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent e) {
                Table catalog_tbl = tables.get(partitonTable.getSelectedRow());
                GraphVisualizationPanel<DesignerVertex, DesignerEdge> visualizer = PartitionPlanPanel.this.parent.getCurrentVisualizer();
                IGraph<DesignerVertex, DesignerEdge> graph = (IGraph<DesignerVertex, DesignerEdge>)visualizer.getGraph();
                DesignerVertex vertex = graph.getVertex(catalog_tbl);
                visualizer.selectVertex(vertex);
                return;
            }
        });
        
        //
        // Set the column widths
        //
        partitonTable.getColumnModel().getColumn(0).setPreferredWidth(40);
        partitonTable.getColumnModel().getColumn(1).setPreferredWidth(20);
        partitonTable.getColumnModel().getColumn(3).setPreferredWidth(40);
        
        JScrollPane scrollPane = new JScrollPane(partitonTable);
        scrollPane.setPreferredSize(new Dimension(DesignerVisualization.WINDOW_WIDTH, 175));
        //scrollPane.setMaximumSize(this.columnSetTable.getPreferredScrollableViewportSize());
        scrollPane.setHorizontalScrollBarPolicy(ScrollPaneConstants.HORIZONTAL_SCROLLBAR_NEVER);
        this.add(scrollPane, BorderLayout.SOUTH);
        
        return;
    }

}
