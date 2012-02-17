package edu.brown.gui.stats;

import java.awt.BorderLayout;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.table.AbstractTableModel;

import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;


public class TableListPanel extends JPanel {
    private static final long serialVersionUID = 1L;
    
    private JTable tableListTable;
    private final TableListPanel.TableModel tableModel = new TableModel();

    public TableListPanel() {
        super();
        this.init();
    }
    
    public ListSelectionModel getSelectionModel() {
        return (this.tableListTable.getSelectionModel());
    }
    
    public void setTables(Iterable<Table> tables) {
        this.tableModel.setTables(tables);
    }
    
    public String getSelectedTable() {
        int idx = this.tableListTable.getSelectedRow();
        String table_key = null;
        if (idx != -1) {
            Table catalog_tbl = this.tableModel.getTables().get(idx);
            assert(catalog_tbl != null);
            table_key = CatalogKey.createKey(catalog_tbl);
        }
        return (table_key);
    }
    
    private void init() {
        this.setLayout(new BorderLayout());
        tableListTable = new JTable(this.tableModel);
        tableListTable.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
        tableListTable.setFillsViewportHeight(false);
        tableListTable.setDragEnabled(false);
        tableListTable.setColumnSelectionAllowed(false);
        tableListTable.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        
//        //
//        // Set the column widths
//        //
//        tableListTable.getColumnModel().getColumn(0).setPreferredWidth(40);
//        tableListTable.getColumnModel().getColumn(1).setPreferredWidth(20);
//        tableListTable.getColumnModel().getColumn(3).setPreferredWidth(40);
        
        JScrollPane scrollPane = new JScrollPane(tableListTable);
        //scrollPane.setPreferredSize(new Dimension(DesignerVisualization.WINDOW_WIDTH, 175));
        //scrollPane.setMaximumSize(this.columnSetTable.getPreferredScrollableViewportSize());
        this.add(scrollPane, BorderLayout.CENTER);
    }

    private class TableModel extends AbstractTableModel {
        private static final long serialVersionUID = 1L;
        protected final List<Table> tables =  new ArrayList<Table>(); 
        protected final String columns[] = { "Table", "Partition Attribute", "Is Replicated" };
        
        public TableModel() {
            super();
        }
        
        public List<Table> getTables() {
            return (this.tables);
        }
        public void setTables(Iterable<Table> tables) {
            this.tables.clear();
            for (Table catalog_tbl : tables) {
                this.tables.add(catalog_tbl);
            }
        }
        
        public String getColumnName(int col) {
            return (this.columns[col]);
        }
        public int getColumnCount() {
            return (this.columns.length);
        }
        public int getRowCount() {
            return (this.tables.size());
        }
        public Object getValueAt(int row, int col) {
            String ret = null;
            
            Table catalog_tbl = this.tables.get(row);
            switch (col) {
                case 0:
                    ret = catalog_tbl.getName();
                    break;
                case 1:
                    ret = (catalog_tbl.getPartitioncolumn() != null ? catalog_tbl.getPartitioncolumn().getName() : "-");
                    break;
                case 2:
                    ret = Boolean.toString(catalog_tbl.getIsreplicated());
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
    } // END CLASS
}
