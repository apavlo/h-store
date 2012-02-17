package edu.brown.gui.stats;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.table.AbstractTableModel;

import edu.brown.catalog.CatalogKey;
import edu.brown.gui.AbstractInfoPanel;
import edu.brown.gui.AbstractViewer;
import edu.brown.statistics.TableStatistics;

/**
 * 
 * @author pavlo
 *
 */
public class TableStatsPanel extends AbstractInfoPanel<TableStatistics> {
    private static final long serialVersionUID = 1L;

    private JLabel tableLabel;
    private JLabel partitionLabel;
    private Map<TableStatistics.Members, JComponent> field_map;

    protected class MapFieldEditor<T> extends JPanel {
        private static final long serialVersionUID = 1L;
        protected SortedMap<T, Long> map;
        protected List<T> elements = new ArrayList<T>();
        protected JTable table;
        
        public MapFieldEditor() {
            super();
            this.init();
        }
        
        public void setMap(SortedMap<T, Long> map) {
            this.map = map;
            this.elements.addAll(this.map.keySet());
            this.table.revalidate();
        }
        
        private void init() {
            this.setLayout(new BorderLayout());
            
            this.table = new JTable(new AbstractTableModel() {
                private static final long serialVersionUID = 1L;
                protected final String columns[] = { "ID", "VALUE" };
                
                public String getColumnName(int col) {
                    return (this.columns[col]);
                }
                public int getColumnCount() {
                    return (this.columns.length);
                }
                public int getRowCount() {
                    return (elements.size());
                }
                public Object getValueAt(int row, int col) {
                    T element = elements.get(row);
                    Long value = map.get(element);
                    return (col == 0 ? element : value).toString();
                }
                public void setValueAt(Object aValue, int row, int column) {
                    T element = elements.get(row);
                    Long value = Long.valueOf(aValue.toString());
                    map.put(element, value);
                }
                public boolean isCellEditable(int row, int col) {
                    return (col != 0);
                }
                public Class<?> getColumnClass(int c) {
                    return getValueAt(0, c).getClass();
                } 
            });
            this.table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
            this.table.setFillsViewportHeight(true);
            this.table.setDragEnabled(false);
            this.table.setColumnSelectionAllowed(false);
            this.table.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
            this.add(new JScrollPane(this.table), BorderLayout.CENTER);
        }
    } // END CLASS
    
    public TableStatsPanel() {
        super();
    }
    
    public TableStatsPanel(TableStatistics stats) {
        this();
        this.update(stats);
    }
    
    @Override
    protected void init() throws Exception {
        this.field_map = new HashMap<TableStatistics.Members, JComponent>();
        this.setLayout(new GridBagLayout());
        JLabel label = null;
        GridBagConstraints c = AbstractViewer.getConstraints();

        c.gridx = 0;
        c.gridy = 0;
        label = new JLabel("Table:");
        label.setFont(AbstractViewer.key_font);
        this.add(label, c);
        c.gridx = 1;
        this.tableLabel = new JLabel();
        this.tableLabel.setFont(AbstractViewer.value_font);
        this.add(this.tableLabel, c);
        
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Num of Partitions:");
        label.setFont(AbstractViewer.key_font);
        this.add(label, c);
        c.gridx = 1;
        this.partitionLabel = new JLabel();
        this.partitionLabel.setFont(AbstractViewer.value_font);
        this.add(this.partitionLabel, c);

        AbstractViewer.addSeperator(this, c);

        //
        // Editable Fields
        //
        String last_category = null;
        Class<TableStatistics> statsClass = TableStatistics.class;
        for (TableStatistics.Members element : TableStatistics.Members.values()) {
            // FIXME if (element == TableStatistics.Members.PARTITION_COUNT) continue;
            String name = element.toString().replace('_', ' ');
            Field field = statsClass.getDeclaredField(element.toString().toLowerCase());
            JComponent component = null;
            
            String pieces[] = name.split(" ");
            String category = (pieces.length == 3 && pieces[0].equals("TUPLE") ? pieces[1] : pieces[0]);
            if (last_category != null && !last_category.equals(category)) {
                AbstractViewer.addSpacer(this, c);
            }
            //System.out.println(name + " --> " + category);
            
            c.gridx = 0;
            c.gridy++;
            label = new JLabel(name + ":");
            label.setFont(AbstractViewer.key_font);
            this.add(label, c);
            c.gridx = 1;
            
            // Integer fields
            Class<?> elementClass = field.getType(); 
            if (elementClass.equals(Long.class)) {
                component = new JTextField(10);
            // Boolean checkboxes
            } else if (elementClass.equals(Boolean.class)) {
                component = new JCheckBox();
            // Other
            } else {
                // HACK
                /* FIXME
                switch (element) {
                    case TUPLE_COUNT_PARTITIONS:
                        component = new MapFieldEditor<Integer>();
                        break;
                    case TUPLE_SIZE_PARTITIONS:
                        component = new MapFieldEditor<Integer>();
                        break;
                    case QUERY_TYPE_COUNT:
                        component = new MapFieldEditor<QueryType>();
                        break;
                    default:
                        LOG.fatal("Unexpected element '" + element + "'");
                        System.exit(1);
                } // SWITCH
                */
            }
            this.add(component, c);
            this.field_map.put(element, component);
            last_category = category;
        } // FOR
        
    }
    
    @SuppressWarnings("unchecked")
    public void update(TableStatistics stats) {
        this.element = stats;
        this.tableLabel.setText(CatalogKey.getNameFromKey(stats.getCatalogKey()));
        // FIXME this.partitionLabel.setText(stats.partition_count.toString());
        
        Class<TableStatistics> statsClass = TableStatistics.class;
        for (TableStatistics.Members element : this.field_map.keySet()) {
            JComponent component = this.field_map.get(element);
            Field field = null;
            try {
                field = statsClass.getDeclaredField(element.toString().toLowerCase());
                if (component instanceof JTextField) {
                    ((JTextField)component).setText(field.get(stats).toString());
                } else if (component instanceof JCheckBox) {
                    ((JCheckBox)component).setSelected((Boolean)field.get(stats));
                } else {
                    // HACK
                    /* FIXME
                    switch (element) {
                        case TUPLE_COUNT_PARTITIONS:
                            ((MapFieldEditor)component).setMap(stats.tuple_count_partitions);
                            break;
                        case TUPLE_SIZE_PARTITIONS:
                            ((MapFieldEditor)component).setMap(stats.tuple_size_partitions);
                            break;
                        case QUERY_TYPE_COUNT:
                            ((MapFieldEditor)component).setMap(stats.query_type_count);
                            break;
                        default:
                            LOG.fatal("Unexpected element '" + element + "'");
                            System.exit(1);
                    } // SWITCH
                    */
                    Dimension size = ((MapFieldEditor)component).getSize();
                    ((MapFieldEditor)component).setMinimumSize(new Dimension(size.width, size.height+50));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
    
    public void save() {
        Class<TableStatistics> statsClass = TableStatistics.class;
        for (TableStatistics.Members element : this.field_map.keySet()) {
            JComponent component = this.field_map.get(element);
            Field field = null;
            try {
                field = statsClass.getDeclaredField(element.toString().toLowerCase());
                if (component instanceof JTextField) {
                    String value = ((JTextField)component).getText();
                    field.set(this.element, Long.valueOf(value));
                } else if (component instanceof JCheckBox) {
                    Boolean value = ((JCheckBox)component).isSelected();
                    field.set(this.element, value);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        }
    }
}
