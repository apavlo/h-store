/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.gui;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.JTree;
import javax.swing.KeyStroke;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.TreePath;
import javax.swing.tree.TreeSelectionModel;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.CatalogType.UnresolvedInfo;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.gui.catalog.AttributesNode;
import edu.brown.gui.catalog.CatalogTreeModel;
import edu.brown.gui.catalog.PlanTreeCatalogNode;
import edu.brown.gui.catalog.WrapperNode;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.SQLFormatter;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author pavlo
 *
 */
public class CatalogViewer extends AbstractViewer {
    private static final long serialVersionUID = -7566054105094378095L;

    protected static final String WINDOW_TITLE = "H-Store Catalog Browser";
    
    // ----------------------------------------------
    // CONTROL OPTIONS
    // ----------------------------------------------    
    protected static final int SEARCH_MIN_LENGTH = 4;
    
    // ----------------------------------------------
    // CATALOG ELEMENTS
    // ----------------------------------------------
    protected Catalog catalog;
    protected File catalog_file_path;
    protected String catalog_path;
    
    protected boolean text_mode = true;
    
    // ----------------------------------------------
    // GUI ELEMENTS
    // ----------------------------------------------
    protected CatalogTreeModel catalogTreeModel;
    protected JTree catalogTree;
    protected JTextField searchField;
    protected JTextArea textInfoTextArea;
    protected JScrollPane textInfoScroller;
    protected JPanel textInfoPanel;
    protected JPanel mainPanel;

    // ----------------------------------------------
    // MENU OPTIONS
    // ----------------------------------------------
    public enum MenuOptions {
        CATALOG_OPEN_FILE,
        CATALOG_OPEN_JAR,
        CATALOG_SAVE,
        QUIT,
    };
    
    /**
     * 
     * @param catalog
     * @param catalog_path
     */
    public CatalogViewer(ArgumentsParser args) {
        super(args,
              String.format("%s [%s]", WINDOW_TITLE, args.catalog_path),
              1000,
              DEFAULT_WINDOW_HEIGHT);
        this.catalog = args.catalog;
        this.catalog_file_path = args.catalog_path;
        this.catalog_path = args.catalog_path.getAbsolutePath();
        this.menuHandler = new CatalogViewer.MenuHandler();
        this.init();
    }
    
    /**
     * 
     * @param catalog
     * @param catalog_path
     */
    public CatalogViewer(Catalog catalog, String catalog_path) {
        super(new ArgumentsParser(), WINDOW_TITLE);
        this.catalog = catalog;
        this.catalog_path = catalog_path;
        this.menuHandler = new CatalogViewer.MenuHandler();
        this.init();
    }

    public static void show(final Catalog catalog, final String catalog_path) {
        CatalogViewer viewer = new CatalogViewer(catalog, catalog_path);
        viewer.setVisible(true);
    }
    
    /**
     * @param args
     */
    public static void main(final String[] vargs) throws Exception {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
//        Procedure catalog_proc = args.catalog_db.getProcedures().get("slev");
//        Statement catalog_stmt = catalog_proc.getStatements().get("GetStockCount");
//        String jsonString = Encoder.hexDecodeToString(catalog_stmt.getMs_fullplan());
//        JSONObject jsonObject = new JSONObject(jsonString);
//        System.err.println(jsonObject.toString(2));
        
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                CatalogViewer viewer = new CatalogViewer(args);
                viewer.setVisible(true);
            } // RUN
         });
    }
    
    private void generateCatalogTree(Catalog catalog, String catalog_path) {
        this.catalogTreeModel = new CatalogTreeModel(catalog, catalog_path);
        this.catalogTree.setModel(this.catalogTreeModel);
        this.catalog = catalog;
        this.catalog_path = catalog_path;
        
        //
        // Expand clusters and databases
        //
        this.catalogTree.expandPath(new TreePath(this.catalogTreeModel.getTablesNode().getPath()));
        this.catalogTree.expandPath(new TreePath(this.catalogTreeModel.getProceduresNode().getPath()));
    }
    
    /**
     * 
     */
    protected void viewerInit() {
        // ----------------------------------------------
        // MENU
        // ----------------------------------------------
        JMenu menu;
        JMenuItem menuItem;
    
        // 
        // File Menu
        //
        menu = new JMenu("File");
        menu.getPopupMenu().setLightWeightPopupEnabled(false);
        menu.setMnemonic(KeyEvent.VK_F);
        menu.getAccessibleContext().setAccessibleDescription("File Menu");
        menuBar.add(menu);
    
        menuItem = new JMenuItem("Open Catalog From File"); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Catalog From File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.CATALOG_OPEN_FILE);
        menu.add(menuItem);
        
        menuItem = new JMenuItem("Open Catalog From Jar"); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK | ActionEvent.SHIFT_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Catalog From Project Jar");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.CATALOG_OPEN_JAR);
        menu.add(menuItem);
         
        menu.addSeparator();
          
        menuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Quit Program");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.QUIT);
        menu.add(menuItem);
        
        // ----------------------------------------------
        // CATALOG TREE PANEL
        // ----------------------------------------------
        this.catalogTree = new JTree();
        this.catalogTree.setEditable(false);
        this.catalogTree.setCellRenderer(new CatalogViewer.CatalogTreeRenderer());
        this.catalogTree.getSelectionModel().setSelectionMode(TreeSelectionModel.SINGLE_TREE_SELECTION);
        this.catalogTree.addTreeSelectionListener(new TreeSelectionListener() {
            public void valueChanged(TreeSelectionEvent e) {
                DefaultMutableTreeNode node = (DefaultMutableTreeNode)CatalogViewer.this.catalogTree.getLastSelectedPathComponent();
                if (node == null) return;

                Object user_obj = node.getUserObject();
                String new_text = ""; // <html>";
                boolean text_mode = true;
                if (user_obj instanceof WrapperNode) {
                    CatalogType catalog_obj  = ((WrapperNode)user_obj).getCatalogType();
                    new_text += CatalogViewer.this.getAttributesText(catalog_obj);
                } else if (user_obj instanceof AttributesNode) {
                    AttributesNode wrapper = (AttributesNode)user_obj;
                    new_text += wrapper.getAttributes();
                    
                } else if (user_obj instanceof PlanTreeCatalogNode) {
                    final PlanTreeCatalogNode wrapper = (PlanTreeCatalogNode)user_obj;
                    text_mode = false;
                    
                    CatalogViewer.this.mainPanel.remove(0);
                    CatalogViewer.this.mainPanel.add(wrapper.getPanel(), BorderLayout.CENTER);
                    CatalogViewer.this.mainPanel.validate();
                    CatalogViewer.this.mainPanel.repaint();
                    
                    if (SwingUtilities.isEventDispatchThread() == false) {
                        SwingUtilities.invokeLater(new Runnable() {
                            public void run() {
                                wrapper.centerOnRoot();
                            }
                        });
                    } else {
                        wrapper.centerOnRoot();
                    }
                    
                } else {
                    new_text += CatalogViewer.this.getSummaryText();
                }

                // Text Mode
                if (text_mode) {
                    if (CatalogViewer.this.text_mode == false) {
                        CatalogViewer.this.mainPanel.remove(0);
                        CatalogViewer.this.mainPanel.add(CatalogViewer.this.textInfoPanel);
                    }
                    CatalogViewer.this.textInfoTextArea.setText(new_text);
                    
                    // Scroll to top
                    CatalogViewer.this.textInfoTextArea.grabFocus();
                }
                
                CatalogViewer.this.text_mode = text_mode;
            }
        });
        this.generateCatalogTree(this.catalog, this.catalog_file_path.getName());

        //
        // Text Information Panel
        //
        this.textInfoPanel = new JPanel();
        this.textInfoPanel.setLayout(new BorderLayout());
        this.textInfoTextArea = new JTextArea();
        this.textInfoTextArea.setEditable(false);
        this.textInfoTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        this.textInfoTextArea.setText(this.getSummaryText());
        this.textInfoTextArea.addFocusListener(new FocusListener() {
            @Override
            public void focusLost(FocusEvent e) {
                // TODO Auto-generated method stub
            }
            
            @Override
            public void focusGained(FocusEvent e) {
                CatalogViewer.this.scrollTextInfoToTop();
            }
        });
        this.textInfoScroller = new JScrollPane(this.textInfoTextArea);
        this.textInfoPanel.add(this.textInfoScroller, BorderLayout.CENTER);
        this.mainPanel = new JPanel(new BorderLayout());
        this.mainPanel.add(textInfoPanel, BorderLayout.CENTER);
        
        //
        // Search Toolbar
        //
        JPanel searchPanel = new JPanel();
        searchPanel.setLayout(new BorderLayout());
        JPanel innerSearchPanel = new JPanel();
        innerSearchPanel.setLayout(new BoxLayout(innerSearchPanel, BoxLayout.X_AXIS));
        innerSearchPanel.add(new JLabel("Search: "));
        this.searchField = new JTextField(30);
        innerSearchPanel.add(this.searchField);
        searchPanel.add(innerSearchPanel, BorderLayout.EAST);
        
        this.searchField.addKeyListener(new KeyListener() {
            private String last = null; 
            
            @Override
            public void keyReleased(KeyEvent e) {
                String value = CatalogViewer.this.searchField.getText().toLowerCase().trim();
                if (!value.isEmpty() && (this.last == null || !this.last.equals(value))) {
                    CatalogViewer.this.search(value);
                }
                this.last = value;
            }
            @Override
            public void keyTyped(KeyEvent e) {
                // Do nothing...
            }
            @Override
            public void keyPressed(KeyEvent e) {
                // Do nothing...
            }
        });
        
        // Putting it all together
        JScrollPane scrollPane = new JScrollPane(this.catalogTree);
        JPanel topPanel = new JPanel();
        topPanel.setLayout(new BorderLayout());
        topPanel.add(searchPanel, BorderLayout.NORTH);
        topPanel.add(scrollPane, BorderLayout.CENTER);
        
        JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, topPanel, this.mainPanel);
        splitPane.setDividerLocation(400);

        this.add(splitPane, BorderLayout.CENTER);
    }
    
    protected void search(String value) {
        LOG.debug("Searching based on keyword '" + value + "'...");
        
        Set<DefaultMutableTreeNode> found = new HashSet<DefaultMutableTreeNode>();

        Integer guid = null;

        // If it starts with "guid=", then that's a PlanColumn!
        if (value.startsWith("guid=")) {
            try {
                guid = Integer.valueOf(value.split("guid=")[1]);
                if (this.catalogTreeModel.getPlanNodeGuidNodeXref().containsKey(guid)) {
                    found.addAll(this.catalogTreeModel.getPlanNodeGuidNodeXref().get(guid));
                } else {
                    guid = null;
                }
            } catch (Exception ex) {
                // Ignore...
            }
            
        } else {
            
            // Check if it's a digit, which means that they are searching by Catalog guid
            try {
                guid = Integer.parseInt(value);
                if (this.catalogTreeModel.getGuidNodeXref().containsKey(guid)) {
                    found.add(this.catalogTreeModel.getGuidNodeXref().get(guid));
                } else {
                    guid = null;
                }
            } catch (Exception ex) {
                // Ignore...
            }
        }
        
        // Otherwise search by name...
        if (guid == null && value.length() >= SEARCH_MIN_LENGTH) {
            LOG.debug("Searching for matching name '" + value + "'");
            for (String name : this.catalogTreeModel.getNameNodeXref().keySet()) {
                if (name.indexOf(value) != -1) {
                    found.addAll(this.catalogTreeModel.getNameNodeXref().get(name));
                }
            } // FOR
        }
        
        // Display found matches
        if (!found.isEmpty()) {
            this.highlight(found);
            this.searchField.requestFocus();
        }
    }
    
    protected void highlight(Collection<DefaultMutableTreeNode> nodes) {
        // Collapse everything and then show the paths to each node
        for (int ctr = this.catalogTree.getRowCount(); ctr >= 0; ctr--) {
            if (this.catalogTree.isExpanded(ctr)) {
                this.catalogTree.collapseRow(ctr);              
            }
        } // FOR
        this.catalogTree.getSelectionModel().clearSelection();
        
        for (DefaultMutableTreeNode node : nodes) {
            TreePath path = new TreePath(node.getPath());
            this.catalogTree.setSelectionPath(path);
            this.catalogTree.expandPath(path);
        } // FOR
    }
    
    /**
     * Scroll the attributes pane to the top
     */
    protected void scrollTextInfoToTop() {
        JScrollBar verticalScrollBar = this.textInfoScroller.getVerticalScrollBar();
        JScrollBar horizontalScrollBar = this.textInfoScroller.getHorizontalScrollBar();
        verticalScrollBar.setValue(verticalScrollBar.getMinimum());
        horizontalScrollBar.setValue(horizontalScrollBar.getMinimum());
        // System.err.println("VERTICAL=" + verticalScrollBar.getValue() + ", HORIZONTAL=" + horizontalScrollBar.getValue());
    }
    
    /**
     * Return text to be used on the summary page
     * @return
     */
    @SuppressWarnings("unchecked")
    protected String getSummaryText() {
        Map<String, Integer> m[] = (Map<String, Integer>[])new Map<?, ?>[3];
        int idx = -1;
        
        // ----------------------
        // TABLE INFO
        // ----------------------
        m[++idx] = new ListOrderedMap<String, Integer>();
        int cols = 0;
        int fkeys = 0;
        int tables = 0;
        int systables = 0;
        int views = 0;
        Map<Table, MaterializedViewInfo> catalog_views = CatalogUtil.getVerticallyPartitionedTables(catalog);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        for (Table t : catalog_db.getTables()) {
            if (catalog_views.values().contains(t)) {
                views++;
            }
            else if (t.getSystable()) {
                systables++;
            } else {
                tables++;
                cols += t.getColumns().size();
                for (Column c : t.getColumns()) {
                    Column fkey = CatalogUtil.getForeignKeyParent(c);
                    if (fkey != null) fkeys++;
                }
            }
        } // FOR
        m[idx].put("Tables", tables);
        m[idx].put("Columns", cols);
        m[idx].put("Foreign Keys", fkeys);
        m[idx].put("Views", views);
        m[idx].put("Vertical Replicas", systables);
        m[idx].put("System Tables", systables);
        
        // ----------------------
        // PROCEDURES INFO
        // ----------------------
        m[++idx] = new ListOrderedMap<String, Integer>();
        int procs = 0;
        int sysprocs = 0;
        int params = 0;
        int stmts = 0;
        for (Procedure p : catalog_db.getProcedures()) {
            if (p.getSystemproc()) {
                sysprocs++;
            } else {
                procs++;
                params += p.getParameters().size();
                stmts += p.getStatements().size();
            }
        }
        m[idx].put("Procedures", procs);
        m[idx].put("Procedure Parameters", params);
        m[idx].put("Statements", stmts);
        m[idx].put("System Procedures", sysprocs);
        
        // ----------------------
        // HOST INFO
        // ----------------------
        m[++idx] = new ListOrderedMap<String, Integer>();
        m[idx].put("Hosts", catalog_clus.getHosts().size());
        m[idx].put("Sites", catalog_clus.getSites().size());
        m[idx].put("Partitions", CatalogUtil.getNumberOfPartitions(catalog_db));
        
        StringBuilder buffer = new StringBuilder();
        buffer.append(StringUtil.header("Catalog Summary", "-", 50) + "\n\n")
              .append(StringUtil.formatMaps(m));
        return (buffer.toString());
    }
    
    /**
     * 
     * @param catalog_obj
     */
    protected String getAttributesText(CatalogType catalog_obj) {
        final ListOrderedMap<String, Object> map = new ListOrderedMap<String, Object>();
        
//      StringBuilder buffer = new StringBuilder();
        // buffer.append("guid: ").append(catalog_obj.getGuid()).append("\n");
        map.put("relativeIndex", catalog_obj.getRelativeIndex());
        map.put("nodeVersion", catalog_obj.getNodeVersion());
        
        // Default Output
        if ((catalog_obj instanceof Database) == false) {
            Collection<String> skip_fields = CollectionUtil.addAll(new HashSet<String>(),
                    "exptree", "fullplan", "ms_exptree", "ms_fullplan", "plannodetree", "sqltext");
            
            Collection<String> catalog_fields = CollectionUtil.addAll(new HashSet<String>(),
                    "partition_column", "partitioncolumn", "foreignkeytable", "matviewsource"); 
            
            if (catalog_obj instanceof Constraint) {
                catalog_fields.add("index");
            } else if (catalog_obj instanceof Site) {
                catalog_fields.add("host");
                catalog_fields.add("partition");
            }
            
            // Show catalog type
            Set<Class<? extends CatalogType>> show_type = new HashSet<Class<? extends CatalogType>>();
            show_type.add(Host.class);
            show_type.add(Site.class);
            show_type.add(Partition.class);
            
            for (String field : catalog_obj.getFields()) {
                if (skip_fields.contains(field)) continue;
                
                // Default
                Object value = catalog_obj.getField(field);
                map.put(field, value);
                
                // Specialized Output
                if (value != null && catalog_fields.contains(field)) {
                    CatalogType catalog_item = null;
                    if (value instanceof CatalogType) {
                        catalog_item = (CatalogType)value;
                    } else if (value instanceof CatalogType.UnresolvedInfo) {
                        catalog_item = catalog.getItemForRef(((UnresolvedInfo)value).path);
                    } else {
                        assert(false) : "Unexpected value '" + value + "' for field '" + field + "'";
                    }
                    
                    if (catalog_item != null) {
                        boolean include_class = show_type.contains(catalog_item.getClass()); 
                        map.put(field, CatalogUtil.getDisplayName(catalog_item, include_class));
                    } else {
                        map.put(field, catalog_item);
                    }
                } 
                
                // Constraint
                else if (catalog_obj instanceof Constraint) {
                    if (field == "type") {
                        map.put(field, ConstraintType.get((Integer)value));
                    }
                }
                // Index
                else if (catalog_obj instanceof Index) {
                    if (field == "type") {
                        map.put(field, IndexType.get((Integer)value));
                    }
                }
                // Column / StmtParameter / ProcParameter
                else if (catalog_obj instanceof Column || catalog_obj instanceof StmtParameter || catalog_obj instanceof ProcParameter) {
                    String keys[] = { "type", "sqltype", "javatype", "defaultvaluetype" };
                    for (String key : keys) {
                        if (field == key) {
                            map.put(field, VoltType.get(((Integer)value).byteValue()).name());
                            break;
                        }
                    } // FOR
                    if (field.equals("procparameter")) {
                        ProcParameter proc_param = ((StmtParameter)catalog_obj).getProcparameter();
                        if (proc_param != null) {
                            map.put(field, proc_param.fullName()); 
                        }
                    }
                }
            } // FOR
        }
        
        // INDEX
        if (catalog_obj instanceof Index) {
            Index catalog_idx = (Index)catalog_obj;
            Collection<Column> cols = CatalogUtil.getColumns(CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index"));
            map.put("columns", CatalogUtil.getDisplayNames(cols));
        }
        // CONSTRAINT
        else if (catalog_obj instanceof Constraint) {
            Constraint catalog_const = (Constraint)catalog_obj;
            Collection<Column> cols = null;
            if (catalog_const.getType() == ConstraintType.FOREIGN_KEY.getValue()) {
                cols = CatalogUtil.getColumns(catalog_const.getForeignkeycols());    
            } else {
                Index catalog_idx = catalog_const.getIndex();
                cols = CatalogUtil.getColumns(catalog_idx.getColumns());
            }
            map.put("columns", CatalogUtil.getDisplayNames(cols));
        }
        // COLUMN
        else if (catalog_obj instanceof Column) {
            Column catalog_col = (Column)catalog_obj;
            Collection<Constraint> consts = CatalogUtil.getConstraints(catalog_col.getConstraints());
            map.put("constraints", CatalogUtil.getDisplayNames(consts));
        }
        
        StringBuilder buffer = new StringBuilder(StringUtil.formatMaps(map));
        
        // DATABASE
        if (catalog_obj instanceof Database) {
            buffer.append(StringUtil.SINGLE_LINE);
            buffer.append(Encoder.hexDecodeToString(((Database)catalog_obj).getSchema()));
        }
        // PLANFRAGMENT
        else if (catalog_obj instanceof PlanFragment) {
            PlanFragment catalog_frgmt = (PlanFragment)catalog_obj;
            try {
                AbstractPlanNode node = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frgmt);
                buffer.append(StringUtil.SINGLE_LINE);
                buffer.append(PlanNodeUtil.debug(node));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // TABLE
        else if (catalog_obj instanceof Table) {
            buffer.append(StringUtil.SINGLE_LINE);
            buffer.append("\n").append(CatalogUtil.toSchema((Table)catalog_obj)).append("\n");
        }
        // Statement
        else if (catalog_obj instanceof Statement) {
            Statement catalog_stmt = (Statement)catalog_obj;
            SQLFormatter f = new SQLFormatter(catalog_stmt.getSqltext());
            buffer.append(StringUtil.SINGLE_LINE);
            buffer.append("\n").append(f.format()).append("\n");
        }

        return (buffer.toString());
    }
    
    public class CatalogTreeRenderer extends DefaultTreeCellRenderer {
        private static final long serialVersionUID = 1L;

        /**
         * 
         */
        @Override
        public Component getTreeCellRendererComponent(JTree tree, Object value, boolean sel, boolean expanded, boolean leaf, int row, boolean hasFocus) {
            super.getTreeCellRendererComponent(tree, value, sel, expanded, leaf, row, hasFocus);
            if (value instanceof DefaultMutableTreeNode) {
                DefaultMutableTreeNode node = (DefaultMutableTreeNode)value;
                if (node.getUserObject() instanceof WrapperNode) {
                    CatalogType catalog_obj = ((WrapperNode)node.getUserObject()).getCatalogType();
                    this.setFont(this.getFont().deriveFont(Font.BOLD));
                    
                    //
                    // Cluster
                    //
                    if (catalog_obj instanceof Cluster) {
                        this.setIcon(UIManager.getIcon("FileView.computerIcon"));
                    //
                    // Database
                    //
                    } else if (catalog_obj instanceof Database) {
                        this.setIcon(UIManager.getIcon("FileView.hardDriveIcon"));
                    }
                } else {
                    this.setFont(this.getFont().deriveFont(Font.PLAIN));
                }
            }
            if (CatalogViewer.this.catalogTreeModel != null) {
                //
                // Root Node
                //
                Object root_node = CatalogViewer.this.catalogTreeModel.getRoot();
                if (root_node != null && root_node == value) {
                    this.setIcon(UIManager.getIcon("FileView.floppyDriveIcon"));
                    this.setFont(this.getFont().deriveFont(Font.BOLD));
                }
            }
            /*
            if (leaf) {
                this.setIcon(UIManager.getIcon("FileChooser.detailsViewIcon"));
            }
            */
            return (this);
        }
    } // END CLASS
    
    protected class MenuHandler extends AbstractMenuHandler {
        /**
         * 
         */
        @Override
        public void actionPerformed(ActionEvent e) {
            JMenuItem source = (JMenuItem)(e.getSource());
            //
            // Process the event
            //
            MenuOptions opt = MenuOptions.valueOf(source.getClientProperty(MENU_ID).toString());
            switch (opt) {
                // --------------------------------------------------------
                // OPEN CATALOG FILE
                // --------------------------------------------------------
                case CATALOG_OPEN_FILE: {
                    Pair<Catalog, String> result = openCatalogFile();
                    if (result != null) {
                        generateCatalogTree(result.getFirst(), result.getSecond());
                    }
                    break;
                }
                // --------------------------------------------------------
                // OPEN CATALOG JAR
                // --------------------------------------------------------
                case CATALOG_OPEN_JAR: {
                    Pair<Catalog, String> result = openCatalogJar();
                    if (result != null) {
                        generateCatalogTree(result.getFirst(), result.getSecond());
                    }
                    break;
                }
                // --------------------------------------------------------
                // QUIT
                // --------------------------------------------------------
                case QUIT: {
                    quit();
                    break;
                }
                // --------------------------------------------------------
                // UNKNOWN
                // --------------------------------------------------------
                default:
                    System.err.println("Invalid Menu Action: " + source.getName());
            } // SWITCH
        }
    } // END CLASS
}
