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
import java.awt.Color;
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

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.catalog.conflicts.ConflictGraph;
import edu.brown.graphs.GraphvizExport;
import edu.brown.gui.catalog.AttributesNode;
import edu.brown.gui.catalog.CatalogAttributeText;
import edu.brown.gui.catalog.CatalogMapTreeNode;
import edu.brown.gui.catalog.CatalogSummaryUtil;
import edu.brown.gui.catalog.CatalogTreeModel;
import edu.brown.gui.catalog.PlanTreeCatalogNode;
import edu.brown.gui.catalog.ProcedureConflictGraphNode;
import edu.brown.gui.catalog.WrapperNode;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.IOFileFilter;
import edu.brown.utils.StringUtil;

/**
 * Graphical Catalog Viewer Tool
 * @author pavlo
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

    protected CatalogSummaryUtil summaryUtil;
    protected CatalogAttributeText attributeText;
    
    // ----------------------------------------------
    // MENU OPTIONS
    // ----------------------------------------------
    public enum MenuOptions {
        CATALOG_OPEN_FILE,
        CATALOG_OPEN_JAR,
        CATALOG_SAVE,
        CONFLICTGRAPH_EXPORT,
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
              DEFAULT_WINDOW_WIDTH,
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
        this.catalogTreeModel = new CatalogTreeModel(this.args, catalog, catalog_path);
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
        this.summaryUtil = new CatalogSummaryUtil(this.catalog);
        this.attributeText = new CatalogAttributeText(this.catalog);
        
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
        
        menuItem = new JMenuItem("Export ConflictGraph"); 
        // menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK | ActionEvent.SHIFT_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Export ConflictGraph to a Graphviz Dot File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.CONFLICTGRAPH_EXPORT);
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
        this.catalogTree.addTreeSelectionListener(new CatalogTreeSeleciontListener());
        this.generateCatalogTree(this.catalog, this.catalog_file_path.getName());

        // ----------------------------------------------
        // TEXT INFORMATION PANEL
        // ----------------------------------------------
        this.textInfoPanel = new JPanel();
        this.textInfoPanel.setLayout(new BorderLayout());
        this.textInfoTextArea = new JTextArea();
        this.textInfoTextArea.setEditable(false);
        this.textInfoTextArea.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));
        this.textInfoTextArea.setText(StringUtil.header("CATALOG SUMMARY", "-", 50) + // HACK
                                      "\n\n" + this.summaryUtil.getSummaryText());
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
        
        // ----------------------------------------------
        // SEARCH TOOLBAR
        // ----------------------------------------------
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
    
    protected void replaceMainPanel(JPanel newPanel) {
        this.mainPanel.remove(0);
        this.mainPanel.add(newPanel, BorderLayout.CENTER);
        this.mainPanel.validate();
        this.mainPanel.repaint();
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
                Object obj = node.getUserObject();
                if (obj instanceof WrapperNode) {
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
                }
                else if (obj instanceof ProcedureConflictGraphNode) {
                    this.setForeground(new Color(0, 0, 200));
                    this.setFont(this.getFont().deriveFont(Font.BOLD | Font.ITALIC));
                    this.setIcon(UIManager.getIcon("FileChooser.listViewIcon"));
                }
                else {
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
    
    
    protected String exportConflictGraph() {
        IOFileFilter filter = new IOFileFilter("Graphviz Dot File", "dot");
        File defaultFile = new File(String.format("%s-conflict.dot", args.catalogContext.database.getProject()));
        String path = null;
        try {
            path = showSaveDialog("Export ConflictGraph", ".", filter, defaultFile);
            if (path != null) {
                ConflictGraph graph = this.catalogTreeModel.getProcedureConflictGraphNode().getConflictGraph();
                assert(graph != null) : "Unexpected null ConflictGraph";
                String serialized = GraphvizExport.export(graph, args.catalogContext.database.getProject());
                FileUtil.writeStringToFile(new File(path), serialized);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            showErrorDialog("Failed to export ConflictGraph file", ex.getMessage());
        }
        return (path);
    }
    
    private class CatalogTreeSeleciontListener implements TreeSelectionListener {
        
        public void valueChanged(TreeSelectionEvent e) {
            DefaultMutableTreeNode node = (DefaultMutableTreeNode)CatalogViewer.this.catalogTree.getLastSelectedPathComponent();
            if (node == null) return;

            Object user_obj = node.getUserObject();
            String new_header = null;
            String new_text = null;
            boolean text_mode = true;
            if (user_obj instanceof WrapperNode) {
                CatalogType catalog_obj  = ((WrapperNode)user_obj).getCatalogType();
                new_text = CatalogViewer.this.attributeText.getAttributesText(catalog_obj);
            }
            else if (user_obj instanceof AttributesNode) {
                AttributesNode wrapper = (AttributesNode)user_obj;
                new_text = wrapper.getAttributes();
            }
            else if (user_obj instanceof ProcedureConflictGraphNode) {
                ProcedureConflictGraphNode wrapper = (ProcedureConflictGraphNode)user_obj; 
                CatalogViewer.this.replaceMainPanel(wrapper.getVisualization());
                text_mode = false;
            }
            else if (user_obj instanceof PlanTreeCatalogNode) {
                final PlanTreeCatalogNode wrapper = (PlanTreeCatalogNode)user_obj;
                text_mode = false;
                
                CatalogViewer.this.replaceMainPanel(wrapper.getPanel());
                
                if (SwingUtilities.isEventDispatchThread() == false) {
                    SwingUtilities.invokeLater(new Runnable() {
                        public void run() {
                            wrapper.centerOnRoot();
                        }
                    });
                } else {
                    wrapper.centerOnRoot();
                }
                
            }
            // SUMMARY NODES
            else if (node instanceof CatalogMapTreeNode) {
                Class<? extends CatalogType> catalogType = ((CatalogMapTreeNode)node).getCatalogType();
                if (catalogType.equals(Procedure.class)) {
                    new_text = StringUtil.formatMaps(summaryUtil.getProceduresInfo(true));
                }
                else if (catalogType.equals(Table.class)) {
                    new_text = StringUtil.formatMaps(summaryUtil.getTablesInfo(true));
                }
                else if (catalogType.equals(Host.class)) {
                    new_text = StringUtil.formatMaps(summaryUtil.getHostsInfo(true));
                }
                if (new_text != null) {
                    new_header = catalogType.getSimpleName() + "s Summary";
                }
                
                
            }
            // EVERYTHING ELSE
            if (text_mode && new_text == null) {
                new_header = "Catalog Summary";
                new_text = CatalogViewer.this.summaryUtil.getSummaryText();
            }

            // Text Mode
            if (text_mode) {
                if (CatalogViewer.this.text_mode == false) {
                    CatalogViewer.this.replaceMainPanel(CatalogViewer.this.textInfoPanel);
                }
                if (new_header != null) {
                    new_text = StringUtil.header(new_header.toUpperCase(), "-", 50) +
                               "\n\n" + new_text;
                }
                CatalogViewer.this.textInfoTextArea.setText(new_text);
                
                // Scroll to top
                CatalogViewer.this.textInfoTextArea.grabFocus();
            }
            
            CatalogViewer.this.text_mode = text_mode;
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
                // EXPORT CONFLICT GRAPH
                // --------------------------------------------------------
                case CONFLICTGRAPH_EXPORT: {
                    String path = exportConflictGraph();
                    if (path != null) {
                        LOG.info("Exported ConflictGraph to '" + path + "'");
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
