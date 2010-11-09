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

import javax.swing.*;
import javax.swing.event.*;
import javax.swing.tree.*;

import java.awt.*;
import java.awt.event.*;
import java.util.*;
import java.util.Map.Entry;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.voltdb.catalog.*;
import org.voltdb.catalog.CatalogType.UnresolvedInfo;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.*;
import org.voltdb.utils.Encoder;
import org.voltdb.*;

import com.sun.tools.javac.util.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.QueryPlanUtil;
import edu.brown.gui.catalog.*;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.*;

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
	protected String catalog_path;
	
	// ----------------------------------------------
	// GUI ELEMENTS
	// ----------------------------------------------
	protected CatalogTreeModel catalogTreeModel;
	protected JTree catalogTree;
	protected JTextField searchField;
	protected JTextArea textInfoTextArea;
	protected JScrollPane textInfoScroller;

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
	    super(args, WINDOW_TITLE, 1000, DEFAULT_WINDOW_HEIGHT);
		this.catalog = args.catalog;
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
				if (user_obj instanceof WrapperNode) {
					CatalogType catalog_obj  = ((WrapperNode)user_obj).getCatalogType();
					new_text += CatalogViewer.this.getAttributesText(catalog_obj);	
				} else if (user_obj instanceof AttributesNode) {
				    AttributesNode wrapper = (AttributesNode)user_obj;
				    new_text += wrapper.getAttributes();
				} else {
				    new_text += CatalogViewer.this.getSummaryText();
				}
				// new_text += "</html>";
				CatalogViewer.this.textInfoTextArea.setText(new_text);
				
				// Scroll to top
				CatalogViewer.this.textInfoTextArea.grabFocus();
			}
		});
		this.generateCatalogTree(this.catalog, this.catalog_path);

		//
		// Text Information Panel
		//
		JPanel textInfoPanel = new JPanel();
		textInfoPanel.setLayout(new BorderLayout());
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
		textInfoPanel.add(this.textInfoScroller, BorderLayout.CENTER);
		
		//
		// Search Toolbar
		//
		JPanel searchPanel = new JPanel();
		searchPanel.setLayout(new BorderLayout());
		JPanel innerSearchPanel = new JPanel();
		innerSearchPanel.setLayout(new BoxLayout(innerSearchPanel, BoxLayout.X_AXIS));
		innerSearchPanel.add(new JLabel("Search: "));
		this.searchField = new JTextField(20);
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
		
		//
		// Putting it all together
		//
		JScrollPane scrollPane = new JScrollPane(this.catalogTree);
		JPanel topPanel = new JPanel();
		topPanel.setLayout(new BorderLayout());
		topPanel.add(searchPanel, BorderLayout.NORTH);
		topPanel.add(scrollPane, BorderLayout.CENTER);
		
		JSplitPane splitPane = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT, topPanel, textInfoPanel);
		splitPane.setDividerLocation(400);

		this.add(splitPane, BorderLayout.CENTER);
	}
	
	protected void search(String value) {
        LOG.debug("Searching based on keyword '" + value + "'...");
        
        //
        // Check if it's a digit, which means that they are searching by Catalog guid
        //
        Set<DefaultMutableTreeNode> found = new HashSet<DefaultMutableTreeNode>();
        Integer guid = null;
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
        
        //
        // Otherwise search by name...
        //
        if (guid == null && value.length() >= SEARCH_MIN_LENGTH) {
            LOG.debug("Searching for matching name '" + value + "'");
            for (String name : this.catalogTreeModel.getNameNodeXref().keySet()) {
                if (name.indexOf(value) != -1) {
                    found.addAll(this.catalogTreeModel.getNameNodeXref().get(name));
                }
            } // FOR
        }
        
        //
        // Display found matches
        //
        if (!found.isEmpty()) this.highlight(found);
	}
	
	protected void highlight(Collection<DefaultMutableTreeNode> nodes) {
	    //
	    // Collapse everything and then show the paths to each node
	    //
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
	protected String getSummaryText() {
	    StringBuilder buffer = new StringBuilder();

	    buffer.append("Catalog Summary\n").append(StringUtil.repeat("=", 50)).append("\n");
	    
        int cols = 0;
        int fkeys = 0;
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        for (Table t : catalog_db.getTables()) {
            cols += t.getColumns().size();
            for (Column c : t.getColumns()) {
                Column fkey = CatalogUtil.getForeignKeyParent(c);
                if (fkey != null) fkeys++;
            }
        }
        int params = 0;
        for (Procedure p : catalog_db.getProcedures()) {
            params += p.getParameters().size();
        }
        
        final String f = "%-24s%d";
        Map<String, Integer> m = new ListOrderedMap<String, Integer>();
        m.put("Tables", catalog_db.getTables().size());
        m.put("Columns", cols);
        m.put("FKeys", fkeys);
        m.put("Params", params);
        m.put("-", null);
        m.put("Hosts", catalog_clus.getHosts().size());
        m.put("Sites", catalog_clus.getSites().size());
        m.put("Partitions", CatalogUtil.getNumberOfPartitions(catalog_db));
        
        for (Entry<String, Integer> e : m.entrySet()) {
            if (e.getValue() != null) {
                buffer.append(String.format(f, "Number of " + e.getKey() + ":", e.getValue()));
            }
            buffer.append("\n");
        } // FOR
	    
	    return (buffer.toString());
	}
	
	/**
	 * 
	 * @param catalog_obj
	 */
	protected String getAttributesText(CatalogType catalog_obj) {
	    StringBuilder buffer = new StringBuilder();
		// buffer.append("guid: ").append(catalog_obj.getGuid()).append("\n");
	    buffer.append("relativeIndex: ").append(catalog_obj.getRelativeIndex()).append("\n");
	    buffer.append("nodeVersion: ").append(catalog_obj.getNodeVersion()).append("\n");
		
		//
		// Database
		//
		if (catalog_obj instanceof Database) {
			buffer.append(Encoder.hexDecodeToString(((Database)catalog_obj).getSchema()));		    
		//
		// Default
		//
		} else {
		    Set<String> skip_fields = new HashSet<String>();
		    skip_fields.add("exptree");
		    skip_fields.add("fullplan");
		    skip_fields.add("ms_exptree");
            skip_fields.add("ms_fullplan");
		    skip_fields.add("plannodetree");
		    
		    Set<String> catalog_fields = new HashSet<String>();
            catalog_fields.add("partition_column");
            catalog_fields.add("partitioncolumn");
            catalog_fields.add("foreignkeytable");
            if (catalog_obj instanceof Constraint) catalog_fields.add("index");
            
            if (catalog_obj instanceof Site) {
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
			    
			    Object value = catalog_obj.getField(field);
			    buffer.append(field).append(": ");
			    boolean output = false;
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
			            if (show_type.contains(catalog_item.getClass())) buffer.append(catalog_item.getClass().getSimpleName());
			            buffer.append(catalog_item.getName());
			            output = true;
			        } else {
			            buffer.append(catalog_item);
			            output = true;
			        }
			    } else {
			        // Constraint
			        if (catalog_obj instanceof Constraint) {
			            if (field == "type") {
			                buffer.append(ConstraintType.get((Integer)value));
			                output = true;
			            }
		            // Index
			        } else if (catalog_obj instanceof Index) {
			            if (field == "type") {
			                buffer.append(IndexType.get((Integer)value));
			                output = true;
			            }
		            // Column && ProcParameter
			        } else if (catalog_obj instanceof Column || catalog_obj instanceof StmtParameter || catalog_obj instanceof ProcParameter) {
			            String keys[] = { "type", "sqltype", "javatype", "defaultvaluetype" };
			            for (String key : keys) {
			                if (field == key) {
			                    buffer.append(VoltType.get(((Integer)value).byteValue()).name());
			                    output = true;
			                    break;
			                }
			            } // FOR
			            if (field.equals("procparameter")) {
			                ProcParameter proc_param = ((StmtParameter)catalog_obj).getProcparameter();
			                if (proc_param != null) {
			                    buffer.append(value).append(" [Index #").append(proc_param.getIndex()).append("]"); 
	                            output = true;
			                }
			            }
			        }
		            // Default
			        if (!output) buffer.append(value);
			    }
			    buffer.append("\n");
			} // FOR
		}
		String add = "";
		
		// Index
		if (catalog_obj instanceof Index) {
		    Index catalog_idx = (Index)catalog_obj;
		    buffer.append("columns: ");
		    for (ColumnRef catalog_col_ref : CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index")) {
		        buffer.append(add).append(catalog_col_ref.getColumn().getName());
		        add = ", ";
		    } // FOR
		    buffer.append("\n");
	    // Constraint
		} else if (catalog_obj instanceof Constraint) {
            Constraint catalog_const = (Constraint)catalog_obj;
            buffer.append("foreignkeycols: ");
            for (ColumnRef catalog_col_ref : catalog_const.getForeignkeycols()) {
                buffer.append(add).append(catalog_col_ref.getColumn().getName());
                add = ", ";
            } // FOR
            buffer.append("\n");
        // Table
        } else if (catalog_obj instanceof Table) {
            Table catalog_tbl = (Table)catalog_obj;
            buffer.append(StringUtil.SINGLE_LINE);
            buffer.append("\n").append(org.voltdb.utils.CatalogUtil.toSchema(catalog_tbl)).append("\n");
        // Column
        } else if (catalog_obj instanceof Column) {
            Column catalog_col = (Column)catalog_obj;
            buffer.append("constraints: ");
            for (ConstraintRef catalog_const_ref : catalog_col.getConstraints()) {
                buffer.append(add).append(catalog_const_ref.getConstraint().getName());
                add = ", ";
            }
            buffer.append("\n");
            
        // PlanFragment
        } else if (catalog_obj instanceof PlanFragment) {
            PlanFragment catalog_frgmt = (PlanFragment)catalog_obj;
            try {
                AbstractPlanNode node = QueryPlanUtil.deserializePlanFragment(catalog_frgmt);
                buffer.append(StringUtil.SINGLE_LINE);
                buffer.append(PlanNodeUtil.debug(node));
            } catch (Exception e) {
                e.printStackTrace();
            }
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
                        generateCatalogTree(result.fst, result.snd);
                    }
                    break;
                }
                // --------------------------------------------------------
                // OPEN CATALOG JAR
                // --------------------------------------------------------
                case CATALOG_OPEN_JAR: {
                    Pair<Catalog, String> result = openCatalogJar();
                    if (result != null) {
                        generateCatalogTree(result.fst, result.snd);
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
