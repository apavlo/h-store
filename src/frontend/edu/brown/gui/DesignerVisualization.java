/**
 * 
 */
package edu.brown.gui;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ItemEvent;
import java.awt.event.KeyEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JCheckBox;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JSeparator;
import javax.swing.JTabbedPane;
import javax.swing.KeyStroke;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.IGraph;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.gui.designer.EdgeInfoPanel;
import edu.brown.gui.designer.PartitionPlanPanel;
import edu.brown.gui.designer.ProcedureInfoPanel;
import edu.brown.gui.designer.VertexInfoPanel;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.workload.Workload;

/**
 * @author pavlo
 *
 */
public class DesignerVisualization extends AbstractViewer {
    private static final long serialVersionUID = 7789644785781359023L;
    
    // ----------------------------------------------
    // WINDOW OPTIONS
    // ----------------------------------------------
    public static final Integer WINDOW_WIDTH  = 1000;
    public static final Integer WINDOW_HEIGHT = 650;
    public static final Font WINDOW_FIXED_FONT = new Font(Font.MONOSPACED, Font.PLAIN, 10);
    public static final Integer WINDOW_LEFT_SIZE = 325;

    protected final Database catalog_db;
    protected Designer designer;

    // ----------------------------------------------
    // GUI ELEMENTS
    // ----------------------------------------------
    ProcedureInfoPanel procInfoPanel;
    VertexInfoPanel vertexInfoPanel;
    EdgeInfoPanel edgeInfoPanel;
    JCheckBox showErrorsBox;
    
    JTabbedPane tabbedPane;
    Vector<GraphVisualizationPanel<DesignerVertex, DesignerEdge>> visualizers = new Vector<GraphVisualizationPanel<DesignerVertex, DesignerEdge>>();
    
    protected DesignerVisualization.MenuHandler menuHandler = new DesignerVisualization.MenuHandler();
    protected Map<Integer, JMenuItem> menuItems = new HashMap<Integer, JMenuItem>();
    
    // ----------------------------------------------
    // OBSERVERS
    // ----------------------------------------------
    protected final EventObserver<DesignerVertex> OBSERVER_VERTEX = new EventObserver<DesignerVertex>() {
        public void update(EventObservable<DesignerVertex> o, DesignerVertex arg) {
            DesignerVisualization.this.showVertexInformation((DesignerVertex)arg);
        }
    };
    protected final EventObserver<DesignerEdge> OBSERVER_EDGE = new EventObserver<DesignerEdge>() {
        @Override
        public void update(EventObservable<DesignerEdge> o, DesignerEdge t) {
            DesignerVisualization.this.showEdgeInformation((DesignerEdge)t);
        }
    };
    
    
    /**
     * Constructor
     */
    public DesignerVisualization(ArgumentsParser args) {
        super(args, "DB Designer Visualizer");
        this.catalog_db = args.catalog_db;
        this.menuHandler = new DesignerVisualization.MenuHandler();
        this.init();
    }
    
    @Override
    protected void viewerInit() {
        this.setLayout(new BorderLayout());
        this.setSize(WINDOW_WIDTH, WINDOW_HEIGHT);
        
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
    
        menuItem = new JMenuItem("Open Schema File"); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Schema File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuHandler.MENU_SCHEMA_OPEN);
        this.menuItems.put(MenuHandler.MENU_SCHEMA_OPEN, menuItem);
        menu.add(menuItem);
          
        menu.addSeparator();
          
        menuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Quit Program");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuHandler.MENU_QUIT);
        this.menuItems.put(MenuHandler.MENU_QUIT, menuItem);
        menu.add(menuItem);

        // ----------------------------------------------
        // LEFT-SIDE PANEL
        // ----------------------------------------------
        JPanel leftPanel = new JPanel(new BorderLayout());
        
        this.procInfoPanel = new ProcedureInfoPanel(this);
        this.procInfoPanel.setBorder(BorderFactory.createTitledBorder("Stored Procedures"));
        leftPanel.add(this.procInfoPanel, BorderLayout.NORTH);
        
        JPanel graphPanel = new JPanel();
        graphPanel.setLayout(new BoxLayout(graphPanel, BoxLayout.Y_AXIS));
        graphPanel.setBorder(BorderFactory.createTitledBorder("Graph Information"));
        
        this.vertexInfoPanel = new VertexInfoPanel(this);
        graphPanel.add(this.vertexInfoPanel);

        graphPanel.add(Box.createVerticalStrut(5));
        graphPanel.add(new JSeparator());
        graphPanel.add(Box.createVerticalStrut(5));
        
        this.edgeInfoPanel = new EdgeInfoPanel(this);
        graphPanel.add(this.edgeInfoPanel);
        
        leftPanel.add(graphPanel, BorderLayout.CENTER);    
        
        // ----------------------------------------------
        // PUT IT ALL TOGETHER!
        // ----------------------------------------------
        this.tabbedPane = new JTabbedPane();
        this.tabbedPane.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                // TODO Auto-generated method stub
            }
        });
        
        
        Dimension leftSize = new Dimension(WINDOW_LEFT_SIZE, WINDOW_HEIGHT);
        leftPanel.setSize(leftSize);
        leftPanel.setPreferredSize(leftSize);
        leftPanel.setMaximumSize(leftSize);
        leftPanel.setMinimumSize(leftSize);
        
        this.add(leftPanel, BorderLayout.WEST);
        this.add(this.tabbedPane, BorderLayout.CENTER);
        
        //
        // Set Defaults
        //
        for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
            if (!catalog_proc.getSystemproc()) {
                this.procInfoPanel.update(catalog_proc);
                break;
            }
        } // FOR
    }
    
    public ArgumentsParser getArguments() {
        return (this.args);
    }
    public Designer getDesigner() {
        return this.designer;
    }
    public Workload getWorkload() {
        return this.args.workload;
    }
    public VertexInfoPanel getVertexInfoPanel() {
        return this.vertexInfoPanel;
    }
    public EdgeInfoPanel getEdgeInfoPanel() {
        return this.edgeInfoPanel;
    }
    
    /**
     * 
     */
    public void displayGraphs() {
        if (this.designer == null) return;
        Procedure catalog_proc = this.procInfoPanel.getElement();
        
        //
        // We always need to display the full DependencyGraph
        //
        int selected_idx = this.tabbedPane.getSelectedIndex();
        final int extra_graphs = 2; //(this.designer.getTempGraph() != null ? 3 : 2);
        
        GraphVisualizationPanel<DesignerVertex, DesignerEdge> gpanel = null;
        if (this.tabbedPane.getTabCount() == 0) {
            if (this.designer.getFinalGraph() != null) {
                IGraph<DesignerVertex, DesignerEdge> final_graph = this.designer.getFinalGraph();
                gpanel = GraphVisualizationPanel.factory(final_graph, this.OBSERVER_VERTEX, this.OBSERVER_EDGE);
                this.visualizers.add(gpanel);
                JPanel final_panel = new JPanel(new BorderLayout());
                final_panel.add(this.visualizers.lastElement(), BorderLayout.CENTER);
                final_panel.add(new PartitionPlanPanel(this), BorderLayout.SOUTH);
                this.tabbedPane.add(final_panel, "Final");
            }

            AbstractDirectedGraph<DesignerVertex, DesignerEdge> dgraph = this.designer.getDesignerInfo().dgraph;
            this.visualizers.add(GraphVisualizationPanel.factory(dgraph, this.OBSERVER_VERTEX, this.OBSERVER_EDGE));
            this.tabbedPane.add(this.visualizers.lastElement(), dgraph.getClass().getSimpleName());

        } else {
            while (this.tabbedPane.getTabCount() > extra_graphs) {
                this.tabbedPane.remove(extra_graphs);
                this.visualizers.remove(extra_graphs);
            } // WHILE
        }
        
        java.util.List<IGraph<DesignerVertex, DesignerEdge>> graphs = new ArrayList<IGraph<DesignerVertex, DesignerEdge>>();
        graphs.addAll(this.designer.getGraphs(catalog_proc));
        if (graphs != null) {
            for (int ctr = 0; ctr < graphs.size(); ctr++) {
                IGraph<DesignerVertex, DesignerEdge> graph = graphs.get(ctr);
                this.visualizers.add(GraphVisualizationPanel.factory(graph, this.OBSERVER_VERTEX, this.OBSERVER_EDGE));
                String name = (graph.getName() != null ? graph.getName() : graph.getClass().getSimpleName());
                this.tabbedPane.add(this.visualizers.lastElement(), name);
            } // FOR
        } else {
            System.err.println("ERROR: The graphs for " + catalog_proc + " are null");
        }
        if (selected_idx >= 0 && selected_idx < this.visualizers.size()) {
            this.tabbedPane.setSelectedIndex(selected_idx);
        } else {
            selected_idx = 0;
        }
        //
        // Select a random vertex and edge
        //
        IGraph<DesignerVertex, DesignerEdge> graph = (IGraph<DesignerVertex, DesignerEdge>)this.visualizers.get(selected_idx).getGraph();
        for (DesignerVertex v : graph.getVertices()) {
            if (!graph.getIncidentEdges(v).isEmpty()) {
                this.visualizers.get(selected_idx).selectVertex(v);
                this.visualizers.get(selected_idx).selectEdge(CollectionUtil.first(graph.getIncidentEdges(v)));
                break;
            }
        } // FOR
    }
    
    public void executeDesigner() {
        if (this.args.workload == null) return;
        DesignerInfo info = new DesignerInfo(this.args);
        DesignerHints hints = new DesignerHints();
        hints.proc_exclude.add("ResetWarehouse");
//        hints.proc_whitelist.add("neworder");
//        hints.proc_whitelist.add("delivery");
//        hints.proc_include.add("slev");
        try {
            this.designer = new Designer(info, hints, this.args);
        } catch (Exception ex) {
            ex.printStackTrace();
            this.showErrorDialog("Unable to Execute Designer", ex.getMessage());
            return;
        }
//        this.designer = new BranchAndBoundDesigner(info);
        
        new Thread() {
            public void run() {
                try {
                    if (DesignerVisualization.this.designer.process() != null) {
                        DesignerVisualization.this.designer.apply();
                    }
                    DesignerVisualization.this.procInfoPanel.loadProcedures(DesignerVisualization.this.designer.getProcedures());
                    DesignerVisualization.this.displayGraphs();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    DesignerVisualization.this.showErrorDialog("Designer Error", ex.getMessage());
                }        
            };
        }.start();
        return;
    }
    
    public GraphVisualizationPanel<DesignerVertex, DesignerEdge> getCurrentVisualizer() {
        return (this.visualizers.get(this.tabbedPane.getSelectedIndex()));
    }

    public void showVertexInformation(DesignerVertex vertex) {
        this.vertexInfoPanel.update(vertex);
    }
    
    public void showEdgeInformation(DesignerEdge edge) {
        this.edgeInfoPanel.update(edge);
    }

    /**
     * 
     */
    protected class MenuHandler extends AbstractMenuHandler {
        //
        // Schemas
        //
        public static final int MENU_SCHEMA_NEW                = 1;
        public static final int MENU_SCHEMA_OPEN               = 2;
        public static final int MENU_SCHEMA_SAVE               = 3;
        public static final int MENU_QUIT                      = 4;
         
        public void actionPerformed(ActionEvent e) {
            JMenuItem source = (JMenuItem)(e.getSource());
            //
            // Process the event
            //
            Integer id = (Integer)source.getClientProperty(MENU_ID);
            switch (id) {
                // --------------------------------------------------------
                // OPEN SCHEMA FILE
                // --------------------------------------------------------
                case (MENU_SCHEMA_OPEN): {
                    break;
                }
                // --------------------------------------------------------
                // QUIT
                // --------------------------------------------------------
                case (MENU_QUIT): {
                    DesignerVisualization.this.setVisible(false);
                    System.exit(0);
                    break;
                }
                // --------------------------------------------------------
                // UNKNOWN
                // --------------------------------------------------------
                default:
                    System.err.println("Invalid Menu Action: " + source.getName());
            } // SWITCH
        }
        public void itemStateChanged(ItemEvent e) {
            JMenuItem source = (JMenuItem)(e.getSource());
            String s = "Item event detected.\n"
                       + "    Event source: " + source.getText()
                       + " (an instance of " + source.getClass().getName() + ")\n"
                       + "    New state: "
                       + ((e.getStateChange() == ItemEvent.SELECTED) ?
                         "selected":"unselected") + "\n\n";
            System.err.println(s);
         }
    } // END CLASS
    
    /**
     * @param args
     */
    public static void main(final String[] vargs) throws Exception {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        
        //edu.uci.ics.jung.samples.TreeLayoutDemo.main(args);
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                DesignerVisualization gui = new DesignerVisualization(args);
                gui.setVisible(true);
                gui.executeDesigner();
            } // RUN
         });
    }

}
