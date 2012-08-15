/**
 * 
 */
package edu.brown.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ItemEvent;
import java.awt.event.KeyEvent;

import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.KeyStroke;

import org.voltdb.catalog.Database;

import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

/**
 * @author pavlo
 *
 */
public class SchemaVisualization extends AbstractViewer {
    private static final long serialVersionUID = 7789644785781359023L;
    
    // ----------------------------------------------
    // WINDOW OPTIONS
    // ----------------------------------------------
    public static final Integer WINDOW_WIDTH  = 800;
    public static final Integer WINDOW_HEIGHT = 650;

    protected GraphVisualizationPanel<DesignerVertex, DesignerEdge> graph_panel;
    
    public SchemaVisualization(Database catalog_db) {
        super(new ArgumentsParser(), "DB Schema Visualizer");
        this.args.catalog_db = catalog_db;
        this.menuHandler = new SchemaVisualization.MenuHandler();
        this.init();
    }
    
    public SchemaVisualization(ArgumentsParser args) {
        super(args, "DB Schema Visualizer");
        this.menuHandler = new SchemaVisualization.MenuHandler();
        this.init();
    }
    
    @Override
    protected void viewerInit() {
        this.setBounds(50, 50, WINDOW_WIDTH, WINDOW_HEIGHT);
        
        // ----------------------------------------------
        // MENU
        // ----------------------------------------------
        JMenu menu;
        JMenuItem menuItem;
        JMenuBar menuBar = new JMenuBar();
    
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
        menu.add(menuItem);
          
        menu.addSeparator();
          
        menuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Quit Program");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuHandler.MENU_QUIT);
        menu.add(menuItem);
        
        DependencyGraph dgraph = new DependencyGraph(this.args.catalog_db);
        try {
            new DependencyGraphGenerator(new DesignerInfo(args.catalogContext, args.workload, args.stats)).generate(dgraph);
            this.graph_panel = GraphVisualizationPanel.factory(dgraph);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        
        this.graph_panel.EVENT_SELECT_VERTEX.addObserver(new EventObserver<DesignerVertex>() {
            public void update(EventObservable<DesignerVertex> o, DesignerVertex arg) {
                System.out.println("Vertex Selected: " + arg);
            }
        });
        
        this.setContentPane(this.graph_panel);
//        this.addComponentListener(this.graph_panel.resizer);
        
        this.setBounds(50, 50, WINDOW_WIDTH, WINDOW_HEIGHT);
        this.setJMenuBar(menuBar);
    }
    
    public ArgumentsParser getArguments() {
        return (this.args);
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
                    SchemaVisualization.this.setVisible(false);
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
                SchemaVisualization gui = new SchemaVisualization(args);
                gui.setVisible(true);
            } // RUN
         });
    }

}
