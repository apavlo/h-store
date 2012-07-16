package edu.brown.gui;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.KeyStroke;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import org.voltdb.catalog.Catalog;
import org.voltdb.utils.Pair;

import edu.brown.gui.stats.TableListPanel;
import edu.brown.gui.stats.TableStatsPanel;
import edu.brown.statistics.TableStatistics;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;

/**
 * 
 * @author pavlo
 *
 */
public class StatsViewer extends AbstractViewer {
    private static final long serialVersionUID = 1L;

    // ----------------------------------------------
    // MENU OPTIONS
    // ----------------------------------------------
    public enum MenuOptions {
        CATALOG_OPEN_FILE,
        CATALOG_OPEN_JAR,
        WORKLOAD_STATS_OPEN,
        WORKLOAD_STATS_SAVE,
        WORKLOAD_STATS_SAVE_AS,
        QUIT,
    };

    // ----------------------------------------------
    // GUI ELEMENTS
    // ----------------------------------------------
    protected JPanel summaryPanel;
    
    protected JPanel tableStatsWrapperPanel;
    protected TableListPanel tableSelectPanel;
    protected Map<String, TableStatsPanel> tableStatsPanels = new HashMap<String, TableStatsPanel>();
    protected String last_table_key;
    
    protected JPanel procStatsPanel;

    /**
     * 
     * @param args
     */
    public StatsViewer(ArgumentsParser args) {
        super(args, "H-Store Workload Statistics Viewer");
        this.menuHandler = new StatsViewer.MenuHandler();
        this.init();
        if (this.args.catalog != null) {
            this.update();
        }
    }

    @Override
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
    
        menuItem = new JMenuItem("Open Catalog File..."); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Catalog From File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.CATALOG_OPEN_FILE);
        menu.add(menuItem);
        
        menuItem = new JMenuItem("Open Catalog Jar..."); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_O, ActionEvent.CTRL_MASK | ActionEvent.SHIFT_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Catalog From Project Jar");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.CATALOG_OPEN_JAR);
        menu.add(menuItem);
         
        menu.addSeparator();
          
        menuItem = new JMenuItem("Open Stats"); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_L, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Open Workload Statistics File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.WORKLOAD_STATS_OPEN);
        menu.add(menuItem);
                
        menuItem = new JMenuItem("Save Stats"); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Save Workload Statistics File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.WORKLOAD_STATS_SAVE);
        menu.add(menuItem);
        
        menuItem = new JMenuItem("Save Stats As..."); 
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_S, ActionEvent.CTRL_MASK | ActionEvent.SHIFT_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Save Workload Statistics File");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.WORKLOAD_STATS_SAVE_AS);
        menu.add(menuItem);
        
        menu.addSeparator();
        
        menuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q, ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription("Quit Program");
        menuItem.addActionListener(this.menuHandler);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.QUIT);
        menu.add(menuItem);

        // ----------------------------------------------
        // SUMMARY PANEL
        // ----------------------------------------------
        this.summaryPanel = new JPanel();

        // ----------------------------------------------
        // PROC STATS PANEL
        // ----------------------------------------------
        this.procStatsPanel = new JPanel();

        // ----------------------------------------------
        // TABLE STATS PANEL
        // ----------------------------------------------

        //
        // TOP: Stats Viewers
        //
        TableStatsPanel firstPanel = null;
        for (String table_key : this.args.stats.table_stats.keySet()) {
            TableStatistics stats = this.args.stats.getTableStatistics(table_key);
            TableStatsPanel statsPanel = new TableStatsPanel(stats);
            this.tableStatsPanels.put(table_key, statsPanel);
            if (firstPanel == null) firstPanel = statsPanel;
        } // FOR
        
        //
        // BOTTOM: Table Selection
        //
        final TableListPanel tableSelectionPanel = new TableListPanel();
        tableSelectionPanel.getSelectionModel().addListSelectionListener(new ListSelectionListener() {
            @Override
            public void valueChanged(ListSelectionEvent e) {
                String table_key = tableSelectionPanel.getSelectedTable();
                updateTableStatsPanel(table_key);
            }
        });
        tableSelectionPanel.setTables(args.catalog_db.getTables());
        
        this.tableStatsWrapperPanel = new JPanel(new BorderLayout()); 
        this.tableStatsWrapperPanel.add(firstPanel, BorderLayout.CENTER);
        JSplitPane splitPane = new JSplitPane(JSplitPane.VERTICAL_SPLIT, this.tableStatsWrapperPanel, tableSelectionPanel);
        splitPane.setDividerLocation(500);
        tableSelectionPanel.getSelectionModel().setSelectionInterval(0, 0);

        // ----------------------------------------------
        // TABBED PANE
        // ----------------------------------------------
        JTabbedPane tabbedPane = new JTabbedPane();
        tabbedPane.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                // TODO Auto-generated method stub
                
            }
        });
        tabbedPane.add(this.summaryPanel, "Summary");
        tabbedPane.add(this.procStatsPanel, "Procedure Stats");
        tabbedPane.add(splitPane, "Table Stats");
        this.add(tabbedPane, BorderLayout.CENTER);
        tabbedPane.setSelectedIndex(2);
    }
    
    private void update() {
        assert(this.args.catalog != null);
        assert(this.args.stats != null);
        
    }
    
    private void updateTableStatsPanel(String table_key) {
        if (table_key == null || table_key.equals(last_table_key)) return;
        if (this.last_table_key != null) {
            TableStatsPanel old_panel = this.tableStatsPanels.get(this.last_table_key);
            old_panel.setEnabled(false);
            old_panel.setVisible(false);
        }
        
        TableStatsPanel new_panel = this.tableStatsPanels.get(table_key);
        new_panel.setEnabled(true);
        new_panel.setVisible(true);
        this.tableStatsWrapperPanel.add(new_panel, BorderLayout.CENTER);
        this.tableStatsWrapperPanel.revalidate();
        this.last_table_key = table_key;
    }
    
    /**
     * @param args
     */
    public static void main(final String[] vargs) throws Exception {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        assert(args.catalog != null);
        
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                StatsViewer viewer = new StatsViewer(args);
                viewer.setVisible(true);
            } // RUN
         });
    }

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
                        args.catalog = result.getFirst();
                        args.catalog_path = new File(result.getSecond());
                    }
                    break;
                }
                // --------------------------------------------------------
                // OPEN CATALOG JAR
                // --------------------------------------------------------
                case CATALOG_OPEN_JAR: {
                    Pair<Catalog, String> result = openCatalogJar();
                    if (result != null) {
                        args.catalog = result.getFirst();
                        args.catalog_path = new File(result.getSecond());
                    }
                    break;
                }
                // --------------------------------------------------------
                // OPEN WORKLOAD STATS
                // --------------------------------------------------------
                case WORKLOAD_STATS_OPEN: {
                    Pair<WorkloadStatistics, File> result = openWorkloadStats();
                    if (result != null) {
                        args.stats = result.getFirst();
                        args.stats_path = result.getSecond();
                        update();
                    }
                    break;
                }
                // --------------------------------------------------------
                // WORKLOAD STATS SAVE
                // --------------------------------------------------------
                case WORKLOAD_STATS_SAVE: {
                    //
                    // First call all of the objects to update themselves based on the fields
                    //
                    LOG.info("Invoking stats panels to update underlying objects");
                    for (TableStatsPanel statsPanel : StatsViewer.this.tableStatsPanels.values()) {
                        statsPanel.save();
                    } // FOR
                    LOG.info("Saving workload stats...");
                    try {
                        args.stats.save(args.stats_path);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        showErrorDialog("Failed to save workload stats file", ex.getMessage());
                    }
                    break;
                }
                // --------------------------------------------------------
                // WORKLOAD STATS SAVE AS
                // --------------------------------------------------------
                case WORKLOAD_STATS_SAVE_AS: {
                    //
                    // First call all of the objects to update themselves based on the fields
                    //
                    LOG.info("Invoking stats panels to update underlying objects");
                    for (TableStatsPanel statsPanel : StatsViewer.this.tableStatsPanels.values()) {
                        statsPanel.save();
                    } // FOR
                    LOG.info("Saving workload stats...");
                    String path = saveWorkloadStats();
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
