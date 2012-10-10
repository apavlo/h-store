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
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.io.File;

import javax.swing.Box;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JMenuBar;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JSeparator;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.WorkloadStatistics;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.IOFileFilter;

/**
 * @author pavlo
 *
 */
public abstract class AbstractViewer extends JFrame {
    private static final long serialVersionUID = 1L;
    protected static final Logger LOG = Logger.getLogger(AbstractViewer.class);

    // ----------------------------------------------
    // WINDOW OPTIONS
    // ----------------------------------------------
    public static final Integer DEFAULT_WINDOW_WIDTH  = 1200;
    public static final Integer DEFAULT_WINDOW_HEIGHT = 800;
    
    // ----------------------------------------------
    // MENU OPTIONS
    // ----------------------------------------------
    protected AbstractMenuHandler menuHandler;
    protected final JMenuBar menuBar = new JMenuBar();

    // ----------------------------------------------
    // GUI OPTIONS
    // ----------------------------------------------
    protected final int width;
    protected final int height;
    
    public static final Insets insets = new Insets(0, 10, 0, 10);
    public static final Font key_font = new Font(Font.DIALOG, Font.BOLD, 11);
    public static final Font value_font = new Font(Font.DIALOG, Font.PLAIN, 11);
    
    // ----------------------------------------------
    // BASE ATTRIBUTES
    // ----------------------------------------------
    protected final ArgumentsParser args;
    
    /**
     * 
     * @param args
     * @param title
     */
    public AbstractViewer(ArgumentsParser args, String title) {
        this(args, title, DEFAULT_WINDOW_WIDTH, DEFAULT_WINDOW_HEIGHT);
    }
    
    public AbstractViewer(ArgumentsParser args, String title, int width, int height) {
        super(title);
        this.args = args;
        this.width = width;
        this.height = height;
    }
    
    /**
     * The initialization method that must be implemented specifically for the viewer
     */
    protected abstract void viewerInit();
    
    /**
     * Common initialization operations
     */
    protected void init() {
        this.setLayout(new BorderLayout());
        this.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        this.setBounds(50, 50, this.width, this.height);
        this.viewerInit();
        this.setJMenuBar(menuBar);
    }

    protected void quit() {
        setVisible(false);
        System.exit(0);
    }
    
    protected Pair<Catalog, String> openCatalogFile() {
        IOFileFilter filter = new IOFileFilter("Catalog File", "txt");
        Pair<Catalog, String> ret = null;
        try {
            String path = showLoadDialog("Open Catalog File", ".", filter);
            if (path != null) {
                Catalog new_catalog = CatalogUtil.loadCatalog(path);
                ret = new Pair<Catalog, String>(new_catalog, path);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            showErrorDialog("Failed to open catalog from file", ex.getMessage());
        }
        return (ret);
    }
    
    protected Pair<Catalog, String> openCatalogJar() {
        IOFileFilter filter = new IOFileFilter("Project Jar", "jar");
        Pair<Catalog, String> ret = null;
        try {
            String path = showLoadDialog("Open Catalog File from Project Jar", ".", filter);
            if (path != null) {
                CatalogContext new_catalog = CatalogUtil.loadCatalogContextFromJar(new File(path));
                ret = new Pair<Catalog, String>(new_catalog.catalog, path);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            showErrorDialog("Failed to open catalog from jar", ex.getMessage());
        }
        return (ret);
    }
    
    protected Pair<WorkloadStatistics, File> openWorkloadStats() {
        IOFileFilter filter = new IOFileFilter("Workload Stats", "stats");
        Pair<WorkloadStatistics, File> ret = null;
        try {
            String path = showLoadDialog("Open Workload Statistics File", ".", filter);
            if (path != null) {
                WorkloadStatistics new_stats = new WorkloadStatistics(args.catalog_db);
                File f = new File(path);
                new_stats.load(f, args.catalog_db);
                ret = new Pair<WorkloadStatistics, File>(new_stats, f);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            showErrorDialog("Failed to open workload stats file", ex.getMessage());
        }
        return (ret);
    }
    
    protected String saveWorkloadStats() {
        IOFileFilter filter = new IOFileFilter("Workload Stats", "stats");
        String path = null;
        String dir = args.stats_path.getParent();
        try {
            path = showSaveDialog("Save Workload Statistics File", dir, filter);
            if (path != null) this.args.stats.save(new File(path));
        } catch (Exception ex) {
            ex.printStackTrace();
            showErrorDialog("Failed to save workload stats file", ex.getMessage());
        }
        return (path);
    }
    
    public static GridBagConstraints getConstraints() {
        GridBagConstraints c = new GridBagConstraints();
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 0.5;
        c.ipadx = 150;
        c.ipady = 2;
        c.gridx = 0;
        c.gridy = 0;
        c.fill = GridBagConstraints.NONE;
        c.insets = insets;
        c.anchor = GridBagConstraints.LINE_START;
        return (c);
    }
    
    public static void addSeperator(JPanel panel, GridBagConstraints c) {
        Insets orig_insets = c.insets;
        c.insets = new Insets(0, 0, 0, 0);
        c.gridwidth = 2;
        c.gridheight = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.CENTER;
        c.gridx = 0;
        panel.add(Box.createVerticalStrut(5), c);
        c.gridx = 0;
        c.gridy++;
        panel.add(new JSeparator(), c);
        c.gridx = 0;
        c.gridy++;
        panel.add(Box.createVerticalStrut(5), c);
        c.insets = orig_insets;
        return;
    }
    
    public static void addSpacer(JPanel panel, GridBagConstraints c) {
        Insets orig_insets = c.insets;
        c.insets = new Insets(0, 0, 0, 0);
        c.gridwidth = 2;
        c.gridheight = 1;
        c.fill = GridBagConstraints.HORIZONTAL;
        c.anchor = GridBagConstraints.CENTER;
        c.gridx = 0;
        c.gridy++;
        panel.add(Box.createVerticalStrut(20), c);
        c.insets = orig_insets;
        return;
    }
    
    
    /**
     * 
     * @param title
     * @param dir
     * @param filter
     * @return
     * @throws Exception
     */
    protected String showLoadDialog(String title, String dir, IOFileFilter filter) throws Exception {
        JFileChooser chooser = new JFileChooser(dir);
        chooser.setFileFilter(filter);
        chooser.setDialogTitle(title);
        int returnVal = chooser.showOpenDialog(this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            return (chooser.getSelectedFile().toString());
        }
        return (null);
    }
    
    /**
     * 
     * @param title
     * @param dir
     * @param filter
     * @return
     * @throws Exception
     */
    protected String showSaveDialog(String title, String dir, IOFileFilter filter) throws Exception {
        return this.showSaveDialog(title, dir, filter, null);
    }
    
    protected String showSaveDialog(String title, String dir, IOFileFilter filter, File defaultFile) throws Exception {
        JFileChooser chooser = new JFileChooser(dir);
        chooser.setFileFilter(filter);
        chooser.setDialogTitle(title);
        if (defaultFile != null) chooser.setSelectedFile(defaultFile);
        int returnVal = chooser.showSaveDialog(this);
        if (returnVal == JFileChooser.APPROVE_OPTION) {
            return (chooser.getSelectedFile().toString());
        }
        return (null);
    }
    
    /**
     * 
     * @param title
     * @param message
     */
    public void showErrorDialog(String title, String message) {
        JOptionPane.showMessageDialog(this, message, title, JOptionPane.ERROR_MESSAGE);
    }
    
    /**
     * 
     * @param title
     * @param message
     */
    public void showAboutDialog(String title, String message) {
        JOptionPane.showMessageDialog(this, message, title, JOptionPane.INFORMATION_MESSAGE);
    }
}
