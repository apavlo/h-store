package edu.brown.oltpgenerator.gui;

import java.awt.event.ActionEvent;
import java.awt.event.KeyEvent;

import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.KeyStroke;

import edu.brown.gui.AbstractMenuHandler;

public class MenuHandler extends AbstractMenuHandler {

    // ----------------------------------------------
    // MENU OPTIONS
    // ----------------------------------------------
    public static enum MenuOptions {
        QUIT, HELP,
    };

    private GuiMain m_viewer;

    public MenuHandler(GuiMain viewer) {
        m_viewer = viewer;
        installMenu();
    }

    private void installMenu() {
        JMenu menu;
        JMenuItem menuItem;

        // 
        // File Menu
        //
        menu = new JMenu("File");
        menu.getPopupMenu().setLightWeightPopupEnabled(false);
        menu.setMnemonic(KeyEvent.VK_F);
        menu.getAccessibleContext().setAccessibleDescription("File Menu");
        m_viewer.addMenu(menu);

        menu.addSeparator();

        menuItem = new JMenuItem("Quit", KeyEvent.VK_Q);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_Q,
                ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext()
                .setAccessibleDescription("Quit Program");
        menuItem.addActionListener(this);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.QUIT);
        menu.add(menuItem);

        // 
        // Help Menu
        //
        menu = new JMenu("Help");
        menu.getPopupMenu().setLightWeightPopupEnabled(false);
        menu.setMnemonic(KeyEvent.VK_H);
        menu.getAccessibleContext().setAccessibleDescription("Help Menu");
        m_viewer.addMenu(menu);

        menu.addSeparator();

        menuItem = new JMenuItem("About", KeyEvent.VK_A);
        menuItem.setAccelerator(KeyStroke.getKeyStroke(KeyEvent.VK_A,
                ActionEvent.CTRL_MASK));
        menuItem.getAccessibleContext().setAccessibleDescription(
                "About this Program");
        menuItem.addActionListener(this);
        menuItem.putClientProperty(MenuHandler.MENU_ID, MenuOptions.HELP);
        menu.add(menuItem);
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        JMenuItem source = (JMenuItem) (e.getSource());
        //
        // Process the event
        //
        MenuOptions opt = MenuOptions.valueOf(source.getClientProperty(MENU_ID)
                .toString());
        switch (opt) {
        // --------------------------------------------------------
        // QUIT
        // --------------------------------------------------------
        case QUIT: {
            m_viewer.quit();
            break;
        }
            // --------------------------------------------------------
            // HELP
            // --------------------------------------------------------
        case HELP: {
            m_viewer.showAboutDialog("About", "Author: Zhe Zhang.   Email: zz.natit@gmail.com");
            break;
        }
            // --------------------------------------------------------
            // UNKNOWN
            // --------------------------------------------------------
        default:
            System.err.println("Invalid Menu Action: " + source.getName());
        } // SWITCH
    }

}
