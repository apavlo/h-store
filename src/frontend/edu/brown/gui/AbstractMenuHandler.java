/**
 * 
 */
package edu.brown.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

import javax.swing.JMenuItem;

/**
 * @author pavlo
 *
 */
public abstract class AbstractMenuHandler implements ActionListener, ItemListener {
    //
    // Menu Identifiers
    //
    public static final String MENU_ID = "menuID";
    public static final String MENU_LABEL = "menuLabel";


    /* (non-Javadoc)
     * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
     */
    @Override
    public abstract void actionPerformed(ActionEvent e);

    /* (non-Javadoc)
     * @see java.awt.event.ItemListener#itemStateChanged(java.awt.event.ItemEvent)
     */
    @Override
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
}
