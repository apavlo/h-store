package edu.brown.oltpgenerator.gui.common;

import java.awt.Container;
import javax.swing.JList;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;

/**
 * Left Hand Side list pane
 * @author zhe
 *
 */
public class LHSListPane extends JScrollPane
{
    private static final long   serialVersionUID = 1L;

    private static final double PERCENTAGE_WIDTH = 0.2;

    public LHSListPane(Container paneParent, JList lst)
    {
        lst.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        setViewportView(lst);

        int nWidth = (int) ((double) paneParent.getWidth() * PERCENTAGE_WIDTH);
        setSize(nWidth, paneParent.getHeight());
    }
}
