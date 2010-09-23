package edu.brown.oltpgenerator.gui.common;

import java.awt.Container;
import java.awt.FlowLayout;

import javax.swing.JPanel;

public class UpperPane extends JPanel
{
    private static final long       serialVersionUID = 1L;
    private static final FlowLayout LAYOUT           = new FlowLayout();

    public UpperPane(Container paneParent)
    {
        setLayout(LAYOUT);
        int nWidth = paneParent.getWidth();
        int nHeight = (int) ((double) paneParent.getHeight() * 0.065);
        setSize(nWidth, nHeight);
    }

}
