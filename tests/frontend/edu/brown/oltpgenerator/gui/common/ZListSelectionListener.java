package edu.brown.oltpgenerator.gui.common;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

public abstract class ZListSelectionListener implements ListSelectionListener
{
    private Object m_prevSelected = null;

    private int    m_iSelection   = -1;

    @Override
    public void valueChanged(ListSelectionEvent e)
    {
        Object selectee = ((JList) e.getSource()).getSelectedValue();

        if (selectee != null && selectee != m_prevSelected)
        {
            m_prevSelected = selectee;
            m_iSelection = ((JList) e.getSource()).getSelectedIndex();

            whenSelecting((String) selectee);
        }
    }

    public int getSelectedIndex()
    {
        return m_iSelection;
    }

    public abstract void whenSelecting(String selectedItemName);
}
