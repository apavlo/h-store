package edu.brown.oltpgenerator.gui.common.RandomDistribution;

import java.util.Map;

import javax.swing.JPanel;

import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistribution;

public abstract class RandomDistributionEditorRenderer
{
    private JPanel m_paneEditor;

    public JPanel getEditor()
    {
        if (m_paneEditor != null)
        {
            return m_paneEditor;
        }
        else
        {
            m_paneEditor = createEditor();
            return m_paneEditor;
        }
    }

    protected abstract JPanel createEditor();

    public abstract void updateEditor(RandomDistribution property);

    public abstract Map<String, Object> parseEditor() throws Exception;
}
