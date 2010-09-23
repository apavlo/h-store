package edu.brown.oltpgenerator.gui.common.RandomDistribution;

import javax.swing.JPanel;

import org.voltdb.catalog.CatalogType;

import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistribution;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;

/**
 * @author Zhe Zhang
 * 
 */
public class RandomDistributionEditorManager
{

    public static void display(JPanel paneColEdit, CatalogType catalogVal)
    {
        if (RandomDistributionEnv.firstTimeSee(catalogVal))
        {
            RandomDistributionEditor.addEditor(paneColEdit, catalogVal);
        }
        else
        {
            RandomDistribution property = RandomDistributionEnv.get(catalogVal);
            RandomDistributionEditor.addEditor(paneColEdit, catalogVal, property);
        }

        paneColEdit.updateUI();
    }

    public static void clear(JPanel paneColEdit)
    {
        if (paneColEdit.getComponentCount() == 1)
        {
            paneColEdit.remove(0);
            paneColEdit.updateUI();
        }
    }
}