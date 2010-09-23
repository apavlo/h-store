package edu.brown.oltpgenerator.gui;

import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

public class TextFieldWatcher
{
    public static void setWatch(final JTextField watchee, final WatchMethod method)
    {
        watchee.getDocument().addDocumentListener(new DocumentListener()
        {

            @Override
            public void changedUpdate(DocumentEvent e)
            {
                method.updateFrom(watchee);
            }

            @Override
            public void insertUpdate(DocumentEvent e)
            {
                method.updateFrom(watchee);
            }

            @Override
            public void removeUpdate(DocumentEvent e)
            {
                method.updateFrom(watchee);
            }
        });
    }

    public static interface WatchMethod
    {
        void updateFrom(JTextField txtWatchee);
    }
}
