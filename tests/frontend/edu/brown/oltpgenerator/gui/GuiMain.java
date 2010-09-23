package edu.brown.oltpgenerator.gui;

import java.awt.*;

import javax.swing.*;

import edu.brown.gui.AbstractViewer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.oltpgenerator.env.BenchmarkEnv;
import edu.brown.oltpgenerator.gui.common.Notifier;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionEditor;

/**
 * @author zhe zhang
 */
public class GuiMain extends AbstractViewer
{
    private static final long serialVersionUID = 1L;
    private final JLabel      m_lblMsg         = new JLabel("  ");
    private Notifier          m_notifier;

    public GuiMain(ArgumentsParser args, String title)
    {
        super(args, title);
        this.init();
        RandomDistributionEditor.setNotifier(m_notifier);
    }

    @Override
    public void quit()
    {
        super.quit();
    }

    public void addMenu(JMenu menu)
    {
        this.menuBar.add(menu);
    }

    @Override
    protected void viewerInit()
    {
        initNotifier();

        // main frame
        JPanel paneMain = new JPanel(new BorderLayout());
        getContentPane().add(paneMain);
        int nWidth = (int) ((double) getWidth() * 0.95);
        int nHeight = (int) ((double) getHeight() * 0.88);
        paneMain.setSize(nWidth, nHeight);

        // menu
        menuHandler = new MenuHandler(this);

        // tabs
        JTabbedPane paneTab = new JTabbedPane();
        paneMain.add(paneTab);
        paneTab.setSize(nWidth, nHeight);
        paneTab.add("Init Config", new InitConfigGui());
        paneTab.add("Table", new TableGui(paneTab, m_notifier));
        paneTab.add("Procedure", new ProcGui(paneTab, m_notifier));

        // paneTab.setBorder(BorderFactory.createLineBorder(Color.blue));

        // message bar
        m_lblMsg.setHorizontalAlignment(SwingConstants.CENTER);
        paneMain.add(m_lblMsg, BorderLayout.SOUTH);
        // m_lblMsg.setBorder(BorderFactory.createLineBorder(Color.green));
        setLocationRelativeTo(this.getOwner());
    }

    public static void main(String[] vargs) throws Exception
    {
        final ArgumentsParser args = ArgumentsParser.load(vargs);
        BenchmarkEnv.setExternalArgs(args);

        javax.swing.SwingUtilities.invokeLater(new Runnable()
        {
            public void run()
            {
                GuiMain viewer = new GuiMain(args, "OLTP Workload Generator");
                viewer.setVisible(true);
                viewer.setResizable(false);
            } // RUN
        });
    }

    private void initNotifier()
    {
        m_notifier = new Notifier()
        {

            @Override
            public void showMsg(final String msg, boolean alwaysDisplay)
            {
                if (alwaysDisplay)
                {
                    m_lblMsg.setText(msg);
                }
                else
                {
                    new Thread()
                    {
                        public void run()
                        {
                            m_lblMsg.setText(msg);
                            try
                            {
                                sleep(2000);
                            }
                            catch (InterruptedException e)
                            {
                                // TODO Auto-generated catch block
                                e.printStackTrace();
                            }
                            m_lblMsg.setText(" ");
                        }
                    }.start();
                }
            }
        };
    }
}
