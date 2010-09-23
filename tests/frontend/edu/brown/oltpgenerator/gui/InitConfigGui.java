package edu.brown.oltpgenerator.gui;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import edu.brown.oltpgenerator.env.BenchmarkEnv;

import edu.brown.oltpgenerator.gui.TextFieldWatcher.WatchMethod;

public class InitConfigGui extends JPanel
{
    private static final long   serialVersionUID     = 1L;

    // source folder
    private final JLabel        m_lblSrcFolder       = new JLabel("Benchmark destination: ");
    private final JTextField    m_txtSrcFolder       = new JTextField(20);
    private final JButton       m_btChooseSrcFolder  = new JButton("Choose folder: ");

    // package name
    private final JLabel        m_lblPackName        = new JLabel("Benchmark package name: ");
    private final JTextField    m_txtPackName        = new JTextField(30);

    // workload name
    private final JLabel        m_lblBenchmarkName   = new JLabel("Benchmark name: ");
    private final JTextField    m_txtBenchmarkName   = new JTextField(30);

    // tool tips
    private static final String S_TIP_SRC_FOLDER     = "The location you place your benchmark package into";
    private static final String S_TIP_PACK_NAME      = "Name of benchmark package. For instance, edu.brown.benchmark.tpce";
    private static final String S_TIP_BENCHMARK_NAME = "Benchmark Name. For instance, TPCE";

    private final JFileChooser  m_fc                 = new JFileChooser();

    public InitConfigGui()
    {
        m_fc.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
        establishGuiTree();
        addListeners();
        setToolTips();
    }

    private void setToolTips()
    {
        m_lblSrcFolder.setToolTipText(S_TIP_SRC_FOLDER);
        m_txtSrcFolder.setToolTipText(S_TIP_SRC_FOLDER);
        m_txtPackName.setToolTipText(S_TIP_PACK_NAME);
        m_lblPackName.setToolTipText(S_TIP_PACK_NAME);
        m_txtBenchmarkName.setToolTipText(S_TIP_BENCHMARK_NAME);
        m_lblBenchmarkName.setToolTipText(S_TIP_BENCHMARK_NAME);
    }

    private void addListeners()
    {
        addTxtBenchmarkFolderListener();
        addTxtBenchmarkNameListener();
        addTxtPackNameListener();
        addBtChooseBenchmarkFolderListener();
    }

    private void addTxtBenchmarkNameListener()
    {
        TextFieldWatcher.setWatch(m_txtBenchmarkName, new WatchMethod()
        {

            @Override
            public void updateFrom(JTextField txtWatchee)
            {
                BenchmarkEnv.setBenchmarkName(txtWatchee.getText());
            }
        });
    }

    private void addTxtPackNameListener()
    {
        TextFieldWatcher.setWatch(m_txtPackName, new WatchMethod()
        {

            @Override
            public void updateFrom(JTextField txtWatchee)
            {
                BenchmarkEnv.setPackageName(txtWatchee.getText());

            }
        });
    }

    private void addTxtBenchmarkFolderListener()
    {
        TextFieldWatcher.setWatch(m_txtSrcFolder, new WatchMethod()
        {

            @Override
            public void updateFrom(JTextField txtWatchee)
            {
                BenchmarkEnv.setSourceFolderPath(txtWatchee.getText());
            }
        });
    }

    private void addBtChooseBenchmarkFolderListener()
    {
        m_btChooseSrcFolder.addActionListener(new ActionListener()
        {

            @Override
            public void actionPerformed(ActionEvent e)
            {
                int returnVal = m_fc.showOpenDialog(InitConfigGui.this);
                if (returnVal == JFileChooser.APPROVE_OPTION)
                {
                    String path = m_fc.getSelectedFile().getPath();
                    m_txtSrcFolder.setText(path);
                    BenchmarkEnv.setSourceFolderPath(path);
                }
            }
        });
    }

    private void establishGuiTree()
    {

        JPanel pane_north = new JPanel();
        this.add(pane_north, BorderLayout.NORTH);

        pane_north.setLayout(new BoxLayout(pane_north, BoxLayout.Y_AXIS));
        JPanel pane_srcFolder = new JPanel();
        JPanel pane_packageName = new JPanel();
        JPanel pane_workloadName = new JPanel();
        pane_north.add(pane_srcFolder);
        pane_north.add(pane_packageName);
        pane_north.add(pane_workloadName);

        pane_srcFolder.setLayout(new FlowLayout());
        pane_srcFolder.add(m_lblSrcFolder);
        pane_srcFolder.add(m_txtSrcFolder);
        pane_srcFolder.add(m_btChooseSrcFolder);

        pane_packageName.setLayout(new FlowLayout());
        pane_packageName.add(m_lblPackName);
        pane_packageName.add(m_txtPackName);

        pane_workloadName.setLayout(new FlowLayout());
        pane_workloadName.add(m_lblBenchmarkName);
        pane_workloadName.add(m_txtBenchmarkName);
    }
}
