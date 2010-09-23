package edu.brown.oltpgenerator.gui;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JSlider;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

import edu.brown.oltpgenerator.env.ProcEnv;
import edu.brown.oltpgenerator.env.BenchmarkEnv;
import edu.brown.oltpgenerator.env.TableEnv;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.exception.CycleInDagException;
import edu.brown.oltpgenerator.exception.TotalProbabilityExceeds100Exception;
import edu.brown.oltpgenerator.exception.UserInputException;
import edu.brown.oltpgenerator.gui.common.GuiConstants;
import edu.brown.oltpgenerator.gui.common.LHSListPane;
import edu.brown.oltpgenerator.gui.common.Notifier;
import edu.brown.oltpgenerator.gui.common.UpperPane;
import edu.brown.oltpgenerator.gui.common.ZListSelectionListener;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionEditorManager;
import edu.brown.oltpgenerator.velocity.CodeGenerator;

public class ProcGui extends JPanel
{

    private static final long serialVersionUID       = 1L;

    private JButton           m_btLoadTrace          = new JButton();
    private JButton           m_btGenBenchmark       = new JButton();

    private UpperPane         m_paneButtons;

    private JList             m_lstProcNames         = new JList();
    private LHSListPane       m_scrollProcNames;

    private JTabbedPane       m_paneParent;
    private Notifier          m_notifier;

    private JSlider           m_slideProbability     = new JSlider(0, 100, 0);
    private JTextField        m_txtProbability       = new JTextField();

    private UpperPane         m_paneProbability;

    private JList             m_lstProcParaNames     = new JList();
    private JPanel            m_paneParaDistribution = new JPanel();

    private static JPanel     s_paneDummy            = new JPanel();

    public ProcGui(JTabbedPane paneTab, Notifier notifier)
    {
        m_paneParent = paneTab;
        m_notifier = notifier;
        establishGuiTree();
        addListeners();
    }

    private void establishGuiTree()
    {
        this.setLayout(null);
        addButtonPane();
        addProcedureViewEditPane();
    }

    private void addButtonPane()
    {
        m_paneButtons = new UpperPane(m_paneParent);

        m_btLoadTrace.setText("Load workload trace file");
        m_paneButtons.add(m_btLoadTrace);

        m_btGenBenchmark.setText("Generate Benchmark");
        m_paneButtons.add(m_btGenBenchmark);

        add(m_paneButtons);
    }

    private void addProcedureViewEditPane()
    {
        JPanel paneSp = new JPanel();
        paneSp.setLayout(null);
        int nWidth = m_paneParent.getWidth();
        int nHeight = m_paneParent.getHeight() - m_paneButtons.getHeight() - 3 * GuiConstants.GAP_COMPONENT;
        paneSp.setBounds(0, m_paneButtons.getHeight(), nWidth, nHeight);

        m_scrollProcNames = new LHSListPane(paneSp, m_lstProcNames);
        paneSp.add(m_scrollProcNames);

        paneSp.add(createProcEditPane());

        add(paneSp);
    }

    private JPanel createProcEditPane()
    {
        JPanel paneSpEdit = new JPanel();
        paneSpEdit.setLayout(null);
        int nXStart = m_scrollProcNames.getWidth() + GuiConstants.GAP_COMPONENT;
        int nWidth = m_paneParent.getWidth() - m_scrollProcNames.getWidth() - GuiConstants.GAP_COMPONENT;
        paneSpEdit.setBounds(nXStart, 0, nWidth, m_scrollProcNames.getHeight());

        paneSpEdit.add(createProcProbabilityPane(paneSpEdit));
        paneSpEdit.add(createProcParasPane(paneSpEdit));

        return paneSpEdit;
    }

    private JPanel createProcProbabilityPane(JPanel parent)
    {
        m_paneProbability = new UpperPane(parent);
        m_paneProbability.setBorder(BorderFactory.createEtchedBorder());
        m_paneProbability.setLayout(null);

        String sToolTips = "Probability this procedure is executed";

        m_txtProbability.setText(Integer.toString(0));
        int nWidthTxt = (int) ((double) m_paneProbability.getWidth() * 0.07);
        m_txtProbability.setSize(nWidthTxt, m_paneProbability.getHeight());
        m_txtProbability.setEditable(false);
        m_txtProbability.setToolTipText(sToolTips);
        m_paneProbability.add(m_txtProbability);

        JLabel lblPercent = new JLabel("%");
        int nWidthLbl = (int) ((double) nWidthTxt * 0.3);
        lblPercent.setBounds(nWidthTxt, 0, nWidthLbl, m_paneProbability.getHeight());
        m_paneProbability.add(lblPercent);

        m_slideProbability.setMajorTickSpacing(10);
        m_slideProbability.setMinorTickSpacing(1);
        m_slideProbability.setPaintLabels(true);
        m_slideProbability.setPaintTicks(true);
        m_slideProbability.setSnapToTicks(true);
        int nXStart = nWidthTxt + nWidthLbl + GuiConstants.GAP_COMPONENT;
        m_slideProbability.setBounds(nXStart, 0, m_paneProbability.getWidth() - nXStart, m_paneProbability.getHeight());
        m_slideProbability.setToolTipText(sToolTips);
        m_paneProbability.add(m_slideProbability);

        disableProcOptions();

        return m_paneProbability;
    }

    private JPanel createProcParasPane(JPanel parent)
    {
        JPanel paneProcParas = new JPanel();
        paneProcParas.setLayout(null);
        paneProcParas.setBorder(BorderFactory.createEtchedBorder());
        int nYStartPaneProcParas = m_paneProbability.getHeight() + GuiConstants.GAP_COMPONENT;
        paneProcParas.setBounds(0, nYStartPaneProcParas, parent.getWidth(), parent.getHeight() - nYStartPaneProcParas);

        LHSListPane scrollParaNames = new LHSListPane(paneProcParas, m_lstProcParaNames);
        paneProcParas.add(scrollParaNames);

        paneProcParas.add(m_paneParaDistribution);
        int nXStartParaDistribution = scrollParaNames.getWidth() + GuiConstants.GAP_COMPONENT;
        int nWidthParaDistribution = paneProcParas.getWidth() - nXStartParaDistribution - GuiConstants.GAP_COMPONENT;
        int nHeightParaDistribution = scrollParaNames.getHeight() - 2 * GuiConstants.GAP_COMPONENT;
        m_paneParaDistribution.setBounds(nXStartParaDistribution, GuiConstants.GAP_COMPONENT, nWidthParaDistribution,
                                         nHeightParaDistribution);

        m_paneParaDistribution.add(s_paneDummy);

        return paneProcParas;
    }

    private void addListeners()
    {
        addLoadTraceFileListener();
        addSliderListener();
        addProcListListener();
        addProcParaListListener();
        addGenClientListener();
    }

    private void addProcParaListListener()
    {
        m_lstProcParaNames.addListSelectionListener(new ZListSelectionListener()
        {

            @Override
            public void whenSelecting(String selectedItemName)
            {
                Procedure curProc = ProcEnv.getProcedure(m_lstProcNames.getSelectedValue().toString());
                ProcParameter para = curProc.getParameters().get(getSelectedIndex());
                RandomDistributionEditorManager.display(m_paneParaDistribution, para);
            }
        });
    }

    private void addProcListListener()
    {
        m_lstProcNames.addListSelectionListener(new ZListSelectionListener()
        {

            @Override
            public void whenSelecting(String selectedItemName)
            {
                showProcProbability(selectedItemName);
                showProcParaList(selectedItemName);
                clearParaEditPane();
            }

            private void showProcParaList(String sProcName)
            {
                Procedure proc = ProcEnv.getProcedure(sProcName);
                String[] paraTypeNames = ProcEnv.getParaVoltTypeNames(proc);
                m_lstProcParaNames.setListData(paraTypeNames);
            }

            private void showProcProbability(String sProcName)
            {
                Integer nProb = ProcEnv.getProbability(sProcName);
                m_slideProbability.setEnabled(true);
                if (null == nProb)
                {
                    m_txtProbability.setText("0");
                    m_slideProbability.setValue(0);
                }
                else
                {
                    m_txtProbability.setText(nProb.toString());
                    m_slideProbability.setValue(nProb);
                }
            }
        });
    }

    protected void clearParaEditPane()
    {
        RandomDistributionEditorManager.clear(m_paneParaDistribution);
        m_paneParaDistribution.add(s_paneDummy);
    }

    private void addSliderListener()
    {
        m_slideProbability.addChangeListener(new ChangeListener()
        {

            @Override
            public void stateChanged(ChangeEvent e)
            {
                int value = ((JSlider) e.getSource()).getValue();

                String curProc = m_lstProcNames.getSelectedValue().toString();
                try
                {
                    ProcEnv.setProbability(curProc, value);
                    m_txtProbability.setText(Integer.toString(value));
                }
                catch (TotalProbabilityExceeds100Exception e1)
                {
                    Integer nOldVal = ProcEnv.getProbability(curProc);
                    m_slideProbability.setValue((nOldVal == null) ? 0 : nOldVal);
                    m_notifier.showMsg(e1.getMessage(), false);
                }
            }
        });
    }

    private void addLoadTraceFileListener()
    {
        m_btLoadTrace.addActionListener(new ActionListener()
        {
            private Object[] m_emptyArray = new Object[0];

            @Override
            public void actionPerformed(ActionEvent e)
            {
                try
                {
                    ProcEnv.clear();
                    RandomDistributionEnv.clearProcParaEdits();

                    ProcEnv.loadTraceFile();
                    ProcEnv.loadDefaultParaProperty();
                    Procedure[] procs = ProcEnv.getAllProcedures();
                    String[] procNames = ProcEnv.getProcNames(procs);
                    m_lstProcNames.setListData(procNames);

                    disableProcOptions();
                    m_lstProcParaNames.setListData(m_emptyArray);
                    clearParaEditPane();
                }
                catch (Exception e1)
                {
                    e1.printStackTrace();
                }
            }
        });
    }

    private void disableProcOptions()
    {
        m_txtProbability.setText(null);
        m_slideProbability.setValue(0);
        m_slideProbability.setEnabled(false);
    }

    private void addGenClientListener()
    {
        m_btGenBenchmark.addActionListener(new ActionListener()
        {

            @Override
            public void actionPerformed(ActionEvent e)
            {
                try
                {
                    checkUserInputs();
                    CodeGenerator.bigBang();
                }
                catch (CycleInDagException e1)
                {
                    m_notifier.showMsg(e1.getMessage(), false);
                    return;
                }
                catch (UserInputException e2)
                {
                    m_notifier.showMsg(e2.getMessage(), false);
                }
                m_notifier.showMsg("Benchmark generated in " + BenchmarkEnv.getProjectPath(), true);
            }
        });
    }

    protected void checkUserInputs() throws UserInputException
    {
        checkNotNull(BenchmarkEnv.getSourceFolderPath(), "Source folder path not specified");
        checkNotNull(BenchmarkEnv.getPackageName(), "Benchmark package name not specified");
        checkNotNull(BenchmarkEnv.getBenchmarkName(), "Benchmark name not specified");
        checkNotNull(TableEnv.getCatalog(), "Schema file not loaded");
        checkNotNull(ProcEnv.getAllProcedures(), "Transaction trace file not loaded");
    }

    private void checkNotNull(Object obj, String errMsg) throws UserInputException
    {
        if (null == obj)
        {
            throw new UserInputException(errMsg);
        }
    }
}
