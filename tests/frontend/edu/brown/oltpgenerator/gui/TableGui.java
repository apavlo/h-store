package edu.brown.oltpgenerator.gui;

import java.awt.Color;
import java.awt.Container;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusEvent;
import java.awt.event.FocusListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.Arrays;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JList;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.ListSelectionModel;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableColumnModel;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.oltpgenerator.Utils;
import edu.brown.oltpgenerator.env.TableEnv;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.gui.common.GuiConstants;
import edu.brown.oltpgenerator.gui.common.LHSListPane;
import edu.brown.oltpgenerator.gui.common.Notifier;
import edu.brown.oltpgenerator.gui.common.UpperPane;
import edu.brown.oltpgenerator.gui.common.ZListSelectionListener;
import edu.brown.oltpgenerator.gui.common.RandomDistribution.RandomDistributionEditorManager;

public class TableGui extends JPanel
{

    private static final long serialVersionUID      = 1L;

    private Container         m_paneParent;
    private Notifier          m_notifier;

    /*
     * The JComponent fields are here (rather than defined in
     * establishGuiTree()) because they are accessible by functions outside
     * establishGuiTree()
     */
    private JButton           m_btLoadSchema        = new JButton("Load Schema File");
    private JList             m_lstTblName          = new JList();
    private JTextField        m_txtCardinality      = new JTextField();
    private JTable            m_tblColView          = new JTable(new ColumnViewModel(null, new Column[0]));
    private JPanel            m_paneColEdit         = new JPanel();

    private JCheckBox         m_checkLinkCsv        = new JCheckBox("Link table to CSV file");
    private JTextField        m_txtLinkCsvPath      = new JTextField(CsvLinkStatus.UNLINKED.m_sStatus);

    private int               m_nYOffsetInRightPane = 0;

    private LHSListPane       m_scrollTblName;

    private UpperPane         m_paneTblBt;

    private static int        s_nRightPaneWidth     = 600;
    private static int        s_nOneRowPaneHeight   = 40;
    private static JPanel     s_paneDummy           = new JPanel();

    private static enum TableInfoColumn {
        NAME("Column Name"), TYPE("Type"), REFER("Foreign Key Of");

        public String m_sName;

        private TableInfoColumn(String name)
        {
            m_sName = name;
        }
    }

    private static enum CsvLinkStatus {
        UNLINKED("No csv file linked"), LINKED("Linked to ");

        public String m_sStatus;

        private CsvLinkStatus(String sStatus)
        {
            m_sStatus = sStatus;
        }
    }

    public TableGui(JTabbedPane paneTab, Notifier notifier)
    {
        m_paneParent = paneTab;
        m_notifier = notifier;
        establishGuiTree();
        addListeners();
    }

    public void clearColumnEditPane()
    {
        RandomDistributionEditorManager.clear(m_paneColEdit);
        m_paneColEdit.add(s_paneDummy);
    }

    private static class ColumnViewModel extends AbstractTableModel
    {

        private static final long serialVersionUID = 1L;

        Object[][]                data;

        public ColumnViewModel(Table tbl, Column[] cols)
        {
            data = new Object[cols.length][3];

            for (int i = 0; i < cols.length; i++)
            {
                Column c = cols[i];

                data[i][0] = c.getName();
                String voltType = VoltType.get((byte) c.getType()).toString(); // form:
                // VoltType.xxx
                int posDot = voltType.indexOf(".");
                String typeName = voltType.substring(posDot + 1);
                data[i][1] = typeName;
                Column referredC = TableEnv.getReferredColumn(c);
                data[i][2] = referredC == null ? " " : referredC.getParent().getName() + " [" + referredC.getName()
                        + "]";
            }
        }

        @Override
        public int getColumnCount()
        {
            return TableInfoColumn.values().length;
        }

        public String getColumnName(int idx)
        {
            return TableInfoColumn.values()[idx].m_sName;
        }

        @Override
        public int getRowCount()
        {
            return data.length;
        }

        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            return data[rowIndex][columnIndex];
        }

        public void setValueAt(Object value, int row, int col)
        {
            data[row][col] = value;
            fireTableCellUpdated(row, col);
        }
    }

    private void addListeners()
    {
        addLoadSchemaListener();

        addCardinalityListener();

        addTableNameListListener();

        addLinkCsvListener();

        addColumnViewListener();
    }

    private void addColumnViewListener()
    {
        m_tblColView.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);

        m_tblColView.getSelectionModel().addListSelectionListener(new ListSelectionListener()
        {

            int iColPrev = -2;
            int iTblPrev = -2;

            @Override
            public void valueChanged(ListSelectionEvent e)
            {
                int iColCur = m_tblColView.getSelectedRow();
                int iTblCur = m_lstTblName.getSelectedIndex();
                showTableCardinality();
                if (iColCur >= 0 && (iTblCur != iTblPrev || iColCur != iColPrev))
                {
                    iColPrev = iColCur;
                    iTblPrev = iTblCur;

                    String sColName = (String) m_tblColView.getModel().getValueAt(iColCur, 0);
                    Column col = getSelectedColumn(sColName);
                    RandomDistributionEditorManager.display(m_paneColEdit, col);
                }
            }

            private Column getSelectedColumn(String sColName)
            {
                String sTblName = (String) m_lstTblName.getSelectedValue();
                Table table = TableEnv.getTable(sTblName);
                return table.getColumns().get(sColName);
            }
        });
    }

    private void addLinkCsvListener()
    {
        final JFileChooser fc = new JFileChooser();
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);

        m_checkLinkCsv.addItemListener(new ItemListener()
        {
            @Override
            public void itemStateChanged(ItemEvent e)
            {
                String sTblName = m_lstTblName.getSelectedValue().toString();

                switch (e.getStateChange())
                {
                    case ItemEvent.DESELECTED:
                    {
                        m_txtLinkCsvPath.setText(CsvLinkStatus.UNLINKED.m_sStatus);
                        TableEnv.unlinkTalbe(sTblName);
                        return;
                    }
                    case ItemEvent.SELECTED:
                    {
                        int returnVal = fc.showOpenDialog(TableGui.this);
                        if (returnVal == JFileChooser.APPROVE_OPTION)
                        {
                            String sCsvPath = fc.getSelectedFile().getPath();
                            TableEnv.linkTableToCsv(sTblName, sCsvPath);
                            m_txtLinkCsvPath.setText(CsvLinkStatus.LINKED.m_sStatus + sCsvPath);
                        }
                        else
                        {
                            m_checkLinkCsv.setSelected(false);
                        }
                    }
                    default:
                        return;
                }
            }
        });
    }

    private void showTableColInfo(Table table)
    {
        Column[] cols = (table == null) ? new Column[0] : Utils.getNonNullElements(table.getColumns().values());
        ColumnViewModel model = new ColumnViewModel(table, cols);
        m_tblColView.setModel(model);
        setTableColInfoWidth();
    }

    private void setTableColInfoWidth()
    {
        AbstractTableModel abs_model = (AbstractTableModel) m_tblColView.getModel();

        int idx_col = abs_model.findColumn(TableInfoColumn.NAME.m_sName);
        int idx_colType = abs_model.findColumn(TableInfoColumn.TYPE.m_sName);

        TableColumnModel model = m_tblColView.getColumnModel();
        model.getColumn(idx_col).setPreferredWidth(1);
        model.getColumn(idx_colType).setPreferredWidth(1);
    }

    private void addTableNameListListener()
    {
        m_lstTblName.addListSelectionListener(new ZListSelectionListener()
        {
            @Override
            public void whenSelecting(String selectedItemName)
            {
                Table table = TableEnv.getTable(selectedItemName);
                // display selected table's column information
                showTableColInfo(table);
                // display cardinality
                showTableCardinality();
                clearColumnEditPane();
                enableTableOptions();
            }
        });
    }

    private void showTableCardinality()
    {
        String sSelectedTableName = m_lstTblName.getSelectedValue().toString();
        m_txtCardinality.setText(TableEnv.getCardinality(sSelectedTableName).toString());
    }

    private void addCardinalityListener()
    {
        m_txtCardinality.addActionListener(new ActionListener()
        {

            @Override
            public void actionPerformed(ActionEvent e)
            {
                String tblName = m_lstTblName.getSelectedValue().toString();
                String input = m_txtCardinality.getText();
                Integer oldCardinality = TableEnv.getCardinality(tblName);
                String oldText = oldCardinality.toString();
                // System.out.println("old value: " + oldText);

                try
                {
                    int newCardinality = Integer.parseInt(input);
                    if (newCardinality > 0)
                    {
                        TableEnv.setCardinality(tblName, newCardinality);
                    }
                    else
                    {
                        m_notifier.showMsg("Cardinality should be a positive integer", false);
                        m_txtCardinality.setText(oldText);
                    }
                }
                catch (NumberFormatException ex)
                {
                    m_notifier.showMsg("Cardinality should be an integer", false);
                    m_txtCardinality.setText(oldText);
                }
            }
        });

        m_txtCardinality.addFocusListener(new FocusListener()
        {

            String sOldText = null;

            @Override
            public void focusGained(FocusEvent e)
            {
                sOldText = m_txtCardinality.getText();
                m_txtCardinality.setText(null);
            }

            @Override
            public void focusLost(FocusEvent e)
            {
                m_txtCardinality.setText(sOldText);
            }
        });
    }

    private void addLoadSchemaListener()
    {
        final JFileChooser fc = new JFileChooser();
        fc.setFileSelectionMode(JFileChooser.FILES_ONLY);

        // JFileChoose is in the closure of inner class
        m_btLoadSchema.addActionListener(new ActionListener()
        {
            @Override
            public void actionPerformed(ActionEvent e)
            {
                int returnVal = fc.showOpenDialog(TableGui.this);
                if (returnVal == JFileChooser.APPROVE_OPTION)
                {
                    TableEnv.clear();
                    RandomDistributionEnv.clearColumnEdits();

                    String schemaPath = fc.getSelectedFile().getPath();
                    TableEnv.setSrcSchemaPath(schemaPath);
                    m_notifier.showMsg("Loading from " + schemaPath, true);

                    TableEnv.readSchema();
                    TableEnv.loadDefaultColumnProperty();
                    String[] tableNames = TableEnv.getTableNames(TableEnv.getAllTables());
                    Arrays.sort(tableNames);
                    m_lstTblName.setListData(tableNames);
                    showTableColInfo(null);
                    clearColumnEditPane();

                    m_notifier.showMsg("Complete loading from " + schemaPath, false);

                    disableTableOptions();
                }
            }
        });
    }

    private void establishGuiTree()
    {
        setLayout(null);
        addAllTablesPane();
        addTableViewEditPane();
    }

    private void addTableViewEditPane()
    {
        JPanel paneTableDisplayAndEdit = new JPanel();
        paneTableDisplayAndEdit.setLayout(null);
        int nWidth = m_paneParent.getWidth();
        int nHeight = m_paneParent.getHeight() - m_paneTblBt.getHeight() - 3 * GuiConstants.GAP_COMPONENT;
        paneTableDisplayAndEdit.setBounds(0, m_paneTblBt.getHeight(), nWidth, nHeight);
        add(paneTableDisplayAndEdit);
        // paneTableDisplayAndEdit.setBorder(BorderFactory.createLineBorder(Color.red));

        m_scrollTblName = new LHSListPane(paneTableDisplayAndEdit, m_lstTblName);
        paneTableDisplayAndEdit.add(m_scrollTblName);
        paneTableDisplayAndEdit.add(createTableCSVArea());
        paneTableDisplayAndEdit.add(createTableCardinalityArea());
        paneTableDisplayAndEdit.add(createColumnViewArea());
        paneTableDisplayAndEdit.add(createColumnEditArea());
        disableTableOptions();
    }

    private JPanel createColumnEditArea()
    {
        m_paneColEdit.setBorder(BorderFactory.createLineBorder(Color.GRAY));

        /*
         * Hackish: this paneDummy will be removed in display() of ColumnEdit.
         */
        JPanel paneDummy = new JPanel();
        m_paneColEdit.add(paneDummy);
        m_paneColEdit.setBounds(m_scrollTblName.getWidth() + GuiConstants.GAP_COMPONENT, m_nYOffsetInRightPane,
                                s_nRightPaneWidth, m_scrollTblName.getHeight() - m_nYOffsetInRightPane);

        return m_paneColEdit;
    }

    private JScrollPane createColumnViewArea()
    {
        JScrollPane scroll_colEdit = new JScrollPane();
        scroll_colEdit.setViewportView(m_tblColView);
        scroll_colEdit.setBounds(m_scrollTblName.getWidth() + GuiConstants.GAP_COMPONENT, m_nYOffsetInRightPane,
                                 s_nRightPaneWidth, 300);

        m_nYOffsetInRightPane += (scroll_colEdit.getHeight() + GuiConstants.GAP_COMPONENT);

        return scroll_colEdit;
    }

    private JPanel createTableCSVArea()
    {
        JPanel paneCSV = new JPanel();
        paneCSV.setLayout(new BoxLayout(paneCSV, BoxLayout.X_AXIS));
        paneCSV.setBorder(BorderFactory.createLineBorder(Color.GRAY));

        paneCSV.add(m_checkLinkCsv);
        paneCSV.add(m_txtLinkCsvPath);
        m_txtLinkCsvPath.setEditable(false);

        paneCSV.setBounds(m_scrollTblName.getWidth() + GuiConstants.GAP_COMPONENT, m_nYOffsetInRightPane,
                          s_nRightPaneWidth, s_nOneRowPaneHeight);
        m_nYOffsetInRightPane += (s_nOneRowPaneHeight + GuiConstants.GAP_COMPONENT);

        return paneCSV;
    }

    private JPanel createTableCardinalityArea()
    {
        JPanel paneTblCardinality = new JPanel();
        paneTblCardinality.setLayout(new BoxLayout(paneTblCardinality, BoxLayout.LINE_AXIS));
        paneTblCardinality.setBorder(BorderFactory.createLineBorder(Color.GRAY));

        JLabel lblCard = new JLabel("Table Cardinality (press \"Enter\" to confirm): ");
        paneTblCardinality.add(lblCard);

        m_txtCardinality.setColumns(5);
        paneTblCardinality.add(m_txtCardinality);

        paneTblCardinality.setBounds(m_scrollTblName.getWidth() + GuiConstants.GAP_COMPONENT, m_nYOffsetInRightPane,
                                     s_nRightPaneWidth, s_nOneRowPaneHeight);
        m_nYOffsetInRightPane += (s_nOneRowPaneHeight + GuiConstants.GAP_COMPONENT);

        return paneTblCardinality;
    }

    private void enableTableOptions()
    {
        m_checkLinkCsv.setEnabled(true);
        String sCsvLink = TableEnv.getTableCsvLink(m_lstTblName.getSelectedValue().toString());
        if (sCsvLink != null)
        {
            m_txtLinkCsvPath.setText(CsvLinkStatus.LINKED.m_sStatus + sCsvLink);
            // disable check box listener for one moment
            ItemListener listener = m_checkLinkCsv.getItemListeners()[0];
            m_checkLinkCsv.removeItemListener(listener);
            m_checkLinkCsv.setSelected(true);
            m_checkLinkCsv.addItemListener(listener);
        }
        else
        {
            m_txtLinkCsvPath.setText(CsvLinkStatus.UNLINKED.m_sStatus);
            m_checkLinkCsv.setSelected(false);
        }

        m_txtCardinality.setEnabled(true);
    }

    private void disableTableOptions()
    {
        m_checkLinkCsv.setEnabled(false);
        m_txtCardinality.setEnabled(false);
        m_txtCardinality.setText(null);
    }

    private void addAllTablesPane()
    {
        m_paneTblBt = new UpperPane(m_paneParent);
        m_paneTblBt.add(m_btLoadSchema);
        add(m_paneTblBt);
    }
}
