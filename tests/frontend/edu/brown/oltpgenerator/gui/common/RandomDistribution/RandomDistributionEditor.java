package edu.brown.oltpgenerator.gui.common.RandomDistribution;

import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;

import edu.brown.oltpgenerator.env.RandomDistribution.DateDistribution;
import edu.brown.oltpgenerator.env.RandomDistribution.NumericDistributionFactory;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistribution;
import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistributionEnv;
import edu.brown.oltpgenerator.env.RandomDistribution.StringDistribution;
import edu.brown.oltpgenerator.exception.ColumnEditParsingException;
import edu.brown.oltpgenerator.gui.common.Notifier;

public enum RandomDistributionEditor {
    NUMBER_EDITOR(new RandomDistributionEditorRenderer()
    {
        private JPanel m_paneDistributionParams = new JPanel();

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel();
            ret.setLayout(new BoxLayout(ret, BoxLayout.PAGE_AXIS));

            JButton button = new JButton("Confirm");
            button.addActionListener(new ActionListener()
            {

                @Override
                public void actionPerformed(ActionEvent e)
                {
                    NumberDistribution nd = NumberDistribution.getSelectedDistribution();
                    if (nd == null)
                    {
                        return;
                    }

                    try
                    {
                        Map<String, Object> map = parseEditor();
                        RandomDistribution property = NumericDistributionFactory
                                .createNumberColumnEditProperty(nd, map);
                        RandomDistributionEnv.put(s_valSelected, property);
                    }
                    catch (Exception e1)
                    {
                        showMsg(e1.getMessage(), false);
                    }
                }
            });

            ret.add(button);

            ret.add(createDistributionArea());
            ret.add(m_paneDistributionParams);

            return ret;
        }

        private JPanel createDistributionArea()
        {
            NumberDistribution.groupSelectors();
            NumberDistribution.listenForSelection(new ActionListener()
            {

                @Override
                public void actionPerformed(ActionEvent e)
                {
                    NumberDistribution distr = NumberDistribution.dispatch(e.getActionCommand());
                    distr.m_renderer.updateEditor(RandomDistributionEnv.get(s_valSelected));
                    JPanel paneParams = distr.m_renderer.getEditor();
                    paneParams.setBorder(BorderFactory.createTitledBorder("Distribution Parameters"));
                    paneParams.updateUI();
                    if (m_paneDistributionParams.getComponentCount() > 0)
                    {
                        assert (m_paneDistributionParams.getComponentCount() == 1);
                        m_paneDistributionParams.remove(0);
                    }
                    m_paneDistributionParams.add(paneParams);
                    m_paneDistributionParams.updateUI();
                }
            });

            return NumberDistribution.createButtonPane();
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (null == property)
            {
                NumberDistribution.selectNone();

                // removing distribution parameter pane
                if (m_paneDistributionParams.getComponentCount() == 1)
                {
                    m_paneDistributionParams.remove(0);
                    m_paneDistributionParams.updateUI();
                }
            }
            else
            {
                Map<String, Object> propertyMap = property.getUserInputMap();
                String sSelected = (String) propertyMap.get(RandomDistributionKey.SELECTED_DISTRIBUTION.name());
                NumberDistribution dist = NumberDistribution.dispatch(sSelected);
                dist.m_btSelecter.setSelected(true);
                // update and add distribution parameter pane
                dist.m_renderer.updateEditor(property);
                // remove existing distribution parameter pane
                if (m_paneDistributionParams.getComponentCount() == 1)
                {
                    m_paneDistributionParams.remove(0);
                    m_paneDistributionParams.updateUI();
                }
                m_paneDistributionParams.add(dist.m_renderer.getEditor());
                m_paneDistributionParams.updateUI();
            }
        }

        @Override
        public Map<String, Object> parseEditor() throws Exception
        {
            NumberDistribution selected = NumberDistribution.getSelectedDistribution();
            if (selected == null)
            {
                return null;
            }

            Map<String, Object> ret = selected.m_renderer.parseEditor();
            ret.put(RandomDistributionKey.SELECTED_DISTRIBUTION.name(), selected.m_sName);
            return ret;
        }

    }),

    STRING_EDITOR(new RandomDistributionEditorRenderer()
    {

        private JTextField m_txtMin = new JTextField(5);
        private JTextField m_txtMax = new JTextField(5);

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel();
            ret.setLayout(new BoxLayout(ret, BoxLayout.PAGE_AXIS));

            ret.add(createDataRangeArea());

            JButton button = new JButton("Confirm");
            button.addActionListener(new ActionListener()
            {

                @Override
                public void actionPerformed(ActionEvent e)
                {
                    try
                    {
                        Map<String, Object> map = parseEditor();
                        RandomDistribution property = new StringDistribution(map);
                        RandomDistributionEnv.put(s_valSelected, property);
                    }
                    catch (Exception e1)
                    {
                        showMsg(e1.getMessage(), false);
                    }
                }
            });

            ret.add(button);

            return ret;
        }

        private JPanel createDataRangeArea()
        {
            JPanel ret = new JPanel(new FlowLayout());

            ret.add(new JLabel("Min: "));
            ret.add(m_txtMin);
            ret.add(new JLabel("Max: "));
            ret.add(m_txtMax);

            ret.setBorder(BorderFactory.createTitledBorder("String length range"));

            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property == null)
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
            }
            else
            {
                m_txtMin.setText(property.getUserInput(RandomDistributionKey.MIN.name()).toString());
                m_txtMax.setText(property.getUserInput(RandomDistributionKey.MAX.name()).toString());
            }
        }

        @Override
        public Map<String, Object> parseEditor() throws Exception
        {
            Map<String, Object> ret = new HashMap<String, Object>();
            Integer nMin = Double.valueOf(m_txtMin.getText()).intValue();
            Integer nMax = Double.valueOf(m_txtMax.getText()).intValue();
            if (nMin >= nMax)
            {
                throw new ColumnEditParsingException("Min value should be less than Max value");
            }
            ret.put(RandomDistributionKey.MIN.name(), nMin);
            ret.put(RandomDistributionKey.MAX.name(), nMax);
            return ret;
        }

    }),

    DATE_EDITOR(new RandomDistributionEditorRenderer()
    {

        private JTextField m_txtMin = new JTextField(10);
        private JTextField m_txtMax = new JTextField(10);
        private DateFormat m_format = new SimpleDateFormat("MM/dd/yyyy");

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel();
            ret.setLayout(new BoxLayout(ret, BoxLayout.PAGE_AXIS));

            ret.add(createDataRangeArea());

            JButton button = new JButton("Confirm");
            button.addActionListener(new ActionListener()
            {

                @Override
                public void actionPerformed(ActionEvent e)
                {
                    try
                    {
                        Map<String, Object> map = parseEditor();
                        RandomDistribution property = new DateDistribution(map);
                        RandomDistributionEnv.put(s_valSelected, property);
                    }
                    catch (Exception e1)
                    {
                        showMsg(e1.getMessage(), false);
                    }
                }
            });

            ret.add(button);

            return ret;
        }

        private Component createDataRangeArea()
        {
            JPanel ret = new JPanel(new FlowLayout());

            ret.add(new JLabel("Min: "));
            ret.add(m_txtMin);
            ret.add(new JLabel("Max: "));
            ret.add(m_txtMax);

            ret.setBorder(BorderFactory.createTitledBorder("Date range (format: MM/DD/YYYY)"));

            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property == null)
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
            }
            else
            {
                Date dMin = (Date) property.getUserInput(RandomDistributionKey.MIN.name());
                Date dMax = (Date) property.getUserInput(RandomDistributionKey.MAX.name());
                m_txtMin.setText(formatDate(dMin));
                m_txtMax.setText(formatDate(dMax));
            }
        }

        private String formatDate(Date time)
        {
            Calendar cal = Calendar.getInstance();
            cal.setTime(time);
            int nDay = cal.get(Calendar.DATE);
            int nMon = cal.get(Calendar.MONTH) + 1;
            int nYear = cal.get(Calendar.YEAR);

            return (nMon < 10 ? "0" + nMon : nMon) + "/" + (nDay < 10 ? "0" + nDay : nDay) + "/" + nYear;
        }

        @Override
        public Map<String, Object> parseEditor() throws Exception
        {
            Map<String, Object> ret = new HashMap<String, Object>();
            Date dMin = m_format.parse(m_txtMin.getText());
            Date dMax = m_format.parse(m_txtMax.getText());
            if (!(dMax.after(dMin)))
            {
                throw new ColumnEditParsingException("Min value should be less than Max value");
            }

            ret.put(RandomDistributionKey.MIN.name(), dMin);
            ret.put(RandomDistributionKey.MAX.name(), dMax);
            return ret;
        }

    });

    public RandomDistributionEditorRenderer m_lambdaEditorRenderer;

    private static Notifier      s_notifier;

    private RandomDistributionEditor(RandomDistributionEditorRenderer ec)
    {
        m_lambdaEditorRenderer = ec;
    }

    private static CatalogType s_valSelected = null;

    private static RandomDistributionEditor dispatch(VoltType voltType)
    {
        switch (voltType)
        {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DECIMAL:
                return NUMBER_EDITOR;
            case STRING:
                return STRING_EDITOR;
            case TIMESTAMP:
                return DATE_EDITOR;
        }
        throw new RuntimeException("Invalid column type");
    }

    public static void setSelectedCatalogVal(CatalogType catalogVal)
    {
        s_valSelected = catalogVal;
    }

    public static void setNotifier(Notifier notifier)
    {
        s_notifier = notifier;
    }

    public static void showMsg(final String msg, boolean alwaysDisplay)
    {
        s_notifier.showMsg(msg, alwaysDisplay);
    }

    public static void addEditor(JPanel paneColEdit, CatalogType catalogVal)
    {
        setSelectedCatalogVal(catalogVal);
        RandomDistributionEditor dispatchee = dispatch(typeOf(catalogVal));
        paneColEdit.remove(0);
        dispatchee.m_lambdaEditorRenderer.updateEditor(null);
        paneColEdit.add(dispatchee.m_lambdaEditorRenderer.getEditor(), 0);
    }

    public static void addEditor(JPanel paneColEdit, CatalogType catalogVal, RandomDistribution property)
    {
        setSelectedCatalogVal(catalogVal);
        RandomDistributionEditor dispatchee = dispatch(typeOf(catalogVal));
        paneColEdit.remove(0);
        dispatchee.m_lambdaEditorRenderer.updateEditor(property);
        paneColEdit.add(dispatchee.m_lambdaEditorRenderer.getEditor(), 0);
    }

    private static VoltType typeOf(CatalogType catalogVal)
    {
        if (catalogVal instanceof Column)
        {
            return VoltType.get(((Column) catalogVal).getType());
        }

        if (catalogVal instanceof ProcParameter)
        {
            return VoltType.get(((ProcParameter) catalogVal).getType());
        }

        throw new RuntimeException("This CatalogType doesn't have getType() method");
    }
}
