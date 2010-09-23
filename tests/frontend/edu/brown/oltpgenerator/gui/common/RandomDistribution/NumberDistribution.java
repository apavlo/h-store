package edu.brown.oltpgenerator.gui.common.RandomDistribution;

import java.awt.FlowLayout;
import java.awt.event.ActionListener;
import java.util.HashMap;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import edu.brown.oltpgenerator.env.RandomDistribution.RandomDistribution;
import edu.brown.oltpgenerator.exception.ColumnEditParsingException;

public enum NumberDistribution {

    DISTRIBUTION_FLAT("Flat", new RandomDistributionEditorRenderer()
    {
        private JTextField m_txtMin = new JTextField(5);
        private JTextField m_txtMax = new JTextField(5);

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel(new FlowLayout());
            ret.add(new JLabel(RandomDistributionKey.MIN.name()));
            ret.add(m_txtMin);
            ret.add(new JLabel(RandomDistributionKey.MAX.name()));
            ret.add(m_txtMax);
            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property != null
                    && property.getUserInput(RandomDistributionKey.SELECTED_DISTRIBUTION.name()).equals(DISTRIBUTION_FLAT.m_sName))
            {
                m_txtMin.setText(property.getUserInput(RandomDistributionKey.MIN.name()).toString());
                m_txtMax.setText(property.getUserInput(RandomDistributionKey.MAX.name()).toString());
            }
            else
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
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

    DISTRIBUTION_GAUSSIAN("Gaussian", new RandomDistributionEditorRenderer()
    {

        private JTextField m_txtMin = new JTextField(5);
        private JTextField m_txtMax = new JTextField(5);

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel(new FlowLayout());
            ret.add(new JLabel(RandomDistributionKey.MIN.name()));
            ret.add(m_txtMin);
            ret.add(new JLabel(RandomDistributionKey.MAX.name()));
            ret.add(m_txtMax);
            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property != null
                    && property.getUserInput(RandomDistributionKey.SELECTED_DISTRIBUTION.name())
                            .equals(DISTRIBUTION_GAUSSIAN.m_sName))
            {
                m_txtMin.setText(property.getUserInput(RandomDistributionKey.MIN.name()).toString());
                m_txtMax.setText(property.getUserInput(RandomDistributionKey.MAX.name()).toString());
            }
            else
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
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

    DISTRIBUTION_ZIPF("Zipf", new RandomDistributionEditorRenderer()
    {

        private JTextField m_txtMin     = new JTextField(5);
        private JTextField m_txtMax     = new JTextField(5);
        private JTextField m_txtSigma   = new JTextField(5);
        private JTextField m_txtEpsilon = new JTextField(5);

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel(new FlowLayout());
            ret.add(new JLabel(RandomDistributionKey.MIN.name()));
            ret.add(m_txtMin);
            ret.add(new JLabel(RandomDistributionKey.MAX.name()));
            ret.add(m_txtMax);
            ret.add(new JLabel(RandomDistributionKey.SIGMA.name()));
            ret.add(m_txtSigma);
            ret.add(new JLabel(RandomDistributionKey.EPSILON.name()));
            ret.add(m_txtEpsilon);
            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property != null
                    && property.getUserInput(RandomDistributionKey.SELECTED_DISTRIBUTION.name()).equals(DISTRIBUTION_ZIPF.m_sName))
            {
                m_txtMin.setText(property.getUserInput(RandomDistributionKey.MIN.name()).toString());
                m_txtMax.setText(property.getUserInput(RandomDistributionKey.MAX.name()).toString());
                m_txtSigma.setText(property.getUserInput(RandomDistributionKey.SIGMA.name()).toString());
                m_txtEpsilon.setText(property.getUserInput(RandomDistributionKey.EPSILON.name()).toString());
            }
            else
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
                m_txtSigma.setText(null);
                m_txtEpsilon.setText(null);
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
            Double nSigma = Double.valueOf(m_txtSigma.getText());
            if (Double.compare(nSigma, 1.0) <= 0)
            {
                throw new ColumnEditParsingException("Sigma should be greater than 1.0");
            }
            Double nEpsilon = Double.valueOf(m_txtEpsilon.getText());
            if (!(Double.compare(nEpsilon, 0) > 0 && Double.compare(nEpsilon, 1) < 0))
            {
                throw new ColumnEditParsingException("Epsilon should be in range (0, 1.0)");
            }
            ret.put(RandomDistributionKey.MIN.name(), nMin);
            ret.put(RandomDistributionKey.MAX.name(), nMax);
            ret.put(RandomDistributionKey.SIGMA.name(), nSigma);
            ret.put(RandomDistributionKey.EPSILON.name(), nEpsilon);
            return ret;
        }
    }),

    DISTRIBUTION_BINOMIAL("Binomial", new RandomDistributionEditorRenderer()
    {

        private JTextField m_txtMin = new JTextField(5);
        private JTextField m_txtMax = new JTextField(5);
        private JTextField m_txtP   = new JTextField(5);

        @Override
        protected JPanel createEditor()
        {
            JPanel ret = new JPanel(new FlowLayout());
            ret.add(new JLabel(RandomDistributionKey.MIN.name()));
            ret.add(m_txtMin);
            ret.add(new JLabel(RandomDistributionKey.MAX.name()));
            ret.add(m_txtMax);
            ret.add(new JLabel(RandomDistributionKey.P.name()));
            ret.add(m_txtP);
            return ret;
        }

        @Override
        public void updateEditor(RandomDistribution property)
        {
            if (property != null
                    && property.getUserInput(RandomDistributionKey.SELECTED_DISTRIBUTION.name())
                            .equals(DISTRIBUTION_BINOMIAL.m_sName))
            {
                m_txtMin.setText(property.getUserInput(RandomDistributionKey.MIN.name()).toString());
                m_txtMax.setText(property.getUserInput(RandomDistributionKey.MAX.name()).toString());
                m_txtP.setText(property.getUserInput(RandomDistributionKey.P.name()).toString());
            }
            else
            {
                m_txtMin.setText(null);
                m_txtMax.setText(null);
                m_txtP.setText(null);
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
            Double nP = Double.valueOf(m_txtP.getText());
            ret.put(RandomDistributionKey.MIN.name(), nMin);
            ret.put(RandomDistributionKey.MAX.name(), nMax);
            ret.put(RandomDistributionKey.P.name(), nP);
            return ret;
        }
    });

    public String               m_sName;
    public RandomDistributionEditorRenderer m_renderer;
    public JRadioButton         m_btSelecter;

    private NumberDistribution(String sName, RandomDistributionEditorRenderer renderer)
    {
        m_sName = sName;
        m_renderer = renderer;

        m_btSelecter = new JRadioButton(m_sName);
        m_btSelecter.setActionCommand(m_sName);
    }

    public static NumberDistribution dispatch(String sName)
    {
        for (NumberDistribution member : NumberDistribution.values())
        {
            if (member.m_sName.equals(sName))
            {
                return member;
            }
        }
        throw new RuntimeException("Couldn't dispatch " + sName + " to a NumberDistribution member");
    }

    private static final ButtonGroup BT_GROUP = new ButtonGroup();

    public static NumberDistribution getSelectedDistribution()
    {
        for (NumberDistribution member : values())
        {
            if (member.m_btSelecter.isSelected())
            {
                return member;
            }
        }
        return null;
    }

    public static void groupSelectors()
    {
        for (NumberDistribution member : values())
        {
            BT_GROUP.add(member.m_btSelecter);
        }
    }

    public static void ungroupSelectors()
    {
        for (NumberDistribution member : values())
        {
            BT_GROUP.remove(member.m_btSelecter);
        }
    }

    public static void listenForSelection(ActionListener listenerDistribution)
    {
        for (NumberDistribution member : values())
        {
            member.m_btSelecter.addActionListener(listenerDistribution);
        }
    }

    public static JPanel createButtonPane()
    {
        JPanel ret = new JPanel(new FlowLayout());
        for (NumberDistribution member : values())
        {
            ret.add(member.m_btSelecter);
        }
        ret.setBorder(BorderFactory.createTitledBorder("Number Distribution"));
        return ret;
    }

    public static void selectNone()
    {
        NumberDistribution selected = NumberDistribution.getSelectedDistribution();
        if (selected == null)
        {
            return;
        }
        NumberDistribution.ungroupSelectors();
        selected.m_btSelecter.setSelected(false);
        NumberDistribution.groupSelectors();
    }
}
