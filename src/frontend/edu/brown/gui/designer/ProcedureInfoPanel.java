package edu.brown.gui.designer;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.swing.JComboBox;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.voltdb.catalog.Procedure;

import edu.brown.gui.AbstractInfoPanel;
import edu.brown.gui.AbstractViewer;
import edu.brown.gui.DesignerVisualization;
import edu.brown.statistics.ProcedureStatistics;

public class ProcedureInfoPanel extends AbstractInfoPanel<Procedure> {
    private static final long serialVersionUID = -295100895965501422L;

    protected final DesignerVisualization parent;
    protected Procedure selected;
    
    JLabel statementsLabel;
    JLabel workloadLabel;
    JLabel readonlyLabel;
    JComboBox procComboBox; 
    
    public ProcedureInfoPanel(DesignerVisualization parent) {
        super();
        this.parent = parent;
    }

    protected void init() {
        this.setLayout(new BorderLayout());
        JPanel panel = new JPanel();
        panel.setLayout(new GridBagLayout());
        this.add(panel, BorderLayout.NORTH);
        
        GridBagConstraints c = new GridBagConstraints();
        c.gridwidth = 1;
        c.gridheight = 1;
        c.weightx = 0.1;
        c.fill = GridBagConstraints.NONE;
        c.gridy = 0;
        c.insets = AbstractViewer.insets;
        
        c.gridx = 0;
        c.gridy++;
        c.gridwidth = 2;
        c.anchor = GridBagConstraints.LINE_START;
        
        JLabel label = null;
        
        this.procComboBox = new JComboBox();
        this.procComboBox.setEnabled(false);
        this.procComboBox.setFont(AbstractViewer.value_font);
        this.procComboBox.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String proc_name = (String)procComboBox.getSelectedItem();
                ProcedureInfoPanel.this.update(ProcedureInfoPanel.this.parent.getArguments().catalog_db.getProcedures().get(proc_name));
            }
        });
        panel.add(this.procComboBox, c);
        
        c.gridy++;
        AbstractViewer.addSeperator(panel, c);
        
        c.gridwidth = 1;
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("# of Statements:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        c.gridx = 1;
        this.statementsLabel = new JLabel("");
        this.statementsLabel.setFont(AbstractViewer.value_font);
        panel.add(this.statementsLabel, c);
        
        c.gridwidth = 1;
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Workload Invocations:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        c.gridx = 1;
        this.workloadLabel = new JLabel("");
        this.workloadLabel.setFont(AbstractViewer.value_font);
        panel.add(this.workloadLabel, c);
        
        c.gridwidth = 1;
        c.gridx = 0;
        c.gridy++;
        label = new JLabel("Read Only:");
        label.setFont(AbstractViewer.key_font);
        panel.add(label, c);
        c.gridx = 1;
        this.readonlyLabel = new JLabel("");
        this.readonlyLabel.setFont(AbstractViewer.value_font);
        panel.add(this.readonlyLabel, c);
    }
    
    public void loadProcedures(Set<Procedure> procedures) {
        SortedSet<String> sorted = new TreeSet<String>();
        for (Procedure catalog_proc : procedures) {
            sorted.add(catalog_proc.getName());
        }
        for (String proc_name : sorted) {
            this.procComboBox.addItem(proc_name);
        }
        this.procComboBox.setEnabled(true);
    }
    
    @Override
    public void update(Procedure catalog_proc) {
        this.element = catalog_proc;
        this.statementsLabel.setText(Integer.toString(catalog_proc.getStatements().size()));
        
        ProcedureStatistics stats = null;
        if (this.parent.getDesigner() != null && this.parent.getDesigner().getDesignerInfo() != null) {
            stats = this.parent.getDesigner().getDesignerInfo().getStats().getProcedureStatistics(catalog_proc);
        }
        if (stats != null) {
            this.workloadLabel.setText(stats.proc_counts.toString());
            this.readonlyLabel.setText(stats.proc_readonly.toString());
        } else {
            this.workloadLabel.setText("-");
            this.readonlyLabel.setText("-");
        }
        
        //
        // Tell the parent to show the graphs for this procedure
        //
        this.parent.displayGraphs();
    }
    
}
