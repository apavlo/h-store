package edu.brown.gui;

import java.awt.Color;
import java.awt.Container;
import java.awt.Paint;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Point2D;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JPopupMenu;

import org.apache.commons.collections15.Transformer;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.markov.MarkovEdge;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovVertex;
import edu.brown.utils.ArgumentsParser;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.visualization.VisualizationViewer;
import edu.uci.ics.jung.visualization.control.PickingGraphMousePlugin;
import edu.uci.ics.jung.visualization.control.PluggableGraphMouse;
import edu.uci.ics.jung.visualization.control.TranslatingGraphMousePlugin;
import edu.uci.ics.jung.visualization.decorators.ToStringLabeller;

/**
 * 
 *
 */
public class MarkovViewer extends AbstractViewer {

    /**
     * @param args
     * @param title
     */
    public MarkovViewer(ArgumentsParser args, String title) {
        super(args, title);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see edu.brown.gui.AbstractViewer#viewerInit()
     */
    @Override
    protected void viewerInit() {
        // TODO Auto-generated method stub

    }
    

    public static GraphVisualizationPanel<MarkovVertex, MarkovEdge> getPanel(
            Graph<MarkovVertex, MarkovEdge> test_graph) {
        GraphVisualizationPanel<MarkovVertex, MarkovEdge> graph_panel = GraphVisualizationPanel
                .factory(test_graph);
        Transformer<MarkovVertex, Paint> transformer = new VertexTransformer<MarkovVertex, Paint>();
        graph_panel.getRenderContext().setVertexFillPaintTransformer(
                transformer);
        graph_panel.getRenderContext().setEdgeLabelTransformer(
                new ToStringLabeller<MarkovEdge>());
        PluggableGraphMouse gm = new PluggableGraphMouse();
        gm.add(new PopupMousePlugin<MarkovVertex, MarkovEdge>());
        gm.add(new TranslatingGraphMousePlugin(MouseEvent.BUTTON1_MASK));
        graph_panel.setGraphMouse(gm);
        Transformer<MarkovVertex, String> labelTransform = new VertexLabelTransformer<MarkovVertex, String>();
        graph_panel.getRenderContext()
                .setVertexLabelTransformer(labelTransform);
        return graph_panel;
    }

    static JComboBox makePartitionComboBox(
            Map<Pair<Procedure, Integer>, MarkovGraph> grafs) {
        JComboBox jcb = new JComboBox(grafs.keySet().toArray());
        ActionListener l = new PartitionActionListener(grafs);
        jcb.addActionListener(l);
        return jcb;
    }

    static JComboBox makeComboBox(Map<Procedure, MarkovGraph> grafs) {
        String[] nameArray = new String[grafs.keySet().size()];
        int i = 0;
        for (Object p : grafs.keySet().toArray()) {
            nameArray[i] = ((Procedure) p).toString();
            i++;
        }
        JComboBox jcb = new JComboBox(nameArray);
        ActionListener l = null; // FIXME(svelagap) new MyActionListener(grafs);
        jcb.addActionListener(l);
        return jcb;
    }

    static JComboBox makeAlphabetizedComboBox(Map<Procedure, MarkovGraph> grafs) {
        List<Procedure> ls = new LinkedList<Procedure>();
        List<String> names = new LinkedList<String>();
        ls.addAll(grafs.keySet());
        for (Procedure p : ls) {
            names.add(p.getName().toLowerCase());
        }
        Collections.sort(names);
        JComboBox jcb = new JComboBox(names.toArray());
        ActionListener l = null; // FIXME(svelagap) new MyActionListener(grafs);
        jcb.addActionListener(l);
        return jcb;
    }

    public class MyActionListener implements ActionListener {
        Map<Procedure, MarkovGraph> procedureGraphs;
        public MyActionListener(Map<Procedure, MarkovGraph> procedureGraphs){
            this.procedureGraphs = procedureGraphs;
        }
        @Override
        public void actionPerformed(ActionEvent e) {
            JComboBox cb = (JComboBox) e.getSource();
            JFrame frame = getFrame(cb);
            Procedure proc = MarkovViewer.this.args.catalog_db.getProcedures()
                    .get((String) cb.getSelectedItem());
            MarkovGraph g = procedureGraphs.get(
                    proc);
            assert (g != null);
            GraphVisualizationPanel<MarkovVertex, MarkovEdge> graph_panel = MarkovViewer.getPanel(g);
            graph_panel.add(cb);
            frame.setContentPane(graph_panel);
            frame.setBounds(50, 50, 500, 500);
            frame.setVisible(true);
            // The BasicVisualizationServer<V,E> is parameterized by the edge
            // types
        }

    }

    public static class PartitionActionListener implements ActionListener {
        Map<Pair<Procedure, Integer>, MarkovGraph> partitionGraphs;
        public PartitionActionListener(Map<Pair<Procedure, Integer>, MarkovGraph> partitionGraphs){
            this.partitionGraphs = partitionGraphs;
        }
        @Override
        public void actionPerformed(ActionEvent e) {
            JComboBox cb = (JComboBox) e.getSource();
            JFrame frame = (JFrame) cb.getParent().getParent().getParent().getParent();
            Pair<Procedure, Integer> proc = (Pair<Procedure, Integer>) cb.getSelectedItem();
            MarkovGraph g = partitionGraphs.get(proc);
            assert (g != null);
            GraphVisualizationPanel<MarkovVertex, MarkovEdge> graph_panel = MarkovViewer.getPanel(g);
            graph_panel.add(cb);
            frame.setContentPane(graph_panel);
            frame.setBounds(50, 50, 500, 500);
            frame.setVisible(true);

        }

    }
    public static class PopupMousePlugin<V,E> extends PickingGraphMousePlugin<V,E> {
        public PopupMousePlugin(){
            this.modifiers = MouseEvent.BUTTON3_MASK;    
        }
        public void mouseClicked(MouseEvent e){
            super.mouseClicked(e);
            VisualizationViewer<V,E> vv = (GraphVisualizationPanel<V,E>)e.getSource();
            Layout layout = vv.getGraphLayout();
            Point2D p = e.getPoint();
            vertex = vv.getPickSupport().getVertex(layout,p.getX(), p.getY());
            if(vertex != null){
                MarkovVertex v = (MarkovVertex)vertex;
/**                JPopupMenu j = new JPopupMenu(v.toString());
                j.setLightWeightPopupEnabled(false);
                j.addSeparator();
                j.addMouseListener(new PopupMouseListener());
                j.add(("Single Sited: "+v.getSingleSitedProbability()));
                j.addSeparator();
                j.add(("AbortProbability: "+v.getAbortProbability()));
                j.addSeparator();
                JMenu readonly = new JMenu("Read Only");
                //TODO: This doesn't show the read-only information right now
                for(int i : MarkovViewer.getAllPartitions()){
                    readonly.add(new JMenuItem(i+" "+v.getReadOnlyProbability(i)));
                }
                j.add(readonly);
                j.addSeparator();
                j.add(("Done: "+v.getReadOnlyProbability(0)));
                j.setLocation(e.getLocationOnScreen());
                j.setVisible(true);
                **/
                // FIXME(pavlo) v.printProbabilities(MarkovViewer.this.args.catalog_db);
            }
        }
    }
    public static class PopupMouseListener implements MouseListener{

        @Override
        public void mouseClicked(MouseEvent e) {
            if(e.getButton() == MouseEvent.BUTTON3){
                ((JPopupMenu)(e.getSource())).setVisible(false);
            }
        }

        @Override
        public void mouseEntered(MouseEvent e) {
            
        }

        @Override
        public void mouseExited(MouseEvent e) {
            
        }

        @Override
        public void mousePressed(MouseEvent e) {

        }

        @Override
        public void mouseReleased(MouseEvent e) {
            if(e.getButton() == MouseEvent.BUTTON1){
                ((JPopupMenu)(e.getSource())).setLocation(e.getLocationOnScreen());            
            }
        }
    }
    public static class VertexTransformer<T1, T2> implements
            Transformer<MarkovVertex, Paint> {

        @Override
        public Paint transform(MarkovVertex v) {
            Color ret = Color.GREEN;
            if(v.isReadOnly()){
                ret = Color.GRAY;
            }
            switch (v.getType()) {
            case START:
                ret = Color.BLACK;
                break;
            case COMMIT:
                ret = Color.red;
                break;
            case ABORT:
                ret = Color.BLUE;
                break;
            } // SWITCH
            return (ret);
        }

    }

    public static class VertexLabelTransformer<T1, T2> implements
            Transformer<MarkovVertex, String> {

        @Override
        public String transform(MarkovVertex arg0) {
            return arg0.getCatalogKey()+"\n"+arg0.getPartitions().toString();
        }

    }

    public static JFrame getFrame(Container cb) {
        return (JFrame) cb.getParent().getParent().getParent().getParent().getParent();
    }

    public static void launch(
            Map<Pair<Procedure, Integer>, MarkovGraph> partitionGraphs,
            Pair<Procedure,Integer> selection) {
        JFrame frame = new JFrame("Simple Graph View");
        JComboBox partcombo = MarkovViewer.makePartitionComboBox(partitionGraphs);
        MarkovGraph test_graph = partitionGraphs.get(selection);
        frame.setContentPane(MarkovViewer.getPanel(test_graph));
        frame.add(partcombo);
        partcombo.setSelectedItem(selection);
        frame.setBounds(50, 50, 2000, 500);
        frame.setVisible(true);
        
    }
    public static void visualize(final MarkovGraph g){
            JFrame frame = new JFrame("Simple Graph View");
            frame.setContentPane(MarkovViewer.getPanel(g));
            frame.setBounds(50, 50, 2000, 500);
            frame.setVisible(true);        
    }

}
