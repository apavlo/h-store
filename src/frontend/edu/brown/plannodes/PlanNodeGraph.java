package edu.brown.plannodes;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.plannodes.AbstractPlanNode;

import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.ArgumentsParser;
import edu.uci.ics.jung.graph.DelegateForest;

public class PlanNodeGraph extends DelegateForest<AbstractPlanNode, PlanNodeGraph.Edge> {
    private static final long serialVersionUID = 1L;

    public static class Edge {
        // Nothing to see, nothing to do...

        @Override
        public String toString() {
            return "";
        }
    }

    private final AbstractPlanNode root;

    public PlanNodeGraph(AbstractPlanNode root) {
        this.root = root;
        this.init();
    }

    private void init() {
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(AbstractPlanNode element) {
                AbstractPlanNode parent = this.getPrevious();
                if (parent == null) {
                    PlanNodeGraph.this.addVertex(element);
                } else {
                    PlanNodeGraph.this.addEdge(new Edge(), parent, element);
                }
            }
        }.traverse(this.root);
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);

        String proc_name = args.getOptParam(0);
        String stmt_name = args.getOptParam(1);

        Procedure catalog_proc = args.catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert (catalog_proc != null) : "Invalid Procedure Name: " + proc_name;

        Statement catalog_stmt = catalog_proc.getStatements().getIgnoreCase(stmt_name);
        assert (catalog_stmt != null) : "Invalid Statement Name: " + proc_name + "." + stmt_name;

        AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
        PlanNodeGraph graph = new PlanNodeGraph(root);

        GraphVisualizationPanel.createFrame(graph).setVisible(true);
    }
}
