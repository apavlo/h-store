package edu.brown.catalog.conflicts;

import java.io.File;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.conflicts.ConflictGraph.ConflictEdge;
import edu.brown.catalog.conflicts.ConflictGraph.ConflictVertex;
import edu.brown.graphs.GraphUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

public abstract class ConflictGraphExport {
    private static final Logger LOG = Logger.getLogger(ConflictGraphExport.class);

    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
                ArgumentsParser.PARAM_CATALOG
        );
        
        ConflictSetCalculator calculator = new ConflictSetCalculator(args.catalog);
        
        // Procedures to exclude in ConflictGraph
        if (args.hasParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_PROCEDURES)) {
            String param = args.getParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_PROCEDURES);
            for (String procName : param.split(",")) {
                Procedure catalog_proc = args.catalogContext.procedures.getIgnoreCase(procName);
                if (catalog_proc != null) {
                    calculator.ignoreProcedure(catalog_proc);
                } else {
                    LOG.warn("Invalid procedure name to exclude '" + procName + "'");
                }
            } // FOR
        }
        
        // Statements to exclude in ConflictGraph
        if (args.hasParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_STATEMENTS)) {
            String param = args.getParam(ArgumentsParser.PARAM_CONFLICTS_EXCLUDE_STATEMENTS);
            for (String name : param.split(",")) {
                String splits[] = name.split("\\.");
                if (splits.length != 2) {
                    LOG.warn("Invalid procedure name to exclude '" + name + "': " + Arrays.toString(splits));
                    continue;
                }
                Procedure catalog_proc = args.catalogContext.procedures.getIgnoreCase(splits[0]);
                if (catalog_proc == null) {
                    LOG.warn("Invalid procedure name to exclude '" + name + "'");
                    continue;
                }
                    
                Statement catalog_stmt = catalog_proc.getStatements().getIgnoreCase(splits[1]);
                if (catalog_stmt != null) {
                    calculator.ignoreStatement(catalog_stmt);
                } else {
                    LOG.warn("Invalid statement name to exclude '" + name + "'");
                }
            } // FOR
        }
        
        calculator.process();
        ConflictGraph graph = new ConflictGraph(args.catalog_db);
        
        // If we have a Procedure to "focus" on, then we need to remove any edges
        // that don't involve that Procedure
        if (args.hasParam(ArgumentsParser.PARAM_CONFLICTS_FOCUS_PROCEDURE)) {
            String procName = args.getParam(ArgumentsParser.PARAM_CONFLICTS_FOCUS_PROCEDURE);
            Procedure catalog_proc = args.catalogContext.procedures.getIgnoreCase(procName);
            if (catalog_proc != null) {
                ConflictVertex v = graph.getVertex(catalog_proc);
                assert(v != null);
                GraphUtil.removeEdgesWithoutVertex(graph, v);
                GraphUtil.removeDisconnectedVertices(graph);
            } else {
                LOG.warn("Invalid procedure name to focus '" + procName + "'");
            }
        }
        
        // Export!
        GraphvizExport<ConflictVertex, ConflictEdge> gvx = new GraphvizExport<ConflictVertex, ConflictEdge>(graph);
        gvx.setEdgeLabels(true);
        String graphviz = gvx.export(args.catalog_type.name());
        if (!graphviz.isEmpty()) {
            String output = args.getOptParam(0);
            if (output == null) {
                output = args.catalog_type.name().toLowerCase() + "-conflicts.dot";
            }
            File path = new File(output);
            FileUtil.writeStringToFile(path, graphviz);
            System.out.println("Wrote contents to '" + path.getAbsolutePath() + "'");
        } else {
            System.err.println("ERROR: Failed to generate graphviz data");
            System.exit(1);
        }
    }

}
