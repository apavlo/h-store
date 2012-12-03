package edu.brown.catalog.conflicts;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.GraphUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

/**
 * Dump out all conflicts
 * @author pavlo
 */
public abstract class ConflictSetTableDumper {
    private static final Logger LOG = Logger.getLogger(ConflictSetTableDumper.class);
    
    private static class ProcedureTable extends Procedure {
        private final Pair<Procedure, Table> pair;
        private ProcedureTable(Procedure proc, Table table) { 
            assert(proc != null);
            assert(table != null);
            this.pair = Pair.of(proc, table);
        }
        @Override
        public String toString() {
            return this.pair.toString();
        }
        @Override
        public int hashCode() {
            return this.pair.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return this.pair.equals(obj);
        }
        @Override
        public int compareTo(CatalogType o) {
            return this.pair.compareTo(((ProcedureTable)o).pair);
        }
        @SuppressWarnings("unchecked")
        @Override
        public <T extends CatalogType> T getParent() {
            return (T)this.pair.getFirst();
        }
    } // CLASS
    private static class Vertex extends AbstractVertex {
        private Vertex(Procedure proc, Table table) {
            super(new ProcedureTable(proc, table));
        }
        @Override
        public String toString() {
            return this.getCatalogItem().toString();
        }
        @Override
        public int hashCode() {
            return this.getCatalogItem().hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Vertex) {
                return this.getCatalogItem().equals(((Vertex)obj).getCatalogItem());
            }
            return false;
        }
    } // CLASS
    
    private static class Edge extends AbstractEdge {
        private final Pair<Statement, Statement> pair;
        private Edge(DumpGraph graph, Statement stmt0, Statement stmt1) {
            super(graph);
            assert(stmt0 != null);
            assert(stmt0 != null);
            this.pair = Pair.of(stmt0, stmt1);
        }
        @Override
        public String toString() {
            return this.pair.toString();
        }
        @Override
        public int hashCode() {
            return this.pair.hashCode();
        }
        @Override
        public boolean equals(Object obj) {
            return this.pair.equals(obj);
        }
    } // CLASS
    
    private static class DumpGraph extends AbstractDirectedGraph<Vertex, Edge> {
        private static final long serialVersionUID = 1;
        final Map<Pair<Procedure, Table>, Vertex> procTblXref = new HashMap<Pair<Procedure,Table>, Vertex>();
        final Map<Procedure, Set<Vertex>> procXref = new HashMap<Procedure, Set<Vertex>>();
        
        private DumpGraph(CatalogContext catalogContext) {
            super(catalogContext.database);
            for (Procedure proc : catalogContext.procedures) {
                if (proc.getName().equalsIgnoreCase("neworder") == false) continue;
                this.populateProcedure(proc);
            } // FOR
        }
        
        public Vertex getVertex(Procedure proc, Table tbl) {
            return this.procTblXref.get(Pair.of(proc, tbl));
        }
        @Override
        public boolean addVertex(Vertex vertex) {
            boolean ret = super.addVertex(vertex);
            if (ret) {
                LOG.debug("Storing xref entry for " + vertex);
                ProcedureTable pt = vertex.getCatalogItem();
                this.procTblXref.put(pt.pair, vertex);
                Set<Vertex> s = this.procXref.get(pt.pair.getFirst());
                if (s == null) {
                    s = new HashSet<Vertex>();
                    this.procXref.put(pt.pair.getFirst(), s);
                }
                s.add(vertex);
            }
            return (ret);
        }
        
        private void populateProcedure(Procedure proc0) {
            for (ConflictPair cp : ConflictSetUtil.getAllConflictPairs(proc0)) {
                Statement stmt0 = cp.getStatement0();
                Statement stmt1 = cp.getStatement1();
                Procedure proc1 = stmt1.getParent();
                LOG.debug(String.format("%s -> %s+%s", cp, stmt0, stmt1));
                
                for (Table tbl : CatalogUtil.getReferencedTables(stmt0)) {
                    Vertex v0 = this.getVertex(proc0, tbl);
                    if (v0 == null) {
                        v0 = new Vertex(proc0, tbl);
                        this.addVertex(v0);
                    }
                    Vertex v1 = this.getVertex(proc1, tbl);
                    if (v1 == null) {
                        v1 = new Vertex(proc1, tbl);
                        this.addVertex(v1);
                    }
                    Edge e = new Edge(this, stmt0, stmt1);
                    LOG.debug(String.format("%s ---%s---> %s\n", v0, e, v1));
                    this.addEdge(e, v0, v1);
                } // FOR
            } // FOR
        }
    } // CLASS
    
    
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
        DumpGraph graph = new DumpGraph(args.catalogContext);
        
        // If we have a Procedure to "focus" on, then we need to remove any edges
        // that don't involve that Procedure
        if (args.hasParam(ArgumentsParser.PARAM_CONFLICTS_FOCUS_PROCEDURE)) {
            String procName = args.getParam(ArgumentsParser.PARAM_CONFLICTS_FOCUS_PROCEDURE);
            Procedure catalog_proc = args.catalogContext.procedures.getIgnoreCase(procName);
            if (catalog_proc != null) {
                if (graph.procXref.containsKey(catalog_proc)) {
                    Vertex vertices[] = graph.procXref.get(catalog_proc).toArray(new Vertex[0]); 
                    LOG.debug("Remove Edges Without: " + Arrays.toString(vertices));
                    GraphUtil.removeEdgesWithoutVertex(graph, vertices);
                    GraphUtil.removeLoopEdges(graph);
                    GraphUtil.removeDisconnectedVertices(graph);
                }
            } else {
                LOG.warn("Invalid procedure name to focus '" + procName + "'");
            }
        }
        
        // Export!
        GraphvizExport<Vertex, Edge> gvx = new GraphvizExport<Vertex, Edge>(graph);
        gvx.setEdgeLabels(true);
        
        for (Vertex v : graph.getVertices()) {
            // Generate subgraphs based on procedure
            ProcedureTable procTbl = v.getCatalogItem();
            Procedure proc = procTbl.pair.getFirst();
            gvx.addSubgraph(proc.getName(), v);
        } // FOR
        
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