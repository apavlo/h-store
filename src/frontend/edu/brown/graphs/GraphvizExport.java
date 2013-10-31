package edu.brown.graphs;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.DependencyUtil;
import edu.brown.designer.DependencyGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.DependencyGraphGenerator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.StringUtil;

/**
 * Dump an IGraph to a Graphviz dot file
 * @author pavlo
 *
 * @param <V>
 * @param <E>
 */
public class GraphvizExport<V extends AbstractVertex, E extends AbstractEdge> {
    private static final Logger LOG = Logger.getLogger(GraphvizExport.class);
    
    // http://www.graphviz.org/doc/info/attrs.html
    public enum Attribute {
        BGCOLOR,
        COLOR,
        STYLE,
        FILLCOLOR,
        FONTSIZE,
        FONTCOLOR,
        FONTNAME,
        PACK,
        PENWIDTH,
        RATIO,
        SHAPE,
        SIZE,
        LABEL,
        NOJUSTIFY,
    };

    public static class AttributeValues extends HashMap<Attribute, String> {
        private static final long serialVersionUID = 1L;
        
        @Override
        public String put(Attribute key, String value) {
            LOG.debug(key + " => " + value);
            return super.put(key, value);
        }
        
        public String toString(String delimiter, Attribute...exclude) {
            Set<Attribute> exclude_set = new HashSet<Attribute>();
            if (exclude.length > 0) CollectionUtil.addAll(exclude_set, exclude);
            
            final String f = (delimiter.equals("\n") ? StringUtil.SPACER : "") + "%s=\"%s\"" + delimiter;
            StringBuilder sb = new StringBuilder();
            for (Attribute a : this.keySet()) {
                if (exclude_set.contains(a) == false) {
                    sb.append(String.format(f, a.name().toLowerCase(), this.get(a)));        
                }
            } // FOR
            return sb.toString();
        }
    } // END CLASS

    // The graph that we will export
    private final IGraph<V, E> graph;
    
    
    // Global Options
    private boolean edge_labels = true;
    private boolean allow_isolated = true;
    private boolean collapse_edges = false;
    private String graph_label = null;
    
    private final AttributeValues global_graph_attrs = new AttributeValues() {
        private static final long serialVersionUID = 1L;
        {
            this.put(Attribute.BGCOLOR, "white");
            this.put(Attribute.PACK, "true");
            this.put(Attribute.RATIO, "compress");
        }
    };
    private final AttributeValues global_vertex_attrs = new AttributeValues() {
        private static final long serialVersionUID = 1L;
        {
            this.put(Attribute.SHAPE, "rectangle");
            this.put(Attribute.FILLCOLOR, "grey");
            this.put(Attribute.COLOR, "black");
            this.put(Attribute.STYLE, "filled");
            this.put(Attribute.FONTSIZE, "11");
            this.put(Attribute.NOJUSTIFY, "true");
        }
    };
    private final AttributeValues global_edge_attrs = new AttributeValues() {
        private static final long serialVersionUID = 1L;
        {
            this.put(Attribute.FONTSIZE, "10");
        }
    };
    private final AttributeValues graph_label_attrs = new AttributeValues() {
        private static final long serialVersionUID = 1L;
        {
            this.put(Attribute.SHAPE, "ellipse");
            this.put(Attribute.FILLCOLOR, "black");
            this.put(Attribute.FONTCOLOR, "white");
            this.put(Attribute.STYLE, "filled");
            this.put(Attribute.FONTSIZE, "12");
        }
    };
    
    private final Map<V, AttributeValues> vertex_attrs = new HashMap<V, AttributeValues>(); 
    private final Map<E, AttributeValues> edge_attrs = new HashMap<E, AttributeValues>();
    private final Map<String, Set<V>> subgraphs = new HashMap<String, Set<V>>();
    
    /**
     * Constructor
     * @param graph
     */
    public GraphvizExport(IGraph<V, E> graph) {
        this.graph = graph;
    }
    
    @SuppressWarnings("unchecked")
    public void addSubgraph(String subgraph, V...vertices) {
        Set<V> subVertices = this.subgraphs.get(subgraph);
        if (subVertices == null) {
            subVertices = new HashSet<V>();
            this.subgraphs.put(subgraph, subVertices);
        }
        for (V v : vertices) subVertices.add(v);
    }
    
    public void setGlobalLabel(String contents) {
        this.graph_label = contents;
    }
    
    /**
     * If set to true, the Graphviz file will contain edge labels
     * @param edge_labels
     */
    public void setEdgeLabels(boolean edge_labels) {
        this.edge_labels = edge_labels;
    }

    /**
     * If set to true, the Graphviz file will include vertices that do not have any edges
     * @param allowIsolated
     */
    public void setAllowIsolated(boolean allowIsolated) {
        this.allow_isolated = allowIsolated;
    }
    
    /**
     * If set to true, multiple edges between the same pairs of vertices will be collapsed into a single edge
     * @param value
     */
    public void setCollapseEdges(boolean value) {
        this.collapse_edges = value;
    }

    public AttributeValues getGlobalGraphAttributes() {
        return this.global_graph_attrs;
    }
    public AttributeValues getGlobalVertexAttributes() {
        return this.global_vertex_attrs;
    }
    public AttributeValues getGlobalEdgeAttributes() {
        return this.global_edge_attrs;
    }

    // Custom Vertex Attributes
    
    public AttributeValues getAttributes(V vertex) {
        return (this.getAttributes(vertex, true));
    }
    
    private AttributeValues getAttributes(V vertex, boolean create_if_null) {
        AttributeValues av = this.vertex_attrs.get(vertex);
        if (av == null && create_if_null) {
            av = new AttributeValues();
            this.vertex_attrs.put(vertex, av);
        }
        return (av);
    }
    public boolean hasAttributes(V vertex) {
        return (this.vertex_attrs.containsKey(vertex));
    }
    
    // Custom Edge Attributes
    public AttributeValues getAttributes(E edge) {
        if (!this.edge_attrs.containsKey(edge)) {
            this.edge_attrs.put(edge, new AttributeValues());
        }
        return (this.edge_attrs.get(edge));
    }
    public boolean hasAttributes(E edge) {
        return (this.edge_attrs.containsKey(edge));
    }
    
    /**
     * Export a graph into the Graphviz Dotty format
     * @param <V>
     * @param <E>
     * @param graph
     * @param graphName
     * @return
     * @throws Exception
     */
    public String export(String graphName) throws Exception {
        LOG.debug("Exporting " + this.graph.getClass().getSimpleName() + " to Graphviz " +
                  "[vertices=" + this.graph.getVertexCount() + ",edges=" + this.graph.getEdgeCount() + "]");
        StringBuilder b = new StringBuilder();
        boolean digraph = (this.graph instanceof AbstractDirectedGraph<?, ?> || this.graph instanceof AbstractDirectedTree<?, ?>);
        
        // Start Graph
        String graph_type = (digraph ? "digraph" : "graph");
        String edge_type = " " + (digraph ? "->" : "--") + " ";
        b.append(graph_type + " " + graphName + " {\n");

        // Subgraphs
        Set<V> subgraph_vertices = new HashSet<V>();
        for (String subgraph : this.subgraphs.keySet()) {
            b.append(StringUtil.SPACER).append("subgraph ").append(subgraph).append(" {\n");
            for (V v : this.subgraphs.get(subgraph)) {
                AttributeValues av = this.getAttributes(v, false);
                this.writeVertex(b, v.toString(), av, StringUtil.SPACER + StringUtil.SPACER);
            } // FOR
            b.append(StringUtil.SPACER).append("}\n");
        } // FOR
        
        // Global Graph Attributes
        b.append(StringUtil.SPACER).append("graph [\n");
        b.append(StringUtil.addSpacers(this.getGlobalGraphAttributes().toString("\n")));
        b.append(StringUtil.SPACER).append("]\n");
        
        // Global Vertex Attributes
        b.append(StringUtil.SPACER).append("node [\n");
        b.append(StringUtil.addSpacers(this.getGlobalVertexAttributes().toString("\n")));
        b.append(StringUtil.SPACER).append("]\n");
        
        // Global Edge Attributes
        b.append(StringUtil.SPACER).append("edge [\n");
        b.append(StringUtil.addSpacers(this.getGlobalEdgeAttributes().toString("\n")));
        b.append(StringUtil.SPACER).append("]\n");

        // Edges
        Set<V> all_vertices = new HashSet<V>();
        Set<List<V>> redundant_edges = (this.collapse_edges ? new HashSet<List<V>>() : null);
        // FORMAT: <Vertex0> <edgetype> <Vertex1>
        final String edge_f = StringUtil.SPACER + "\"%s\" %s \"%s\" ";
        for (E edge : this.graph.getEdges()) {
            List<V> edge_vertices = new ArrayList<V>(graph.getIncidentVertices(edge));
            assert(edge_vertices.isEmpty() == false) : "No vertice for edge " + edge;
            all_vertices.addAll(edge_vertices);
            
            // Check whether we've seen an edge between these two guys before
            if (this.collapse_edges) {
                Collections.sort(edge_vertices);
                if (redundant_edges.contains(edge_vertices)) {
                    LOG.debug("Skipping redundant edge for " + edge_vertices);
                    continue;
                }
                redundant_edges.add(edge_vertices);
            }

            V v0 = edge_vertices.get(0);
            assert(v0 != null) : "Source vertex is null for edge " + edge;
            V v1 = edge_vertices.get(1);
            assert(v1 != null) : "Destination vertex is null for edge " + edge;
            
            // Print Edge
            b.append(String.format(edge_f, v0.toString(), edge_type, v1.toString())); 
            
            // Edge Attributes
            if (this.edge_labels || this.hasAttributes(edge)) {
                String label = null;
                AttributeValues av = this.getAttributes(edge);
                if (av.containsKey(Attribute.LABEL)) {
                    label = av.get(Attribute.LABEL);
                } else if (this.edge_labels) {
                    label = edge.toString();
                }
                b.append("[");
                if (this.hasAttributes(edge)) b.append(this.getAttributes(edge).toString(" ", Attribute.LABEL));
                if (label != null) b.append("label=\"").append(this.escapeLabel(label)).append("\"");
                b.append("] ");
            }
            b.append(";\n");
        } // FOR
        
        // Vertices
        b.append("\n");
        for (V v : this.graph.getVertices()) {
            // If this vertex wasn't a part of an edge and we don't allow for disconnected. then skip
            if (!all_vertices.contains(v) && !this.allow_isolated) continue;
            // If this vertex was a part of an edge but it doesn't have any custom attributes, then skip
            if (all_vertices.contains(v) && !this.hasAttributes(v)) continue;
            // If it's already in a subgraph, then skip it
            if (subgraph_vertices.contains(v)) continue;
            
            AttributeValues av = this.getAttributes(v, false);
            this.writeVertex(b, v.toString(), av, StringUtil.SPACER);
        } // FOR
        
        // Global Graph Label
        if (this.graph_label != null) {
            this.graph_label_attrs.put(Attribute.LABEL, this.graph_label);
            this.writeVertex(b, "__global__", this.graph_label_attrs, StringUtil.SPACER);
        }
        
        // Close graph
        b.append("}\n");
        
        return (b.toString());
    }

    private void writeVertex(StringBuilder b, String id, AttributeValues av, String spacer) {
        b.append(String.format("%s\"%s\"", spacer, id));
        
        // Vertex Attributes
        if (av != null) {
            String label = null;
            if (av.containsKey(Attribute.LABEL)) {
                label = av.get(Attribute.LABEL);
            }
            b.append("[");
            b.append(av.toString(" ", Attribute.LABEL));
            if (label != null) b.append("label=\"").append(this.escapeLabel(label)).append("\"");
            b.append("] ");
        }
        b.append(" ;\n");
    }
    
    private String escapeLabel(String label) {
        return (label.replace("\n", "\\n"));
    }
    
    /**
     * Highlights the given edge path (with vertices) using the given color
     * @param path
     * @param highlight_color
     */
    public void highlightPath(List<E> path, String highlight_color) {
        Integer highlight_width = 4;
        for (E e : path) {
            this.getAttributes(e).put(Attribute.COLOR, highlight_color);
            this.getAttributes(e).put(Attribute.PENWIDTH, Integer.toString(highlight_width * 2));
            this.getAttributes(e).put(Attribute.STYLE, "bold");
            
            for (V v : this.graph.getIncidentVertices(e)) {
                this.getAttributes(v).put(Attribute.COLOR, highlight_color);
                this.getAttributes(v).put(Attribute.PENWIDTH, highlight_width.toString());
            } // FOR
        } // FOR
    }
    
    /**
     * Convenience method to write the GraphvizExport handle to a file the temporary directory
     * @param catalog_obj
     * @return
     * @throws Exception
     */
    public File writeToTempFile(CatalogType catalog_obj) {
        return (this.writeToTempFile(catalog_obj.fullName(), null));
    }
    public File writeToTempFile(CatalogType catalog_obj, int i) {
        return (this.writeToTempFile(catalog_obj.fullName(), Integer.toString(i)));
    }
    public File writeToTempFile(CatalogType catalog_obj, String suffix) {
        return (this.writeToTempFile(catalog_obj.fullName(), suffix));
    }
    public File writeToTempFile(String name) {
        return (this.writeToTempFile(name, null));
    }
    public File writeToTempFile() {
        File f = FileUtil.getTempFile("dot", false);
        String name = f.getName().replace(".dot", "");
        try {
            FileUtil.writeStringToFile(f, this.export(name));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return (f);
    }
    
    /**
     * Export the graph to a temp file
     * @param name
     * @param suffix
     * @return
     * @throws Exception
     */
    public File writeToTempFile(String name, String suffix) {
        if (suffix != null && suffix.length() > 0) suffix = "-" + suffix;
        else if (suffix == null) suffix = "";
        String filename = String.format("/tmp/%s%s.dot", name, suffix);
        try {
            return (FileUtil.writeStringToFile(filename, this.export(name)));
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Export a graph into the Graphviz Dotty format
     * @param <V>
     * @param <E>
     * @param graph
     * @param graphName
     * @return
     * @throws Exception
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> String export(IGraph<V, E> graph, String graphName) {
        GraphvizExport<V, E> gv = new GraphvizExport<V, E>(graph);
        try {
            return gv.export(graphName);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        DependencyGraph dgraph = DependencyGraphGenerator.generate(args.catalogContext);
        GraphUtil.removeDuplicateEdges(dgraph);
        
        // Any optional parameters are tables we should ignore
        // To do that we need to just remove them from the DependencyGraph
        for (String opt : args.getOptParams()) {
            for (String tableName : opt.split(",")) {
                Table catalog_tbl = args.catalog_db.getTables().getIgnoreCase(tableName);
                if (catalog_tbl == null) {
                    LOG.warn("Unknown table '" + tableName + "'");
                    continue;
                }
                DesignerVertex v = dgraph.getVertex(catalog_tbl);
                assert(v != null) : "Failed to get vertex for " + catalog_tbl;
                dgraph.removeVertex(v);
            } // FOR
        } // FOR
        
        GraphvizExport<DesignerVertex, DesignerEdge> gvx = new GraphvizExport<DesignerVertex, DesignerEdge>(dgraph);
        
        // Enable full edge labels
        if (args.getBooleanParam(ArgumentsParser.PARAM_CATALOG_LABELS, false)) {
            gvx.setEdgeLabels(true);
            DependencyUtil dependUtil = DependencyUtil.singleton(args.catalog_db);
            for (DesignerEdge e : dgraph.getEdges()) {
                Table tbl0 = dgraph.getSource(e).getCatalogItem();
                Table tbl1 = dgraph.getDest(e).getCatalogItem();
                String label = "";
                for (Column col0 : CatalogUtil.getSortedCatalogItems(tbl0.getColumns(), "index")) {
                    for (Column col1 : dependUtil.getDescendants(col0)) {
                        if (col1.getParent().equals(tbl1) == false) continue;
                        if (label.isEmpty() == false) label += "\n";
                        label += col0.getName() +
                                 StringUtil.UNICODE_RIGHT_ARROW +
                                 col1.getName();
                    } // FOR
                } // FOR
                
                AttributeValues attrs = gvx.getAttributes(e);
                attrs.put(Attribute.LABEL, label);
            } // FOR
        } else {
            gvx.setEdgeLabels(false);
        }
        
        String graphviz = gvx.export(args.catalog_type.name());
        if (!graphviz.isEmpty()) {
            File path = new File(args.catalog_type.name().toLowerCase() + ".dot");
            FileUtil.writeStringToFile(path, graphviz);
            System.out.println("Wrote contents to '" + path.getAbsolutePath() + "'");
        } else {
            System.err.println("ERROR: Failed to generate graphviz data");
            System.exit(1);
        }
    }
}
