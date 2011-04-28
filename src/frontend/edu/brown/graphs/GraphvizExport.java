package edu.brown.graphs;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;

import edu.brown.designer.DependencyGraph;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
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
    
    private final Map<V, AttributeValues> vertex_attrs = new HashMap<V, AttributeValues>(); 
    private final Map<E, AttributeValues> edge_attrs = new HashMap<E, AttributeValues>();
    
    
    /**
     * Constructor
     * @param graph
     */
    public GraphvizExport(IGraph<V, E> graph) {
        this.graph = graph;
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
        allow_isolated = allowIsolated;
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
        if (!this.vertex_attrs.containsKey(vertex)) {
            this.vertex_attrs.put(vertex, new AttributeValues());
        }
        return (this.vertex_attrs.get(vertex));
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
     * @param name
     * @return
     * @throws Exception
     */
    public String export(String name) throws Exception {
        LOG.debug("Exporting " + this.graph.getClass().getSimpleName() + " to Graphviz " +
                  "[vertices=" + this.graph.getVertexCount() + ",edges=" + this.graph.getEdgeCount() + "]");
        StringBuilder b = new StringBuilder();
        boolean digraph = (this.graph instanceof AbstractDirectedGraph<?, ?> || this.graph instanceof AbstractDirectedTree<?, ?>);
        
        // Start Graph
        String graph_type = (digraph ? "digraph" : "graph");
        String edge_type = " " + (digraph ? "->" : "--") + " ";
        b.append(graph_type + " " + name + " {\n");

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
        // FORMAT: <Vertex0> <edgetype> <Vertex1>
        final String edge_f = StringUtil.SPACER + "\"%s\" %s \"%s\" ";
        for (E edge : graph.getEdges()) {
            List<V> edge_vertices = new ArrayList<V>(graph.getIncidentVertices(edge));
            assert(edge_vertices.isEmpty() == false) : "No vertice for edge " + edge;
            all_vertices.addAll(edge_vertices);

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
        String add = "\n";
        for (V v : graph.getVertices()) {
            // If this vertex wasn't a part of an edge and we don't allow for disconnected. then skip
            if (!all_vertices.contains(v) && !this.allow_isolated) continue;
            // If this vertex was a part of an edge but it doesn't have any custom attributes, then skip
            if (all_vertices.contains(v) && !this.hasAttributes(v)) continue;
            
            b.append(add).append(StringUtil.SPACER).append('"').append(v.toString()).append("\"");
            
            // Vertex Attributes
            if (this.hasAttributes(v)) {
                AttributeValues vertex_attrs = this.getAttributes(v);
                b.append(" [" ).append(vertex_attrs.toString(" ")).append("]");
            }
            b.append(" ;\n");
            add = "";
        } // FOR
        
        // Close graph
        b.append("}\n");
        
        return (b.toString());
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
    public String writeToTempFile(CatalogType catalog_obj) throws Exception {
        return (this.writeToTempFile(catalog_obj.fullName(), null));
    }
    public String writeToTempFile(CatalogType catalog_obj, int i) throws Exception {
        return (this.writeToTempFile(catalog_obj.fullName(), Integer.toString(i)));
    }
    public String writeToTempFile(CatalogType catalog_obj, String suffix) throws Exception {
        return (this.writeToTempFile(catalog_obj.fullName(), suffix));
    }
    public String writeToTempFile(String name) throws Exception {
        return (this.writeToTempFile(name, null));
    }
    
    /**
     * Export the graph to a temp file
     * @param name
     * @param suffix
     * @return
     * @throws Exception
     */
    public String writeToTempFile(String name, String suffix) throws Exception {
        if (suffix != null && suffix.length() > 0) suffix = "-" + suffix;
        else if (suffix == null) suffix = "";
        String filename = String.format("/tmp/%s%s.dot", name, suffix);
        FileUtil.writeStringToFile(filename, this.export(name));
        return (filename);
    }
    
    /**
     * Export a graph into the Graphviz Dotty format
     * @param <V>
     * @param <E>
     * @param graph
     * @param name
     * @return
     * @throws Exception
     */
    public static <V extends AbstractVertex, E extends AbstractEdge> String export(IGraph<V, E> graph, String name) {
        GraphvizExport<V, E> gv = new GraphvizExport<V, E>(graph);
        try {
            return gv.export(name);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG);
        
        DependencyGraph dgraph = DependencyGraphGenerator.generate(args.catalog_db);
        GraphvizExport<Vertex, Edge> gvx = new GraphvizExport<Vertex, Edge>(dgraph);
        gvx.setEdgeLabels(false);
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
