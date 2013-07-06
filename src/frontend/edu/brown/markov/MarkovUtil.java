package edu.brown.markov;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.GraphvizExport;
import edu.brown.graphs.GraphvizExport.Attribute;
import edu.brown.graphs.GraphvizExport.AttributeValues;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;

/**
 * MarkovUtil class is here to help with comparisons and help cut down on the clutter in 
 * MarkovGraph.
 * 
 * For convenience use stop_stmt,start_stmt, and abort_stmt to get the vertices you would like
 * 
 * Constants are defined here as well. Define FIRST_PARTITION and NUM_PARTITIONS for your install
 * 
 * @author svelagap
 * @author pavlo
 */
public abstract class MarkovUtil {
    private static final Logger LOG = Logger.getLogger(MarkovUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * 
     */
    public static final Integer GLOBAL_MARKOV_CONTAINER_ID = -1;
    
    
    /**
     * Wrapper class for our special "marker" vertices
     */
    public static class StatementWrapper extends Statement {
        private final Catalog catalog;
        private final MarkovVertex.Type type;
        private String id;
        private final Procedure parent = new Procedure() {
            public String getName() {
                return ("markov");
            };
            public Catalog getCatalog() {
                return (StatementWrapper.this.catalog);
            };
        };

        public StatementWrapper(Catalog catalog, MarkovVertex.Type type) {
            this.catalog = catalog;
            this.type = type;
            this.id="";
            this.setReadonly(true);
        }
        public StatementWrapper(Catalog catalog, MarkovVertex.Type type, boolean readonly, String id) {
            this.catalog = catalog;
            this.type = type;
            this.id = id;
            this.setReadonly(readonly);
        }
        @Override
        public Catalog getCatalog() {
            return (this.catalog);
        }
        @Override
        public String getTypeName() {
            return ("--" + this.type.toString() + id +"--");
        }
        @Override
        public String fullName() {
            return this.getName();
        }
        @SuppressWarnings("unchecked")
        @Override
        public CatalogType getParent() {
            return (this.parent);
        }
    } // END CLASS

    /**
     * Return the Statement object representing the special Vertex type
     * @param catalog_db TODO
     * @param type
     * @return
     */
    public static Statement getSpecialStatement(Database catalog_db, MarkovVertex.Type type) {
        Map<MarkovVertex.Type, Statement> cache = CACHE_specialStatements.get(catalog_db);
        if (cache == null) {
            cache = new HashMap<MarkovVertex.Type, Statement>();
            CACHE_specialStatements.put(catalog_db, cache);
        }
        
        Statement catalog_stmt = cache.get(type);
        if (catalog_stmt == null) {
            catalog_stmt = new StatementWrapper(catalog_db.getCatalog(), type);
            cache.put(type, catalog_stmt);
        }
        return (catalog_stmt);
    }
    private final static Map<Database, Map<MarkovVertex.Type, Statement>> CACHE_specialStatements = new HashMap<Database, Map<MarkovVertex.Type, Statement>>();
    
    /**
     * For the given Vertex type, return a unique Vertex instance
     * @param type
     * @return
     */
    public static MarkovVertex getSpecialVertex(Database catalog_db, MarkovVertex.Type type) {
        Statement catalog_stmt = MarkovUtil.getSpecialStatement(catalog_db, type);
        assert(catalog_stmt != null);

        MarkovVertex v = new MarkovVertex(catalog_stmt, type);
        
        if (type == MarkovVertex.Type.ABORT) {
            v.setAbortProbability(1.0f);
        }
        if (type != MarkovVertex.Type.START) {
            @SuppressWarnings("deprecation")
            PartitionSet partitions = CatalogUtil.getAllPartitionIds(catalog_db);
            for (int partition : partitions.values()) {
                v.setDoneProbability(partition, 1.0f);
            } // FOR
        }
        assert(v != null);
        return (v);
    }
    
    /**
     * Return a new instance of the special start vertex
     * @return
     */
    public static MarkovVertex getStartVertex(CatalogContext catalogContext) {
        return (MarkovUtil.getSpecialVertex(catalogContext.database, MarkovVertex.Type.START));
    }
    /**
     * A stop vertex has done probability of 1.0 at all partitions
     * @return a stop Vertex
     */
    public static MarkovVertex getCommitVertex(CatalogContext catalogContext) {
        return (MarkovUtil.getSpecialVertex(catalogContext.database, MarkovVertex.Type.COMMIT));
    }
    /**
     * An aborted vertex has an abort and done probability of 1.0 at all partitions
     * @return an abort Vertex
     */
    public static MarkovVertex getAbortVertex(CatalogContext catalogContext) {
        return (MarkovUtil.getSpecialVertex(catalogContext.database, MarkovVertex.Type.ABORT));
    }
    
    /**
     * Give a blank graph to fill in for each partition for the given procedure. Called by 
     * anyone who wants to create MarkovGraphs
     * @param numPartitions the number of partitions/sites in this installation
     * @param catalog_proc - the given procedure
     * @return mapping of Partition to MarkovGraph
     */
    public static Map<Integer, MarkovGraph> getBlankPartitionGraphs(CatalogContext catalogContext, Procedure catalog_proc) {
        HashMap<Integer, MarkovGraph> graphs = new HashMap<Integer, MarkovGraph>();
        for (int partition : catalogContext.getAllPartitionIds().values()) {
            MarkovGraph g = new MarkovGraph(catalog_proc);
            g.addVertex(MarkovUtil.getStartVertex(catalogContext));
            g.addVertex(MarkovUtil.getAbortVertex(catalogContext));
            g.addVertex(MarkovUtil.getCommitVertex(catalogContext));
            graphs.put(partition, g);
        } // FOR
        return (graphs);
    }

    /**
     * Returns a unique set of Statement catalog objects for a given collection of vertices
     * @param vertices
     * @return
     */
    public static Set<Statement> getStatements(Collection<MarkovVertex> vertices) {
        Set<Statement> ret = new HashSet<Statement>();
        for (MarkovVertex v : vertices) {
            Statement catalog_stmt = v.getCatalogItem();
            assert(catalog_stmt != null);
            ret.add(catalog_stmt);
        } // FOR
        return (ret);
    }


    /**
     * Load a list of Markov objects from a serialized for a particular id
     * @param catalog_db
     * @param input_path
     * @param id
     * @return
     * @throws Exception
     */
    public static MarkovGraphsContainer loadId(CatalogContext catalogContext, File input_path, int id) throws Exception {
        Set<Integer> idset = (Set<Integer>)CollectionUtil.addAll(new HashSet<Integer>(), Integer.valueOf(id));
        Map<Integer, MarkovGraphsContainer> markovs = MarkovGraphsContainerUtil.load(catalogContext, input_path, null, idset);
        assert(markovs.size() == 1);
        assert(markovs.containsKey(id));
        return (markovs.get(id));
    }
    
    public static Map<Integer, MarkovGraphsContainer> load(CatalogContext catalogContext, File input_path) throws Exception {
        return (MarkovGraphsContainerUtil.load(catalogContext, input_path, null, null));
    }
    
    /**
     * Return a GraphvizExport handle for the given MarkovGraph.
     * This will highlight all of the special vertices
     * @param markov
     * @param use_full_output - Whether to use the full debug information for vertex labels
     * @param path - A path to highlight (can be null)
     * @return
     * @throws Exception
     */
    public static GraphvizExport<MarkovVertex, MarkovEdge> exportGraphviz(MarkovGraph markov, boolean use_full_output, List<MarkovEdge> path) {
        return MarkovUtil.exportGraphviz(markov, use_full_output, false, false, path);
    }
    
    /**
     * Return a GraphvizExport handle for the given MarkovGraph.
     * This will highlight all of the special vertices
     * @param markov
     * @param use_full_output - Whether to use the full debug information for vertex labels
     * @param use_vldb_output - Whether to use labels for paper figures
     * @param high_invalid - Whether to highlight invalid edges/vertices
     * @param path - A path to highlight (can be null)
     * @return
     * @throws Exception
     */
    public static GraphvizExport<MarkovVertex, MarkovEdge> exportGraphviz(MarkovGraph markov, boolean use_full_output, boolean use_vldb_output, boolean highlight_invalid, List<MarkovEdge> path) {
        GraphvizExport<MarkovVertex, MarkovEdge> graphviz = new GraphvizExport<MarkovVertex, MarkovEdge>(markov);
        graphviz.setEdgeLabels(true);
        graphviz.getGlobalGraphAttributes().put(Attribute.PACK, "true");
        graphviz.getGlobalVertexAttributes().put(Attribute.FONTNAME, "Courier");
        graphviz.setGlobalLabel(String.format("MarkovGraph %s - GraphId:%d", markov.getProcedure().getName(), markov.getGraphId()));
        
        MarkovVertex v = markov.getStartVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "blue");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");

        v = markov.getCommitVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "darkgreen");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");
        
        v = markov.getAbortVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "firebrick4");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");

        // Highlight Path
        if (path != null) graphviz.highlightPath(path, "red");
        
        if (use_vldb_output || use_full_output) {
            final String empty_set = "\u2205";
            
            if (use_full_output) {
                for (MarkovEdge e : markov.getEdges()) {
                    AttributeValues av = graphviz.getAttributes(e);
                    av.put(Attribute.LABEL, e.toString(true));
                } // FOR
            }
            
            for (MarkovVertex v0 : markov.getVertices()) {
                AttributeValues av = graphviz.getAttributes(v0);
                
                if (highlight_invalid && v0.isValid(markov) == false) {
                    av.put(Attribute.FILLCOLOR, "red");
                    if (debug.val) LOG.warn("Highlighting " + v0 + " as invalid");
                }
                
                String label = "";
            
                // VLDB Figure Output
                if (use_vldb_output) {
                    if (v0.isAbortVertex()) {
                        label = "abort";
                    } else if (v0.isStartVertex()) {
                        label = "begin";
                    } else if (v0.isCommitVertex()) {
                        label = "commit";
                    } else {
                        String name = v0.getCatalogItem().getName();
                        name = StringUtil.title(name.replace("_", " "), true).replace(" ", "");
                        
                        label = name + "\n";
                        label += "Counter: " + v0.getQueryCounter() + "\n";
                        
                        label += "Partitions: ";
                        if (v0.getPartitions().isEmpty()) {
                            label += empty_set;
                        } else {
                            label += "{ ";
                            String add = "";
                            for (Integer p : v0.getPartitions()) {
                                label += add + p;
                                add = ", ";
                            } // FOR
                            label += " }";
                        }
                        label += "\n";
                        
                        label += "Previous: ";
                        if (v0.getPastPartitions().isEmpty()) {
                            label += empty_set;
                        } else {
                            label += "{ ";
                            String add = "";
                            for (Integer p : v0.getPastPartitions()) {
                                label += add + p;
                                add = ", ";
                            } // FOR
                            label += " }";
                        }
                    }
                } else {
                    label = v0.debug();
                }
                av.put(Attribute.LABEL, label);
            } // FOR
        }
        
        return (graphviz);
    }
    
    /**
     * Get the set of all partitions touched (either read or write) by the list of vertices
     * @param path
     * @return
     */
    public static PartitionSet getTouchedPartitions(List<MarkovVertex> path) {
        PartitionSet partitions = new PartitionSet();
        for (MarkovVertex v : path) {
            partitions.addAll(v.getPartitions());
        } // FOR
        return (partitions);
    }
    
    /**
     * Get the read/write partition counts for the given path
     * @param path
     * @return Set<ReadPartitions>, Set<WritePartitions>
     */
    public static Pair<PartitionSet, PartitionSet> getReadWritePartitions(List<MarkovVertex> path) {
        PartitionSet read_p = new PartitionSet();
        PartitionSet write_p = new PartitionSet();
        MarkovUtil.getReadWritePartitions(path, read_p, write_p);
        return (Pair.of(read_p, write_p));
    }

    /**
     * Get the read/write partition counts for the given path
     * @param path
     * @param read_p
     * @param write_p
     */
    public static void getReadWritePartitions(List<MarkovVertex> path, PartitionSet read_p, PartitionSet write_p) {
        for (MarkovVertex v : path) {
            if (v.isQueryVertex() == false) continue;
            if (trace.val)
                LOG.trace(String.format("%s - R:%s / W:%s", v, read_p, write_p));
            
            Statement catalog_stmt = v.getCatalogItem();
            QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
            switch (qtype) {
                case SELECT:
                    read_p.addAll(v.getPartitions());
                    break;
                case INSERT:
                case UPDATE:
                case DELETE:
                    write_p.addAll(v.getPartitions());
                    break;
                default:
                    assert(false) : "Invalid QueryType: " + qtype;
            } // SWITCH
        } // FOR
    }
}
