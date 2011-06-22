package edu.brown.markov;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONStringer;
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
import edu.brown.hashing.AbstractHasher;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.ProcedureNameFilter;

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

    /**
     * The value to use to indicate that a probability is null
     */
    public static final float NULL_MARKER = -1.0f;
    
    /**
     * 
     */
    public static final Integer GLOBAL_MARKOV_CONTAINER_ID = -1;
    
    
    /**
     * Wrapper class for our special "marker" vertices
     */
    public static class StatementWrapper extends Statement {
        private final Catalog catalog;
        private final Vertex.Type type;
        private String id;
        private final Procedure parent = new Procedure() {
            public String getName() {
                return ("markov");
            };
            public Catalog getCatalog() {
                return (StatementWrapper.this.catalog);
            };
        };

        public StatementWrapper(Catalog catalog, Vertex.Type type) {
            this.catalog = catalog;
            this.type = type;
            this.id="";
            this.setReadonly(true);
        }
        public StatementWrapper(Catalog catalog, Vertex.Type type, boolean readonly, String id) {
            this.catalog = catalog;
            this.type = type;
            this.id = id;
            this.setReadonly(readonly);
        }
        @Override
        public Catalog getCatalog() {
            return (this.catalog);
        }
        public String getName() {
            return ("--" + this.type.toString() + id +"--");
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
    public static Statement getSpecialStatement(Database catalog_db, Vertex.Type type) {
        Map<Vertex.Type, Statement> cache = CACHE_specialStatements.get(catalog_db);
        if (cache == null) {
            cache = new HashMap<Vertex.Type, Statement>();
            CACHE_specialStatements.put(catalog_db, cache);
        }
        
        Statement catalog_stmt = cache.get(type);
        if (catalog_stmt == null) {
            catalog_stmt = new StatementWrapper(catalog_db.getCatalog(), type);
            cache.put(type, catalog_stmt);
        }
        return (catalog_stmt);
    }
    private final static Map<Database, Map<Vertex.Type, Statement>> CACHE_specialStatements = new HashMap<Database, Map<Vertex.Type, Statement>>();
    
    /**
     * For the given Vertex type, return a unique Vertex instance
     * @param type
     * @return
     */
    public static Vertex getSpecialVertex(Database catalog_db, Vertex.Type type) {
        Statement catalog_stmt = MarkovUtil.getSpecialStatement(catalog_db, type);
        assert(catalog_stmt != null);

        Vertex v = new Vertex(catalog_stmt, type);
        
        if (type == Vertex.Type.ABORT) {
            v.setAbortProbability(1.0f);
        }
        if (type != Vertex.Type.START) {
            for (int i : CatalogUtil.getAllPartitionIds(catalog_db)) {
                v.setDoneProbability(i, 1.0f);
            } // FOR
        }
        assert(v != null);
        return (v);
    }
    
    /**
     * Return a new instance of the special start vertex
     * @return
     */
    public static Vertex getStartVertex(Database catalog_db) {
        return (MarkovUtil.getSpecialVertex(catalog_db, Vertex.Type.START));
    }
    /**
     * A stop vertex has done probability of 1.0 at all partitions
     * @return a stop Vertex
     */
    public static Vertex getCommitVertex(Database catalog_db) {
        return (MarkovUtil.getSpecialVertex(catalog_db, Vertex.Type.COMMIT));
    }
    /**
     * An aborted vertex has an abort and done probability of 1.0 at all partitions
     * @return an abort Vertex
     */
    public static Vertex getAbortVertex(Database catalog_db) {
        return (MarkovUtil.getSpecialVertex(catalog_db, Vertex.Type.ABORT));
    }
    
    /**
     * Give a blank graph to fill in for each partition for the given procedure. Called by 
     * anyone who wants to create MarkovGraphs
     * @param numPartitions the number of partitions/sites in this installation
     * @param catalog_proc - the given procedure
     * @return mapping of Partition to MarkovGraph
     */
    public static Map<Integer, MarkovGraph> getBlankPartitionGraphs(Procedure catalog_proc) {
        HashMap<Integer, MarkovGraph> graphs = new HashMap<Integer, MarkovGraph>();
        Database catalog_db = CatalogUtil.getDatabase(catalog_proc);
        for (int i : CatalogUtil.getAllPartitionIds(catalog_db)) {
            MarkovGraph g = new MarkovGraph(catalog_proc);
            g.addVertex(MarkovUtil.getStartVertex(catalog_db));
            g.addVertex(MarkovUtil.getAbortVertex(catalog_db));
            g.addVertex(MarkovUtil.getCommitVertex(catalog_db));
            graphs.put(i, g);
        } // FOR
        return (graphs);
    }

    /**
     * Returns a unique set of Statement catalog objects for a given collection of vertices
     * @param vertices
     * @return
     */
    public static Set<Statement> getStatements(Collection<Vertex> vertices) {
        Set<Statement> ret = new HashSet<Statement>();
        for (Vertex v : vertices) {
            Statement catalog_stmt = v.getCatalogItem();
            assert(catalog_stmt != null);
            ret.add(catalog_stmt);
        } // FOR
        return (ret);
    }

    /**
     * Construct all of the Markov graphs for a workload+catalog split by the txn's base partition
     * @param catalog_db
     * @param workload
     * @param p_estimator
     * @return
     */
    public static MarkovGraphsContainer createBasePartitionGraphs(final Database catalog_db, final Workload workload, final PartitionEstimator p_estimator) {
        assert(workload != null);
        assert(p_estimator != null);
        
        final List<Integer> partitions = CatalogUtil.getAllPartitionIds(catalog_db);
        final MarkovGraphsContainer graphs_per_partition = new MarkovGraphsContainer();
        
        List<Runnable> runnables = new ArrayList<Runnable>();
        for (final Procedure catalog_proc : workload.getProcedures(catalog_db)) {
            if (catalog_proc.getSystemproc()) continue;
            runnables.add(new Runnable() {
                @Override
                public void run() {
                    LOG.info("Creating MarkovGraphs for " + catalog_proc);
                    Map<Integer, MarkovGraph> gs = MarkovUtil.createBasePartitionGraphsForProcedure(catalog_proc, workload, p_estimator);
                    for (int partition : gs.keySet()) {
                        assert(partitions.contains(partition));
                        MarkovGraph markov = gs.get(partition);
                        graphs_per_partition.put(partition, markov);
                    } // FOR
                }
            });
        } // FOR
        LOG.info(String.format("Waiting for %d MarkovGraphs to get populated", runnables.size()));
        ThreadUtil.runGlobalPool(runnables);
        return (graphs_per_partition);
    }
    
    /**
     * Here we create a MarkovGraph for each partition for each procedure The
     * partition in this instance signifies the base partition of the
     * transaction
     * 
     * @param catalog_proc
     *            the procedure we are building the graphs for
     * @param catalog_db
     *            database
     * @param workload
     *            workload to trace through
     * @return
     */
    public static Map<Integer, MarkovGraph> createBasePartitionGraphsForProcedure(Procedure catalog_proc, Workload workload, PartitionEstimator p_estimator) {
        final boolean trace = LOG.isTraceEnabled();
        assert(catalog_proc != null);
        assert(workload != null);
        assert(p_estimator != null);
        
        // read in from Workload
        ProcedureNameFilter filter = new ProcedureNameFilter();
        filter.include(catalog_proc.getName());
        Iterator<AbstractTraceElement<? extends CatalogType>> it = workload.iterator(filter);

        Map<Integer, MarkovGraph> partitiongraphs = MarkovUtil.getBlankPartitionGraphs(catalog_proc);
        while (it.hasNext()) {
            AbstractTraceElement<? extends CatalogType> element = it.next();
            if (element instanceof TransactionTrace) {
                // Estimate which partition this transaction started on (i.e., the base_partition)
                TransactionTrace xact = (TransactionTrace) element;
                assert(xact != null);
                Integer base_partition = null;
                try {
                    base_partition = p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
                } catch (Exception ex) {
                    LOG.fatal("Failed to calculate base partition for " + xact, ex);
                    System.exit(1);
                }
                // IMPORTANT: We should never have a null base_partition 
                assert(base_partition != null);
                if (trace) LOG.trace("Process " + xact + " => Partition #" + base_partition);
                
                MarkovGraph g = partitiongraphs.get(base_partition);
                assert(g != null) : "No MarkovGraph exists for base partition #" + base_partition;
                try {
                    g.processTransaction(xact, p_estimator);
                } catch (Exception ex) {
                    LOG.fatal("Failed to process " + xact, ex);
                    System.exit(1);
                }
            }
        } // WHILE
        // After we created all of the graphs for each partition, invoke the calculateProbabilities()
        // method to compute the final edge/vertex probabilities 
        for (MarkovGraph g : partitiongraphs.values()) {
            g.calculateProbabilities();
            boolean valid = g.isValid(); 
            if (valid == false) {
                GraphvizExport<Vertex, Edge> gv = MarkovUtil.exportGraphviz(g, true, null);
                LOG.warn("Wrote invalid MarkovGraph out to file: " + gv.writeToTempFile());
            }
            assert(valid) : "Invalid MarkovGraph"; 
        } // FOR
        return partitiongraphs;
    }
    
    /**
     * Generate all of the procedure-based MarkovGraphs
     * @param catalog_db
     * @param workload
     * @param p_estimator
     * @return
     */
    public static MarkovGraphsContainer createProcedureGraphs(final Database catalog_db, final Workload workload, final PartitionEstimator p_estimator) {
        assert(workload != null);
        assert(p_estimator != null);
        
        MarkovGraphsContainer markovs = new MarkovGraphsContainer(true);
        List<Runnable> runnables = new ArrayList<Runnable>();
        final Set<Procedure> procedures = workload.getProcedures(catalog_db);
        final AtomicInteger ctr = new AtomicInteger(0);
        for (final Procedure catalog_proc : procedures) {
            final MarkovGraph g = markovs.getOrCreate(GLOBAL_MARKOV_CONTAINER_ID, catalog_proc, true);
            assert(g != null) : "Failed to create global MarkovGraph for " + catalog_proc;
            
            runnables.add(new Runnable() {
                public void run() {
                    List<TransactionTrace> traces = workload.getTraces(catalog_proc); 
                    LOG.info(String.format("Populating global MarkovGraph for %s [#traces=%d]", catalog_proc.getName(), traces.size()));
                    for (TransactionTrace xact : traces) {
                        try {
                            g.processTransaction(xact, p_estimator);
                        } catch (Exception ex) {
                            LOG.fatal("Failed to process " + xact, ex);
                            throw new RuntimeException(ex);
                        }
                    } // FOR
                    g.calculateProbabilities();
                    LOG.info(String.format("Finished creating MarkovGraph for %s [%d/%d]", catalog_proc.getName(), ctr.incrementAndGet(), procedures.size()));
                }
            });
        } // FOR
        LOG.info(String.format("Waiting for %d MarkovGraphs to get populated", runnables.size()));
        ThreadUtil.runGlobalPool(runnables);
        return (markovs);
    }

    /**
     * Utility method to calculate the probabilities at all of the MarkovGraphsContainers
     * @param markovs
     */
    public static void calculateProbabilities(Map<Integer, ? extends MarkovGraphsContainer> markovs) {
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Calculating probabilities for %d ids", markovs.size()));
        for (MarkovGraphsContainer m : markovs.values()) {
            m.calculateProbabilities();
        }
        return;
    }
    
    /**
     * Utility method
     * @param markovs
     * @param hasher
     */
    public static void setHasher(Map<Integer, ? extends MarkovGraphsContainer> markovs, AbstractHasher hasher) {
        if (LOG.isDebugEnabled()) LOG.debug(String.format("Setting hasher for for %d ids", markovs.size()));
        for (MarkovGraphsContainer m : markovs.values()) {
            m.setHasher(hasher);
        }
        return;
    }
    
    /**
     * Load a list of Markov objects from a serialized for a particular id
     * @param catalog_db
     * @param input_path
     * @param id
     * @return
     * @throws Exception
     */
    public static MarkovGraphsContainer loadId(Database catalog_db, String input_path, int id) throws Exception {
        Set<Integer> idset = (Set<Integer>)CollectionUtil.addAll(new HashSet<Integer>(), Integer.valueOf(id));
        Map<Integer, MarkovGraphsContainer> markovs = MarkovUtil.load(catalog_db, input_path, null, idset);
        assert(markovs.size() == 1);
        assert(markovs.containsKey(id));
        return (markovs.get(id));
    }
    
    public static Map<Integer, MarkovGraphsContainer> loadIds(Database catalog_db, String input_path, Collection<Integer> ids) throws Exception {
        return (MarkovUtil.load(catalog_db, input_path, null, ids));
    }
    
    public static Map<Integer, MarkovGraphsContainer> loadProcedures(Database catalog_db, String input_path, Collection<Procedure> procedures) throws Exception {
        return (MarkovUtil.load(catalog_db, input_path, procedures, null));
    }
    
    public static Map<Integer, MarkovGraphsContainer> load(final Database catalog_db, String input_path) throws Exception {
        return (MarkovUtil.load(catalog_db, input_path, null, null));
    }
    
    /**
     * 
     * @param catalog_db
     * @param input_path
     * @param ids
     * @return
     * @throws Exception
     */
    public static Map<Integer, MarkovGraphsContainer> load(final Database catalog_db, String input_path, Collection<Procedure> procedures, Collection<Integer> ids) throws Exception {
        final boolean d = LOG.isDebugEnabled();
        
        final Map<Integer, MarkovGraphsContainer> ret = new HashMap<Integer, MarkovGraphsContainer>();
        final File file = new File(input_path);
        LOG.info(String.format("Loading in serialized MarkovGraphContainers from '%s' [procedures=%s, ids=%s]", file.getName(), procedures, ids));
        
        try {
            // File Format: One PartitionId per line, each with its own MarkovGraphsContainer 
            BufferedReader in = FileUtil.getReader(file);
            
            // Line# -> Partition#
            final Map<Integer, Integer> line_xref = new HashMap<Integer, Integer>();
            
            int line_ctr = 0;
            while (in.ready()) {
                final String line = in.readLine();
                
                // If this is the first line, then it is our index
                if (line_ctr == 0) {
                    // Construct our line->partition mapping
                    JSONObject json_object = new JSONObject(line);
                    for (String key : CollectionUtil.wrapIterator(json_object.keys())) {
                        Integer partition = Integer.valueOf(key);
                        
                        // We want the MarkovGraphContainer pointed to by this line if
                        // (1) This partition is the same as our GLOBAL_MARKOV_CONTAINER_ID, which means that
                        //     there isn't going to be partition-specific graphs. There should only be one entry
                        //     in this file and we're always going to want to load it
                        // (2) They didn't pass us any ids, so we'll take everything we see
                        // (3) They did pass us ids, so check whether its included in the set
                        if (partition.equals(GLOBAL_MARKOV_CONTAINER_ID) || ids == null || ids.contains(partition)) {
                            Integer offset = json_object.getInt(key);
                            line_xref.put(offset, partition);
                        }
                    } // FOR
                    if (d) LOG.debug(String.format("Loading %d MarkovGraphsContainers", line_xref.size()));
                    
                // Otherwise check whether this is a line number that we care about
                } else if (line_xref.containsKey(Integer.valueOf(line_ctr))) {
                    final Integer partition = line_xref.remove(Integer.valueOf(line_ctr));
                    final JSONObject json_object = new JSONObject(line).getJSONObject(partition.toString());
                    
                    // We should be able to get the classname of the container from JSON
                    String className = MarkovGraphsContainer.class.getCanonicalName();
                    if (json_object.has(MarkovGraphsContainer.Members.CLASSNAME.name())) {
                        className = json_object.getString(MarkovGraphsContainer.Members.CLASSNAME.name());    
//                    } else {
//                        assert(false) : "Missing class name: " + CollectionUtil.toList(json_object.keys());
                    }
                    MarkovGraphsContainer markovs = ClassUtil.newInstance(className,
                                                                          new Object[]{ procedures},
                                                                          new Class<?>[]{ Collection.class }); 
                    assert(markovs != null);
                    
                    if (d) LOG.debug(String.format("Populating %s for partition %d", className, partition));
                    markovs.fromJSON(json_object, catalog_db);
                    ret.put(partition, markovs);        
                    
                    if (line_xref.isEmpty()) break;
                }
                line_ctr++;
            } // WHILE
            if (line_ctr == 0) throw new IOException("The MarkovGraphsContainer file '" + input_path + "' is empty");
            
        } catch (Exception ex) {
            LOG.error("Failed to deserialize the MarkovGraphsContainer from file '" + input_path + "'", ex);
            throw new IOException(ex);
        }
        if (d) LOG.debug("The loading of the MarkovGraphsContainer is complete");
        return (ret);
    }
    
    /**
     * For the given MarkovGraphContainer, serialize them out to a file
     * @param markovs
     * @param output_path
     * @throws Exception
     */
    public static void save(Map<Integer, ? extends MarkovGraphsContainer> markovs, String output_path) throws Exception {
        final String className = MarkovGraphsContainer.class.getSimpleName();
        LOG.info("Writing out graphs of " + className + " to '" + output_path + "'");
        
        // Sort the list of partitions so we always iterate over them in the same order
        SortedSet<Integer> sorted = new TreeSet<Integer>(markovs.keySet());
        
        File file = new File(output_path);
        try {
            FileOutputStream out = new FileOutputStream(file);
            
            // First construct an index that allows us to quickly find the partitions that we want
            JSONStringer stringer = (JSONStringer)(new JSONStringer().object());
            int offset = 1;
            for (Integer partition : sorted) {
                stringer.key(Integer.toString(partition)).value(offset++);
            } // FOR
            out.write((stringer.endObject().toString() + "\n").getBytes());
            
            // Now roll through each id and create a single JSONObject on each line
            for (Integer partition : sorted) {
                MarkovGraphsContainer markov = markovs.get(partition);
                assert(markov != null) : "Null MarkovGraphsContainer for partition #" + partition;
                
                stringer = (JSONStringer)new JSONStringer().object();
                stringer.key(partition.toString()).object();
                markov.toJSON(stringer);
                stringer.endObject().endObject();
                out.write((stringer.toString() + "\n").getBytes());
            } // FOR
            out.close();
        } catch (Exception ex) {
            LOG.error("Failed to serialize the " + className + " file '" + output_path + "'", ex);
            throw new IOException(ex);
        }
        LOG.debug(className + " objects were written out to '" + output_path + "'");
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
    public static GraphvizExport<Vertex, Edge> exportGraphviz(MarkovGraph markov, boolean use_full_output, List<Edge> path) {
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
    public static GraphvizExport<Vertex, Edge> exportGraphviz(MarkovGraph markov, boolean use_full_output, boolean use_vldb_output, boolean highlight_invalid, List<Edge> path) {
        GraphvizExport<Vertex, Edge> graphviz = new GraphvizExport<Vertex, Edge>(markov);
        graphviz.setEdgeLabels(true);
        graphviz.getGlobalGraphAttributes().put(Attribute.PACK, "true");
        graphviz.getGlobalVertexAttributes().put(Attribute.FONTNAME, "Courier");
        
        Vertex v = markov.getStartVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "darkblue");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled,bold");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");

        v = markov.getCommitVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "darkgreen");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled,bold");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");
        
        v = markov.getAbortVertex();
        graphviz.getAttributes(v).put(Attribute.FILLCOLOR, "firebrick4");
        graphviz.getAttributes(v).put(Attribute.FONTCOLOR, "white");
        graphviz.getAttributes(v).put(Attribute.STYLE, "filled,bold");
        graphviz.getAttributes(v).put(Attribute.FONTSIZE, "24");

        // Highlight Path
        if (path != null) graphviz.highlightPath(path, "red");
        
        if (use_vldb_output || use_full_output) {
            final String empty_set = "\u2205";
            for (Vertex v0 : markov.getVertices()) {
                AttributeValues av = graphviz.getAttributes(v0);
                
                if (highlight_invalid && v0.isValid() == false) {
                    av.put(Attribute.FILLCOLOR, "red");
                    LOG.warn("Highlighting " + v0 + " as invalid");
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
                        label += "Counter: " + v0.getQueryInstanceIndex() + "\n";
                        
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
    public static Set<Integer> getTouchedPartitions(List<Vertex> path) {
        Set<Integer> partitions = new HashSet<Integer>();
        for (Vertex v : path) {
            partitions.addAll(v.getPartitions());
        } // FOR
        return (partitions);
    }
    
    /**
     * Get the read/write partition counts for the given path
     * @param path
     * @return Set<ReadPartitions>, Set<WritePartitions>
     */
    public static Pair<Set<Integer>, Set<Integer>> getReadWritePartitions(List<Vertex> path) {
        Set<Integer> read_p = new HashSet<Integer>();
        Set<Integer> write_p = new HashSet<Integer>();
        MarkovUtil.getReadWritePartitions(path, read_p, write_p);
        return (Pair.of(read_p, write_p));
    }

    /**
     * Get the read/write partition counts for the given path
     * @param path
     * @param read_p
     * @param write_p
     */
    public static void getReadWritePartitions(List<Vertex> path, Set<Integer> read_p, Set<Integer> write_p) {
        for (Vertex v : path) {
            if (v.isQueryVertex() == false) continue;
            
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
