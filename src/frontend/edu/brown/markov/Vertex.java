package edu.brown.markov;

import java.util.*;

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.graphs.AbstractVertex;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author svelagap
 */
public class Vertex extends AbstractVertex {
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA ENUMS
    // ----------------------------------------------------------------------------
    
    public enum Members {
        QUERY_INSTANCE_INDEX,
        TYPE,
        PARTITIONS,
        TOTALHITS,
        PROBABILITIES,
        EXECUTION_TIME,
    };

    public enum Type {
        QUERY, START, COMMIT, ABORT
    };

    public enum Probability {
        SINGLE_SITED,
        ABORT,
        READ_ONLY,
        WRITE,
        DONE;
        
        protected static final Map<Integer, Probability> idx_lookup = new HashMap<Integer, Probability>();
        protected static final Map<String, Probability> name_lookup = new HashMap<String, Probability>();
        static {
            for (Probability vt : EnumSet.allOf(Probability.class)) {
                Probability.idx_lookup.put(vt.ordinal(), vt);
                Probability.name_lookup.put(vt.name().toLowerCase().intern(), vt);
            }
        }
        
        public static Probability get(Integer idx) {
            assert(idx >= 0);
            return (idx_lookup.get(idx));
        }

        public static Probability get(String name) {
            return (name_lookup.get(name.toLowerCase().intern()));
        }
    };

    // ----------------------------------------------------------------------------
    // GLOBAL CONFIGURATION
    // ----------------------------------------------------------------------------

    /**
     * This is the partition id that is used for probabilities that are not partition specific
     * For example, the ABORT probability is global to all partitions, so we only need to store one
     * value for it
     */
    private static final int DEFAULT_PARTITION_ID = Integer.MAX_VALUE;

    // ----------------------------------------------------------------------------
    // EXECUTION STATE DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The Query Instance Index is the counter for the number of times this particular Statement
     * was executed in the transaction 
     */
    public long query_instance_index;
    
    /**
     * The number of times this Vertex has been traversed
     */
    public long totalhits = 0;
    
    /**
     * The type of this vertex: Abort/Stop/Query/Start
     */
    public Type type;
    
    /**
     * The partitions this query touches
     */
    public Set<Integer> partitions = new HashSet<Integer>();

    // ----------------------------------------------------------------------------
    // ADDITIONAL NON-STATE DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The average execution time of this transaction
     */
    public long execution_time = 0l;

    /**
     * Mapping from Probability type to another map from partition id
     */
    public Map<Vertex.Probability, SortedMap<Integer, Float>> probabilities = new HashMap<Probability, SortedMap<Integer, Float>>();
    
    // ----------------------------------------------------------------------------
    // TRANSIENT DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The number of times this vertex has been touched in the current on-line run
     */
    private transient long instancehits = 0;
    
    /**
     * The execution times of the transactions in the on-line run
     * A map of the xact_id to the time it took to get to this vertex
     */
    private transient Map<Long,Long> instancetimes = new HashMap<Long,Long>();
    
    /**
     * The count, used to figure out the average execution time above
     */
    private transient long execution_time_count = 0l;

    // ----------------------------------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------------------------------

    /**
     * Empty constructor
     */
    public Vertex() {
        // This is needed for serialization
        super();
        this.init();
    }
    
    /**
     * Constructor for barebones vertices such as STOP, ABORT, and START
     * @param catalog_stmt
     * @param type
     */
    public Vertex(Statement catalog_stmt, Vertex.Type type) {
        this(catalog_stmt, type, 0, new HashSet<Integer>());
    }
    
    /**
     * Constructor used to make the unit tests
     * @param catalog_stmt
     * @param partitions
     * @param xact_count
     */
    public Vertex(Statement catalog_stmt, Integer[] partitions) {
        this(catalog_stmt, Type.QUERY, 0, Arrays.asList(partitions));
    }

    /**
     * Constructor used to create the actual graphs
     * @param catalog_stmt - query this vertex is associated with
     * @param type - QUERY, ABORT, START, or STOP
     * @param partitions - the partitions this procedure touches
     * @param id - the query trace index of this particular vertex, helps to identify
     * @param i - the number of transactions in this workload. used to figure out whether we should recompute
     */
    public Vertex(Statement catalog_stmt, Vertex.Type type, long query_instance_index, Collection<Integer> partitions) {
        super(catalog_stmt);
        this.type = type;
        this.partitions.addAll(partitions);
        this.query_instance_index = query_instance_index;
        this.init();
    }
    
    /**
     * Initialize the probability tables
     */
    private void init() {
        for (Vertex.Probability probability_type : Vertex.Probability.values()) {
            this.probabilities.put(probability_type, new TreeMap<Integer, Float>());
        } // FOR
    }

    // ----------------------------------------------------------------------------
    // DATA MEMBER METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * 
     * @return
     */
    public Type getType() {
        return this.type;
    }

    public Integer getQueryInstanceIndex() {
        return (int)this.query_instance_index;
    }
    
    public void increment() {
        totalhits++;
    }

    public long getTotalHits() {
        return totalhits;
    }

    public Set<Integer> getPartitions() {
        return partitions;
    }

    public boolean equals(Object o) {
        if (o instanceof Vertex) {
            Vertex v = (Vertex) o;
            return (this.type.equals(v.type) &&
                    this.catalog_item.equals(v.catalog_item) &&
                    this.partitions.equals(v.partitions) &&
                    this.query_instance_index == v.query_instance_index);
        }
        return false;
    }
    
    @Override
    public String toString() {
        return ("{" + 
               this.catalog_item.getName() + "," +
               "Indx:" + this.query_instance_index + "," +
               "Prtns:" + this.partitions +
               "}");
    }

    public boolean isSingleSited() {
        return partitions.size() <= 1;
    }

    public boolean isReadOnly() {
        return ((Statement) this.catalog_item).getReadonly();
    }
    
    /**
     * Returns the probability of name if it is found in the mapping, otherwise returns d
     * @param name
     * @param d
     * @return
     */
    private float getSpecificProbability(Vertex.Probability type, int partition, float d) {
        SortedMap<Integer, Float> prob = this.probabilities.get(type);
        assert(prob != null);
        if (prob.containsKey(partition)) {
            return (prob.get(partition));
        }
        prob.put(partition, d);
        return (d);
    }
    
    /**
     * Use for incrementing a certain probability
     * @param name
     * @param probability
     */
    private void addToProbability(Vertex.Probability type, int partition, float probability) {
        float previous = (float) 0.0;
        SortedMap<Integer, Float> prob = this.probabilities.get(type);
        assert(prob != null);
        if (prob.containsKey(partition)) {
            previous = prob.get(partition);
        }
        setProbability(type, partition, (previous + probability));
    }

    /**
     * 
     * @param type
     * @param partition
     * @param probability
     */
    private void setProbability(Vertex.Probability type, int partition, float probability) {
        this.probabilities.get(type).put(partition, probability);
    }

    // ----------------------------------------------------------------------------
    // SINGLE-SITED PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addSingleSitedProbability(float probability) {
        this.addToProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, probability);
    }
    public void setSingleSitedProbability(float probability) {
        this.setProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, probability);
    }
    public float getSingleSitedProbability() {
        return (this.getSpecificProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, 0.0f));
    }
    public static void calculateSingleSitedProbability(Set<Vertex> vs, MarkovGraph g) {
        g.unmarkAllEdges();
        calculateSingleSited(vs, g);
    }
    private static void calculateSingleSited(Set<Vertex> vs, MarkovGraph g) {
        if (vs.size() == 0)
            return;
        Set<Edge> edges = getEdgesTo(vs, g);
        Set<Vertex> vertices = new HashSet<Vertex>();
        for (Edge e : edges) {
            if (!e.marked()) {
                Vertex v = g.getSource(e);
                Vertex dest = g.getDest(e);
                if (dest.isSingleSited() && dest.isQueryVertex()) {
                    v.addSingleSitedProbability((float) (e.getProbability() * 1.0));
                }
                vertices.add(v);
                e.mark();
            }
        }
        calculateSingleSited(vertices, g);
    }

    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addReadOnlyProbability(int partition, float probability) {
        this.addToProbability(Probability.READ_ONLY, partition, probability);
    }
    public void setReadOnlyProbability(int partition, float probability) {
        this.setProbability(Probability.READ_ONLY, partition, probability);
    }
    public float getReadOnlyProbability(int partition) {
        return (this.getSpecificProbability(Probability.READ_ONLY, partition, 0.0f));
    }
    public static void calculateReadOnlyProbability(Set<Vertex> vs, MarkovGraph g) {
        g.unmarkAllEdges();
        calculateReadWriteProbability(vs, g);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addWriteProbability(int partition, float probability) {
        this.addToProbability(Probability.WRITE, partition, probability);
    }
    public void setWriteProbability(int partition, float probability) {
        this.setProbability(Probability.WRITE, partition, probability);
    }
    public float getWriteProbability(int partition) {
        return (this.getSpecificProbability(Probability.WRITE, partition, 0.0f));
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addDoneProbability(int partition, float probability) {
        this.addToProbability(Probability.DONE, partition, probability);
    }
    public void setDoneProbability(int partition, float probability) {
        this.setProbability(Probability.DONE, partition, probability);
    }
    public float getDoneProbability(int partition) {
        return (this.getSpecificProbability(Probability.DONE, partition, 1.0f));
    }
    /**
     * Calculates the probability that each vertex will be done at each partition.
     * @param vs
     * @param markovGraph
     */
    public static void calculateDoneProbability(Vertex vs, MarkovGraph markovGraph) {
        markovGraph.unmarkAllEdges();
        calculateMultiDoneProbability(vs, markovGraph, new HashSet<Integer>());
    }


    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------

    public void addAbortProbability(float probability) {
        this.addToProbability(Probability.ABORT, DEFAULT_PARTITION_ID, probability);
    }
    public void setAbortProbability(float probability) {
        this.setProbability(Probability.ABORT, DEFAULT_PARTITION_ID, probability);
    }
    public float getAbortProbability() {
        return (this.getSpecificProbability(Probability.ABORT, DEFAULT_PARTITION_ID, 0.0f));
    }
    public static void calculateAbortProbability(Set<Vertex> vs, MarkovGraph g) {
        g.unmarkAllEdges();
        calculateAllAbortProbability(vs, g);
    }

    /**
     * Calculates abort probability. 
     * This method is recursive: it should be fed the abort node. Then it will trace back through the graph
     * and propagate the abort probability through the graph based on the probability of traversals of the edge
     * we are backtracking on.
     * @param vs
     * @param g
     * @param probtype
     */
    private static void calculateAllAbortProbability(Set<Vertex> vs, MarkovGraph g) {
        if (vs.size() == 0)
            return;
        Set<Edge> edges = getEdgesTo(vs, g);
        Set<Vertex> vertices = new HashSet<Vertex>();
        for (Edge e : edges) {
            if (!e.marked()) {
                Vertex v = g.getSource(e);
                Vertex dest = g.getDest(e);
                v.addAbortProbability((float) (e.getProbability() * dest.getAbortProbability()));
                vertices.add(v);
                e.mark();
            }
        }
        calculateAllAbortProbability(vertices, g);
    }

    /**
     * Calculates the probability that each vertex will be done at each partition.
     * @param vs
     * @param g
     * @param seen
     */
    private static void calculateMultiDoneProbability(Vertex vs, MarkovGraph g, Set<Integer> seen) {
        if (vs == null)
            return;
        Set<Vertex> one = new HashSet<Vertex>();
        one.add(vs);
        Set<Edge> edges = getEdgesTo(one, g);
        // Keep track of the new partition IDs we have seen so far at this depth
        // of the graph
        for (Edge e : edges) {
            Vertex v = g.getSource(e);
            Vertex dest = g.getDest(e);
            Set<Integer> allpartitions = new HashSet<Integer>(g.getAllPartitions());
            Set<Integer> seensofar = new HashSet<Integer>(v.getPartitions());
            allpartitions.removeAll(seen);
            allpartitions.removeAll(seensofar);
            if (!e.marked()) {
                for (int i : allpartitions) {
                    // set all we havent seen at all to 1.0
                    if (v.equals(g.getStartVertex())) {
                        v.getDoneProbability(i);
                    }
                    dest.getDoneProbability(i);
                }
                for (int i : seen) {
                    // set all we've seen based on the neighbor's probabilities
                    v.addDoneProbability(i, (float)e.getProbability() * dest.getDoneProbability(i));
                }
                seensofar.removeAll(seen);
                for (int i : seensofar) {
                    seensofar.add(i);
                    v.setDoneProbability(i, 0.0f);
                } // FOR
                e.mark();
                Set<Integer> newseen = new HashSet<Integer>(seen);
                newseen.addAll(seensofar);
                // System.out.println(newseen);
                calculateMultiDoneProbability(v, g, newseen);
            }
        }
    }

    /**
     * We start at the end with the commit/stop vertex. We work backwards, if the destination vertex
     * is readonly, we set it's read-only probability to 1.0 at all the partitions it touches, otherwise
     * we set it's write probability to 1.0 at all the partitions it touches.
     * To calculate further down the graph we use the probability of transitioning to the next vertex
     * times the RO or W probability at the next vertex for a particular partition.
     * @param vs
     * @param g
     */
    private static void calculateReadWriteProbability(Set<Vertex> vs, MarkovGraph g) {
        if (vs.size() == 0)
            return;
        Set<Edge> edges = getEdgesTo(vs, g);
        Set<Vertex> vertices = new HashSet<Vertex>();
        List<Integer> partitions = g.getAllPartitions();
        for (Edge e : edges) {
            if (!e.marked()) {
                Vertex v = g.getSource(e);
                Vertex dest = g.getDest(e);
                for (int i : partitions) {
                    if (dest.isQueryVertex() && dest.partitions.contains(i)) {
                        if (!dest.isReadOnly()) {
                            dest.setWriteProbability(i, (float) 1.0);
                        } else {
                            dest.setReadOnlyProbability(i, (float) 1.0);
                        }
                    }
                    float read = (float) (e.getProbability() * dest.getReadOnlyProbability(i));
                    float write = (float) (e.getProbability() * dest.getWriteProbability(i));
                    v.addReadOnlyProbability(i, read);
                    v.addWriteProbability(i, write);
                }
                vertices.add(v);
                e.mark();
            }
        }
        calculateReadWriteProbability(vertices, g);
    }

    public boolean isQueryVertex() {
        return this.getType().equals(Type.QUERY);
    }
    
    /**
     * Perform equality check distinct from equals() method. Checks partitions, catalog_statement,
     * and the index of the query within the transaction
     * @param a
     * @param partitions2
     * @param queryInstanceIndex
     * @return
     */
    public boolean isEqual(Statement a, Collection<Integer> partitions2, long queryInstanceIndex) {
        return a.equals(this.catalog_item) && partitions2.equals(partitions)
                && queryInstanceIndex == query_instance_index;
    }
    
    public boolean isMatch(Statement catalog_stmt, int[] partitions2) {
        return this.catalog_key.equals(CatalogKey.createKey(catalog_stmt))
                && partitions.equals(partitions2);
    }

    /**
     * Gives all edges to the set of vertices vs in the MarkovGraph g
     * @param vs
     * @param g
     * @return
     */
    static Set<Edge> getEdgesTo(Set<Vertex> vs, MarkovGraph g) {
        Set<Edge> edges = new HashSet<Edge>();
        for (Vertex v : vs) {
            if (v != null) {
                edges.addAll(g.getInEdges(v));
            }
        }
        return edges;
    }



    /**
     * The 'score' of a vertex is a measure of how often it has been hit in the current workload.
     * When this value differs enough from getOriginalScore() shoudlRecompute() will return true
     * @param xact_count
     * @return
     */
    public float getChangeScore(int xact_count) {
        return (float) (instancehits * 1.0 / xact_count);
    }

    /**
     * When the hits this vertex has received in this current run differs from the original hitrate enough,
     * it returns true.
     * @param xact_count
     * @param recomputeTolerance - the threshold at which we should recompute
     * @param workload_count - the transaction count of the workload used to make the graph this Vertex is a part of
     * @return
     */
    public boolean shouldRecompute(int xact_count, double recomputeTolerance, int workload_count) {
        double original_score = this.totalhits / (1.0f * workload_count); 
        return (getChangeScore(xact_count) - original_score) / original_score >= recomputeTolerance;
    }

    public void incrementTotalhits(long instancehits2) {
        totalhits += instancehits2;
    }

    /**
     * Produce a table of all the partitions
     */
    public String debug() {
        StringBuilder top = new StringBuilder();
        StringBuilder bot = new StringBuilder();
        
        // First get the list of all the partitions that we know about
        Set<Integer> partitions = new HashSet<Integer>();
        Set<Probability> single_ptypes = new HashSet<Probability>();
        for (Vertex.Probability type : Vertex.Probability.values()) {
            Map<Integer, Float> probs = this.probabilities.get(type);
            partitions.addAll(probs.keySet());

            if (probs.size() == 1) {
                top.append(String.format("+%-12s %.3g\n", type.toString() + ":", probs.get(DEFAULT_PARTITION_ID)));
                single_ptypes.add(type);
            } else {
                bot.append("\t" + StringUtil.abbrv(type.name(), 6, false));
            }
        } // FOR
        partitions.remove(DEFAULT_PARTITION_ID); 
        bot.append("\n");


        // Now construct the debug output table
        for (int partition : partitions) {
            bot.append(String.format("[%02d]", partition));
            
            for (Vertex.Probability type : Vertex.Probability.values()) {
                if (single_ptypes.contains(type)) continue;
                Float prob = this.probabilities.get(type).get(partition);
                
                bot.append("\t");
                if (prob == null) {
                    bot.append("<NONE>");
                } else {
                    bot.append(String.format("%.3g", prob)); 
                }
            } // FOR
            bot.append("\n");
        } // FOR
//        
//        
//        for (Vertex.Probability type : Vertex.Probability.values()) {
//            SortedMap<Integer, Float> prob = this.probabilities.get(type);
//            if (prob.size() == 1) {
//                top += String.format("+%-12s %.3g\n", type.toString() + ":", prob.get(DEFAULT_PARTITION_ID));
//            } else {
//                bot += "+" + type.toString() + ": " + prob.keySet() + "\n";
//                if (prob.isEmpty()) {
//                    bot += "   <NONE>\n";
//                } else {
//                    for (int partition : prob.keySet()) {
//                        bot += String.format("   [%02d] %.3g\n", partition, prob.get(partition));  
//                    } // FOR
//                }
//            }
//        }
        String ret = "Vertex[" + this.getCatalogItem().getName() + "] :: " +
                     "ExecutionTime " + this.getExecutiontime() + "\n" +
                     top + bot;
        return (ret);

    }

    public void setExecutiontime(Long executiontime) {
        this.execution_time = executiontime;
    }


    public Long getExecutiontime() {
        return execution_time;
    }

    public void addExecutionTime(long l) {
        this.execution_time = (this.execution_time * execution_time_count + l) / ++execution_time_count;
    }
    public void addToExecutionTime(long l){
        this.execution_time += l;
    }
    // ----------------------------------------------------------------------------
    // ONLINE UPDATE METHODS
    // ----------------------------------------------------------------------------
   
    /**
     * Set the number of instance hits, useful for testing
     */
    public void setInstancehits(long instancehits) {
        this.instancehits = instancehits;
    }

    /**
     * Get the number of times this vertex has been hit in the on-line versino
     */
    public long getInstancehits() {
        return instancehits;
    }

    /**
     * Increments the number of times this vertex has been hit in the on-line version
     */
    public void incrementInstancehits() {
        instancehits++;
    }

    /**
     * @return a map of xact_ids to times
     */
    public Map<Long,Long> getInstanceTimes() {
        return instancetimes;
    }
    
    /**
     * Add another instance time to the map. We use these times to figure out how long each
     * transaction takes to execute in the on-line model.
     */
    public void addInstanceTime(long xact_id, long time){
        instancetimes.put(xact_id,time);
    }
    
    /**
     * Since we cannot know when a transaction ends in the on-line updates world, we wait until we find out
     * that we need to recompute, then we normalize all the vertices in every graph with the end times, to
     * get how long the xact actually lasted
     * @param end_times
     */
    public void normalizeInstanceTimes(Map<Long, Long> end_times) {
        List<Long> remove = new ArrayList<Long>();
        for(Long l: instancetimes.keySet()){
            long time = end_times.get(l) - instancetimes.get(l);
            addExecutionTime(time);
            remove.add(time);
        }
        for(long time:remove){
            end_times.remove(time);
        }
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    /**
     * Implementation of the toJSONString method for an AbstractVertex
     */
    public void toJSONStringImpl(JSONStringer stringer) throws JSONException {
        super.toJSONStringImpl(stringer);
     
        Set<Members> members_set = CollectionUtil.getAllExcluding(Members.values(), Members.PROBABILITIES);
        Members members[] = new Members[members_set.size()];
        members_set.toArray(members);
        super.fieldsToJSONString(stringer, Vertex.class, members);
        
        //
        // Probabilities Map
        //
        stringer.key(Members.PROBABILITIES.name()).object();
        for (Probability type : Probability.values()) {
            SortedMap<Integer, Float> probs = this.probabilities.get(type);

            stringer.key(type.name()).object();
            for (Integer partition : probs.keySet()) {
                stringer.key(partition.toString()).value(probs.get(partition));
            } // FOR
            stringer.endObject();
        } // FOR
        stringer.endObject();
    }

    @SuppressWarnings("unchecked")
    public void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
        // Lists in Java suck. We want to let fieldsFromJSONObject handle all our fields except for TYPE
        Set<Members> members_set = CollectionUtil.getAllExcluding(Members.values(), Members.TYPE, Members.PROBABILITIES);
        Members members[] = new Members[members_set.size()];
        members_set.toArray(members);
        super.fieldsFromJSONObject(object, catalog_db, Vertex.class, members);

        //
        // Probabilities Map
        //
        JSONObject json_probabilities = object.getJSONObject(Members.PROBABILITIES.name());
        Iterator<String> keys = json_probabilities.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            Probability type = Probability.get(key);
            assert(type != null) : "Invalid name '" + key + "'";
            this.probabilities.get(type).clear();
            
            JSONObject json_map = json_probabilities.getJSONObject(key);
            Iterator<String> keys_partitions = json_map.keys();
            while (keys_partitions.hasNext()) {
                String partition_key = keys_partitions.next();
                Integer partition = Integer.valueOf(partition_key);
                assert(partition != null);
                this.probabilities.get(type).put(partition, (float)json_map.getDouble(partition_key));
            } // WHILE
        } // WHILE
        
        // I'm lazy...
        String s = object.getString(Members.TYPE.name());
        for (Type e : Type.values()) {
            if (e.name().startsWith(s)) {
                this.type = e;
                break;
            }
        } // FOR

        // We have to call this ourselves because we need to be able to handle
        // our special START/STOP/ABORT catalog objects
        super.fromJSONObjectImpl(object, catalog_db);
        this.fieldsFromJSONObject(object, catalog_db, AbstractVertex.class, AbstractVertex.Members.values());
        this.catalog_class = (Class<? extends CatalogType>) ClassUtil.getClass(object.getString(AbstractVertex.Members.CATALOG_CLASS.name()));
        assert (this.catalog_class != null);

        switch (this.type) {
            case START:
            case COMMIT:
            case ABORT:
                this.catalog_item = MarkovUtil.getSpecialStatement(this.type);
                break;
            default:
                this.catalog_item = CatalogKey.getFromKey(catalog_db, this.catalog_key, this.catalog_class);
            break;
        } // SWITCH
    }


}
