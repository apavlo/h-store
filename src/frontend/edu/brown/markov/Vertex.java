package edu.brown.markov;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.graphs.AbstractVertex;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author svelagap
 * @author pavlo
 */
public class Vertex extends AbstractVertex {
    private static final Logger LOG = Logger.getLogger(Vertex.class);
    
    // ----------------------------------------------------------------------------
    // INTERNAL DATA ENUMS
    // ----------------------------------------------------------------------------
    
    public enum Members {
        QUERY_INSTANCE_INDEX,
        TYPE,
        PARTITIONS,
        PAST_PARTITIONS,
        TOTALHITS,
        PROBABILITIES,
        EXECUTION_TIME,
    };

    public enum Type {
        QUERY,
        START,
        COMMIT,
        ABORT
    };

    public enum Probability {
        SINGLE_SITED    (true),
        ABORT           (true),
        READ_ONLY       (false),
        WRITE           (false),
        DONE            (false);
        
        final boolean single_value;
        Probability(boolean single_value) {
            this.single_value = single_value;
        }
        
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
    private static final int DEFAULT_PARTITION_ID = -1; // Integer.MAX_VALUE;
    
    /**
     * I'm getting back some funky results so we'll just round everything to this
     * number of decimal places.
     */
    private static final int PROBABILITY_PRECISION = 7;

    // ----------------------------------------------------------------------------
    // EXECUTION STATE DATA MEMBERS
    // ----------------------------------------------------------------------------

    /**
     * The Query Instance Index is the counter for the number of times this particular Statement
     * was executed in the transaction 
     */
    public int query_instance_index;
    
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
    
    /**
     * The partitions that the txn has touched in the past
     */
    public Set<Integer> past_partitions = new HashSet<Integer>();

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
    public Map<Vertex.Probability, SortedMap<Integer, Double>> probabilities = new HashMap<Probability, SortedMap<Integer, Double>>();
    
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
    private transient Map<Long, Long> instancetimes = new HashMap<Long,Long>();
    
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
        this(catalog_stmt, type, 0, new HashSet<Integer>(), new HashSet<Integer>());
    }
    
    /**
     * Constructor used to make the unit tests
     * @param catalog_stmt
     * @param partitions
     * @param xact_count
     */
    public Vertex(Statement catalog_stmt, Integer[] partitions, Integer[] past_partitions) {
        this(catalog_stmt, Type.QUERY, 0, Arrays.asList(partitions), Arrays.asList(past_partitions));
    }

    /**
     * Constructor used to create the actual graphs
     * @param catalog_stmt - query this vertex is associated with
     * @param type - QUERY, ABORT, START, or STOP
     * @param partitions - the partitions this procedure touches
     * @param id - the query trace index of this particular vertex, helps to identify
     * @param i - the number of transactions in this workload. used to figure out whether we should recompute
     */
    public Vertex(Statement catalog_stmt, Vertex.Type type, int query_instance_index, Collection<Integer> partitions, Collection<Integer> past_partitions) {
        super(catalog_stmt);
        this.type = type;
        this.partitions.addAll(partitions);
        this.past_partitions.addAll(past_partitions);
        this.query_instance_index = query_instance_index;
        this.init();
    }
    
    /**
     * Copy Constructor
     * Only really used for testing
     * @param v
     */
    public Vertex(Vertex v) {
        super(v.getCatalogItem());
        this.type = v.type;
        this.partitions.addAll(v.partitions);
        this.past_partitions.addAll(v.past_partitions);
        this.query_instance_index = v.query_instance_index;
        this.probabilities.putAll(v.probabilities);
    }
    
    /**
     * Initialize the probability tables
     */
    private void init() {
        for (Vertex.Probability ptype : Vertex.Probability.values()) {
            if (this.probabilities.containsKey(ptype) == false)
                this.probabilities.put(ptype, new TreeMap<Integer, Double>());
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

    public int getQueryInstanceIndex() {
        return (int)this.query_instance_index;
    }
    
    public void increment() {
        totalhits++;
    }

    public long getTotalHits() {
        return totalhits;
    }

    /**
     * Return the set of partitions that the query represented by this vertex touches
     * @return
     */
    public Set<Integer> getPartitions() {
        return partitions;
    }
    
    /**
     * Return the set of partitions that the txn has touched in the past
     * @return
     */
    public Set<Integer> getPastPartitions() {
        return past_partitions;
    }

    public boolean equals(Object o) {
        if (o instanceof Vertex) {
            Vertex v = (Vertex) o;
            return (this.type.equals(v.type) &&
                    this.catalog_item.equals(v.catalog_item) &&
                    this.partitions.equals(v.partitions) &&
                    (MarkovGraph.USE_PAST_PARTITIONS == false || this.past_partitions.equals(v.past_partitions)) &&
                    this.query_instance_index == v.query_instance_index);
        }
        return false;
    }
    
    /**
     * Perform equality check distinct from equals() method. Checks partitions, catalog_statement,
     * and the index of the query within the transaction
     * @param other_stmt
     * @param other_partitions
     * @param other_past
     * @param other_queryInstanceIndex
     * @return
     */
    public boolean isEqual(Statement other_stmt, Collection<Integer> other_partitions, Collection<Integer> other_past, int other_queryInstanceIndex) {
        return (this.isEqual(other_stmt, other_partitions, other_past, other_queryInstanceIndex, MarkovGraph.USE_PAST_PARTITIONS));
    }
    
    /**
     * Perform equality check distinct from equals() method. Checks partitions, catalog_statement,
     * and the index of the query within the transaction
     * This version of isEqual() allows you to pass in the use_past_partitions flag
     * 
     * @param other_stmt
     * @param other_partitions
     * @param other_past
     * @param other_queryInstanceIndex
     * @param use_past_partitions
     * @return
     */
    public boolean isEqual(Statement other_stmt, Collection<Integer> other_partitions, Collection<Integer> other_past, int other_queryInstanceIndex, boolean use_past_partitions) {
        return (other_stmt.equals(this.catalog_item) &&
                other_partitions.equals(this.partitions) &&
                (use_past_partitions == false || other_past.equals(this.past_partitions)) &&
                other_queryInstanceIndex == this.query_instance_index);
    }
    
    @Override
    public String toString() {
        return ("{" + 
               this.catalog_item.getName() + "," +
               "Indx:" + this.query_instance_index + "," +
               "Prtns:" + this.partitions + ", " +
               "Past:" + this.past_partitions +
               "}");
    }
    
    /**
     * Produce a table of all the partitions
     */
    public String debug() {
        StringBuilder top = new StringBuilder();
        StringBuilder bot = new StringBuilder();
        
        final DecimalFormat formatter = new DecimalFormat("0.000");
        
        // First get the list of all the partitions that we know about
        Set<Integer> partitions = new HashSet<Integer>();
        Set<Probability> single_ptypes = new HashSet<Probability>();
        for (Vertex.Probability type : Vertex.Probability.values()) {
            Map<Integer, Double> probs = this.probabilities.get(type);
            partitions.addAll(probs.keySet());

            if (probs.size() == 1) {
                top.append(String.format("+%-12s %s\n", type.toString() + ":", formatter.format(probs.get(DEFAULT_PARTITION_ID))));
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
                Double prob = this.probabilities.get(type).get(partition);
                
                bot.append("\t");
                if (prob == null) {
                    bot.append("<NONE>");
                } else {
                    bot.append(formatter.format(prob)); 
                }
            } // FOR
            bot.append("\n");
        } // FOR
//        
//        
//        for (Vertex.Probability type : Vertex.Probability.values()) {
//            SortedMap<Integer, Double> prob = this.probabilities.get(type);
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
        String ret = this.toString() + // "Vertex[" + this.getCatalogItem().getName() + "]"
                     " :: " +
                     "ExecutionTime " + this.getExecutiontime() + "\n" +
                     top + bot;
        return (ret);

    }

    // ----------------------------------------------------------------------------
    // PROBABILITY METHODS
    // ----------------------------------------------------------------------------

    /**
     * Returns true if the query for this vertex only touches one partition and that
     * partition is the as the base_partition (i.e., where the procedure's Java code is executing)
     * @param base_partition
     * @return
     */
    public boolean isLocalPartitionOnly(int base_partition) {
        return ((this.type != Type.QUERY) || 
                (this.partitions.size() == 1 && CollectionUtil.getFirst(this.partitions) == base_partition));
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
     * @param default_value
     * @return
     */
    private Double getSpecificProbability(Vertex.Probability ptype, int partition, Double default_value) {
        SortedMap<Integer, Double> probs = this.probabilities.get(ptype);
        assert(probs != null);
        
        Double prob = probs.get(partition);
        if (prob == null) {
            probs.put(partition, default_value);
            prob = default_value;
        }
        // Handle funky rounding error that I think is due to casting
        // Note that we only round when we hand out the number. If we try to round it before we 
        // stick it in then it still comes out wrong sometimes...
        return (prob != null ? MathUtil.roundToDecimals(prob, PROBABILITY_PRECISION) : null); 
    }
    
    /**
     * Use for incrementing a certain probability
     * @param name
     * @param probability
     */
    private void addToProbability(Vertex.Probability type, int partition, double probability) {
        SortedMap<Integer, Double> prob = this.probabilities.get(type);
        assert(prob != null);
        Double previous = prob.get(partition);
        if (previous == null) previous = 0.0;
        this.setProbability(type, partition, (previous+probability));
    }

    /**
     * 
     * @param ptype
     * @param partition
     * @param probability
     */
    private void setProbability(Vertex.Probability ptype, int partition, double probability) {
        LOG.trace("(" + ptype + ", " + partition + ") => " + probability);
        this.probabilities.get(ptype).put(partition, probability);
    }

    /**
     * Reset all probabilities. Keeps partitions in maps
     */
    public synchronized void resetAllProbabilities() {
        for (Vertex.Probability ptype : this.probabilities.keySet()) {
            if (ptype.single_value) {
                this.probabilities.get(ptype).put(DEFAULT_PARTITION_ID, null);
            } else {
                for (Entry<Integer, Double> e : this.probabilities.get(ptype).entrySet()) {
                    this.probabilities.get(ptype).put(e.getKey(), null);
                } // FOR
            }
        } // FOR
    }
    
    // ----------------------------------------------------------------------------
    // SINGLE-SITED PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addSingleSitedProbability(double probability) {
        this.addToProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, probability);
    }
    public void setSingleSitedProbability(double probability) {
        this.setProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, probability);
    }
    public Double getSingleSitedProbability() {
        return (this.getSpecificProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, 0.0d));
    }
    public boolean isSingleSitedProbabilitySet() {
        return (this.getSpecificProbability(Probability.SINGLE_SITED, DEFAULT_PARTITION_ID, null) != null);
    }

    // ----------------------------------------------------------------------------
    // READ-ONLY PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addReadOnlyProbability(int partition, double probability) {
        this.addToProbability(Probability.READ_ONLY, partition, probability);
    }
    public void setReadOnlyProbability(int partition, double probability) {
        this.setProbability(Probability.READ_ONLY, partition, probability);
    }
    public Double getReadOnlyProbability(int partition) {
        return (this.getSpecificProbability(Probability.READ_ONLY, partition, 0.0d));
    }
    public boolean isReadOnlyProbabilitySet(int partition) {
        return (this.getSpecificProbability(Probability.READ_ONLY, partition, null) != null);
    }
    
    // ----------------------------------------------------------------------------
    // WRITE PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addWriteProbability(int partition, double probability) {
        this.addToProbability(Probability.WRITE, partition, probability);
    }
    public void setWriteProbability(int partition, double probability) {
        this.setProbability(Probability.WRITE, partition, probability);
    }
    public Double getWriteProbability(int partition) {
        return (this.getSpecificProbability(Probability.WRITE, partition, 0.0d));
    }
    public boolean isWriteProbabilitySet(int partition) {
        return (this.getSpecificProbability(Probability.WRITE, partition, null) != null);
    }
    
    // ----------------------------------------------------------------------------
    // DONE PROBABILITY
    // ----------------------------------------------------------------------------
    
    public void addDoneProbability(int partition, double probability) {
        this.addToProbability(Probability.DONE, partition, probability);
    }
    public void setDoneProbability(int partition, double probability) {
        this.setProbability(Probability.DONE, partition, probability);
    }
    public Double getDoneProbability(int partition) {
        return (this.getSpecificProbability(Probability.DONE, partition, 1.0d));
    }
    public boolean isDoneProbabilitySet(int partition) {
        return (this.getSpecificProbability(Probability.DONE, partition, null) != null);
    }

    // ----------------------------------------------------------------------------
    // ABORT PROBABILITY
    // ----------------------------------------------------------------------------

    public void addAbortProbability(double probability) {
        this.addToProbability(Probability.ABORT, DEFAULT_PARTITION_ID, probability);
    }
    public void setAbortProbability(double probability) {
        this.setProbability(Probability.ABORT, DEFAULT_PARTITION_ID, probability);
    }
    public Double getAbortProbability() {
        return (this.getSpecificProbability(Probability.ABORT, DEFAULT_PARTITION_ID, 0.0d));
    }
    public boolean isAbortProbabilitySet() {
        return (this.getSpecificProbability(Probability.DONE, DEFAULT_PARTITION_ID, null) != null);
    }

    /**
     * The 'score' of a vertex is a measure of how often it has been hit in the current workload.
     * When this value differs enough from getOriginalScore() shoudlRecompute() will return true
     * @param xact_count
     * @return
     */
    public double getChangeScore(int xact_count) {
        return (double) (instancehits * 1.0 / xact_count);
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
        this.instancetimes.put(xact_id, time);
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
            SortedMap<Integer, Double> probs = this.probabilities.get(type);

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
                Double probability = null;
                try {
                    if (!json_map.isNull(partition_key)) probability = json_map.getDouble(partition_key);    
                } catch (JSONException ex) {
                    LOG.error("Failed to get " + type + " probability at Partition #" + partition);
                    throw ex;
                }
                this.probabilities.get(type).put(partition, probability);
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
