package edu.brown.markov;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractGraphElement;
import edu.brown.graphs.IGraph;
import edu.brown.graphs.exceptions.InvalidGraphElementException;
import edu.brown.utils.MathUtil;


/**
 * There are only two important things in edge: hits - the number of times this edge has been traversed probability -
 * calculated by the source of this edge. the probability of traversing this edge
 * 
 * There are also 'instance' versions of these variables. This is for managing online updates to the edges
 * 
 * @author svelagap
 * 
 */
public class MarkovEdge extends AbstractEdge implements MarkovHitTrackable {
    enum Members {
        PROBABILITY,
        TOTALHITS,
        INSTANCEHITS,
    }

    /**
     * This is the probability that the source of the edge will transition to the destination vertex
     */
    public float probability;

    /**
     * This is the total number of times that we have traversed over this edge
     */
    public int totalhits;

    /**
     * This is the temporary number of times that we have traversed over this edge in the current "period" of the
     * MarkovGraph. This will eventually get folded into the global hits count, but we need to keep it separate so that
     * we can determine whether the current workload is deviating from the training set
     */
    public transient int instancehits = 0;

    /**
     * Constructor
     * 
     * @param graph
     */
    public MarkovEdge(IGraph<MarkovVertex, MarkovEdge> graph) {
        super(graph);
        this.totalhits = 0;
        this.probability = 0;
    }

    public MarkovEdge(IGraph<MarkovVertex, MarkovEdge> graph, int hits, float probability) {
        super(graph);
        this.totalhits = hits;
        this.probability = (float)probability;
    }

    @Override
    public int compareTo(AbstractGraphElement o) {
        assert (o != null);
        if (o instanceof MarkovEdge) {
            MarkovEdge me = (MarkovEdge)o;
            if (MathUtil.equals(this.probability, me.probability, MarkovGraph.PROBABILITY_EPSILON) == false) {
                return (int) (me.probability * 100 - this.probability * 100);
            }
            return (this.hashCode() - me.hashCode());
        }
        return super.compareTo(o);
    }

    public float getProbability() {
        return this.probability;
    }

    /**
     * Calculates the probability for this edge.
     * Divides the number of hits this edge has had by the parameter
     * @param allHits number of hits of the vertex that is the source of this edge
     */
    public void calculateProbability(long allHits) {
        assert(this.totalhits <= allHits) : String.format("Edge hits is greater than new allHits: " + this.totalhits + " > " + allHits);
        if (allHits == 0) {
            this.probability = 0f;
        } else {
            this.probability = (float) (this.totalhits / (double)allHits);
        }
        assert(MathUtil.greaterThanEquals(this.probability, 0.0f, MarkovGraph.PROBABILITY_EPSILON) &&
               MathUtil.lessThanEquals(this.probability, 1.0f, MarkovGraph.PROBABILITY_EPSILON)) :
           String.format("Invalid new edge probability: %d / %d = %f", this.totalhits, allHits, this.probability);
    }

    // ----------------------------------------------------------------------------
    // VALIDATION METHODS
    // ----------------------------------------------------------------------------
    
    public boolean isValid(MarkovGraph markov) {
        try {
            this.validate(markov);
        } catch (InvalidGraphElementException ex) {
            return (false);
        }
        return (true);
    }

    protected void validate(MarkovGraph markov) throws InvalidGraphElementException {
        MarkovVertex v0 = markov.getSource(this);
        MarkovVertex v1 = markov.getDest(this);
        float e_prob = this.getProbability();
        
        // Make sure the edge probability is between [0, 1]
        if (MathUtil.greaterThanEquals(e_prob, 0.0f, MarkovGraph.PROBABILITY_EPSILON) == false ||
            MathUtil.lessThanEquals(e_prob, 1.0f, MarkovGraph.PROBABILITY_EPSILON) == false) {
            throw new InvalidGraphElementException(markov, this, String.format("Edge probability is %.4f", e_prob));
        }
        
        // Make sure that the special states are linked to each other
        if (v0.isQueryVertex() == false && v1.isQueryVertex() == false) {
            throw new InvalidGraphElementException(markov, this, "Invalid edge between non-query states");
        }    
    }
    
    // ----------------------------------------------------------------------------
    // ONLINE UPDATE METHODS
    // ----------------------------------------------------------------------------
    
    @Override
    public void applyInstanceHitsToTotalHits() {
        this.totalhits += this.instancehits;
        this.instancehits = 0;
    }
    @Override
    public void incrementTotalHits() {
        this.totalhits++;
    }
    @Override
    public long getTotalHits() {
        return this.totalhits;
    }
    @Override
    public void setInstanceHits(int instancehits) {
        this.instancehits = instancehits;
    }
    @Override
    public int getInstanceHits() {
        return this.instancehits;
    }
    @Override
    public int incrementInstanceHits() {
        return (++this.instancehits);
    }
    
    
    @Override
    public String toString() {
        return String.format("%.02f", this.probability); // FORMAT.format(this.probability);
    }
    
    @Override
    public String toString(boolean verbose) {
        if (verbose) {
            return (String.format("ElementId:%d, Prob:%.02f, TotalHits:%d, InstanceHits:%d",
                                  this.getElementId(), this.probability, this.totalhits, this.instancehits));
        } else {
            return this.toString();
        }
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    public void toJSONStringImpl(JSONStringer stringer) throws JSONException {
        super.toJSONStringImpl(stringer);
        super.fieldsToJSONString(stringer, MarkovEdge.class, Members.values());
    }

    public void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
        super.fromJSONObjectImpl(object, catalog_db);
        super.fieldsFromJSONObject(object, catalog_db, MarkovEdge.class, Members.values());
    }

}