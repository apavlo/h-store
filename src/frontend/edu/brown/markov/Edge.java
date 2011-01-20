package edu.brown.markov;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;

import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.IGraph;


/**
 * There are only two important things in edge: hits - the number of times this edge has been traversed probability -
 * calculated by the source of this edge. the probability of traversing this edge
 * 
 * There are also 'instance' versions of these variables. This is for managing online updates to the edges
 * 
 * @author svelagap
 * 
 */
public class Edge extends AbstractEdge implements Comparable<Edge> {
    enum Members {
        PROBABILITY,
        HITS,
    }

    /**
     * This is the probability that the source of the edge will transition to the destination vertex
     */
    public float probability;

    /**
     * This is the total number of times that we have traversed over this edge
     */
    public long hits;

    /**
     * This is the temporary number of times that we have traversed over this edge in the current "period" of the
     * MarkovGraph. This will eventually get folded into the global hits count, but we need to keep it separate so that
     * we can determine whether the current workload is deviating from the training set
     */
    private transient long instancehits = 0;

    /**
     * Constructor
     * 
     * @param graph
     */
    public Edge(IGraph<Vertex, Edge> graph) {
        super(graph);
        this.hits = 0;
        this.probability = 0;
    }

    public Edge(IGraph<Vertex, Edge> graph, int hits, double probability) {
        super(graph);
        this.hits = hits;
        this.probability = (float)probability;
    }

    @Override
    public int compareTo(Edge o) {
        assert (o != null);
        if (this.probability != o.probability) {
            return (int) (o.probability * 100 - this.probability * 100);
        }
        return (this.hashCode() - o.hashCode());
    }

    public long getHits() {
        return (this.hits);
    }

    public double getProbability() {
        return probability;
    }

    /**
     * Sets the probability for this edge. Divides the number of hits this edge has had by the parameter
     * 
     * @param totalhits
     *            number of hits of the vertex that is the source of this edge
     */
    public void setProbability(long totalhits) {
        probability = (float) (hits * 1.0 / totalhits);
    }

    public void increment() {
        hits++;
    }

    public void incrementHits(long howmuch) {
        hits += howmuch;
    }

    public long getInstancehits() {
        return instancehits;
    }

    public void incrementInstancehits() {
        instancehits++;
    }

    public void setInstancehits(int i) {
        instancehits = i;
    }

    @Override
    public String toString() {
        return String.format("%.03f", this.probability); // FORMAT.format(this.probability);
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    public void toJSONStringImpl(JSONStringer stringer) throws JSONException {
        super.toJSONStringImpl(stringer);
        super.fieldsToJSONString(stringer, Edge.class, Members.values());
    }

    public void fromJSONObjectImpl(JSONObject object, Database catalog_db) throws JSONException {
        super.fromJSONObjectImpl(object, catalog_db);
        super.fieldsFromJSONObject(object, catalog_db, Edge.class, Members.values());
    }

}