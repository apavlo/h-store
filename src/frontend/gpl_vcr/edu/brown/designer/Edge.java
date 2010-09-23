package edu.brown.designer;

import java.util.*;

import edu.brown.designer.AccessGraph.EdgeAttributes;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.IGraph;
import edu.uci.ics.jung.graph.util.EdgeType;

public class Edge extends AbstractEdge {
    
    public enum Members {
        WEIGHTS,
    }
    
    private transient Double total_weight = null;
    
    /**
     * Edge weights are two-dimensional
     */
    private final Map<Integer, Double> weights = new TreeMap<Integer, Double>();
    {
        this.setAttribute(EdgeAttributes.WEIGHT.name(), this.weights);
    }
    
    /**
     * Base constructor
     * @param graph
     * @param vertices
     */
    public Edge(IGraph<Vertex, Edge> graph) {
        super(graph);
    }
    
    /**
     * Copy constructor
     * @param graph
     * @param copy
     */
    public Edge(IGraph<Vertex, Edge> graph, AbstractEdge copy) {
        super(graph, copy);
    }
    
    public Collection<Double> getWeights() {
        return (this.weights.values());
    }
    
    public double getTotalWeight() {
        if (this.total_weight == null) {
            this.total_weight = 0d;
            for (double w : this.weights.values()) this.total_weight += w;
        }
        return (this.total_weight);
    }
    
    public Double getWeight(int time) {
        return (this.weights.get(time));
    }
    
    public int getIntervalCount() {
        return (this.weights.size());
    }
    
    public void setWeight(int time, double weight) {
        assert(time >= 0);
        int size = this.getIntervalCount();
        if (time >= size) {
            for (int i = size; i < time; i++) {
                this.weights.put(i, 0d);
            } // FOR
            this.total_weight = null;
        }
        this.weights.put(time, weight);
    }
    
    public void addToWeight(int time, double delta) {
        Double weight = this.weights.get(time);
        if (weight != null) {
            delta += weight;
        }
        this.setWeight(time, delta);
    }
    
    @Override
    public String toString() {
        String ret = "";
        if (this.graph.getEdgeType(this) == EdgeType.DIRECTED) {
            ret = this.graph.getSource(this).toString().substring(0, 2) + "->" +  this.graph.getDest(this).toString().substring(0, 2);
        } else {
            String add = "";
            for (AbstractVertex vertex : this.graph.getIncidentVertices(this)) {
                ret += add + vertex.toString().substring(0, 2);
                add = "--";
            } // FOR
        }
        ret += String.format(" [%.03f]", this.getTotalWeight());
        return (ret);
    }
}